package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

func TestPhaseF3HTTPConditionalRequestReturns304AfterInvalidateAll(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	requestURL := env.httpServer.URL + "/?" + params.Encode()

	first, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	firstResp, err := http.DefaultClient.Do(first)
	if err != nil {
		t.Fatalf("do first request: %v", err)
	}
	firstResp.Body.Close()
	if firstResp.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", firstResp.StatusCode)
	}
	etag := firstResp.Header.Get(bsHTTPETagHeaderKey)
	if etag == "" {
		t.Fatal("first response missing etag")
	}

	env.server.ensureBrowsingStateCache().InvalidateAll()

	second, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		t.Fatalf("new second request: %v", err)
	}
	second.Header.Set(bsHTTPIfNoneMatchKey, etag)
	secondResp, err := http.DefaultClient.Do(second)
	if err != nil {
		t.Fatalf("do second request: %v", err)
	}
	secondResp.Body.Close()
	if secondResp.StatusCode != http.StatusNotModified {
		t.Fatalf("second status = %d, want 304", secondResp.StatusCode)
	}
	if got := secondResp.Header.Get(bsHTTPETagHeaderKey); got != etag {
		t.Fatalf("etag = %q, want %q", got, etag)
	}
}

func TestPhaseF3GRPCConditionalRequestReturnsNotModifiedAfterInvalidateAll(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	req := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
	}

	stream, err := env.grpcClient.GetBrowsingState2(context.Background(), req)
	if err != nil {
		t.Fatalf("GetBrowsingState2 first call: %v", err)
	}
	header, err := stream.Header()
	if err != nil {
		t.Fatalf("stream.Header first call: %v", err)
	}
	etag := header.Get(bsGRPCETagKey)
	if len(etag) != 1 || etag[0] == "" {
		t.Fatalf("first response missing etag header: %+v", header)
	}
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("first stream recv: %v", err)
		}
	}

	env.server.ensureBrowsingStateCache().InvalidateAll()

	ctx := metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs(bsGRPCIfNoneMatchKey, etag[0]),
	)
	stream, err = env.grpcClient.GetBrowsingState2(ctx, req)
	if err != nil {
		t.Fatalf("GetBrowsingState2 second call: %v", err)
	}
	header, err = stream.Header()
	if err != nil {
		t.Fatalf("stream.Header second call: %v", err)
	}
	if got := header.Get(bsGRPCNotModifiedKey); len(got) != 1 || got[0] != bsNotModifiedTrueValue {
		t.Fatalf("bs-not-modified header = %+v, want %q", got, bsNotModifiedTrueValue)
	}
	if got := header.Get(bsGRPCETagKey); len(got) != 1 || got[0] != etag[0] {
		t.Fatalf("etag header = %+v, want %q", got, etag[0])
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second stream recv err = %v, want EOF", err)
	}
}

func TestPhaseF3GRPCMultiVectorConditionalRequestReusesVectorCacheAfterInvalidateAll(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	req := &pb.GetBrowsingStateRequest{
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("siglip2", 1, pb.AxisType_X_AXIS),
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Y_AXIS),
		},
	}

	stream, err := env.grpcClient.GetBrowsingState2(context.Background(), req)
	if err != nil {
		t.Fatalf("GetBrowsingState2 first call: %v", err)
	}
	header, err := stream.Header()
	if err != nil {
		t.Fatalf("stream.Header first call: %v", err)
	}
	etag := header.Get(bsGRPCETagKey)
	if len(etag) != 1 || etag[0] == "" {
		t.Fatalf("first response missing etag header: %+v", header)
	}
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("first stream recv: %v", err)
		}
	}

	firstKNNCalls := env.vectorKV.KNNCallCount()
	if firstKNNCalls != 2 {
		t.Fatalf("first KNN call count = %d, want 2", firstKNNCalls)
	}

	env.server.ensureBrowsingStateCache().InvalidateAll()

	ctx := metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs(bsGRPCIfNoneMatchKey, etag[0]),
	)
	stream, err = env.grpcClient.GetBrowsingState2(ctx, req)
	if err != nil {
		t.Fatalf("GetBrowsingState2 second call: %v", err)
	}
	header, err = stream.Header()
	if err != nil {
		t.Fatalf("stream.Header second call: %v", err)
	}
	if got := header.Get(bsGRPCNotModifiedKey); len(got) != 1 || got[0] != bsNotModifiedTrueValue {
		t.Fatalf("bs-not-modified header = %+v, want %q", got, bsNotModifiedTrueValue)
	}
	if got := header.Get(bsGRPCETagKey); len(got) != 1 || got[0] != etag[0] {
		t.Fatalf("etag header = %+v, want %q", got, etag[0])
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second stream recv err = %v, want EOF", err)
	}

	if got := env.vectorKV.KNNCallCount(); got != firstKNNCalls {
		t.Fatalf("second call triggered new KNN RPCs: got=%d want=%d", got, firstKNNCalls)
	}
}

func TestPhaseF3TagFilterDependenciesResolveToTagset(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	prepared, err := prepareBrowsingStateRequest(&pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			},
			{
				AxisFilterType: pb.AxisType_FILTER,
				Value:          101,
				ValueType:      pb.FilterValueType_TAG,
			},
		},
	})
	if err != nil {
		t.Fatalf("prepare request: %v", err)
	}

	dependencies, err := env.server.extractBrowsingStateCacheDependencies(
		context.Background(),
		prepared,
	)
	if err != nil {
		t.Fatalf("extract dependencies: %v", err)
	}
	if !containsBSDependency(dependencies, bsCacheDependency{
		Kind: bsDependencyKindTagset,
		Key:  "1",
	}) {
		t.Fatalf("dependencies = %+v, want tagset:1", dependencies)
	}
}

func containsBSDependency(
	dependencies []bsCacheDependency,
	target bsCacheDependency,
) bool {
	for _, dependency := range dependencies {
		if dependency == target {
			return true
		}
	}
	return false
}
