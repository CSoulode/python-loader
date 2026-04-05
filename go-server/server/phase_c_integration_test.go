package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

func phaseCSeedVectorDimension() *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  0.5,
		},
		MaxResults: 2,
		Axis:       pb.AxisType_Y_AXIS,
	}
}

func phaseCRebucketVectorDimension(strategy pb.BucketStrategy) *pb.VectorSearchDimension {
	cfg := phaseCSeedVectorDimension()
	cfg.BucketCfg.Strategy = strategy
	if strategy == pb.BucketStrategy_CUSTOM {
		cfg.BucketCfg.CustomBreaks = []float32{0, 0.2, 0.5}
		cfg.BucketCfg.Count = 0
	}
	return cfg
}

func phaseCSeedCacheWithFilters(t *testing.T, env *phaseATestEnv, filters []*pb.AxisFilter) {
	t.Helper()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		Filters:         filters,
		VectorDimension: phaseCSeedVectorDimension(),
	})
	assertBucketInfosOnFirstResponse(t, responses, 2)
}

func phaseCSeedCache(t *testing.T, env *phaseATestEnv) {
	t.Helper()
	phaseCSeedCacheWithFilters(t, env, []*pb.AxisFilter{{
		AxisFilterType: pb.AxisType_X_AXIS,
		Value:          1,
		ValueType:      pb.FilterValueType_TAGSET,
	}})
}

func totalVectorSearchCalls(env *phaseATestEnv) int32 {
	return env.vectorKV.KNNCallCount() + env.vectorKV.FilteredKNNCallCount()
}

func TestPhaseCRebucketOnlyAllGRPCVariants2DAnd3D(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	phaseCSeedCache(t, env)
	phaseCSeedCacheWithFilters(t, env, []*pb.AxisFilter{
		{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
		{AxisFilterType: pb.AxisType_Z_AXIS, Value: 2, ValueType: pb.FilterValueType_TAGSET},
	})
	seedGet := env.vectorKV.GetCallCount()
	seedSearch := totalVectorSearchCalls(env)
	seedList := env.vectorKV.ListModelsCallCount()

	req2D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		RebucketOnly:    true,
		VectorDimension: phaseCRebucketVectorDimension(pb.BucketStrategy_EQUAL_DEPTH),
	}
	req3D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_Z_AXIS, Value: 2, ValueType: pb.FilterValueType_TAGSET},
		},
		RebucketOnly:    true,
		VectorDimension: phaseCRebucketVectorDimension(pb.BucketStrategy_CUSTOM),
	}

	for _, rpc := range allBrowsingStateRPCs(env.grpcClient) {
		t.Run(rpc.name+"/2d", func(t *testing.T) {
			responses := collectBrowsingStateResponsesWithCall(t, rpc.call, req2D)
			assertBucketInfosOnFirstResponse(t, responses, 2)
			assertStateCellsByXY(t, responses, map[string]cellExpectation{
				"1:1": {count: 1, representativeID: 1},
				"2:2": {count: 1, representativeID: 2},
			})
		})

		t.Run(rpc.name+"/3d", func(t *testing.T) {
			responses := collectBrowsingStateResponsesWithCall(t, rpc.call, req3D)
			assertBucketInfosOnFirstResponse(t, responses, 2)
			assertStateCellsByXYZ(t, responses, map[string]cellExpectation{
				"1:1:1": {count: 1, representativeID: 1},
				"2:2:2": {count: 1, representativeID: 2},
			})
		})
	}

	if env.vectorKV.GetCallCount() != seedGet || totalVectorSearchCalls(env) != seedSearch || env.vectorKV.ListModelsCallCount() != seedList {
		t.Fatalf(
			"rebucket-only variant coverage should not trigger vectorkv RPCs: get=%d/%d search=%d/%d list=%d/%d",
			env.vectorKV.GetCallCount(), seedGet,
			totalVectorSearchCalls(env), seedSearch,
			env.vectorKV.ListModelsCallCount(), seedList,
		)
	}
}

func TestPhaseCRebucketOnlyHTTP3DEnvelope(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	phaseCSeedCacheWithFilters(t, env, []*pb.AxisFilter{
		{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
		{AxisFilterType: pb.AxisType_Z_AXIS, Value: 2, ValueType: pb.FilterValueType_TAGSET},
	})
	seedGet := env.vectorKV.GetCallCount()
	seedSearch := totalVectorSearchCalls(env)
	seedList := env.vectorKV.ListModelsCallCount()

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("zAxis", `{"type":"tagset","id":2}`)
	params.Set("rebucketOnly", "true")
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketStrategy":"logarithmic","bucketCount":2,"distMin":0,"distMax":0.5,"maxResults":2}`)

	var response compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &response)
	if len(response.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(response.BucketInfos))
	}
	assertCompatStateCellsByXYZ(t, response.Cells, map[string]cellExpectation{
		"1:1:1": {count: 1, representativeID: 1},
		"2:2:2": {count: 1, representativeID: 2},
	})
	if env.vectorKV.GetCallCount() != seedGet || totalVectorSearchCalls(env) != seedSearch || env.vectorKV.ListModelsCallCount() != seedList {
		t.Fatalf(
			"http rebucket-only 3d should not trigger vectorkv RPCs: get=%d/%d search=%d/%d list=%d/%d",
			env.vectorKV.GetCallCount(), seedGet,
			totalVectorSearchCalls(env), seedSearch,
			env.vectorKV.ListModelsCallCount(), seedList,
		)
	}
}

func TestPhaseCRebucketOnlyUsesSharedCacheAcrossGRPCAndHTTP(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	phaseCSeedCache(t, env)
	if totalVectorSearchCalls(env) != 1 || env.vectorKV.GetCallCount() != 1 || env.vectorKV.ListModelsCallCount() != 1 {
		t.Fatalf("unexpected seed RPC counts: get=%d search=%d list=%d", env.vectorKV.GetCallCount(), totalVectorSearchCalls(env), env.vectorKV.ListModelsCallCount())
	}

	httpParams := url.Values{}
	httpParams.Set("xAxis", `{"type":"tagset","id":1}`)
	httpParams.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":1,"bucketStrategy":"equal_depth","distMin":0,"distMax":0.5,"maxResults":2}`)
	httpParams.Set("rebucketOnly", "true")

	var envelope compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+httpParams.Encode(), &envelope)
	if len(envelope.BucketInfos) != 1 {
		t.Fatalf("bucket infos = %d, want 1", len(envelope.BucketInfos))
	}
	if totalVectorSearchCalls(env) != 1 || env.vectorKV.GetCallCount() != 1 || env.vectorKV.ListModelsCallCount() != 1 {
		t.Fatalf("rebucketOnly should not trigger RPCs: get=%d search=%d list=%d", env.vectorKV.GetCallCount(), totalVectorSearchCalls(env), env.vectorKV.ListModelsCallCount())
	}
}

func TestPhaseCRebucketOnlyErrorsWithoutCache(t *testing.T) {
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
		RebucketOnly: true,
		VectorDimension: &pb.VectorSearchDimension{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    2,
				DistMin:  0,
				DistMax:  0.5,
			},
			MaxResults: 2,
			Axis:       pb.AxisType_Y_AXIS,
		},
	}

	stream, err := env.grpcClient.GetBrowsingState2(context.Background(), req)
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("error code = %s, want FailedPrecondition", status.Code(err))
	}

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":0.5,"maxResults":2}`)
	params.Set("rebucketOnly", "true")

	resp, err := http.Get(env.httpServer.URL + "/?" + params.Encode())
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("http status = %d, want 409", resp.StatusCode)
	}
}

func TestPhaseCCustomBreaksHTTPCompatAndModelsEndpoint(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketStrategy":"custom","customBreaks":[0,0.3,0.5],"distMin":0,"distMax":0.5,"maxResults":2}`)

	var envelope compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &envelope)
	if len(envelope.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(envelope.BucketInfos))
	}

	resp, err := http.Get(env.httpServer.URL + "/api/vector/models")
	if err != nil {
		t.Fatalf("http.Get(models): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("models status = %d", resp.StatusCode)
	}

	var models vectorModelsHTTPResponse
	if err := json.NewDecoder(resp.Body).Decode(&models); err != nil {
		t.Fatalf("decode models json: %v", err)
	}
	if models.DefaultModel != "siglip2" {
		t.Fatalf("defaultModel = %q, want siglip2", models.DefaultModel)
	}
	if len(models.Models) != 1 || models.Models[0].DistanceMetric != "cosine" {
		t.Fatalf("models response = %+v", models)
	}
}
