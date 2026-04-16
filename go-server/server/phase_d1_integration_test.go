package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func phaseD1VectorDimension(distMax float32, strategy pb.BucketStrategy) *pb.VectorSearchDimension {
	cfg := &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		BucketCfg: &pb.BucketConfig{
			Strategy: strategy,
			Count:    2,
			DistMin:  0,
			DistMax:  distMax,
		},
		MaxResults: 2,
		Axis:       pb.AxisType_Y_AXIS,
	}
	if strategy == pb.BucketStrategy_CUSTOM {
		cfg.BucketCfg.Count = 0
		cfg.BucketCfg.CustomBreaks = []float32{0, 0.5, distMax}
	}
	return cfg
}

func phaseD1TagFilterRequest(tagID int32, distMax float32, strategy pb.BucketStrategy) *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_FILTER, Value: tagID, ValueType: pb.FilterValueType_TAG},
		},
		VectorDimension: phaseD1VectorDimension(distMax, strategy),
	}
}

func TestPhaseD1GlobalVectorDimensionUsesPostFilter(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		VectorDimension: phaseD1VectorDimension(0.5, pb.BucketStrategy_EQUAL_WIDTH),
	})
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"1:2": {count: 1, representativeID: 2},
	})
	if env.vectorKV.KNNCallCount() != 1 || env.vectorKV.FilteredKNNCallCount() != 0 {
		t.Fatalf("unexpected strategy calls: knn=%d filtered=%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount())
	}
}

func TestPhaseD1StrictMetadataUsesPreFilter(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, phaseD1TagFilterRequest(101, 1.0, pb.BucketStrategy_EQUAL_WIDTH))
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
	})
	if env.vectorKV.KNNCallCount() != 0 || env.vectorKV.FilteredKNNCallCount() != 1 {
		t.Fatalf("unexpected strategy calls: knn=%d filtered=%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount())
	}
}

func TestPhaseD1FilterAwareCacheSharedAcrossGRPCAndHTTP(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	collectBrowsingStateResponses(t, env.grpcClient, phaseD1TagFilterRequest(101, 1.0, pb.BucketStrategy_EQUAL_WIDTH))
	collectBrowsingStateResponses(t, env.grpcClient, phaseD1TagFilterRequest(102, 1.0, pb.BucketStrategy_EQUAL_WIDTH))
	seedFilteredCalls := env.vectorKV.FilteredKNNCallCount()
	if seedFilteredCalls != 2 {
		t.Fatalf("expected two prefilter searches, got %d", seedFilteredCalls)
	}

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("filters", `[{"type":"tag","ids":[101]}]`)
	params.Set("rebucketOnly", "true")
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":2}`)

	var envelope compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &envelope)
	assertCompatStateCells(t, envelope.Cells, map[int32]cellExpectation{
		1: {count: 1, representativeID: 1},
	})
	if env.vectorKV.FilteredKNNCallCount() != seedFilteredCalls || env.vectorKV.KNNCallCount() != 0 {
		t.Fatalf("rebucketOnly should reuse cache: knn=%d filtered=%d/%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount(), seedFilteredCalls)
	}
}

func TestPhaseD1PrefilterBucketDrillDownReturnsSelectedBucketOnly(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	gridResponses := collectBrowsingStateResponses(t, env.grpcClient, phaseD1TagFilterRequest(102, 1.0, pb.BucketStrategy_EQUAL_WIDTH))
	assertStateCellsByXY(t, gridResponses, map[string]cellExpectation{
		"2:1": {count: 1, representativeID: 2},
		"2:2": {count: 1, representativeID: 3},
	})

	bucketID := int32(0)
	allReq := phaseD1TagFilterRequest(102, 1.0, pb.BucketStrategy_EQUAL_WIDTH)
	allReq.All = "[]"
	allReq.VectorBucketId = &bucketID
	allResponses := collectBrowsingStateResponses(t, env.grpcClient, allReq)
	assertAllCubeObjectIDs(t, allResponses, []int32{2})

	if env.vectorKV.FilteredKNNCallCount() < 1 || env.vectorKV.KNNCallCount() != 0 {
		t.Fatalf(
			"expected cached prefilter path without global fallback, got knn=%d filtered=%d",
			env.vectorKV.KNNCallCount(),
			env.vectorKV.FilteredKNNCallCount(),
		)
	}
}

func TestPhaseD1ExecuteMetadataFilterReturnsCandidateIDs(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	ids, err := env.server.executeMetadataFilter(context.Background(), []qg.ParsedFilter{{Type: "tag", Ids: []int{101}}}, []qg.ParsedAxis{{Type: "tagset", Id: 1}})
	if err != nil {
		t.Fatalf("executeMetadataFilter: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("candidate ids = %v, want [1]", ids)
	}
}
