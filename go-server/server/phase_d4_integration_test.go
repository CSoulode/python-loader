package main

import (
	"net/url"
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
)

func phaseD4RangeDimension(
	minDistance float32,
	maxDistance float32,
	maxResults int32,
	axis pb.AxisType,
) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  1,
		},
		MaxResults: maxResults,
		Axis:       axis,
		DistanceRange: &pb.DistanceRange{
			MinDistance: minDistance,
			MaxDistance: maxDistance,
		},
		RangeSemantics: pb.RangeSemantics_DISTANCE,
	}
}

func phaseD4TagFilterRequest(tagID int32, forced pb.HybridStrategy) *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_FILTER, Value: tagID, ValueType: pb.FilterValueType_TAG},
		},
		VectorDimension: phaseD4RangeDimension(0, 1.0, 3, pb.AxisType_Y_AXIS),
		HybridStrategy:  forced,
	}
}

func TestPhaseD4GlobalBallRangeUsesRangeSearch(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		VectorDimension: phaseD4RangeDimension(0, 0.5, 2, pb.AxisType_Y_AXIS),
	})
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"1:2": {count: 1, representativeID: 2},
	})
	if env.vectorKV.RangeSearchCallCount() != 1 || env.vectorKV.KNNCallCount() != 0 {
		t.Fatalf("unexpected calls: range=%d knn=%d", env.vectorKV.RangeSearchCallCount(), env.vectorKV.KNNCallCount())
	}
}

func TestPhaseD4PrefilterUsesFilteredRangeSearch(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, phaseD4TagFilterRequest(102, pb.HybridStrategy_PRE_FILTER))
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"2:1": {count: 1, representativeID: 2},
		"2:2": {count: 1, representativeID: 3},
	})
	if env.vectorKV.FilteredRangeSearchCallCount() != 1 || env.vectorKV.RangeSearchCallCount() != 0 {
		t.Fatalf("unexpected calls: filtered_range=%d range=%d", env.vectorKV.FilteredRangeSearchCallCount(), env.vectorKV.RangeSearchCallCount())
	}
}

func TestPhaseD4HTTPRebucketOnlyReusesRangeCache(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		VectorDimension: phaseD4RangeDimension(0, 0.5, 2, pb.AxisType_Y_AXIS),
	})
	seedRangeCalls := env.vectorKV.RangeSearchCallCount()
	if seedRangeCalls != 1 {
		t.Fatalf("expected initial range search, got %d", seedRangeCalls)
	}

	params := url.Values{}
	params.Set("rebucketOnly", "true")
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":2,"distanceRange":{"min":0,"max":0.5},"rangeSemantics":"distance"}`)

	var envelope compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &envelope)
	assertCompatStateCellsByXY(t, envelope.Cells, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"1:2": {count: 1, representativeID: 2},
	})
	if env.vectorKV.RangeSearchCallCount() != seedRangeCalls {
		t.Fatalf("rebucketOnly should reuse range cache: got %d calls want %d", env.vectorKV.RangeSearchCallCount(), seedRangeCalls)
	}
}
