package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

func TestChooseStrategySelectsHybridInMiddleBand(t *testing.T) {
	got := chooseStrategy(5000, 20000, true, 100, false, Auto)
	if got != Hybrid {
		t.Fatalf("strategy = %v, want Hybrid", got)
	}
}

func TestChooseStrategyUsesIterativeScanThreshold(t *testing.T) {
	got := chooseStrategy(15000, 50000, true, 100, true, Auto)
	if got != PreFilter {
		t.Fatalf("strategy = %v, want PreFilter", got)
	}
}

func TestChooseStrategyHonorsForcedOverride(t *testing.T) {
	got := chooseStrategy(25000, 50000, true, 100, false, PreFilter)
	if got != PreFilter {
		t.Fatalf("strategy = %v, want forced PreFilter", got)
	}
}

func TestStrategyFromProtoRejectsUnknownValue(t *testing.T) {
	if _, err := strategyFromProto(pb.HybridStrategy(99)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestVectorSearchCacheTryGetGlobalKNNRejectsFilteredEntry(t *testing.T) {
	cache := newVectorSearchCache(2, defaultVectorCacheTTL)
	cache.Put("siglip2", 1, 2, searchResult{
		DistanceMetric: "cosine",
		Kind:           searchKindFilteredKNN,
	}, 1)

	if _, ok := cache.TryGetGlobalKNN("siglip2", 1, 2, 1); ok {
		t.Fatal("expected global-only lookup to miss filtered entry")
	}
}

func TestIntersectHybridNeighborsPreservesVectorOrder(t *testing.T) {
	result := intersectHybridNeighbors(searchResult{
		DistanceMetric: "cosine",
		Kind:           searchKindGlobalKNN,
		RawNeighbors: []Neighbor{
			{ObjectID: 2, Distance: 0.2},
			{ObjectID: 1, Distance: 0.3},
			{ObjectID: 3, Distance: 0.4},
		},
	}, []int32{3, 2})

	if result.Kind != searchKindHybridIntersection {
		t.Fatalf("kind = %v, want hybrid intersection", result.Kind)
	}
	if len(result.RawNeighbors) != 2 {
		t.Fatalf("neighbors = %d, want 2", len(result.RawNeighbors))
	}
	if result.RawNeighbors[0].ObjectID != 2 || result.RawNeighbors[1].ObjectID != 3 {
		t.Fatalf("unexpected order: %+v", result.RawNeighbors)
	}
}

func TestParseBrowsingStateRequestRejectsHybridStrategyWithoutVectorDimension(t *testing.T) {
	server := &DataLoaderServer{}

	_, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		HybridStrategy: pb.HybridStrategy_HYBRID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}
