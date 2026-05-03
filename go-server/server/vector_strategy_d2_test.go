package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

func TestChooseStrategySelectsHybridInMiddleBand(t *testing.T) {
	got := chooseStrategy(5000, 20000, true, strategyTestConfig(100, nil), false, Auto)
	if got != Hybrid {
		t.Fatalf("strategy = %v, want Hybrid", got)
	}
}

func TestChooseStrategyUsesIterativeScanThreshold(t *testing.T) {
	got := chooseStrategy(15000, 50000, true, strategyTestConfig(100, nil), true, Auto)
	if got != PreFilter {
		t.Fatalf("strategy = %v, want PreFilter", got)
	}
}

func TestChooseStrategyHonorsForcedOverride(t *testing.T) {
	got := chooseStrategy(25000, 50000, true, strategyTestConfig(100, nil), false, PreFilter)
	if got != PreFilter {
		t.Fatalf("strategy = %v, want forced PreFilter", got)
	}
}

func TestChooseStrategyUsesStricterRingThreshold(t *testing.T) {
	got := chooseStrategy(3000, 50000, true, strategyTestConfig(100, &pb.DistanceRange{
		MinDistance: 0.2,
		MaxDistance: 0.7,
	}), true, Auto)
	if got != Hybrid {
		t.Fatalf("strategy = %v, want Hybrid for ring range", got)
	}
}

func TestStrategyFromProtoRejectsUnknownValue(t *testing.T) {
	if _, err := strategyFromProto(pb.HybridStrategy(99)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestParseCompatHybridStrategy(t *testing.T) {
	tests := map[string]HybridStrategy{
		"":            Auto,
		"auto":        Auto,
		"post_filter": PostFilter,
		"pre-filter":  PreFilter,
		"hybrid":      Hybrid,
	}
	for raw, want := range tests {
		got, err := parseCompatHybridStrategy(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if got != want {
			t.Fatalf("parse %q = %s, want %s", raw, got, want)
		}
	}
}

func TestParseCompatHybridStrategyRejectsUnknownValue(t *testing.T) {
	if _, err := parseCompatHybridStrategy("bad"); status.Code(err) != codes.InvalidArgument {
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

func TestVectorSearchCacheStoresSearchKindsSeparately(t *testing.T) {
	cache := newVectorSearchCache(4, defaultVectorCacheTTL)
	cfg := &pb.VectorSearchDimension{ModelName: "siglip2", MaxResults: 10}
	cache.PutForConfig(cfg, 1, 2, searchResult{
		RawNeighbors:   []Neighbor{{ObjectID: 1}},
		DistanceMetric: "cosine",
		Kind:           searchKindGlobalKNN,
	})
	cache.PutForConfig(cfg, 1, 2, searchResult{
		RawNeighbors:   []Neighbor{{ObjectID: 2}},
		DistanceMetric: "cosine",
		Kind:           searchKindFilteredKNN,
	})

	global, ok := cache.TryGetForConfigKind(cfg, 1, 2, searchKindGlobalKNN)
	if !ok || global.RawNeighbors[0].ObjectID != 1 {
		t.Fatalf("global cache result = %+v hit=%v", global.RawNeighbors, ok)
	}
	filtered, ok := cache.TryGetForConfigKind(cfg, 1, 2, searchKindFilteredKNN)
	if !ok || filtered.RawNeighbors[0].ObjectID != 2 {
		t.Fatalf("filtered cache result = %+v hit=%v", filtered.RawNeighbors, ok)
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

func strategyTestConfig(maxResults int32, distanceRange *pb.DistanceRange) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName:     "siglip2",
		MaxResults:    maxResults,
		DistanceRange: distanceRange,
	}
}
