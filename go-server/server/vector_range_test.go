package main

import (
	"context"
	"math"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func TestNormalizeDistanceRangeCosineSimilarity(t *testing.T) {
	got, err := normalizeDistanceRange(&pb.DistanceRange{
		MinDistance: 0.7,
		MaxDistance: 0.9,
	}, pb.RangeSemantics_SIMILARITY, "cosine")
	if err != nil {
		t.Fatalf("normalizeDistanceRange returned error: %v", err)
	}
	if !approxFloat32(got.GetMinDistance(), 0.1) || !approxFloat32(got.GetMaxDistance(), 0.3) {
		t.Fatalf("normalized range = [%f,%f], want [0.1,0.3]", got.GetMinDistance(), got.GetMaxDistance())
	}
}

func TestNormalizeDistanceRangeL2Similarity(t *testing.T) {
	got, err := normalizeDistanceRange(&pb.DistanceRange{
		MinDistance: 0.5,
		MaxDistance: 0.9,
	}, pb.RangeSemantics_SIMILARITY, "l2")
	if err != nil {
		t.Fatalf("normalizeDistanceRange returned error: %v", err)
	}
	if !approxFloat32(got.GetMinDistance(), float32(math.Sqrt(0.2))) {
		t.Fatalf("min distance = %f", got.GetMinDistance())
	}
	if !approxFloat32(got.GetMaxDistance(), 1.0) {
		t.Fatalf("max distance = %f, want 1.0", got.GetMaxDistance())
	}
}

func TestEffectiveVectorMaxResultsUsesRangeDefault(t *testing.T) {
	if got := effectiveVectorMaxResults(&pb.VectorSearchDimension{
		DistanceRange: &pb.DistanceRange{MinDistance: 0, MaxDistance: 1},
	}); got != defaultRangeMaxResults {
		t.Fatalf("effectiveVectorMaxResults = %d, want %d", got, defaultRangeMaxResults)
	}
}

func TestParseCompatVectorDimensionIncludesRangeFields(t *testing.T) {
	cfg, err := parseCompatVectorDimension(`{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":1,"maxResults":0,"distanceRange":{"min":0.7,"max":0.9},"rangeSemantics":"similarity"}`)
	if err != nil {
		t.Fatalf("parseCompatVectorDimension returned error: %v", err)
	}
	if cfg.GetRangeSemantics() != pb.RangeSemantics_SIMILARITY {
		t.Fatalf("range semantics = %s, want SIMILARITY", cfg.GetRangeSemantics())
	}
	if cfg.GetDistanceRange().GetMinDistance() != 0.7 || cfg.GetDistanceRange().GetMaxDistance() != 0.9 {
		t.Fatalf("distance range = %+v", cfg.GetDistanceRange())
	}
}

func TestSearchNeighborsTreatsEmptyGlobalRangeAsSuccess(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
			rangeErr: status.Error(codes.NotFound, "not found"),
		}),
	}

	cfg := &pb.VectorSearchDimension{
		ModelName:     "siglip2",
		DistanceRange: &pb.DistanceRange{MinDistance: 0.1, MaxDistance: 0.2},
		MaxResults:    500,
	}
	result, err := server.searchNeighbors(context.Background(), cfg, &vectorSearchInputs{
		Config: cfg,
		ModelInfo: &kvstorev1.ModelInfo{
			Name:           "siglip2",
			DistanceMetric: "cosine",
		},
		QueryVector: []float32{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("searchNeighbors returned error: %v", err)
	}
	if len(result.RawNeighbors) != 0 {
		t.Fatalf("raw neighbors = %v, want empty", result.RawNeighbors)
	}
	if result.Kind != searchKindGlobalRange {
		t.Fatalf("search kind = %s, want %s", result.Kind.String(), searchKindGlobalRange.String())
	}
}

func approxFloat32(left float32, right float32) bool {
	return math.Abs(float64(left-right)) < 1e-5
}
