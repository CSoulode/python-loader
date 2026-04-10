package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func TestVectorSearchCacheTryGetReturnsLimitedCopy(t *testing.T) {
	cache := newVectorSearchCache(2, time.Minute)
	cache.Put("siglip2", 1, 0, searchResult{
		RawNeighbors: []Neighbor{
			{ObjectID: 1, Distance: 0.1},
			{ObjectID: 2, Distance: 0.2},
			{ObjectID: 3, Distance: 0.3},
		},
		DistanceMetric: "cosine",
	}, 3)

	got, ok := cache.TryGet("siglip2", 1, 0, 2)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got.RawNeighbors) != 2 {
		t.Fatalf("neighbors = %d, want 2", len(got.RawNeighbors))
	}

	got.RawNeighbors[0].ObjectID = 99
	again, ok := cache.TryGet("siglip2", 1, 0, 3)
	if !ok {
		t.Fatal("expected second cache hit")
	}
	if again.RawNeighbors[0].ObjectID != 1 {
		t.Fatalf("cached neighbor mutated: %+v", again.RawNeighbors[0])
	}
}

func TestVectorSearchCacheExpiresEntries(t *testing.T) {
	now := time.Date(2026, 3, 12, 0, 0, 0, 0, time.UTC)
	cache := newVectorSearchCache(2, time.Minute)
	cache.now = func() time.Time { return now }
	cache.Put("siglip2", 1, 0, searchResult{RawNeighbors: []Neighbor{{ObjectID: 1, Distance: 0.1}}}, 1)

	now = now.Add(2 * time.Minute)
	if _, ok := cache.TryGet("siglip2", 1, 0, 1); ok {
		t.Fatal("expected expired cache miss")
	}
}

func TestVectorSearchCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newVectorSearchCache(2, time.Minute)
	cache.Put("siglip2", 1, 0, searchResult{RawNeighbors: []Neighbor{{ObjectID: 1, Distance: 0.1}}}, 1)
	cache.Put("siglip2", 2, 0, searchResult{RawNeighbors: []Neighbor{{ObjectID: 2, Distance: 0.2}}}, 1)

	if _, ok := cache.TryGet("siglip2", 1, 0, 1); !ok {
		t.Fatal("expected cache hit for key 1")
	}

	cache.Put("siglip2", 3, 0, searchResult{RawNeighbors: []Neighbor{{ObjectID: 3, Distance: 0.3}}}, 1)
	if _, ok := cache.TryGet("siglip2", 2, 0, 1); ok {
		t.Fatal("expected key 2 to be evicted")
	}
}

func TestVectorSearchCacheRangeSupportsExactAndBallSubsetHits(t *testing.T) {
	cache := newVectorSearchCache(2, time.Minute)
	cache.PutForConfig(rangeCacheConfig(3, 0, 0.6), 1, 0, searchResult{
		RawNeighbors: []Neighbor{
			{ObjectID: 1, Distance: 0.1},
			{ObjectID: 2, Distance: 0.2},
			{ObjectID: 3, Distance: 0.5},
		},
		DistanceMetric: "cosine",
		Kind:           searchKindGlobalRange,
	})

	exact, ok := cache.TryGetForConfig(rangeCacheConfig(2, 0, 0.6), 1, 0)
	if !ok || len(exact.RawNeighbors) != 2 {
		t.Fatalf("exact hit = %v, neighbors = %d", ok, len(exact.RawNeighbors))
	}

	subset, ok := cache.TryGetForConfig(rangeCacheConfig(2, 0, 0.25), 1, 0)
	if !ok {
		t.Fatal("expected ball subset hit")
	}
	if !reflect.DeepEqual(subset.RawNeighbors, []Neighbor{
		{ObjectID: 1, Distance: 0.1},
		{ObjectID: 2, Distance: 0.2},
	}) {
		t.Fatalf("subset neighbors = %+v", subset.RawNeighbors)
	}
}

func TestVectorSearchCacheRangeRejectsUnsafeRingSubsetHit(t *testing.T) {
	cache := newVectorSearchCache(2, time.Minute)
	cache.PutForConfig(rangeCacheConfig(2, 0.2, 1.0), 1, 0, searchResult{
		RawNeighbors: []Neighbor{
			{ObjectID: 1, Distance: 0.2},
			{ObjectID: 2, Distance: 0.3},
		},
		DistanceMetric: "cosine",
		Kind:           searchKindGlobalRange,
	})

	if _, ok := cache.TryGetForConfig(rangeCacheConfig(1, 0.5, 1.0), 1, 0); ok {
		t.Fatal("expected truncated ring subset to miss")
	}
}

func TestResolveVectorDimensionWithCacheUsesCachedResults(t *testing.T) {
	client := &stubVectorSearchClient{
		getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
		knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{
			{Id: 1, Distance: 0},
			{Id: 2, Distance: 0.25},
			{Id: 3, Distance: 0.49},
		}},
		listModelsResp: &kvstorev1.ListModelsResponse{
			Models: []*kvstorev1.ModelInfo{{
				Name:           "siglip2",
				DistanceMetric: "cosine",
			}},
		},
	}
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(client),
		vectorCache:   newVectorSearchCache(2, time.Minute),
	}

	initialCfg := testVectorDimensionConfig(pb.BucketStrategy_EQUAL_WIDTH, 2)
	if _, err := server.resolveVectorDimensionWithCache(context.Background(), initialCfg, false); err != nil {
		t.Fatalf("initial resolve returned error: %v", err)
	}
	if client.getCalls != 1 || client.knnCalls != 1 || client.listCalls != 1 {
		t.Fatalf("unexpected initial RPC counts: get=%d knn=%d list=%d", client.getCalls, client.knnCalls, client.listCalls)
	}

	rebucketCfg := testVectorDimensionConfig(pb.BucketStrategy_EQUAL_DEPTH, 2)
	result, err := server.resolveVectorDimensionWithCache(context.Background(), rebucketCfg, true)
	if err != nil {
		t.Fatalf("rebucket resolve returned error: %v", err)
	}
	if client.getCalls != 1 || client.knnCalls != 1 || client.listCalls != 1 {
		t.Fatalf("cache hit should skip RPCs: get=%d knn=%d list=%d", client.getCalls, client.knnCalls, client.listCalls)
	}
	if len(result.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(result.BucketInfos))
	}
}

func TestParseBrowsingStateRequestRejectsRebucketOnlyWithoutVectorDimension(t *testing.T) {
	server := &DataLoaderServer{}

	_, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		RebucketOnly: true,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestResolveVectorDimensionWithCacheRejectsInvalidReferenceBeforeCacheLookup(t *testing.T) {
	server := &DataLoaderServer{vectorCache: newVectorSearchCache(2, time.Minute)}

	_, err := server.resolveVectorDimensionWithCache(context.Background(), &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 0}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  0.5,
		},
		MaxResults: 2,
		Axis:       pb.AxisType_Y_AXIS,
	}, true)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestParseBrowsingStateRequestRejectsAllWithRebucketOnly(t *testing.T) {
	server := &DataLoaderServer{}

	_, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		All:             "[]",
		RebucketOnly:    true,
		VectorDimension: testVectorDimensionConfig(pb.BucketStrategy_EQUAL_WIDTH, 2),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestParseCompatVectorDimensionIncludesCustomBreaks(t *testing.T) {
	cfg, err := parseCompatVectorDimension(`{"model":"siglip2","objectId":1,"axis":"y","bucketStrategy":"custom","customBreaks":[0,0.25,0.5],"distMax":0.5,"maxResults":3}`)
	if err != nil {
		t.Fatalf("parseCompatVectorDimension returned error: %v", err)
	}

	if cfg.GetBucketCfg().GetStrategy() != pb.BucketStrategy_CUSTOM {
		t.Fatalf("strategy = %s, want CUSTOM", cfg.GetBucketCfg().GetStrategy())
	}
	if !reflect.DeepEqual(cfg.GetBucketCfg().GetCustomBreaks(), []float32{0, 0.25, 0.5}) {
		t.Fatalf("customBreaks = %v", cfg.GetBucketCfg().GetCustomBreaks())
	}
}

func testVectorDimensionConfig(strategy pb.BucketStrategy, count int32) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		BucketCfg: &pb.BucketConfig{
			Strategy: strategy,
			Count:    count,
			DistMin:  0,
			DistMax:  0.5,
		},
		MaxResults: 3,
		Axis:       pb.AxisType_Y_AXIS,
	}
}

func rangeCacheConfig(maxResults int32, minDistance float32, maxDistance float32) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName:  "siglip2",
		MaxResults: maxResults,
		DistanceRange: &pb.DistanceRange{
			MinDistance: minDistance,
			MaxDistance: maxDistance,
		},
	}
}
