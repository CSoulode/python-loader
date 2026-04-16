package main

import (
	"reflect"
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func TestBrowsingStateCacheTryGetReturnsClonedCopy(t *testing.T) {
	cache := newBrowsingStateCache(2, time.Minute)
	key := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "k"}
	cache.PutEntry(key, testBSCacheValue(), "etag-a", []bsCacheDependency{{
		Kind: bsDependencyKindTagset,
		Key:  "1",
	}})

	entry, ok := cache.TryGetEntry(key)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if entry.ETag != "etag-a" {
		t.Fatalf("etag = %q, want %q", entry.ETag, "etag-a")
	}
	if len(entry.Dependencies) != 1 || entry.Dependencies[0].Key != "1" {
		t.Fatalf("dependencies = %+v", entry.Dependencies)
	}
	got := entry.Value
	got.Cells[0].CubeObjects[0].ID = 99
	got.BucketInfos[0].Label = "mutated"

	again, ok := cache.TryGetEntry(key)
	if !ok {
		t.Fatal("expected second cache hit")
	}
	if again.Value.Cells[0].CubeObjects[0].ID != 7 {
		t.Fatalf("cube object mutated: %+v", again.Value.Cells[0].CubeObjects[0])
	}
	if again.Value.BucketInfos[0].Label != "near" {
		t.Fatalf("bucket info mutated: %+v", again.Value.BucketInfos[0])
	}
}

func TestBrowsingStateCacheExpiresEntries(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	cache := newBrowsingStateCache(2, time.Minute)
	cache.now = func() time.Time { return now }
	key := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "k"}
	cache.Put(key, testBSCacheValue())

	now = now.Add(2 * time.Minute)
	if _, ok := cache.TryGet(key); ok {
		t.Fatal("expected expired cache miss")
	}
}

func TestBrowsingStateCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newBrowsingStateCache(2, time.Minute)
	keyA := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "a"}
	keyB := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "b"}
	keyC := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "c"}

	cache.Put(keyA, testBSCacheValue())
	cache.Put(keyB, testBSCacheValue())
	if _, ok := cache.TryGet(keyA); !ok {
		t.Fatal("expected keyA hit")
	}

	cache.Put(keyC, testBSCacheValue())
	if _, ok := cache.TryGet(keyB); ok {
		t.Fatal("expected keyB eviction")
	}
}

func TestComputeBSETagStableAndDistinct(t *testing.T) {
	value := testBSCacheValue()
	if got, want := computeBSETag(value), computeBSETag(value); got != want {
		t.Fatalf("etag changed across identical values: %q vs %q", got, want)
	}

	mutated := testBSCacheValue()
	mutated.Cells[0].Count = 9
	if computeBSETag(value) == computeBSETag(mutated) {
		t.Fatal("different values produced the same etag")
	}
}

func TestComputeBSETagIgnoresCellAndBucketOrder(t *testing.T) {
	value := bsCacheValue{
		Cells: []cachedBrowsingStateCell{
			{
				X:     2,
				Y:     1,
				Z:     1,
				Count: 1,
				CubeObjects: []cachedCubeObject{
					{ID: 9, FileURI: "/media/9", ThumbnailURI: "/thumb/9"},
					{ID: 3, FileURI: "/media/3", ThumbnailURI: "/thumb/3"},
				},
			},
			{
				X:     1,
				Y:     1,
				Z:     1,
				Count: 2,
				CubeObjects: []cachedCubeObject{
					{ID: 7, FileURI: "/media/7", ThumbnailURI: "/thumb/7"},
				},
			},
		},
		BucketInfos: []*pb.BucketInfo{
			{BucketId: 2, LowerBound: 0.4, UpperBound: 1.0, Label: "far"},
			{BucketId: 1, LowerBound: 0.0, UpperBound: 0.4, Label: "near"},
		},
	}
	reordered := bsCacheValue{
		Cells: []cachedBrowsingStateCell{
			value.Cells[1],
			value.Cells[0],
		},
		BucketInfos: []*pb.BucketInfo{
			value.BucketInfos[1],
			value.BucketInfos[0],
		},
	}

	if got, want := computeBSETag(value), computeBSETag(reordered); got != want {
		t.Fatalf("etag depends on ordering: %q vs %q", got, want)
	}
}

func TestBrowsingStateCachePutEntryCanonicalizesStoredCellOrder(t *testing.T) {
	cache := newBrowsingStateCache(2, time.Minute)
	key := bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "canonical"}
	cache.PutEntry(
		key,
		bsCacheValue{
			Cells: []cachedBrowsingStateCell{
				{X: 3, Y: 1, Z: 1, Count: 1, CubeObjects: []cachedCubeObject{{ID: 3}}},
				{X: 1, Y: 1, Z: 1, Count: 1, CubeObjects: []cachedCubeObject{{ID: 1}}},
				{X: 2, Y: 1, Z: 1, Count: 1, CubeObjects: []cachedCubeObject{{ID: 2}}},
			},
		},
		"etag-canonical",
		nil,
	)

	entry, ok := cache.TryGetEntry(key)
	if !ok {
		t.Fatal("expected cache hit")
	}
	got := []int32{
		entry.Value.Cells[0].X,
		entry.Value.Cells[1].X,
		entry.Value.Cells[2].X,
	}
	if !reflect.DeepEqual(got, []int32{1, 2, 3}) {
		t.Fatalf("stored cell order = %v, want [1 2 3]", got)
	}
}

func TestBrowsingStateCacheInvalidatesByDependencyAndAll(t *testing.T) {
	cache := newBrowsingStateCache(4, time.Minute)
	cache.PutEntry(
		bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "a"},
		testBSCacheValue(),
		"etag-a",
		[]bsCacheDependency{{Kind: bsDependencyKindTagset, Key: "1"}},
	)
	cache.PutEntry(
		bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "b"},
		testBSCacheValue(),
		"etag-b",
		[]bsCacheDependency{{Kind: bsDependencyKindNode, Key: "2"}},
	)

	if got := cache.InvalidateDependency(bsCacheDependency{
		Kind: bsDependencyKindTagset,
		Key:  "1",
	}); got != 1 {
		t.Fatalf("InvalidateDependency = %d, want 1", got)
	}
	if _, ok := cache.TryGet(bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "a"}); ok {
		t.Fatal("expected tagset entry to be invalidated")
	}
	if _, ok := cache.TryGet(bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "b"}); !ok {
		t.Fatal("expected unrelated entry to remain")
	}

	if got := cache.InvalidateAll(); got != 1 {
		t.Fatalf("InvalidateAll = %d, want 1", got)
	}
	if stats := cache.Stats(); stats.InvalidationsAll != 1 || stats.InvalidationsDependency != 1 {
		t.Fatalf("unexpected invalidation stats: %+v", stats)
	}
}

func TestBrowsingStateCacheStatsExposeNotModifiedCounters(t *testing.T) {
	cache := newBrowsingStateCache(4, time.Minute)
	cache.PutEntry(
		bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "stats"},
		testBSCacheValue(),
		"etag-stats",
		nil,
	)
	cache.RecordHTTPNotModified()
	cache.RecordGRPCNotModified()

	stats := cache.Stats()
	if stats.HTTPNotModified != 1 || stats.GRPCNotModified != 1 {
		t.Fatalf("unexpected not-modified stats: %+v", stats)
	}
	if stats.ApproxMemoryBytes <= 0 {
		t.Fatalf("expected approx memory bytes > 0, got %+v", stats)
	}
}

func TestComputeBSCacheKeyNormalizesFilterAndVectorDimensionOrder(t *testing.T) {
	preparedA := testPreparedBrowsingStateRequest(
		[]qg.ParsedFilter{
			{Type: "tag", Ids: []int{5, 1}},
			{Type: "objectid", Ids: []int{9, 3}},
		},
		[]*pb.VectorSearchDimension{
			testVectorSearchDimension(pb.AxisType_Y_AXIS, 2),
			testVectorSearchDimension(pb.AxisType_X_AXIS, 1),
		},
	)
	preparedB := testPreparedBrowsingStateRequest(
		[]qg.ParsedFilter{
			{Type: "objectid", Ids: []int{3, 9}},
			{Type: "tag", Ids: []int{1, 5}},
		},
		[]*pb.VectorSearchDimension{
			testVectorSearchDimension(pb.AxisType_X_AXIS, 1),
			testVectorSearchDimension(pb.AxisType_Y_AXIS, 2),
		},
	)

	keyA, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, preparedA)
	if err != nil {
		t.Fatalf("compute key A: %v", err)
	}
	keyB, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, preparedB)
	if err != nil {
		t.Fatalf("compute key B: %v", err)
	}
	if keyA != keyB {
		t.Fatalf("keys differ:\nA=%+v\nB=%+v", keyA, keyB)
	}
}

func TestComputeBSCacheKeySeparatesNamespaces(t *testing.T) {
	prepared := testPreparedBrowsingStateRequest(nil, nil)

	keyA, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, prepared)
	if err != nil {
		t.Fatalf("compute key A: %v", err)
	}
	keyB, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState2, prepared)
	if err != nil {
		t.Fatalf("compute key B: %v", err)
	}
	if keyA == keyB {
		t.Fatalf("expected different namespaces to produce different keys")
	}
}

func TestComputeBSCacheKeySeparatesForcedStrategy(t *testing.T) {
	preparedAuto := testPreparedBrowsingStateRequest(nil, nil)
	preparedPreFilter := testPreparedBrowsingStateRequest(nil, nil)
	preparedPreFilter.ForcedStrategy = PreFilter

	keyAuto, err := computeBSCacheKey(
		bsCacheNamespaceGetBrowsingState,
		preparedAuto,
	)
	if err != nil {
		t.Fatalf("compute auto key: %v", err)
	}
	keyPreFilter, err := computeBSCacheKey(
		bsCacheNamespaceGetBrowsingState,
		preparedPreFilter,
	)
	if err != nil {
		t.Fatalf("compute prefilter key: %v", err)
	}
	if keyAuto == keyPreFilter {
		t.Fatalf(
			"expected different forced strategies to produce different keys: %+v vs %+v",
			keyAuto,
			keyPreFilter,
		)
	}
}

func TestShouldCachePreparedBrowsingStateRejectsAllAndTimeline(t *testing.T) {
	if shouldCachePreparedBrowsingState(&preparedBrowsingStateRequest{AllDefined: true}) {
		t.Fatal("all requests must not use L1 cache")
	}
	if shouldCachePreparedBrowsingState(&preparedBrowsingStateRequest{TimelineDefined: true}) {
		t.Fatal("timeline requests must not use L1 cache")
	}
	if !shouldCachePreparedBrowsingState(&preparedBrowsingStateRequest{}) {
		t.Fatal("plain state requests should use L1 cache")
	}
}

func testPreparedBrowsingStateRequest(
	filters []qg.ParsedFilter,
	dims []*pb.VectorSearchDimension,
) *preparedBrowsingStateRequest {
	merged := mergedVectorDimensions{Dims: dims}
	if len(dims) > 1 {
		merged.UseAxisBucketInfos = true
	}
	return &preparedBrowsingStateRequest{
		AxisOrder:        []string{"x", "y", "filter"},
		AxisX:            qg.ParsedAxis{Type: "tagset", Id: 11, Ids: map[int]int{1: 1}},
		AxisY:            qg.ParsedAxis{Type: "tagset", Id: 22, Ids: map[int]int{1: 1}},
		Filters:          filters,
		MergedVectorDims: merged,
		ForcedStrategy:   Auto,
	}
}

func testVectorSearchDimension(
	axis pb.AxisType,
	objectID int32,
) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		Axis:      axis,
		ModelName: "siglip2",
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_ObjectId{ObjectId: objectID},
		},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  1,
		},
		MaxResults: 4,
	}
}

func testBSCacheValue() bsCacheValue {
	return bsCacheValue{
		Cells: []cachedBrowsingStateCell{{
			X:     1,
			Y:     2,
			Z:     1,
			Count: 3,
			CubeObjects: []cachedCubeObject{{
				ID:           7,
				FileURI:      "/media/7",
				ThumbnailURI: "/thumb/7",
			}},
		}},
		BucketInfos: []*pb.BucketInfo{{
			BucketId:   1,
			LowerBound: 0,
			UpperBound: 1,
			Label:      "near",
		}},
	}
}

func TestCachedCellsFromCompatResponsesRoundTrip(t *testing.T) {
	input := []compatBrowsingStateResponse{{
		X:     2,
		Y:     3,
		Z:     1,
		Count: 4,
		CubeObjects: []compatCubeObject{{
			Id:           11,
			FileURI:      "/media/11",
			ThumbnailURI: "/thumb/11",
		}},
	}}
	want := []compatBrowsingStateResponse{{
		X:     2,
		Y:     3,
		Z:     1,
		Count: 4,
		CubeObjects: []compatCubeObject{{
			Id:           11,
			FileURI:      "/media/11",
			ThumbnailURI: "/thumb/11",
			FileUri:      "/media/11",
			ThumbnailUri: "/thumb/11",
		}},
	}}

	got := compatResponsesFromBSCacheValue(bsCacheValue{
		Cells: cachedCellsFromCompatResponses(input),
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\ngot=%+v\nwant=%+v", got, want)
	}
}
