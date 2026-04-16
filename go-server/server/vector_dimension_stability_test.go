package main

import (
	"reflect"
	"testing"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func TestNeighborsFromProtoSortsByDistanceThenObjectID(t *testing.T) {
	got := neighborsFromProto([]*kvstorev1.Neighbor{
		{Id: 9, Distance: 0.4},
		{Id: 3, Distance: 0.2},
		{Id: 7, Distance: 0.4},
		{Id: 1, Distance: 0.1},
	})

	gotIDs := []int32{
		got[0].ObjectID,
		got[1].ObjectID,
		got[2].ObjectID,
		got[3].ObjectID,
	}
	if !reflect.DeepEqual(gotIDs, []int32{1, 3, 7, 9}) {
		t.Fatalf("neighbor order = %v, want [1 3 7 9]", gotIDs)
	}
	if got[0].Distance > got[1].Distance || got[1].Distance > got[2].Distance {
		t.Fatalf("distances are not sorted: %+v", got)
	}
}

func TestBucketSearchResultIgnoresNeighborOrder(t *testing.T) {
	cfg := &pb.VectorSearchDimension{
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_DEPTH,
			Count:    2,
			DistMin:  0,
			DistMax:  1,
		},
		MaxResults: 4,
		Axis:       pb.AxisType_X_AXIS,
	}
	base := searchResult{
		DistanceMetric: "cosine",
		RawNeighbors: []Neighbor{
			{ObjectID: 1, Distance: 0.1},
			{ObjectID: 2, Distance: 0.2},
			{ObjectID: 3, Distance: 0.3},
			{ObjectID: 4, Distance: 0.4},
		},
	}
	reordered := searchResult{
		DistanceMetric: base.DistanceMetric,
		RawNeighbors: []Neighbor{
			{ObjectID: 4, Distance: 0.4},
			{ObjectID: 2, Distance: 0.2},
			{ObjectID: 3, Distance: 0.3},
			{ObjectID: 1, Distance: 0.1},
		},
	}

	gotBase, err := bucketSearchResult(cfg, base)
	if err != nil {
		t.Fatalf("base bucketSearchResult: %v", err)
	}
	gotReordered, err := bucketSearchResult(cfg, reordered)
	if err != nil {
		t.Fatalf("reordered bucketSearchResult: %v", err)
	}

	if !reflect.DeepEqual(gotBase.BucketInfos, gotReordered.BucketInfos) {
		t.Fatalf("bucket infos differ: %+v vs %+v", gotBase.BucketInfos, gotReordered.BucketInfos)
	}
	if !reflect.DeepEqual(gotBase.ObjectIDsByBucket, gotReordered.ObjectIDsByBucket) {
		t.Fatalf("bucket ids differ: %+v vs %+v", gotBase.ObjectIDsByBucket, gotReordered.ObjectIDsByBucket)
	}
	if gotBase.AxisSQL != gotReordered.AxisSQL {
		t.Fatalf("axis sql differs: %q vs %q", gotBase.AxisSQL, gotReordered.AxisSQL)
	}
}
