package main

import (
	"context"
	"reflect"
	"testing"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func neighborsWithDistances(distances ...float32) []*kvstorev1.Neighbor {
	neighbors := make([]*kvstorev1.Neighbor, 0, len(distances))
	for index, distance := range distances {
		neighbors = append(neighbors, &kvstorev1.Neighbor{
			Id:       int32(index + 1),
			Distance: distance,
		})
	}
	return neighbors
}

func TestApplyMetricDefaultsUsesMetricSpecificRange(t *testing.T) {
	cases := []struct {
		name      string
		cfg       bucketConfig
		metric    string
		neighbors []*kvstorev1.Neighbor
		want      float64
	}{
		{name: "cosine default", cfg: bucketConfig{DistanceMin: 0}, metric: "cosine", want: 2.0},
		{name: "l2 default", cfg: bucketConfig{DistanceMin: 0}, metric: "l2", neighbors: neighborsWithDistances(0.25, 0.5), want: 0.505},
		{name: "fallback span", cfg: bucketConfig{DistanceMin: 3}, metric: "unknown", want: 4.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyMetricDefaults(tc.cfg, tc.metric, tc.neighbors)
			if got.DistanceMax != tc.want {
				t.Fatalf("DistanceMax = %v, want %v", got.DistanceMax, tc.want)
			}
		})
	}
}

func TestComputeBucketBoundariesEqualWidth(t *testing.T) {
	boundaries, err := computeBucketBoundaries(bucketConfig{
		Strategy:    pb.BucketStrategy_EQUAL_WIDTH,
		Count:       4,
		DistanceMin: 0,
		DistanceMax: 1,
	}, nil)
	if err != nil {
		t.Fatalf("computeBucketBoundaries returned error: %v", err)
	}

	want := []float64{0, 0.25, 0.5, 0.75, 1}
	if !reflect.DeepEqual(boundaries, want) {
		t.Fatalf("boundaries = %v, want %v", boundaries, want)
	}
}

func TestAssignBucketsCoversBoundaryCases(t *testing.T) {
	cases := []struct {
		name       string
		neighbors  []*kvstorev1.Neighbor
		boundaries []float64
		want       map[int][]int
	}{
		{name: "last bucket includes upper boundary", neighbors: neighborsWithDistances(0, 1), boundaries: []float64{0, 0.5, 1}, want: map[int][]int{0: {1}, 1: {2}}},
		{name: "all results in one bucket", neighbors: neighborsWithDistances(0.01, 0.02, 0.03), boundaries: []float64{0, 0.5, 1}, want: map[int][]int{0: {1, 2, 3}}},
		{name: "bucket count can exceed result count", neighbors: neighborsWithDistances(0.1, 0.8), boundaries: []float64{0, 0.25, 0.5, 0.75, 1}, want: map[int][]int{0: {1}, 3: {2}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := assignBuckets(tc.neighbors, tc.boundaries)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("bucket ids = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterNeighborsByDistanceClipsOutsideRange(t *testing.T) {
	filtered := filterNeighborsByDistance(neighborsWithDistances(0.1, 0.6, 0.9), 0.2, 0.8)
	if len(filtered) != 1 || filtered[0].GetId() != 2 {
		t.Fatalf("filtered = %v", filtered)
	}
}

func TestHandleVectorDimensionReturnsEmptyAxisSQLWhenRangeExcludesAllResults(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
			getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
			knnResp: &kvstorev1.KNNResponse{Neighbors: neighborsWithDistances(0.8, 0.9)},
			listModelsResp: &kvstorev1.ListModelsResponse{
				Models: []*kvstorev1.ModelInfo{{Name: "siglip2", DistanceMetric: "cosine"}},
			},
		}),
	}

	result, err := server.handleVectorDimension(context.Background(), &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 9}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  0.5,
		},
		MaxResults: 2,
		Axis:       pb.AxisType_Y_AXIS,
	})
	if err != nil {
		t.Fatalf("handleVectorDimension returned error: %v", err)
	}

	if len(result.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(result.BucketInfos))
	}
	if result.AxisSQL != "SELECT 0 AS object_id, 0 AS id WHERE false" {
		t.Fatalf("axis sql = %s", result.AxisSQL)
	}
	if len(result.ObjectIDsByBucket) != 0 {
		t.Fatalf("bucket contents = %v, want empty", result.ObjectIDsByBucket)
	}
}
