package main

import (
	"context"
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func TestGlobalVectorSearchForHybridCachesMissResult(t *testing.T) {
	client := &stubVectorSearchClient{
		getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
		knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{
			{Id: 4, Distance: 0.1},
			{Id: 7, Distance: 0.2},
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
	cfg := testVectorDimensionConfig(pb.BucketStrategy_EQUAL_WIDTH, 2)
	inputs, err := server.resolveVectorSearchInputs(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resolveVectorSearchInputs: %v", err)
	}

	first, err := server.globalVectorSearchForHybrid(context.Background(), cfg, inputs, 1, 2)
	if err != nil {
		t.Fatalf("first globalVectorSearchForHybrid: %v", err)
	}
	if client.knnCalls != 1 {
		t.Fatalf("knnCalls after first lookup = %d, want 1", client.knnCalls)
	}

	second, err := server.globalVectorSearchForHybrid(context.Background(), cfg, inputs, 1, 2)
	if err != nil {
		t.Fatalf("second globalVectorSearchForHybrid: %v", err)
	}
	if client.knnCalls != 1 {
		t.Fatalf("cache hit should skip KNN, got %d calls", client.knnCalls)
	}
	if len(first.RawNeighbors) != len(second.RawNeighbors) {
		t.Fatalf("neighbor counts differ: %d vs %d", len(first.RawNeighbors), len(second.RawNeighbors))
	}
}
