package runner

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

type stubVectorKVClient struct {
	rangeErr         error
	filteredRangeErr error
}

func (s *stubVectorKVClient) Put(context.Context, *kvstorev1.PutRequest, ...grpc.CallOption) (*kvstorev1.PutResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) PutBatch(context.Context, *kvstorev1.PutBatchRequest, ...grpc.CallOption) (*kvstorev1.PutBatchResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) Get(context.Context, *kvstorev1.GetRequest, ...grpc.CallOption) (*kvstorev1.GetResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) NN(context.Context, *kvstorev1.NNRequest, ...grpc.CallOption) (*kvstorev1.NNResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) KNN(context.Context, *kvstorev1.KNNRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) FilteredKNN(context.Context, *kvstorev1.FilteredKNNRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error) {
	return nil, nil
}

func (s *stubVectorKVClient) RangeSearch(context.Context, *kvstorev1.RangeSearchRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error) {
	return nil, s.rangeErr
}

func (s *stubVectorKVClient) FilteredRangeSearch(context.Context, *kvstorev1.FilteredRangeSearchRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error) {
	return nil, s.filteredRangeErr
}

func (s *stubVectorKVClient) ListModels(context.Context, *kvstorev1.ListModelsRequest, ...grpc.CallOption) (*kvstorev1.ListModelsResponse, error) {
	return nil, nil
}

func TestSearchRangeTreatsNotFoundAsEmpty(t *testing.T) {
	plan := VectorQueryPlan{DistanceRange: &pb.DistanceRange{MinDistance: 0.1, MaxDistance: 0.2}, MaxResults: 10}
	neighbors, err := SearchRange(context.Background(), &stubVectorKVClient{
		rangeErr: status.Error(codes.NotFound, "not found"),
	}, "siglip2", []float32{1, 2, 3}, plan)
	if err != nil {
		t.Fatalf("SearchRange returned error: %v", err)
	}
	if len(neighbors) != 0 {
		t.Fatalf("neighbors = %v, want empty", neighbors)
	}
}

func TestSearchFilteredRangeTreatsNotFoundAsEmpty(t *testing.T) {
	plan := VectorQueryPlan{DistanceRange: &pb.DistanceRange{MinDistance: 0.1, MaxDistance: 0.2}, MaxResults: 10}
	neighbors, err := SearchFilteredRange(context.Background(), &stubVectorKVClient{
		filteredRangeErr: status.Error(codes.NotFound, "not found"),
	}, "siglip2", []float32{1, 2, 3}, plan, []int32{1, 2})
	if err != nil {
		t.Fatalf("SearchFilteredRange returned error: %v", err)
	}
	if len(neighbors) != 0 {
		t.Fatalf("neighbors = %v, want empty", neighbors)
	}
}
