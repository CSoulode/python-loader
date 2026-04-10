package runner

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func DialVectorKV(ctx context.Context, addr string) (kvstorev1.VectorKVClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return kvstorev1.NewVectorKVClient(conn), conn, nil
}

func FetchQueryVector(ctx context.Context, client kvstorev1.VectorKVClient, model string, objectID int32) ([]float32, error) {
	resp, err := client.Get(ctx, &kvstorev1.GetRequest{Model: model, Id: objectID})
	if err != nil {
		return nil, err
	}
	return append([]float32(nil), resp.GetVector().GetValues()...), nil
}

func SearchKNN(ctx context.Context, client kvstorev1.VectorKVClient, model string, vector []float32, k int32) ([]Neighbor, error) {
	resp, err := client.KNN(ctx, &kvstorev1.KNNRequest{
		Model: model,
		K:     k,
		Query: &kvstorev1.Vector{Values: vector},
	})
	if err != nil {
		return nil, err
	}
	return neighborsFromProto(resp.GetNeighbors()), nil
}

func SearchFilteredKNN(
	ctx context.Context,
	client kvstorev1.VectorKVClient,
	model string,
	vector []float32,
	k int32,
	candidateIDs []int32,
) ([]Neighbor, error) {
	resp, err := client.FilteredKNN(ctx, &kvstorev1.FilteredKNNRequest{
		Model:        model,
		K:            k,
		CandidateIds: candidateIDs,
		Query:        &kvstorev1.Vector{Values: vector},
	})
	if err != nil {
		return nil, err
	}
	return neighborsFromProto(resp.GetNeighbors()), nil
}

func SearchRange(
	ctx context.Context,
	client kvstorev1.VectorKVClient,
	model string,
	vector []float32,
	plan VectorQueryPlan,
) ([]Neighbor, error) {
	resp, err := client.RangeSearch(ctx, &kvstorev1.RangeSearchRequest{
		Model:       model,
		Query:       &kvstorev1.Vector{Values: vector},
		MinDistance: plan.DistanceRange.GetMinDistance(),
		MaxDistance: plan.DistanceRange.GetMaxDistance(),
		MaxResults:  plan.MaxResults,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return neighborsFromProto(resp.GetNeighbors()), nil
}

func SearchFilteredRange(
	ctx context.Context,
	client kvstorev1.VectorKVClient,
	model string,
	vector []float32,
	plan VectorQueryPlan,
	candidateIDs []int32,
) ([]Neighbor, error) {
	resp, err := client.FilteredRangeSearch(ctx, &kvstorev1.FilteredRangeSearchRequest{
		Model:        model,
		Query:        &kvstorev1.Vector{Values: vector},
		CandidateIds: candidateIDs,
		MinDistance:  plan.DistanceRange.GetMinDistance(),
		MaxDistance:  plan.DistanceRange.GetMaxDistance(),
		MaxResults:   plan.MaxResults,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return neighborsFromProto(resp.GetNeighbors()), nil
}

func neighborsFromProto(items []*kvstorev1.Neighbor) []Neighbor {
	out := make([]Neighbor, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, Neighbor{ObjectID: item.GetId(), Distance: float64(item.GetDistance())})
	}
	return out
}
