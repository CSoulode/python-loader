package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

type stubVectorSearchClient struct {
	getResp *kvstorev1.GetResponse
	getErr  error
	knnResp *kvstorev1.KNNResponse
	knnErr  error

	gotGet *kvstorev1.GetRequest
	gotKNN *kvstorev1.KNNRequest
}

func (s *stubVectorSearchClient) Get(_ context.Context, req *kvstorev1.GetRequest, _ ...grpc.CallOption) (*kvstorev1.GetResponse, error) {
	s.gotGet = req
	return s.getResp, s.getErr
}

func (s *stubVectorSearchClient) KNN(_ context.Context, req *kvstorev1.KNNRequest, _ ...grpc.CallOption) (*kvstorev1.KNNResponse, error) {
	s.gotKNN = req
	return s.knnResp, s.knnErr
}

func TestVectorFilterResolverResolveObjectIDsObjectReference(t *testing.T) {
	client := &stubVectorSearchClient{
		getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
		knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{
			{Id: 9},
			{Id: 3},
			{Id: 9},
		}},
	}
	resolver := newVectorFilterResolver(client)

	ids, err := resolver.ResolveObjectIDs(context.Background(), &pb.VectorFilterConfig{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 42}},
		K:         3,
	})
	if err != nil {
		t.Fatalf("ResolveObjectIDs returned error: %v", err)
	}
	if !reflect.DeepEqual(ids, []int{3, 9}) {
		t.Fatalf("ResolveObjectIDs ids = %v, want [3 9]", ids)
	}
	if client.gotGet.GetId() != 42 || client.gotGet.GetModel() != "siglip2" {
		t.Fatalf("Get request = %+v", client.gotGet)
	}
	if !reflect.DeepEqual(client.gotKNN.GetQuery().GetValues(), []float32{1, 2, 3}) {
		t.Fatalf("KNN query = %v", client.gotKNN.GetQuery().GetValues())
	}
}

func TestVectorFilterResolverResolveObjectIDsEmptyResult(t *testing.T) {
	resolver := newVectorFilterResolver(&stubVectorSearchClient{
		knnErr: status.Error(codes.NotFound, "not found"),
	})

	ids, err := resolver.ResolveObjectIDs(context.Background(), &pb.VectorFilterConfig{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_RawEmbedding{RawEmbedding: &pb.Vector{Values: []float32{1, 2}}},
		},
		K: 4,
	})
	if err != nil {
		t.Fatalf("ResolveObjectIDs returned error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ResolveObjectIDs ids = %v, want empty slice", ids)
	}
}

func TestParseBrowsingStateRequestAppendsVectorFilter(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
			getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2}}},
			knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{{Id: 8}, {Id: 5}}},
		}),
	}

	axisOrder, _, _, _, filters, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          11,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		VectorFilter: &pb.VectorFilterConfig{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 12}},
			K:         2,
		},
	})
	if err != nil {
		t.Fatalf("parseBrowsingStateRequest returned error: %v", err)
	}
	if !reflect.DeepEqual(axisOrder, []string{"x", "filter"}) {
		t.Fatalf("axisOrder = %v", axisOrder)
	}
	if got := filters[len(filters)-1]; got.Type != "objectid" || !reflect.DeepEqual(got.Ids, []int{5, 8}) {
		t.Fatalf("vector filter = %+v", got)
	}
}

func TestParseBrowsingStateRequestRejectsTimelineVectorFilter(t *testing.T) {
	server := &DataLoaderServer{vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{})}

	_, _, _, _, _, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		Timeline: "42",
		VectorFilter: &pb.VectorFilterConfig{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{
				Ref: &pb.VectorReference_RawEmbedding{RawEmbedding: &pb.Vector{Values: []float32{1}}},
			},
			K: 1,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestBuildAllMediasSQLWithEmptyObjectIDFilter(t *testing.T) {
	sqlStr, err := buildAllMediasSQL(
		qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		[]qg.ParsedFilter{{Type: "objectid", Ids: nil}},
	)
	if err != nil {
		t.Fatalf("buildAllMediasSQL returned error: %v", err)
	}
	if !strings.Contains(sqlStr, "WHERE 1 = 0") {
		t.Fatalf("buildAllMediasSQL = %s", sqlStr)
	}
}
