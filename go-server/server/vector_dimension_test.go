package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func TestHandleVectorDimensionBuildsAxisPlan(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
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
		MaxResults: 3,
		Axis:       pb.AxisType_Y_AXIS,
	})
	if err != nil {
		t.Fatalf("handleVectorDimension returned error: %v", err)
	}

	if result.AxisType != pb.AxisType_Y_AXIS {
		t.Fatalf("axis type = %v", result.AxisType)
	}
	if result.ParsedAxis.Type != "vector" {
		t.Fatalf("axis type = %q", result.ParsedAxis.Type)
	}
	if len(result.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(result.BucketInfos))
	}
	if !strings.Contains(result.AxisSQL, "(1,0)") || !strings.Contains(result.AxisSQL, "(2,1)") {
		t.Fatalf("axis sql = %s", result.AxisSQL)
	}
	if !reflect.DeepEqual(result.ObjectIDsByBucket[0], []int{1}) {
		t.Fatalf("bucket 0 ids = %v", result.ObjectIDsByBucket[0])
	}
	if !reflect.DeepEqual(result.ObjectIDsByBucket[1], []int{2, 3}) {
		t.Fatalf("bucket 1 ids = %v", result.ObjectIDsByBucket[1])
	}
}

func TestParseBrowsingStateRequestRejectsAllVectorDimensionWithoutBucketID(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
			getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
			knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{
				{Id: 1, Distance: 0},
			}},
			listModelsResp: &kvstorev1.ListModelsResponse{
				Models: []*kvstorev1.ModelInfo{{
					Name:           "siglip2",
					DistanceMetric: "cosine",
				}},
			},
		}),
	}

	_, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		All: "[]",
		VectorDimension: &pb.VectorSearchDimension{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    1,
				DistMax:  1,
			},
			MaxResults: 1,
			Axis:       pb.AxisType_X_AXIS,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestParseBrowsingStateRequestRejectsAxisConflict(t *testing.T) {
	server := &DataLoaderServer{
		vectorFilters: newVectorFilterResolver(&stubVectorSearchClient{
			getResp: &kvstorev1.GetResponse{Vector: &kvstorev1.Vector{Values: []float32{1, 2, 3}}},
			knnResp: &kvstorev1.KNNResponse{Neighbors: []*kvstorev1.Neighbor{
				{Id: 1, Distance: 0},
			}},
			listModelsResp: &kvstorev1.ListModelsResponse{
				Models: []*kvstorev1.ModelInfo{{
					Name:           "siglip2",
					DistanceMetric: "cosine",
				}},
			},
		}),
	}

	_, err := server.parseBrowsingStateRequest(context.Background(), &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		VectorDimension: &pb.VectorSearchDimension{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    1,
				DistMax:  1,
			},
			MaxResults: 1,
			Axis:       pb.AxisType_X_AXIS,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument", status.Code(err))
	}
}
