package main

import (
	"reflect"
	"testing"

	pb "m3.dataloader/dataloader"
)

func TestMergeVectorDimensionsLegacyAndNew(t *testing.T) {
	oldBucketID := int32(2)
	legacy, err := mergeVectorDimensions(&pb.GetBrowsingStateRequest{
		VectorDimension: &pb.VectorSearchDimension{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    2,
				DistMax:  1,
			},
			MaxResults: 3,
			Axis:       pb.AxisType_Y_AXIS,
		},
		VectorBucketId: &oldBucketID,
	})
	if err != nil {
		t.Fatalf("mergeVectorDimensions legacy: %v", err)
	}
	if legacy.UseAxisBucketInfos {
		t.Fatal("legacy request should not switch to axis bucket infos")
	}
	if len(legacy.Dims) != 1 || legacy.BucketIDs["y"] != 2 {
		t.Fatalf("legacy merge = %+v", legacy)
	}

	modern, err := mergeVectorDimensions(&pb.GetBrowsingStateRequest{
		VectorDimensions: []*pb.VectorSearchDimension{
			{
				ModelName: "siglip2",
				Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
				BucketCfg: &pb.BucketConfig{
					Strategy: pb.BucketStrategy_EQUAL_WIDTH,
					Count:    2,
					DistMax:  1,
				},
				MaxResults: 3,
				Axis:       pb.AxisType_X_AXIS,
			},
		},
		VectorBucketIds: map[string]int32{"x": 1},
	})
	if err != nil {
		t.Fatalf("mergeVectorDimensions modern: %v", err)
	}
	if !modern.UseAxisBucketInfos {
		t.Fatal("modern request should use axis bucket infos")
	}
	if len(modern.Dims) != 1 || modern.BucketIDs["x"] != 1 {
		t.Fatalf("modern merge = %+v", modern)
	}

	_, err = mergeVectorDimensions(&pb.GetBrowsingStateRequest{
		VectorDimension:  modern.Dims[0],
		VectorDimensions: modern.Dims,
	})
	if err == nil {
		t.Fatal("expected conflict error when legacy and modern fields are both set")
	}
}

func TestValidateVectorFilterAgainstDimensions(t *testing.T) {
	vectorFilter := &pb.VectorFilterConfig{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		K:         5,
	}
	err := validateVectorFilterAgainstDimensions(vectorFilter, []*pb.VectorSearchDimension{
		{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    2,
				DistMax:  1,
			},
			MaxResults: 5,
			Axis:       pb.AxisType_X_AXIS,
		},
	})
	if err == nil {
		t.Fatal("expected duplicate vector search identity to be rejected")
	}

	err = validateVectorFilterAgainstDimensions(vectorFilter, []*pb.VectorSearchDimension{
		{
			ModelName: "hsv",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg: &pb.BucketConfig{
				Strategy: pb.BucketStrategy_EQUAL_WIDTH,
				Count:    2,
				DistMax:  1,
			},
			MaxResults: 5,
			Axis:       pb.AxisType_Y_AXIS,
		},
	})
	if err != nil {
		t.Fatalf("different model should be allowed: %v", err)
	}
}

func TestBucketInfoAttacherMultiAttachesOnlyOnce(t *testing.T) {
	attacher := newBucketInfoAttacherMulti(map[string][]*pb.BucketInfo{
		"x": {{BucketId: 0, Label: "x0"}},
		"y": {{BucketId: 1, Label: "y1"}},
	})

	first := attacher.Attach(&pb.BrowsingStateResponse{})
	if got := first.GetAxisBucketInfos(); len(got) != 2 {
		t.Fatalf("first attach axis infos = %d, want 2", len(got))
	}
	if !reflect.DeepEqual(first.GetAxisBucketInfos()["x"].GetItems()[0].GetLabel(), "x0") {
		t.Fatalf("x bucket infos = %+v", first.GetAxisBucketInfos()["x"])
	}

	second := attacher.Attach(&pb.BrowsingStateResponse{})
	if len(second.GetAxisBucketInfos()) != 0 || len(second.GetBucketInfos()) != 0 {
		t.Fatalf("second attach should be empty, got axis=%v bucket=%v", second.GetAxisBucketInfos(), second.GetBucketInfos())
	}
}
