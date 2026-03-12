package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

type compatVectorDimension struct {
	Model          string    `json:"model"`
	ObjectID       int32     `json:"objectId"`
	Axis           string    `json:"axis"`
	BucketCount    int32     `json:"bucketCount"`
	BucketStrategy string    `json:"bucketStrategy"`
	DistMin        float32   `json:"distMin"`
	DistMax        float32   `json:"distMax"`
	MaxResults     int32     `json:"maxResults"`
	K              int32     `json:"k"`
	CustomBreaks   []float32 `json:"customBreaks"`
}

func parseCompatVectorDimension(raw string) (*pb.VectorSearchDimension, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var parsed compatVectorDimension
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorDimension JSON: %v", err)
	}

	axis, err := parseCompatAxis(parsed.Axis)
	if err != nil {
		return nil, err
	}
	strategy, err := parseCompatBucketStrategy(parsed.BucketStrategy)
	if err != nil {
		return nil, err
	}

	maxResults := parsed.MaxResults
	if maxResults == 0 {
		maxResults = parsed.K
	}

	return &pb.VectorSearchDimension{
		ModelName: strings.TrimSpace(parsed.Model),
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_ObjectId{ObjectId: parsed.ObjectID},
		},
		BucketCfg: &pb.BucketConfig{
			Strategy:     strategy,
			Count:        parsed.BucketCount,
			DistMin:      parsed.DistMin,
			DistMax:      parsed.DistMax,
			CustomBreaks: append([]float32(nil), parsed.CustomBreaks...),
		},
		MaxResults: maxResults,
		Axis:       axis,
	}, nil
}

func parseCompatVectorBucketID(raw string) (*int32, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	var bucketID int32
	if _, err := fmt.Sscanf(trimmed, "%d", &bucketID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorBucketId: %v", err)
	}
	return &bucketID, nil
}

func parseCompatAxis(raw string) (pb.AxisType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "y", "y_axis", "y-axis":
		return pb.AxisType_Y_AXIS, nil
	case "x", "x_axis", "x-axis":
		return pb.AxisType_X_AXIS, nil
	case "z", "z_axis", "z-axis":
		return pb.AxisType_Z_AXIS, nil
	default:
		return pb.AxisType_FILTER, status.Error(codes.InvalidArgument, "vector_dimension.axis must be x, y, or z")
	}
}

func parseCompatBucketStrategy(raw string) (pb.BucketStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "equal_width", "equal-width", "equalwidth":
		return pb.BucketStrategy_EQUAL_WIDTH, nil
	case "equal_depth", "equal-depth", "equaldepth":
		return pb.BucketStrategy_EQUAL_DEPTH, nil
	case "logarithmic", "log":
		return pb.BucketStrategy_LOGARITHMIC, nil
	case "custom":
		return pb.BucketStrategy_CUSTOM, nil
	default:
		return pb.BucketStrategy_EQUAL_WIDTH, status.Error(codes.InvalidArgument, "invalid vector_dimension.bucket_cfg.strategy")
	}
}
