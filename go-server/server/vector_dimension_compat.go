package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

type compatVectorDimension struct {
	Model          string               `json:"model"`
	ObjectID       int32                `json:"objectId"`
	Axis           string               `json:"axis"`
	BucketCount    int32                `json:"bucketCount"`
	BucketStrategy string               `json:"bucketStrategy"`
	DistMin        float32              `json:"distMin"`
	DistMax        float32              `json:"distMax"`
	MaxResults     int32                `json:"maxResults"`
	K              int32                `json:"k"`
	CustomBreaks   []float32            `json:"customBreaks"`
	DistanceRange  *compatDistanceRange `json:"distanceRange,omitempty"`
	RangeSemantics string               `json:"rangeSemantics,omitempty"`
}

type compatDistanceRange struct {
	Min float32 `json:"min"`
	Max float32 `json:"max"`
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
	return buildCompatVectorDimension(parsed)
}

func buildCompatVectorDimension(parsed compatVectorDimension) (*pb.VectorSearchDimension, error) {
	axis, err := parseCompatAxis(parsed.Axis)
	if err != nil {
		return nil, err
	}
	strategy, err := parseCompatBucketStrategy(parsed.BucketStrategy)
	if err != nil {
		return nil, err
	}
	rangeSemantics, err := parseCompatRangeSemantics(parsed.RangeSemantics)
	if err != nil {
		return nil, err
	}

	maxResults := parsed.MaxResults
	if maxResults == 0 {
		maxResults = parsed.K
	}
	if parsed.DistanceRange == nil && strings.TrimSpace(parsed.RangeSemantics) != "" {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.range_semantics requires distance_range")
	}

	cfg := &pb.VectorSearchDimension{
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
	}
	if parsed.DistanceRange != nil {
		cfg.DistanceRange = &pb.DistanceRange{
			MinDistance: parsed.DistanceRange.Min,
			MaxDistance: parsed.DistanceRange.Max,
		}
		cfg.RangeSemantics = rangeSemantics
	}
	return cfg, nil
}

func parseCompatVectorDimensions(query url.Values) (mergedVectorDimensions, error) {
	newRaw := strings.TrimSpace(query.Get("vectorDimensions"))
	oldRaw := strings.TrimSpace(query.Get("vectorDimension"))
	if newRaw != "" && oldRaw != "" {
		return mergedVectorDimensions{}, status.Error(
			codes.InvalidArgument,
			"vectorDimension and vectorDimensions cannot be used together",
		)
	}

	newBucketRaw := strings.TrimSpace(query.Get("vectorBucketIds"))
	oldBucketRaw := strings.TrimSpace(query.Get("vectorBucketId"))
	if newRaw != "" {
		if oldBucketRaw != "" {
			return mergedVectorDimensions{}, status.Error(
				codes.InvalidArgument,
				"vectorBucketId cannot be used with vectorDimensions; use vectorBucketIds",
			)
		}
		var parsed []compatVectorDimension
		decoder := json.NewDecoder(strings.NewReader(newRaw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&parsed); err != nil {
			return mergedVectorDimensions{}, status.Errorf(codes.InvalidArgument, "invalid vectorDimensions JSON: %v", err)
		}

		dims := make([]*pb.VectorSearchDimension, 0, len(parsed))
		for _, item := range parsed {
			dim, err := buildCompatVectorDimension(item)
			if err != nil {
				return mergedVectorDimensions{}, err
			}
			dims = append(dims, dim)
		}
		bucketIDs, err := parseCompatVectorBucketIDs(newBucketRaw)
		if err != nil {
			return mergedVectorDimensions{}, err
		}
		return mergedVectorDimensions{
			Dims:               dims,
			BucketIDs:          bucketIDs,
			UseAxisBucketInfos: len(dims) > 0,
		}, nil
	}

	if newBucketRaw != "" {
		return mergedVectorDimensions{}, status.Error(
			codes.InvalidArgument,
			"vectorBucketIds requires vectorDimensions",
		)
	}
	dim, err := parseCompatVectorDimension(oldRaw)
	if err != nil {
		return mergedVectorDimensions{}, err
	}
	bucketID, err := parseCompatVectorBucketID(oldBucketRaw)
	if err != nil {
		return mergedVectorDimensions{}, err
	}

	merged := mergedVectorDimensions{}
	if dim == nil {
		if bucketID != nil {
			return mergedVectorDimensions{}, status.Error(
				codes.InvalidArgument,
				"vectorBucketId requires vectorDimension",
			)
		}
		return merged, nil
	}
	merged.Dims = []*pb.VectorSearchDimension{dim}
	if bucketID != nil {
		merged.BucketIDs = map[string]int32{axisToken(dim.GetAxis()): *bucketID}
	}
	return merged, nil
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

func parseCompatVectorBucketIDs(raw string) (map[string]int32, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var parsed map[string]int32
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorBucketIds JSON: %v", err)
	}

	normalized := make(map[string]int32, len(parsed))
	for axisKey, bucketID := range parsed {
		normalizedAxis, err := parseCompatAxisKey(axisKey)
		if err != nil {
			return nil, err
		}
		normalized[normalizedAxis] = bucketID
	}
	return normalized, nil
}

func parseCompatAxisKey(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "x":
		return "x", nil
	case "y":
		return "y", nil
	case "z":
		return "z", nil
	default:
		return "", status.Error(codes.InvalidArgument, "vector bucket axis must be x, y, or z")
	}
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

func parseCompatRangeSemantics(raw string) (pb.RangeSemantics, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "distance":
		return pb.RangeSemantics_DISTANCE, nil
	case "similarity":
		return pb.RangeSemantics_SIMILARITY, nil
	default:
		return pb.RangeSemantics_DISTANCE, status.Error(codes.InvalidArgument, "invalid vector_dimension.range_semantics")
	}
}
