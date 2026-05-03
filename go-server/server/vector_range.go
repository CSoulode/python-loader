package main

import (
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

const defaultRangeMaxResults int32 = 10000

func isRangeQuery(cfg *pb.VectorSearchDimension) bool {
	return cfg != nil && cfg.GetDistanceRange() != nil
}

func effectiveVectorMaxResults(cfg *pb.VectorSearchDimension) int32 {
	if cfg == nil {
		return 0
	}
	if isRangeQuery(cfg) && cfg.GetMaxResults() == 0 {
		return defaultRangeMaxResults
	}
	return cfg.GetMaxResults()
}

func normalizeVectorSearchConfig(cfg *pb.VectorSearchDimension, metric string) (*pb.VectorSearchDimension, error) {
	if cfg == nil {
		return nil, nil
	}

	cloned := cloneVectorSearchConfig(cfg)
	if !isRangeQuery(cfg) {
		return cloned, nil
	}

	rangeCfg, err := normalizeDistanceRange(cfg.GetDistanceRange(), cfg.GetRangeSemantics(), metric)
	if err != nil {
		return nil, err
	}
	cloned.DistanceRange = rangeCfg
	cloned.MaxResults = effectiveVectorMaxResults(cfg)
	return cloned, nil
}

func normalizeDistanceRange(
	dr *pb.DistanceRange,
	sem pb.RangeSemantics,
	metric string,
) (*pb.DistanceRange, error) {
	if dr == nil {
		return nil, nil
	}

	minValue := dr.GetMinDistance()
	maxValue := dr.GetMaxDistance()
	switch sem {
	case pb.RangeSemantics_DISTANCE:
		return validateDistanceRange(minValue, maxValue)
	case pb.RangeSemantics_SIMILARITY:
		if minValue < 0 || maxValue > 1 || maxValue <= minValue {
			return nil, status.Error(codes.InvalidArgument, "vector_dimension.distance_range similarity bounds must satisfy 0 <= min < max <= 1")
		}
		switch metric {
		case "cosine":
			return validateDistanceRange(1-maxValue, 1-minValue)
		case "l2":
			return validateDistanceRange(similarityToL2Distance(maxValue), similarityToL2Distance(minValue))
		default:
			return nil, status.Errorf(codes.InvalidArgument, "vector_dimension.range_semantics similarity is unsupported for metric %q", metric)
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "vector_dimension.range_semantics %v is invalid", sem)
	}
}

func validateDistanceRange(minValue float32, maxValue float32) (*pb.DistanceRange, error) {
	if minValue < 0 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.distance_range.min_distance must be >= 0")
	}
	if maxValue <= minValue {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.distance_range.max_distance must be greater than min_distance")
	}
	return &pb.DistanceRange{
		MinDistance: minValue,
		MaxDistance: maxValue,
	}, nil
}

func similarityToL2Distance(value float32) float32 {
	return float32(math.Sqrt(math.Max(0, 2*(1-float64(value)))))
}

func vectorQueryMode(cfg *pb.VectorSearchDimension) string {
	if !isRangeQuery(cfg) {
		return "knn"
	}
	if cfg.GetDistanceRange().GetMinDistance() == 0 {
		return "range_ball"
	}
	return "range_ring"
}

func appendVectorQueryBenchmarkFields(fields map[string]any, cfg *pb.VectorSearchDimension) map[string]any {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["query_mode"] = vectorQueryMode(cfg)
	if isRangeQuery(cfg) {
		fields["range_min"] = cfg.GetDistanceRange().GetMinDistance()
		fields["range_max"] = cfg.GetDistanceRange().GetMaxDistance()
	}
	return fields
}

func cloneVectorSearchConfig(cfg *pb.VectorSearchDimension) *pb.VectorSearchDimension {
	if cfg == nil {
		return nil
	}
	cloned, ok := proto.Clone(cfg).(*pb.VectorSearchDimension)
	if !ok {
		return nil
	}
	return cloned
}

func vectorDistanceRange(cfg *pb.VectorSearchDimension) (float64, float64, bool) {
	if !isRangeQuery(cfg) {
		return 0, 0, false
	}
	return float64(cfg.GetDistanceRange().GetMinDistance()), float64(cfg.GetDistanceRange().GetMaxDistance()), true
}

func isEmptyRangeSearchError(err error) bool {
	return status.Code(err) == codes.NotFound
}

func emptySearchResult(metric string, kind SearchKind) searchResult {
	return searchResult{
		DistanceMetric: metric,
		Kind:           kind,
	}
}
