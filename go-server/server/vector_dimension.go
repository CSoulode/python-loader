package main

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

type vectorDimensionResult struct {
	AxisType          pb.AxisType
	ParsedAxis        qg.ParsedAxis
	AxisSQL           string
	BucketInfos       []*pb.BucketInfo
	ObjectIDsByBucket map[int][]int
}

func validateVectorDimensionConfig(cfg *pb.VectorSearchDimension) error {
	if cfg == nil {
		return nil
	}
	if strings.TrimSpace(cfg.GetModelName()) == "" {
		return status.Error(codes.InvalidArgument, "vector_dimension.model_name is required")
	}
	if cfg.GetReference() == nil {
		return status.Error(codes.InvalidArgument, "vector_dimension.reference is required")
	}
	if err := validateVectorReference(cfg.GetReference(), "vector_dimension.reference"); err != nil {
		return err
	}
	if isRangeQuery(cfg) {
		if cfg.GetMaxResults() < 0 {
			return status.Error(codes.InvalidArgument, "vector_dimension.max_results must be >= 0 in range mode")
		}
	} else if cfg.GetMaxResults() <= 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.max_results must be > 0")
	}
	if err := validateVectorBucketConfig(cfg.GetBucketCfg()); err != nil {
		return err
	}
	if isRangeQuery(cfg) && cfg.GetBucketCfg().GetStrategy() == pb.BucketStrategy_CUSTOM {
		if err := validateCustomBreaks(
			cfg.GetBucketCfg().GetCustomBreaks(),
			cfg.GetDistanceRange().GetMinDistance(),
			cfg.GetDistanceRange().GetMaxDistance(),
		); err != nil {
			return err
		}
	}

	switch cfg.GetAxis() {
	case pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS, pb.AxisType_Z_AXIS:
		return nil
	default:
		return status.Error(codes.InvalidArgument, "vector_dimension.axis must be X_AXIS, Y_AXIS, or Z_AXIS")
	}
}

func validateVectorBucketConfig(cfg *pb.BucketConfig) error {
	if cfg == nil {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg is required")
	}
	if cfg.GetDistMin() < 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_min must be >= 0")
	}
	if cfg.GetDistMax() < 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_max must be >= 0")
	}
	if cfg.GetDistMax() > 0 && cfg.GetDistMax() <= cfg.GetDistMin() {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_max must be greater than dist_min")
	}

	switch cfg.GetStrategy() {
	case pb.BucketStrategy_EQUAL_WIDTH, pb.BucketStrategy_EQUAL_DEPTH, pb.BucketStrategy_LOGARITHMIC:
		if cfg.GetCount() <= 0 {
			return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.count must be > 0")
		}
		if len(cfg.GetCustomBreaks()) > 0 {
			return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks is only valid with CUSTOM strategy")
		}
	case pb.BucketStrategy_CUSTOM:
		if err := validateCustomBreaks(cfg.GetCustomBreaks(), cfg.GetDistMin(), cfg.GetDistMax()); err != nil {
			return err
		}
	default:
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.strategy is invalid")
	}
	return nil
}

func validateCustomBreaks(breaks []float32, distMin float32, distMax float32) error {
	if len(breaks) < 2 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks must contain at least 2 values")
	}
	if distMin > 0 && breaks[0] < distMin {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks must start at or above dist_min")
	}
	if distMax > 0 && breaks[len(breaks)-1] > distMax {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks must end at or below dist_max")
	}

	prev := breaks[0]
	for _, current := range breaks[1:] {
		if current <= prev {
			return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks must be strictly increasing")
		}
		prev = current
	}
	return nil
}

func (s *DataLoaderServer) listVectorModels(ctx context.Context) (*kvstorev1.ListModelsResponse, error) {
	if s == nil || s.vectorFilters == nil || s.vectorFilters.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "vector filter resolver is not configured")
	}

	resp, err := s.vectorFilters.client.ListModels(ctx, &kvstorev1.ListModelsRequest{})
	if err != nil {
		return nil, wrapVectorKVError("vector_dimension models", err)
	}
	return resp, nil
}

func (s *DataLoaderServer) resolveModelInfo(ctx context.Context, modelName string) (*kvstorev1.ModelInfo, error) {
	resp, err := s.listVectorModels(ctx)
	if err != nil {
		return nil, err
	}

	want := strings.TrimSpace(modelName)
	for _, model := range resp.GetModels() {
		if strings.TrimSpace(model.GetName()) == want {
			return model, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "vector_dimension model %q not found", want)
}

func (s *DataLoaderServer) handleVectorDimension(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*vectorDimensionResult, error) {
	if err := validateVectorDimensionConfig(cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}

	inputs, err := s.resolveVectorSearchInputs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	result, err := s.searchNeighbors(ctx, cfg, inputs)
	if err != nil {
		return nil, err
	}
	return bucketSearchResult(inputs.Config, result)
}

func (s *DataLoaderServer) resolveVectorDimensionWithCache(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	rebucketOnly bool,
) (*vectorDimensionResult, error) {
	return s.resolveVectorDimensionForMetadata(ctx, cfg, nil, nil, rebucketOnly, Auto)
}

func (s *DataLoaderServer) searchNeighbors(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	inputs *vectorSearchInputs,
) (searchResult, error) {
	if inputs == nil || inputs.ModelInfo == nil {
		return searchResult{}, status.Error(codes.FailedPrecondition, "vector search inputs are not configured")
	}
	effectiveCfg := cfg
	if inputs.Config != nil {
		effectiveCfg = inputs.Config
	}
	maxResults := effectiveVectorMaxResults(effectiveCfg)

	start := time.Now()
	if isRangeQuery(effectiveCfg) {
		resp, err := s.vectorFilters.client.RangeSearch(ctx, &kvstorev1.RangeSearchRequest{
			Query:       &kvstorev1.Vector{Values: inputs.QueryVector},
			Model:       strings.TrimSpace(effectiveCfg.GetModelName()),
			MinDistance: effectiveCfg.GetDistanceRange().GetMinDistance(),
			MaxDistance: effectiveCfg.GetDistanceRange().GetMaxDistance(),
			MaxResults:  maxResults,
		})
		if err != nil {
			if isEmptyRangeSearchError(err) {
				result := emptySearchResult(inputs.ModelInfo.GetDistanceMetric(), searchKindGlobalRange)
				logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
					"model_name":       inputs.ModelInfo.GetName(),
					"search_kind":      result.Kind.String(),
					"vector_search_ms": durationMillis(start),
					"result_count":     0,
					"candidate_count":  0,
					"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
					"requested_k":      maxResults,
				}, effectiveCfg))
				return result, nil
			}
			return searchResult{}, wrapVectorKVError("vector_dimension range search", err)
		}

		result := searchResult{
			RawNeighbors:   neighborsFromProto(resp.GetNeighbors()),
			DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
			Kind:           searchKindGlobalRange,
		}
		logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
			"model_name":       inputs.ModelInfo.GetName(),
			"search_kind":      result.Kind.String(),
			"vector_search_ms": durationMillis(start),
			"result_count":     len(result.RawNeighbors),
			"candidate_count":  0,
			"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
			"requested_k":      maxResults,
		}, effectiveCfg))
		return result, nil
	}

	resp, err := s.vectorFilters.client.KNN(ctx, &kvstorev1.KNNRequest{
		Query: &kvstorev1.Vector{Values: inputs.QueryVector},
		K:     maxResults,
		Model: strings.TrimSpace(effectiveCfg.GetModelName()),
	})
	if err != nil {
		return searchResult{}, wrapVectorKVError("vector_dimension search", err)
	}

	result := searchResult{
		RawNeighbors:   neighborsFromProto(resp.GetNeighbors()),
		DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
		Kind:           searchKindGlobalKNN,
	}
	logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
		"model_name":       inputs.ModelInfo.GetName(),
		"search_kind":      result.Kind.String(),
		"vector_search_ms": durationMillis(start),
		"result_count":     len(result.RawNeighbors),
		"candidate_count":  0,
		"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
		"requested_k":      maxResults,
	}, effectiveCfg))
	return result, nil
}

func bucketSearchResult(cfg *pb.VectorSearchDimension, result searchResult) (*vectorDimensionResult, error) {
	localCfg := bucketConfigFromProto(cfg.GetBucketCfg())
	if minDistance, maxDistance, ok := vectorDistanceRange(cfg); ok {
		localCfg.DistanceMin = minDistance
		localCfg.DistanceMax = maxDistance
	} else {
		localCfg = applyMetricDefaults(localCfg, result.DistanceMetric, result.RawNeighbors)
	}
	filtered := filterNeighborsByRange(result.RawNeighbors, localCfg.DistanceMin, localCfg.DistanceMax, effectiveVectorMaxResults(cfg))
	boundaries, err := computeBucketBoundaries(localCfg, filtered)
	if err != nil {
		return nil, err
	}

	buckets := buildVectorBuckets(boundaries)
	bucketInfos := buildBucketInfos(buckets, result.DistanceMetric)
	bucketedResults, idsByBucket := assignBuckets(filtered, boundaries)
	return &vectorDimensionResult{
		AxisType: cfg.GetAxis(),
		ParsedAxis: qg.ParsedAxis{
			Type: "vector",
			Id:   unsetAxisId,
			Ids:  buildVectorAxisIDs(buckets),
		},
		AxisSQL:           buildVectorAxisSQL(bucketedResults),
		BucketInfos:       bucketInfos,
		ObjectIDsByBucket: idsByBucket,
	}, nil
}

func neighborsFromProto(neighbors []*kvstorev1.Neighbor) []Neighbor {
	if len(neighbors) == 0 {
		return nil
	}

	out := make([]Neighbor, 0, len(neighbors))
	for _, neighbor := range neighbors {
		if neighbor == nil {
			continue
		}
		out = append(out, Neighbor{
			ObjectID: neighbor.GetId(),
			Distance: float64(neighbor.GetDistance()),
		})
	}
	return out
}
