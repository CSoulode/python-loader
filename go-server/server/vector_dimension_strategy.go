package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

func (s *DataLoaderServer) resolveVectorDimensionForMetadata(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) (*vectorDimensionResult, error) {
	if err := validateVectorDimensionConfig(cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}

	refHash, err := hashVectorReference(cfg.GetReference())
	if err != nil {
		return nil, err
	}
	filterHash, err := computeFilterHash(metadataFilters, metadataAxes)
	if err != nil {
		return nil, err
	}

	cache := s.ensureVectorCache()
	if rebucketOnly {
		effectiveCfg, err := s.resolveVectorCacheLookupConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
		modelName := strings.TrimSpace(effectiveCfg.GetModelName())
		result, ok := cache.TryGetForConfig(effectiveCfg, refHash, filterHash)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "rebucket_only: no cached search results for this model+reference+filters; re-send without rebucket_only to trigger a new search")
		}
		logBenchmarkEvent(ctx, "vector_cache_hit", appendVectorQueryBenchmarkFields(map[string]any{
			"model_name":  modelName,
			"filter_hash": filterHash,
			"ref_hash":    refHash,
			"req_k":       effectiveVectorMaxResults(effectiveCfg),
		}, effectiveCfg))
		bucketStart := time.Now()
		bucketed, err := bucketSearchResult(effectiveCfg, result)
		if err != nil {
			return nil, err
		}
		logBenchmarkEvent(ctx, "bucketing_done", appendVectorQueryBenchmarkFields(map[string]any{
			"model_name":    modelName,
			"bucket_count":  len(bucketed.BucketInfos),
			"result_count":  len(result.RawNeighbors),
			"bucketing_ms":  durationMillis(bucketStart),
			"search_kind":   result.Kind.String(),
			"cache_hit":     true,
			"rebucket_only": true,
		}, effectiveCfg))
		return bucketed, nil
	}

	inputs, err := s.resolveVectorSearchInputs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	effectiveCfg := cfg
	if inputs.Config != nil {
		effectiveCfg = inputs.Config
	}
	modelName := strings.TrimSpace(effectiveCfg.GetModelName())
	logBenchmarkEvent(ctx, "vector_cache_miss", appendVectorQueryBenchmarkFields(map[string]any{
		"model_name":  modelName,
		"filter_hash": filterHash,
		"ref_hash":    refHash,
		"req_k":       effectiveVectorMaxResults(effectiveCfg),
	}, effectiveCfg))
	result, err := s.executeVectorSearchWithStrategy(
		ctx,
		effectiveCfg,
		inputs,
		metadataFilters,
		metadataAxes,
		refHash,
		filterHash,
		forcedStrategy,
	)
	if err != nil {
		return nil, err
	}
	cache.PutForConfig(effectiveCfg, refHash, filterHash, result)
	bucketStart := time.Now()
	bucketed, err := bucketSearchResult(effectiveCfg, result)
	if err != nil {
		return nil, err
	}
	logBenchmarkEvent(ctx, "bucketing_done", appendVectorQueryBenchmarkFields(map[string]any{
		"model_name":    modelName,
		"bucket_count":  len(bucketed.BucketInfos),
		"result_count":  len(result.RawNeighbors),
		"bucketing_ms":  durationMillis(bucketStart),
		"search_kind":   result.Kind.String(),
		"cache_hit":     false,
		"rebucket_only": false,
	}, effectiveCfg))
	logBenchmarkEvent(ctx, "vector_cache_put", appendVectorQueryBenchmarkFields(map[string]any{
		"model_name":   modelName,
		"filter_hash":  filterHash,
		"ref_hash":     refHash,
		"req_k":        effectiveVectorMaxResults(effectiveCfg),
		"search_kind":  result.Kind.String(),
		"result_count": len(result.RawNeighbors),
	}, effectiveCfg))
	return bucketed, nil
}

func (s *DataLoaderServer) executeVectorSearchWithStrategy(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	inputs *vectorSearchInputs,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	refHash uint64,
	filterHash uint64,
	forcedStrategy HybridStrategy,
) (searchResult, error) {
	strategy, err := s.determineVectorStrategy(
		ctx,
		cfg,
		inputs.ModelInfo,
		metadataFilters,
		metadataAxes,
		forcedStrategy,
	)
	if err != nil {
		return searchResult{}, err
	}

	switch strategy {
	case PreFilter:
		candidateIDs, err := s.executeMetadataFilter(ctx, metadataFilters, metadataAxes)
		if err != nil {
			return searchResult{}, err
		}
		return s.searchNeighborsInCandidates(ctx, cfg, inputs, candidateIDs)
	case Hybrid:
		return s.executeHybridSearch(ctx, cfg, inputs, metadataFilters, metadataAxes, refHash, filterHash)
	default:
		return s.searchNeighbors(ctx, cfg, inputs)
	}
}

func (s *DataLoaderServer) determineVectorStrategy(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	modelInfo *kvstorev1.ModelInfo,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	forcedStrategy HybridStrategy,
) (HybridStrategy, error) {
	if !hasMetadataPredicates(metadataFilters, metadataAxes) {
		logBenchmarkEvent(ctx, "strategy_selected", appendVectorQueryBenchmarkFields(map[string]any{
			"strategy":                 PostFilter.String(),
			"forced_strategy":          forcedStrategy.String(),
			"has_metadata_predicates":  false,
			"iterative_scan_available": false,
			"vector_k":                 effectiveVectorMaxResults(cfg),
		}, cfg))
		return PostFilter, nil
	}
	if modelInfo == nil {
		return PostFilter, status.Error(codes.FailedPrecondition, "vector_dimension model info is not configured")
	}

	estimator := newSelectivityEstimator(s.db)
	totalMediaCount, err := estimator.DomainSize(ctx)
	if err != nil {
		return PostFilter, fmt.Errorf("selectivity domainSize: %w", err)
	}
	estimatedFilteredCount, err := estimator.estimateFilteredCountWithDomain(ctx, metadataFilters, metadataAxes, totalMediaCount)
	if err != nil {
		return PostFilter, fmt.Errorf("selectivity EstimateFilteredCount: %w", err)
	}
	strategy := chooseStrategy(
		estimatedFilteredCount,
		totalMediaCount,
		true,
		cfg,
		modelInfo.GetIterativeScanAvailable(),
		forcedStrategy,
	)
	logBenchmarkEvent(ctx, "strategy_selected", appendVectorQueryBenchmarkFields(map[string]any{
		"strategy":                 strategy.String(),
		"forced_strategy":          forcedStrategy.String(),
		"has_metadata_predicates":  true,
		"iterative_scan_available": modelInfo.GetIterativeScanAvailable(),
		"estimated_filtered_count": estimatedFilteredCount,
		"total_media_count":        totalMediaCount,
		"vector_k":                 effectiveVectorMaxResults(cfg),
		"model_name":               modelInfo.GetName(),
		"ann_index":                modelInfo.GetAnnIndex(),
	}, cfg))
	return strategy, nil
}

func (s *DataLoaderServer) executeMetadataFilter(
	ctx context.Context,
	filters []qg.ParsedFilter,
	axes []qg.ParsedAxis,
) ([]int32, error) {
	sqlStr, err := qg.GenerateCandidateObjectIDs(filters, axes)
	if err != nil {
		return nil, fmt.Errorf("candidate extraction sql: %w", err)
	}
	if strings.TrimSpace(sqlStr) == "" {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("candidate extraction: %w", err)
	}
	defer rows.Close()

	ids := make([]int32, 0)
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i int, j int) bool {
		return ids[i] < ids[j]
	})
	logBenchmarkEvent(ctx, "metadata_filter_done", map[string]any{
		"candidate_count": len(ids),
		"filter_count":    len(filters),
		"axis_count":      len(axes),
	})
	return ids, nil
}

func (s *DataLoaderServer) searchNeighborsInCandidates(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	inputs *vectorSearchInputs,
	candidateIDs []int32,
) (searchResult, error) {
	if inputs == nil || inputs.ModelInfo == nil {
		return searchResult{}, status.Error(codes.FailedPrecondition, "vector search inputs are not configured")
	}
	effectiveCfg := cfg
	if inputs.Config != nil {
		effectiveCfg = inputs.Config
	}
	maxResults := effectiveVectorMaxResults(effectiveCfg)
	searchKind := searchKindFilteredKNN
	if isRangeQuery(effectiveCfg) {
		searchKind = searchKindFilteredRange
	}
	if len(candidateIDs) == 0 {
		logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
			"model_name":       inputs.ModelInfo.GetName(),
			"search_kind":      searchKind.String(),
			"vector_search_ms": 0.0,
			"result_count":     0,
			"candidate_count":  0,
			"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
			"requested_k":      maxResults,
		}, effectiveCfg))
		return searchResult{
			DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
			Kind:           searchKind,
		}, nil
	}

	start := time.Now()
	var (
		resp *kvstorev1.KNNResponse
		err  error
	)
	if isRangeQuery(effectiveCfg) {
		resp, err = s.vectorFilters.client.FilteredRangeSearch(ctx, &kvstorev1.FilteredRangeSearchRequest{
			Query:        &kvstorev1.Vector{Values: inputs.QueryVector},
			Model:        strings.TrimSpace(effectiveCfg.GetModelName()),
			CandidateIds: candidateIDs,
			MinDistance:  effectiveCfg.GetDistanceRange().GetMinDistance(),
			MaxDistance:  effectiveCfg.GetDistanceRange().GetMaxDistance(),
			MaxResults:   maxResults,
		})
	} else {
		resp, err = s.vectorFilters.client.FilteredKNN(ctx, &kvstorev1.FilteredKNNRequest{
			Query:        &kvstorev1.Vector{Values: inputs.QueryVector},
			K:            maxResults,
			Model:        strings.TrimSpace(effectiveCfg.GetModelName()),
			CandidateIds: candidateIDs,
		})
	}
	if status.Code(err) == codes.NotFound {
		logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
			"model_name":       inputs.ModelInfo.GetName(),
			"search_kind":      searchKind.String(),
			"vector_search_ms": durationMillis(start),
			"result_count":     0,
			"candidate_count":  len(candidateIDs),
			"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
			"requested_k":      maxResults,
		}, effectiveCfg))
		return searchResult{
			DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
			Kind:           searchKind,
		}, nil
	}
	if err != nil {
		return searchResult{}, wrapVectorKVError("vector_dimension filtered search", err)
	}
	result := searchResult{
		RawNeighbors:   neighborsFromProto(resp.GetNeighbors()),
		DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
		Kind:           searchKind,
	}
	logBenchmarkEvent(ctx, "vector_search_done", appendVectorQueryBenchmarkFields(map[string]any{
		"model_name":       inputs.ModelInfo.GetName(),
		"search_kind":      result.Kind.String(),
		"vector_search_ms": durationMillis(start),
		"result_count":     len(result.RawNeighbors),
		"candidate_count":  len(candidateIDs),
		"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
		"requested_k":      maxResults,
	}, effectiveCfg))
	return result, nil
}

func (s *DataLoaderServer) resolveVectorSearchInputs(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*vectorSearchInputs, error) {
	normalizedCfg, modelInfo, err := s.resolveNormalizedVectorSearchConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	queryVector, err := s.vectorFilters.resolveQueryVector(ctx, &pb.VectorFilterConfig{
		ModelName: normalizedCfg.GetModelName(),
		Reference: normalizedCfg.GetReference(),
		K:         effectiveVectorMaxResults(normalizedCfg),
	})
	if err != nil {
		return nil, err
	}
	return &vectorSearchInputs{
		Config:      normalizedCfg,
		ModelInfo:   modelInfo,
		QueryVector: queryVector,
	}, nil
}

func (s *DataLoaderServer) resolveNormalizedVectorSearchConfig(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*pb.VectorSearchDimension, *kvstorev1.ModelInfo, error) {
	modelInfo, err := s.resolveModelInfo(ctx, cfg.GetModelName())
	if err != nil {
		return nil, nil, err
	}
	normalizedCfg, err := normalizeVectorSearchConfig(cfg, modelInfo.GetDistanceMetric())
	if err != nil {
		return nil, nil, err
	}
	return normalizedCfg, modelInfo, nil
}

func (s *DataLoaderServer) resolveVectorCacheLookupConfig(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*pb.VectorSearchDimension, error) {
	if cfg == nil {
		return nil, nil
	}

	if !isRangeQuery(cfg) || cfg.GetRangeSemantics() == pb.RangeSemantics_DISTANCE {
		cloned := cloneVectorSearchConfig(cfg)
		cloned.MaxResults = effectiveVectorMaxResults(cfg)
		return cloned, nil
	}

	normalizedCfg, _, err := s.resolveNormalizedVectorSearchConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return normalizedCfg, nil
}

func metadataAxesFromPlan(plan *browsingStateRequestPlan) []qg.ParsedAxis {
	if plan == nil {
		return nil
	}

	axes := make([]qg.ParsedAxis, 0, 3)
	for _, axis := range []qg.ParsedAxis{plan.AxisX, plan.AxisY, plan.AxisZ} {
		if strings.TrimSpace(axis.Type) == "" {
			continue
		}
		axes = append(axes, axis)
	}
	return axes
}

func hasMetadataPredicates(filters []qg.ParsedFilter, axes []qg.ParsedAxis) bool {
	if len(filters) > 0 {
		return true
	}
	for _, axis := range axes {
		if strings.TrimSpace(axis.Type) != "" {
			return true
		}
	}
	return false
}
