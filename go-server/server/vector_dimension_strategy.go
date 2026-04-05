package main

import (
	"context"
	"database/sql"
	"fmt"
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
	modelName := strings.TrimSpace(cfg.GetModelName())
	if rebucketOnly {
		result, ok := cache.TryGet(modelName, refHash, filterHash, cfg.GetMaxResults())
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "rebucket_only: no cached search results for this model+reference+filters; re-send without rebucket_only to trigger a new search")
		}
		logBenchmarkEvent(ctx, "vector_cache_hit", map[string]any{
			"model_name":  modelName,
			"filter_hash": filterHash,
			"ref_hash":    refHash,
			"req_k":       cfg.GetMaxResults(),
		})
		bucketStart := time.Now()
		bucketed, err := bucketSearchResult(cfg, result)
		if err != nil {
			return nil, err
		}
		logBenchmarkEvent(ctx, "bucketing_done", map[string]any{
			"model_name":    modelName,
			"bucket_count":  len(bucketed.BucketInfos),
			"result_count":  len(result.RawNeighbors),
			"bucketing_ms":  durationMillis(bucketStart),
			"search_kind":   result.Kind.String(),
			"cache_hit":     true,
			"rebucket_only": true,
		})
		return bucketed, nil
	}
	logBenchmarkEvent(ctx, "vector_cache_miss", map[string]any{
		"model_name":  modelName,
		"filter_hash": filterHash,
		"ref_hash":    refHash,
		"req_k":       cfg.GetMaxResults(),
	})

	inputs, err := s.resolveVectorSearchInputs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	result, err := s.executeVectorSearchWithStrategy(
		ctx,
		cfg,
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
	cache.Put(modelName, refHash, filterHash, result, cfg.GetMaxResults())
	bucketStart := time.Now()
	bucketed, err := bucketSearchResult(cfg, result)
	if err != nil {
		return nil, err
	}
	logBenchmarkEvent(ctx, "bucketing_done", map[string]any{
		"model_name":    modelName,
		"bucket_count":  len(bucketed.BucketInfos),
		"result_count":  len(result.RawNeighbors),
		"bucketing_ms":  durationMillis(bucketStart),
		"search_kind":   result.Kind.String(),
		"cache_hit":     false,
		"rebucket_only": false,
	})
	logBenchmarkEvent(ctx, "vector_cache_put", map[string]any{
		"model_name":   modelName,
		"filter_hash":  filterHash,
		"ref_hash":     refHash,
		"req_k":        cfg.GetMaxResults(),
		"search_kind":  result.Kind.String(),
		"result_count": len(result.RawNeighbors),
	})
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
		inputs.ModelInfo,
		metadataFilters,
		metadataAxes,
		cfg.GetMaxResults(),
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
	modelInfo *kvstorev1.ModelInfo,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	vectorK int32,
	forcedStrategy HybridStrategy,
) (HybridStrategy, error) {
	if !hasMetadataPredicates(metadataFilters, metadataAxes) {
		logBenchmarkEvent(ctx, "strategy_selected", map[string]any{
			"strategy":                 PostFilter.String(),
			"forced_strategy":          forcedStrategy.String(),
			"has_metadata_predicates":  false,
			"iterative_scan_available": false,
			"vector_k":                 vectorK,
		})
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
		vectorK,
		modelInfo.GetIterativeScanAvailable(),
		forcedStrategy,
	)
	logBenchmarkEvent(ctx, "strategy_selected", map[string]any{
		"strategy":                 strategy.String(),
		"forced_strategy":          forcedStrategy.String(),
		"has_metadata_predicates":  true,
		"iterative_scan_available": modelInfo.GetIterativeScanAvailable(),
		"estimated_filtered_count": estimatedFilteredCount,
		"total_media_count":        totalMediaCount,
		"vector_k":                 vectorK,
		"model_name":               modelInfo.GetName(),
		"ann_index":                modelInfo.GetAnnIndex(),
	})
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
	if len(candidateIDs) == 0 {
		logBenchmarkEvent(ctx, "vector_search_done", map[string]any{
			"model_name":       inputs.ModelInfo.GetName(),
			"search_kind":      searchKindFilteredKNN.String(),
			"vector_search_ms": 0.0,
			"result_count":     0,
			"candidate_count":  0,
			"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
			"requested_k":      cfg.GetMaxResults(),
		})
		return searchResult{
			DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
			Kind:           searchKindFilteredKNN,
		}, nil
	}

	start := time.Now()
	resp, err := s.vectorFilters.client.FilteredKNN(ctx, &kvstorev1.FilteredKNNRequest{
		Query:        &kvstorev1.Vector{Values: inputs.QueryVector},
		K:            cfg.GetMaxResults(),
		Model:        strings.TrimSpace(cfg.GetModelName()),
		CandidateIds: candidateIDs,
	})
	if status.Code(err) == codes.NotFound {
		logBenchmarkEvent(ctx, "vector_search_done", map[string]any{
			"model_name":       inputs.ModelInfo.GetName(),
			"search_kind":      searchKindFilteredKNN.String(),
			"vector_search_ms": durationMillis(start),
			"result_count":     0,
			"candidate_count":  len(candidateIDs),
			"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
			"requested_k":      cfg.GetMaxResults(),
		})
		return searchResult{
			DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
			Kind:           searchKindFilteredKNN,
		}, nil
	}
	if err != nil {
		return searchResult{}, wrapVectorKVError("vector_dimension filtered search", err)
	}
	result := searchResult{
		RawNeighbors:   neighborsFromProto(resp.GetNeighbors()),
		DistanceMetric: inputs.ModelInfo.GetDistanceMetric(),
		Kind:           searchKindFilteredKNN,
	}
	logBenchmarkEvent(ctx, "vector_search_done", map[string]any{
		"model_name":       inputs.ModelInfo.GetName(),
		"search_kind":      result.Kind.String(),
		"vector_search_ms": durationMillis(start),
		"result_count":     len(result.RawNeighbors),
		"candidate_count":  len(candidateIDs),
		"distance_metric":  inputs.ModelInfo.GetDistanceMetric(),
		"requested_k":      cfg.GetMaxResults(),
	})
	return result, nil
}

func (s *DataLoaderServer) resolveVectorSearchInputs(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*vectorSearchInputs, error) {
	modelInfo, err := s.resolveModelInfo(ctx, cfg.GetModelName())
	if err != nil {
		return nil, err
	}
	queryVector, err := s.vectorFilters.resolveQueryVector(ctx, &pb.VectorFilterConfig{
		ModelName: cfg.GetModelName(),
		Reference: cfg.GetReference(),
		K:         cfg.GetMaxResults(),
	})
	if err != nil {
		return nil, err
	}
	return &vectorSearchInputs{
		ModelInfo:   modelInfo,
		QueryVector: queryVector,
	}, nil
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
