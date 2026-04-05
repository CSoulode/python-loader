package main

import (
	"context"
	"strings"

	"golang.org/x/sync/errgroup"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func (s *DataLoaderServer) executeHybridSearch(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	inputs *vectorSearchInputs,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	refHash uint64,
	filterHash uint64,
) (searchResult, error) {
	group, groupCtx := errgroup.WithContext(ctx)

	var (
		vectorResult searchResult
		candidateIDs []int32
	)

	group.Go(func() error {
		result, err := s.globalVectorSearchForHybrid(groupCtx, cfg, inputs, refHash, filterHash)
		if err != nil {
			return err
		}
		vectorResult = result
		return nil
	})

	group.Go(func() error {
		ids, err := s.executeMetadataFilter(groupCtx, metadataFilters, metadataAxes)
		if err != nil {
			return err
		}
		candidateIDs = ids
		return nil
	})

	if err := group.Wait(); err != nil {
		return searchResult{}, err
	}
	result := intersectHybridNeighbors(vectorResult, candidateIDs)
	logBenchmarkEvent(ctx, "hybrid_intersection_done", map[string]any{
		"model_name":      strings.TrimSpace(cfg.GetModelName()),
		"candidate_count": len(candidateIDs),
		"input_count":     len(vectorResult.RawNeighbors),
		"result_count":    len(result.RawNeighbors),
		"distance_metric": result.DistanceMetric,
	})
	return result, nil
}

func (s *DataLoaderServer) globalVectorSearchForHybrid(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	inputs *vectorSearchInputs,
	refHash uint64,
	filterHash uint64,
) (searchResult, error) {
	cache := s.ensureVectorCache()
	modelName := strings.TrimSpace(cfg.GetModelName())
	if result, ok := cache.TryGetGlobalKNN(modelName, refHash, filterHash, cfg.GetMaxResults()); ok {
		logBenchmarkEvent(ctx, "vector_cache_hit", map[string]any{
			"model_name":  modelName,
			"filter_hash": filterHash,
			"ref_hash":    refHash,
			"req_k":       cfg.GetMaxResults(),
			"search_kind": searchKindGlobalKNN.String(),
		})
		return result, nil
	}
	return s.searchNeighbors(ctx, cfg, inputs)
}

func intersectHybridNeighbors(result searchResult, candidateIDs []int32) searchResult {
	intersection := searchResult{
		DistanceMetric: result.DistanceMetric,
		Kind:           searchKindHybridIntersection,
	}
	if len(result.RawNeighbors) == 0 || len(candidateIDs) == 0 {
		return intersection
	}

	candidateSet := make(map[int32]struct{}, len(candidateIDs))
	for _, candidateID := range candidateIDs {
		candidateSet[candidateID] = struct{}{}
	}

	neighbors := make([]Neighbor, 0, len(result.RawNeighbors))
	for _, neighbor := range result.RawNeighbors {
		if _, ok := candidateSet[neighbor.ObjectID]; !ok {
			continue
		}
		neighbors = append(neighbors, neighbor)
	}
	intersection.RawNeighbors = neighbors
	return intersection
}
