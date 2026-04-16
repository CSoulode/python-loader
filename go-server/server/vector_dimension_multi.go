package main

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

type preparedVectorDimension struct {
	Config     *pb.VectorSearchDimension
	Inputs     *vectorSearchInputs
	RefHash    uint64
	FilterHash uint64
	Strategy   HybridStrategy
}

func (s *DataLoaderServer) resolveMultiVectorDimensions(
	ctx context.Context,
	dims []*pb.VectorSearchDimension,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) ([]*vectorDimensionResult, error) {
	if len(dims) == 0 {
		return nil, nil
	}

	filterHash, err := computeFilterHash(metadataFilters, metadataAxes)
	if err != nil {
		return nil, err
	}
	if rebucketOnly {
		return s.resolveMultiVectorDimensionsFromCache(ctx, dims, filterHash)
	}

	prepared, err := s.prepareMultiVectorDimensions(
		ctx,
		dims,
		metadataFilters,
		metadataAxes,
		filterHash,
		forcedStrategy,
	)
	if err != nil {
		return nil, err
	}

	candidateIDs, err := s.resolveSharedMetadataCandidates(ctx, prepared, metadataFilters, metadataAxes)
	if err != nil {
		return nil, err
	}

	results := make([]*vectorDimensionResult, len(prepared))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, preparedDim := range prepared {
		index := index
		preparedDim := preparedDim
		group.Go(func() error {
			bucketed, err := s.resolvePreparedVectorDimension(groupCtx, preparedDim, candidateIDs)
			if err != nil {
				return formatMultiVectorError(index, preparedDim.Config, err)
			}
			results[index] = bucketed
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *DataLoaderServer) resolveMultiVectorDimensionsFromCache(
	ctx context.Context,
	dims []*pb.VectorSearchDimension,
	filterHash uint64,
) ([]*vectorDimensionResult, error) {
	cache := s.ensureVectorCache()
	results := make([]*vectorDimensionResult, len(dims))

	group, _ := errgroup.WithContext(ctx)
	for index, dim := range dims {
		index := index
		dim := dim
		group.Go(func() error {
			refHash, err := hashVectorReference(dim.GetReference())
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			normalizedCfg, err := s.resolveVectorCacheLookupConfig(ctx, dim)
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			result, ok := cache.TryGetForConfig(normalizedCfg, refHash, filterHash)
			if !ok {
				return formatMultiVectorError(
					index,
					dim,
					statusRebucketOnlyCacheMiss(),
				)
			}
			bucketed, err := bucketSearchResult(normalizedCfg, result)
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			results[index] = bucketed
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *DataLoaderServer) prepareMultiVectorDimensions(
	ctx context.Context,
	dims []*pb.VectorSearchDimension,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
	filterHash uint64,
	forcedStrategy HybridStrategy,
) ([]preparedVectorDimension, error) {
	prepared := make([]preparedVectorDimension, len(dims))
	group, groupCtx := errgroup.WithContext(ctx)

	for index, dim := range dims {
		index := index
		dim := dim
		group.Go(func() error {
			refHash, err := hashVectorReference(dim.GetReference())
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			inputs, err := s.resolveVectorSearchInputs(groupCtx, dim)
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			strategy, err := s.determineVectorStrategy(
				groupCtx,
				inputs.Config,
				inputs.ModelInfo,
				metadataFilters,
				metadataAxes,
				forcedStrategy,
			)
			if err != nil {
				return formatMultiVectorError(index, dim, err)
			}
			prepared[index] = preparedVectorDimension{
				Config:     inputs.Config,
				Inputs:     inputs,
				RefHash:    refHash,
				FilterHash: filterHash,
				Strategy:   strategy,
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *DataLoaderServer) resolveSharedMetadataCandidates(
	ctx context.Context,
	prepared []preparedVectorDimension,
	metadataFilters []qg.ParsedFilter,
	metadataAxes []qg.ParsedAxis,
) ([]int32, error) {
	if !requiresSharedMetadataCandidates(prepared) {
		return nil, nil
	}
	return s.executeMetadataFilter(ctx, metadataFilters, metadataAxes)
}

func requiresSharedMetadataCandidates(prepared []preparedVectorDimension) bool {
	for _, preparedDim := range prepared {
		if preparedDim.Strategy == PreFilter || preparedDim.Strategy == Hybrid {
			return true
		}
	}
	return false
}

func (s *DataLoaderServer) executePreparedVectorSearch(
	ctx context.Context,
	prepared preparedVectorDimension,
	candidateIDs []int32,
) (searchResult, error) {
	switch prepared.Strategy {
	case PreFilter:
		return s.searchNeighborsInCandidates(ctx, prepared.Config, prepared.Inputs, candidateIDs)
	case Hybrid:
		return s.executeHybridSearchWithCandidates(
			ctx,
			prepared.Config,
			prepared.Inputs,
			candidateIDs,
			prepared.RefHash,
			prepared.FilterHash,
		)
	default:
		return s.searchNeighbors(ctx, prepared.Config, prepared.Inputs)
	}
}

func formatMultiVectorError(index int, dim *pb.VectorSearchDimension, err error) error {
	if err == nil {
		return nil
	}
	if dim == nil {
		return fmt.Errorf("vector_dimensions[%d]: %w", index, err)
	}
	return fmt.Errorf(
		"vector_dimensions[%d] (axis=%s, model=%s): %w",
		index,
		axisToken(dim.GetAxis()),
		strings.TrimSpace(dim.GetModelName()),
		err,
	)
}

func statusRebucketOnlyCacheMiss() error {
	return status.Error(
		codes.FailedPrecondition,
		"rebucket_only: no cached search results for this model+reference+filters; re-send without rebucket_only to trigger a new search",
	)
}
