package main

import (
	"context"

	pb "m3.dataloader/dataloader"
)

func (s *DataLoaderServer) resolvePreparedVectorDimension(
	ctx context.Context,
	prepared preparedVectorDimension,
	candidateIDs []int32,
) (*vectorDimensionResult, error) {
	kind := searchKindForStrategy(prepared.Strategy, prepared.Config)
	if cached, ok := s.ensureVectorCache().TryGetForConfigKind(
		prepared.Config,
		prepared.RefHash,
		prepared.FilterHash,
		kind,
	); ok {
		return bucketSearchResult(prepared.Config, cached)
	}

	result, err := s.executePreparedVectorSearch(ctx, prepared, candidateIDs)
	if err != nil {
		return nil, err
	}
	s.ensureVectorCache().PutForConfig(prepared.Config, prepared.RefHash, prepared.FilterHash, result)
	return bucketSearchResult(prepared.Config, result)
}

func searchKindForStrategy(strategy HybridStrategy, cfg *pb.VectorSearchDimension) SearchKind {
	switch strategy {
	case PreFilter:
		if isRangeQuery(cfg) {
			return searchKindFilteredRange
		}
		return searchKindFilteredKNN
	case Hybrid:
		return searchKindHybridIntersection
	default:
		return globalSearchKindForConfig(cfg)
	}
}
