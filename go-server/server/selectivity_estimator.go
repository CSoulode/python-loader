package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

const (
	preFilterBaseThreshold          = 5000
	preFilterIterativeScanThreshold = 20000
	rangeRingPreFilterThreshold     = 2500
	postFilterThreshold             = 0.3
	largeKThreshold                 = 1000
)

type HybridStrategy int

const (
	Auto HybridStrategy = iota
	PostFilter
	PreFilter
	Hybrid
)

func (s HybridStrategy) String() string {
	switch s {
	case PostFilter:
		return "post_filter"
	case PreFilter:
		return "pre_filter"
	case Hybrid:
		return "hybrid"
	default:
		return "auto"
	}
}

type SelectivityEstimator struct {
	db *sql.DB
}

func newSelectivityEstimator(db *sql.DB) *SelectivityEstimator {
	return &SelectivityEstimator{db: db}
}

func (e *SelectivityEstimator) DomainSize(ctx context.Context) (int64, error) {
	if e == nil || e.db == nil {
		return 0, fmt.Errorf("selectivity estimator database is not configured")
	}

	var count int64
	if err := e.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM medias`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (e *SelectivityEstimator) EstimateFilteredCount(
	ctx context.Context,
	filters []qg.ParsedFilter,
	axes []qg.ParsedAxis,
) (int64, error) {
	domainSize, err := e.DomainSize(ctx)
	if err != nil {
		return 0, fmt.Errorf("domainSize: %w", err)
	}
	return e.estimateFilteredCountWithDomain(ctx, filters, axes, domainSize)
}

func (e *SelectivityEstimator) estimateFilteredCountWithDomain(
	ctx context.Context,
	filters []qg.ParsedFilter,
	axes []qg.ParsedAxis,
	domainSize int64,
) (int64, error) {
	if domainSize == 0 {
		return 0, nil
	}

	counts := make([]float64, 0, len(filters)+len(axes))
	for index, filter := range filters {
		count, err := e.countForFilter(ctx, filter)
		if err != nil {
			return 0, fmt.Errorf("countForFilter[%d]: %w", index, err)
		}
		counts = append(counts, float64(count))
	}
	for index, axis := range axes {
		count, err := e.countForAxis(ctx, axis)
		if err != nil {
			return 0, fmt.Errorf("countForAxis[%d]: %w", index, err)
		}
		counts = append(counts, float64(count))
	}

	if len(counts) == 0 {
		return domainSize, nil
	}

	product := 1.0
	for _, count := range counts {
		product *= count
	}
	estimate := int64(product / math.Pow(float64(domainSize), float64(len(counts)-1)))
	if estimate < 0 {
		return 0, nil
	}
	if estimate > domainSize {
		return domainSize, nil
	}
	return estimate, nil
}

func (e *SelectivityEstimator) countForFilter(ctx context.Context, filter qg.ParsedFilter) (int64, error) {
	if filter.Type == "objectid" {
		return int64(len(filter.Ids)), nil
	}

	sqlStr, err := qg.BuildFilterIDSQL(filter)
	if err != nil {
		return 0, err
	}
	return e.countDistinctObjectIDs(ctx, sqlStr)
}

func (e *SelectivityEstimator) countForAxis(ctx context.Context, axis qg.ParsedAxis) (int64, error) {
	if axis.Type == "vector" {
		return 0, fmt.Errorf("vector axis must not be passed to the selectivity estimator")
	}

	sqlStr, err := qg.BuildAxisObjectIDSQLForState(axis.Type, axis.Id)
	if err != nil {
		return 0, err
	}
	if sqlStr == "" {
		return 0, fmt.Errorf("axis type is required for selectivity estimation")
	}
	return e.countDistinctObjectIDs(ctx, sqlStr)
}

func (e *SelectivityEstimator) countDistinctObjectIDs(ctx context.Context, sourceSQL string) (int64, error) {
	if e == nil || e.db == nil {
		return 0, fmt.Errorf("selectivity estimator database is not configured")
	}

	query := fmt.Sprintf(`SELECT COUNT(DISTINCT object_id) FROM (%s) AS candidates`, sourceSQL)
	var count int64
	if err := e.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func chooseStrategy(
	estimatedFilteredCount int64,
	totalMediaCount int64,
	hasMetadataPredicates bool,
	cfg *pb.VectorSearchDimension,
	iterativeScanAvailable bool,
	forced HybridStrategy,
) HybridStrategy {
	if !hasMetadataPredicates || totalMediaCount <= 0 {
		return PostFilter
	}
	if forced != Auto {
		return forced
	}

	threshold := preFilterThreshold(cfg, iterativeScanAvailable)
	if effectiveVectorMaxResults(cfg) > largeKThreshold {
		threshold /= 2
	}
	if estimatedFilteredCount < threshold {
		return PreFilter
	}
	selectivity := float64(estimatedFilteredCount) / float64(totalMediaCount)
	if selectivity > postFilterThreshold {
		return PostFilter
	}
	return Hybrid
}

func preFilterThreshold(cfg *pb.VectorSearchDimension, iterativeScanAvailable bool) int64 {
	if isRingRangeQuery(cfg) {
		return rangeRingPreFilterThreshold
	}
	if iterativeScanAvailable {
		return preFilterIterativeScanThreshold
	}
	return preFilterBaseThreshold
}

func isRingRangeQuery(cfg *pb.VectorSearchDimension) bool {
	return isRangeQuery(cfg) && cfg.GetDistanceRange().GetMinDistance() > 0
}

func strategyFromProto(value pb.HybridStrategy) (HybridStrategy, error) {
	switch value {
	case pb.HybridStrategy_AUTO:
		return Auto, nil
	case pb.HybridStrategy_POST_FILTER:
		return PostFilter, nil
	case pb.HybridStrategy_PRE_FILTER:
		return PreFilter, nil
	case pb.HybridStrategy_HYBRID:
		return Hybrid, nil
	default:
		return Auto, status.Errorf(codes.InvalidArgument, "invalid hybrid_strategy: %v", value)
	}
}
