package runner

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	pb "m3.dataloader/dataloader"
)

type VectorQueryMode string

const (
	QueryModeKNN       VectorQueryMode = "knn"
	QueryModeRangeBall VectorQueryMode = "range_ball"
	QueryModeRangeRing VectorQueryMode = "range_ring"
)

type VectorQueryPlan struct {
	Mode          VectorQueryMode
	MaxResults    int32
	DistanceRange *pb.DistanceRange
}

func (p VectorQueryPlan) Label() string {
	return string(p.Mode)
}

func (p VectorQueryPlan) QueryType() string {
	switch p.Mode {
	case QueryModeRangeBall:
		return "ball"
	case QueryModeRangeRing:
		return "ring"
	default:
		return "knn"
	}
}

func (p VectorQueryPlan) DistanceMin() float64 {
	if p.DistanceRange == nil {
		return 0
	}
	return float64(p.DistanceRange.GetMinDistance())
}

func (p VectorQueryPlan) DistanceMax() float64 {
	if p.DistanceRange == nil {
		return 0
	}
	return float64(p.DistanceRange.GetMaxDistance())
}

func (p VectorQueryPlan) RangeWidth() float64 {
	if p.DistanceRange == nil {
		return 0
	}
	return math.Max(0, p.DistanceMax()-p.DistanceMin())
}

func KNNPlan(k int32) VectorQueryPlan {
	return VectorQueryPlan{Mode: QueryModeKNN, MaxResults: k}
}

func BuildVectorPlans(
	ctx context.Context,
	db *sql.DB,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	k int32,
) ([]VectorQueryPlan, error) {
	plans := []VectorQueryPlan{KNNPlan(k)}
	if k <= 1 {
		return plans, nil
	}

	exact, err := exactNeighborsForPlanCalibration(ctx, db, query, vector, candidateIDs, int(k))
	if err != nil {
		return nil, err
	}
	if len(exact) == 0 {
		return plans, nil
	}

	ball := &pb.DistanceRange{MinDistance: 0, MaxDistance: float32(exact[len(exact)-1].Distance)}
	if ball.GetMaxDistance() > 0 {
		plans = append(plans, VectorQueryPlan{
			Mode:          QueryModeRangeBall,
			MaxResults:    k,
			DistanceRange: ball,
		})
	}

	if ring := ringDistanceRange(exact); ring != nil {
		plans = append(plans, VectorQueryPlan{
			Mode:          QueryModeRangeRing,
			MaxResults:    k,
			DistanceRange: ring,
		})
	}
	return plans, nil
}

func exactNeighborsForPlanCalibration(
	ctx context.Context,
	db *sql.DB,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	k int,
) ([]Neighbor, error) {
	if len(candidateIDs) == 0 && query.TargetSelectivity >= 1.0 {
		return ExactKNN(ctx, db, query.Model, vector, k)
	}
	if len(candidateIDs) == 0 {
		return nil, fmt.Errorf("candidate ids are required for filtered range calibration")
	}
	return ExactFilteredKNN(ctx, db, query.Model, vector, candidateIDs, k)
}

func ringDistanceRange(neighbors []Neighbor) *pb.DistanceRange {
	if len(neighbors) < 4 {
		return nil
	}

	minIndex := len(neighbors) / 4
	maxIndex := (len(neighbors) * 3) / 4
	if maxIndex >= len(neighbors) {
		maxIndex = len(neighbors) - 1
	}
	minDistance := float32(neighbors[minIndex].Distance)
	maxDistance := float32(neighbors[maxIndex].Distance)
	if maxDistance <= minDistance {
		return nil
	}
	return &pb.DistanceRange{
		MinDistance: minDistance,
		MaxDistance: maxDistance,
	}
}
