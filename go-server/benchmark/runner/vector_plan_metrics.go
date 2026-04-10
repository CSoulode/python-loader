package runner

import (
	"context"
	"database/sql"
)

func runVectorPlanRecallCase(
	ctx context.Context,
	session *ActiveSession,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	plan VectorQueryPlan,
) ([]Neighbor, float64, error) {
	approx, err := searchNeighborsForPlan(ctx, session, query, vector, candidateIDs, plan)
	if err != nil {
		return nil, 0, err
	}
	exact, err := exactNeighborsForPlan(ctx, session.DB, query, vector, candidateIDs, plan)
	if err != nil {
		return nil, 0, err
	}
	return approx, RecallAtK(approx, exact, int(plan.MaxResults)), nil
}

func searchNeighborsForPlan(
	ctx context.Context,
	session *ActiveSession,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	plan VectorQueryPlan,
) ([]Neighbor, error) {
	switch plan.Mode {
	case QueryModeKNN:
		if len(candidateIDs) == 0 && query.TargetSelectivity >= 1.0 {
			return SearchKNN(ctx, session.VectorClient, query.Model.Name, vector, plan.MaxResults)
		}
		return SearchFilteredKNN(ctx, session.VectorClient, query.Model.Name, vector, plan.MaxResults, candidateIDs)
	default:
		if len(candidateIDs) == 0 && query.TargetSelectivity >= 1.0 {
			return SearchRange(ctx, session.VectorClient, query.Model.Name, vector, plan)
		}
		return SearchFilteredRange(ctx, session.VectorClient, query.Model.Name, vector, plan, candidateIDs)
	}
}

func exactNeighborsForPlan(
	ctx context.Context,
	db *sql.DB,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	plan VectorQueryPlan,
) ([]Neighbor, error) {
	switch plan.Mode {
	case QueryModeKNN:
		return exactNeighborsForPlanCalibration(ctx, db, query, vector, candidateIDs, int(plan.MaxResults))
	default:
		if len(candidateIDs) == 0 && query.TargetSelectivity >= 1.0 {
			return ExactRange(ctx, db, query.Model, vector, plan)
		}
		return ExactFilteredRange(ctx, db, query.Model, vector, candidateIDs, plan)
	}
}

func totalResponseCount(items []TimedResponse) int32 {
	var total int32
	for _, item := range items {
		total += item.Response.GetCount()
	}
	return total
}
