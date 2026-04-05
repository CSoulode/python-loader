package runner

import (
	"context"
	"database/sql"
	"fmt"
)

const minMixedCandidateCount int64 = 500

func SelectivityBenchmarkState() RepresentativeState {
	return RepresentativeState{
		Name:       "selectivity",
		Complexity: "SEL",
	}
}

func MixedBenchmarkStates() []RepresentativeState {
	states := DefaultRepresentativeStates()
	return []RepresentativeState{
		{Name: "gh_axes", Complexity: "GH", Axes: states[1].Axes},
		{Name: "rh_axes", Complexity: "RH", Axes: states[2].Axes},
		{Name: "hw_axes", Complexity: "HW", Axes: states[3].Axes},
	}
}

func BuildQuerySet(
	ctx context.Context,
	db *sql.DB,
	states []RepresentativeState,
	model BenchmarkModel,
	targets []float64,
	limit int,
	minCandidates int64,
	prefix string,
) ([]*BenchmarkQuery, error) {
	queries := make([]*BenchmarkQuery, 0, limit)
	maxAttempts := limit * len(states) * len(targets) * 4
	for attempt := 0; attempt < maxAttempts && len(queries) < limit; attempt++ {
		state := states[attempt%len(states)]
		target := targets[(attempt/len(states))%len(targets)]
		query, err := BuildBenchmarkQuery(ctx, db, state, model, target, expQueryID(prefix, attempt), attempt)
		if err != nil {
			return nil, err
		}
		if query.ActualCandidateCount < minCandidates {
			continue
		}
		queries = append(queries, query)
	}
	if len(queries) < limit {
		return nil, fmt.Errorf("only built %d/%d queries with min candidate count %d", len(queries), limit, minCandidates)
	}
	return queries, nil
}
