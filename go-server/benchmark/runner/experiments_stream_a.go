package runner

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	pb "m3.dataloader/dataloader"
)

func (r *BenchRunner) RunExperiment1(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	state := SelectivityBenchmarkState()
	rows := make([][]string, 0, len(r.opts.Datasets)*len(fullSelectivities)*len(defaultKValues)*12)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			for selIndex, selectivity := range fullSelectivities {
				query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, selectivity, expQueryID("exp1", selIndex), selIndex)
				if err != nil {
					return err
				}
				for _, k := range defaultKValues {
					currentRows, err := r.runStrategyComparisonCases(ctx, session, dataset, query, k)
					if err != nil {
						return err
					}
					rows = append(rows, currentRows...)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp1_strategy_comparison.csv"),
		[]string{"selectivity", "k", "dataset_label", "dataset_size", "strategy", "query_mode", "dist_min", "dist_max", "ttfb_ms", "ttlb_ms", "vector_search_ms", "join_ms"},
		rows,
	)
}

func (r *BenchRunner) runStrategyComparisonCases(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
	k int32,
) ([][]string, error) {
	vector, err := fetchReferenceVector(ctx, session, query)
	if err != nil {
		return nil, err
	}
	candidateIDs := []int32(nil)
	if query.TargetSelectivity < 1.0 {
		candidateIDs, err = LoadModelCandidateIDs(ctx, session.DB, query)
		if err != nil {
			return nil, err
		}
	}
	plans, err := BuildVectorPlans(ctx, session.DB, query, vector, candidateIDs, k)
	if err != nil {
		return nil, err
	}

	out := make([][]string, 0, len(plans)*4)
	for _, plan := range plans {
		currentRows, err := r.runStrategyComparisonPlanCases(ctx, session, dataset, query, plan)
		if err != nil {
			return nil, err
		}
		out = append(out, currentRows...)
	}
	return out, nil
}

func (r *BenchRunner) runStrategyComparisonPlanCases(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
	plan VectorQueryPlan,
) ([][]string, error) {
	out := make([][]string, 0, 4)
	distMin, distMax := planDistanceBoundsCSV(plan)
	for _, item := range []struct {
		name     string
		strategy pb.HybridStrategy
	}{
		{name: "post_filter", strategy: pb.HybridStrategy_POST_FILTER},
		{name: "pre_filter", strategy: pb.HybridStrategy_PRE_FILTER},
		{name: "hybrid", strategy: pb.HybridStrategy_HYBRID},
		{name: "auto", strategy: pb.HybridStrategy_AUTO},
	} {
		caseID := fmt.Sprintf("%s-%s-%s-k%d", query.ID, plan.Label(), item.name, plan.MaxResults)
		summary, err := streamMedian(ctx, session, dataset, Experiment1, caseID, func() *pb.GetBrowsingStateRequest {
			return query.RequestForPlan(plan, item.strategy, false, DefaultBucketConfig())
		})
		if err != nil {
			return nil, err
		}
		out = append(out, []string{
			FormatFloat(query.ActualSelectivity),
			planKForCSV(plan),
			r.opts.DatasetLabel(dataset),
			r.opts.DatasetSizeLabel(dataset),
			item.name,
			plan.Label(),
			distMin,
			distMax,
			FormatFloat(summary.TTFB),
			FormatFloat(summary.TTLB),
			FormatFloat(summary.VectorSearchMS),
			FormatFloat(summary.SQLExecMS),
		})
	}
	return out, nil
}

func (r *BenchRunner) RunExperiment2(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	queryCount := len(DefaultRepresentativeStates()) * len(filteredSelectivities) * exp2RepeatCount
	rows := make([][]string, 0, len(r.opts.Datasets)*queryCount)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			queries, err := buildEstimationQueries(ctx, session.DB, model)
			if err != nil {
				return err
			}
			for _, query := range queries {
				estimated, err := r.measureEstimatedCount(ctx, session, dataset, query)
				if err != nil {
					return err
				}
				actual, err := CountCandidates(ctx, session.DB, query)
				if err != nil {
					return err
				}
				ratio := 0.0
				if actual > 0 {
					ratio = float64(estimated) / float64(actual)
				}
				rows = append(rows, []string{
					r.opts.DatasetLabel(dataset),
					r.opts.DatasetSizeLabel(dataset),
					query.ID,
					query.Complexity,
					query.FilterTypes,
					fmt.Sprintf("%d", estimated),
					fmt.Sprintf("%d", actual),
					FormatFloat(ratio),
					fmt.Sprintf("%d", absInt64(estimated-actual)),
				})
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp2_selectivity_accuracy.csv"),
		[]string{"dataset_label", "dataset_size", "query_id", "complexity", "filter_types", "estimated", "actual", "ratio", "abs_error"},
		rows,
	)
}

func (r *BenchRunner) withDefaultSession(
	ctx context.Context,
	dataset DatasetID,
	fn func(session *ActiveSession) error,
) error {
	return r.withDefaultSessionServerEnv(ctx, dataset, nil, fn)
}

func (r *BenchRunner) withDefaultSessionServerEnv(
	ctx context.Context,
	dataset DatasetID,
	serverEnv map[string]string,
	fn func(session *ActiveSession) error,
) error {
	session, err := r.startDefaultSession(ctx, dataset, serverEnv)
	if err != nil {
		return err
	}
	defer session.Close()
	return fn(session)
}

func (r *BenchRunner) startDefaultSession(
	ctx context.Context,
	dataset DatasetID,
	serverEnv map[string]string,
) (*ActiveSession, error) {
	return StartSession(ctx, r.runtimeConfigWithServerEnv(dataset, r.vectorEnvForSpec(defaultRuntimeSpec()), serverEnv))
}

func (r *BenchRunner) measureEstimatedCount(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
) (int64, error) {
	result, err := executeMeasuredStream(ctx, session, dataset, Experiment2, query.ID, 0, query.Request(100, pb.HybridStrategy_AUTO, false, DefaultBucketConfig()))
	if err != nil {
		return 0, err
	}
	for _, event := range result.Events {
		if event.Event == "strategy_selected" {
			return event.EstimatedFilteredCount, nil
		}
	}
	return 0, fmt.Errorf("strategy_selected event missing for %s", query.ID)
}

func buildEstimationQueries(
	ctx context.Context,
	db *sql.DB,
	model BenchmarkModel,
) ([]*BenchmarkQuery, error) {
	states := DefaultRepresentativeStates()
	queries := make([]*BenchmarkQuery, 0, len(states)*len(filteredSelectivities)*exp2RepeatCount)
	for repeat := 0; repeat < exp2RepeatCount; repeat++ {
		for _, state := range states {
			for _, selectivity := range filteredSelectivities {
				index := len(queries)
				query, err := BuildBenchmarkQuery(
					ctx,
					db,
					state,
					model,
					selectivity,
					expQueryID("q", index),
					index,
				)
				if err != nil {
					return nil, err
				}
				queries = append(queries, query)
			}
		}
	}
	return queries, nil
}

func expQueryID(prefix string, index int) string {
	return fmt.Sprintf("%s%02d", prefix, index+1)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
