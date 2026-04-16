package runner

import (
	"context"
	"fmt"
	"path/filepath"

	benchindex "m3.dataloader/benchmark/index"
	pb "m3.dataloader/dataloader"
)

const expRangeProbeK int32 = 500

func (r *BenchRunner) RunExperiment8(ctx context.Context) error {
	specs := []benchindex.Spec{
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.IVFFlat, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.DiskANN, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.NoIndex, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
	}
	rows := make([][]string, 0, len(r.opts.Datasets)*len(specs)*len(r.catalog.Models)*18)
	for _, dataset := range r.opts.Datasets {
		for _, spec := range specs {
			err := r.withIndexedSession(ctx, dataset, spec, r.catalog.Models, func(session *ActiveSession, _ map[string]*benchindex.RebuildResult) error {
				currentRows, err := r.runRangeQueryMatrix(ctx, session, dataset, spec)
				if err != nil {
					return err
				}
				rows = append(rows, currentRows...)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp8_range_query.csv"),
		[]string{"model", "query_type", "range_position", "range_width_label", "dist_min", "dist_max", "range_width", "index_type", "strategy", "result_count", "latency_ms", "recall", "dataset_label", "dataset_size"},
		rows,
	)
}

func (r *BenchRunner) RunExperiment9(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}

	rows := make([][]string, 0, len(r.opts.Datasets)*len(filteredSelectivities)*8)
	state := SelectivityBenchmarkState()
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			for selIndex, selectivity := range filteredSelectivities {
				query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, selectivity, expQueryID("exp9", selIndex), selIndex)
				if err != nil {
					return err
				}
				vector, err := fetchReferenceVector(ctx, session, query)
				if err != nil {
					return err
				}
				candidateIDs, err := LoadModelCandidateIDs(ctx, session.DB, query)
				if err != nil {
					return err
				}
				plans, err := BuildVectorPlans(ctx, session.DB, query, vector, candidateIDs, expRangeProbeK)
				if err != nil {
					return err
				}
				for _, plan := range rangeOnlyPlans(plans) {
					currentRows, err := r.runRangeStrategyCases(ctx, session, dataset, query, plan)
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
		filepath.Join(r.paths.RawDir, "exp9_range_filter_strategy.csv"),
		[]string{"selectivity", "query_type", "dist_min", "dist_max", "strategy", "selected_strategy", "candidate_count", "result_count", "ttfb_ms", "ttlb_ms", "vector_search_ms", "join_ms", "dataset_label", "dataset_size"},
		rows,
	)
}

func (r *BenchRunner) runRangeQueryMatrix(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	spec benchindex.Spec,
) ([][]string, error) {
	state := SelectivityBenchmarkState()
	rows := make([][]string, 0, len(r.catalog.Models)*18)
	for modelIndex, model := range r.catalog.Models {
		query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, 1.0, expQueryID("exp8", modelIndex), modelIndex)
		if err != nil {
			return nil, err
		}
		vector, err := fetchReferenceVector(ctx, session, query)
		if err != nil {
			return nil, err
		}
		cases, err := BuildRangeMatrixCases(ctx, session.DB, query, vector, nil, expRangeProbeK)
		if err != nil {
			return nil, err
		}
		for _, item := range cases {
			approx, latencyMS, err := measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
				return searchNeighborsForPlan(runCtx, session, query, vector, nil, item.Plan)
			})
			if err != nil {
				return nil, err
			}
			exact, err := exactNeighborsForPlan(ctx, session.DB, query, vector, nil, item.Plan)
			if err != nil {
				return nil, err
			}
			rows = append(rows, []string{
				query.Model.Name,
				item.Plan.QueryType(),
				item.RangePosition,
				item.WidthLabel,
				FormatFloat(item.Plan.DistanceMin()),
				FormatFloat(item.Plan.DistanceMax()),
				FormatFloat(item.Plan.RangeWidth()),
				string(spec.Type),
				rangeExecutionPath(item.Plan),
				fmt.Sprintf("%d", len(approx)),
				FormatFloat(latencyMS),
				FormatFloat(RecallAtK(approx, exact, int(item.Plan.MaxResults))),
				r.opts.DatasetLabel(dataset),
				r.opts.DatasetSizeLabel(dataset),
			})
		}
	}
	return rows, nil
}

func (r *BenchRunner) runRangeStrategyCases(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
	plan VectorQueryPlan,
) ([][]string, error) {
	out := make([][]string, 0, 4)
	for _, item := range []struct {
		name     string
		strategy pb.HybridStrategy
	}{
		{name: "range_post", strategy: pb.HybridStrategy_POST_FILTER},
		{name: "range_pre", strategy: pb.HybridStrategy_PRE_FILTER},
		{name: "range_hybrid", strategy: pb.HybridStrategy_HYBRID},
		{name: "auto", strategy: pb.HybridStrategy_AUTO},
	} {
		caseID := fmt.Sprintf("%s-%s-%s", query.ID, plan.Label(), item.name)
		summary, err := streamMedian(ctx, session, dataset, Experiment9, caseID, func() *pb.GetBrowsingStateRequest {
			return query.RequestForPlan(plan, item.strategy, false, DefaultBucketConfig())
		})
		if err != nil {
			return nil, err
		}
		sample, err := executeMeasuredStreamAllowMissingEvents(ctx, session, dataset, Experiment9, caseID+"-sample", 0, query.RequestForPlan(plan, item.strategy, false, DefaultBucketConfig()))
		if err != nil {
			return nil, err
		}
		out = append(out, []string{
			FormatFloat(query.ActualSelectivity),
			plan.QueryType(),
			FormatFloat(plan.DistanceMin()),
			FormatFloat(plan.DistanceMax()),
			item.name,
			resolvedSelectedStrategy(summary.Strategy, item.strategy),
			fmt.Sprintf("%d", query.ActualCandidateCount),
			fmt.Sprintf("%d", totalResponseCount(sample.Items)),
			FormatFloat(summary.TTFB),
			FormatFloat(summary.TTLB),
			FormatFloat(summary.VectorSearchMS),
			FormatFloat(summary.SQLExecMS),
			r.opts.DatasetLabel(dataset),
			r.opts.DatasetSizeLabel(dataset),
		})
	}
	return out, nil
}

func rangeExecutionPath(plan VectorQueryPlan) string {
	if plan.Mode == QueryModeRangeBall {
		return "knn_adapter"
	}
	return "brute_force"
}

func rangeOnlyPlans(plans []VectorQueryPlan) []VectorQueryPlan {
	out := make([]VectorQueryPlan, 0, len(plans))
	for _, plan := range plans {
		if plan.Mode == QueryModeKNN {
			continue
		}
		out = append(out, plan)
	}
	return out
}
