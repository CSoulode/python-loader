package runner

import (
	"context"
	"fmt"
	"path/filepath"

	pb "m3.dataloader/dataloader"
)

const expRangeProbeK int32 = 500

func (r *BenchRunner) RunExperiment8(ctx context.Context) error {
	rows := make([][]string, 0, len(r.opts.Datasets)*len(r.catalog.Models)*2)
	state := SelectivityBenchmarkState()

	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			for modelIndex, model := range r.catalog.Models {
				query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, 1.0, expQueryID("exp8", modelIndex), modelIndex)
				if err != nil {
					return err
				}
				vector, err := fetchReferenceVector(ctx, session, query)
				if err != nil {
					return err
				}
				plans, err := BuildVectorPlans(ctx, session.DB, query, vector, nil, expRangeProbeK)
				if err != nil {
					return err
				}
				for _, plan := range rangeOnlyPlans(plans) {
					approx, latencyMS, err := measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
						return searchNeighborsForPlan(runCtx, session, query, vector, nil, plan)
					})
					if err != nil {
						return err
					}
					exact, err := exactNeighborsForPlan(ctx, session.DB, query, vector, nil, plan)
					if err != nil {
						return err
					}
					rows = append(rows, []string{
						query.Model.Name,
						plan.Label(),
						FormatFloat(float64(plan.DistanceRange.GetMinDistance())),
						FormatFloat(float64(plan.DistanceRange.GetMaxDistance())),
						FormatFloat(float64(plan.DistanceRange.GetMaxDistance() - plan.DistanceRange.GetMinDistance())),
						string(defaultRuntimeSpec().Type),
						"vector_only",
						fmt.Sprintf("%d", len(approx)),
						FormatFloat(latencyMS),
						FormatFloat(RecallAtK(approx, exact, int(plan.MaxResults))),
						r.opts.DatasetLabel(dataset),
						r.opts.DatasetSizeLabel(dataset),
					})
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp8_range_query.csv"),
		[]string{"model", "query_type", "dist_min", "dist_max", "range_width", "index_type", "strategy", "result_count", "latency_ms", "recall", "dataset_label", "dataset_size"},
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
		[]string{"selectivity", "range_type", "dist_min", "dist_max", "strategy", "candidate_count", "result_count", "ttlb_ms", "vector_search_ms", "join_ms", "dataset_label", "dataset_size"},
		rows,
	)
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
		sample, err := executeMeasuredStream(ctx, session, dataset, Experiment9, caseID+"-sample", 0, query.RequestForPlan(plan, item.strategy, false, DefaultBucketConfig()))
		if err != nil {
			return nil, err
		}
		out = append(out, []string{
			FormatFloat(query.ActualSelectivity),
			plan.Label(),
			FormatFloat(float64(plan.DistanceRange.GetMinDistance())),
			FormatFloat(float64(plan.DistanceRange.GetMaxDistance())),
			item.name,
			fmt.Sprintf("%d", query.ActualCandidateCount),
			fmt.Sprintf("%d", totalResponseCount(sample.Items)),
			FormatFloat(summary.TTLB),
			FormatFloat(summary.VectorSearchMS),
			FormatFloat(summary.SQLExecMS),
			r.opts.DatasetLabel(dataset),
			r.opts.DatasetSizeLabel(dataset),
		})
	}
	return out, nil
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
