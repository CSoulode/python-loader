package runner

import (
	"context"
	"fmt"
	"path/filepath"

	benchindex "m3.dataloader/benchmark/index"
	pb "m3.dataloader/dataloader"
)

func (r *BenchRunner) RunExperiment5(ctx context.Context) error {
	rows := make([][]string, 0, len(r.opts.Datasets)*len(r.catalog.Models)*len(fullSelectivities)*len(defaultKValues)*12)
	specs := []benchindex.Spec{
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.IVFFlat, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.DiskANN, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.NoIndex, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
	}
	for _, dataset := range r.opts.Datasets {
		for _, spec := range specs {
			err := r.withIndexedSession(ctx, dataset, spec, r.catalog.Models, func(session *ActiveSession, rebuild map[string]*benchindex.RebuildResult) error {
				currentRows, err := r.runIndexComparisonCases(ctx, session, dataset, spec, rebuild)
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
		filepath.Join(r.paths.RawDir, "exp5_index_comparison.csv"),
		[]string{"model", "index_type", "query_mode", "k", "dist_min", "dist_max", "selectivity", "ttfb_ms", "ttlb_ms", "recall_at_k", "index_size_mb", "dataset_label", "dataset_size"},
		rows,
	)
}

func (r *BenchRunner) runIndexComparisonCases(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	spec benchindex.Spec,
	rebuild map[string]*benchindex.RebuildResult,
) ([][]string, error) {
	state := SelectivityBenchmarkState()
	rows := make([][]string, 0, len(r.catalog.Models)*len(fullSelectivities)*len(defaultKValues))
	for modelIndex, model := range r.catalog.Models {
		for selIndex, selectivity := range fullSelectivities {
			query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, selectivity, expQueryID("q", modelIndex*10+selIndex), modelIndex*100+selIndex)
			if err != nil {
				return nil, err
			}
			vector, err := fetchReferenceVector(ctx, session, query)
			if err != nil {
				return nil, err
			}
			candidateIDs := []int32(nil)
			if selectivity < 1.0 {
				candidateIDs, err = LoadModelCandidateIDs(ctx, session.DB, query)
				if err != nil {
					return nil, err
				}
			}
			for _, k := range defaultKValues {
				plans, err := BuildVectorPlans(ctx, session.DB, query, vector, candidateIDs, k)
				if err != nil {
					return nil, err
				}
				for _, plan := range plans {
					row, err := r.runSingleIndexComparison(ctx, session, dataset, spec, rebuild[model.Name], query, vector, candidateIDs, plan)
					if err != nil {
						return nil, err
					}
					rows = append(rows, row)
				}
			}
		}
	}
	return rows, nil
}

func (r *BenchRunner) runSingleIndexComparison(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	spec benchindex.Spec,
	rebuild *benchindex.RebuildResult,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	plan VectorQueryPlan,
) ([]string, error) {
	strategyPlan := strategyForSelectivity(query.ActualSelectivity)
	summary, err := streamMedian(ctx, session, dataset, Experiment5, fmt.Sprintf("%s-%s-%s-k%d", query.ID, spec.Type, plan.Label(), plan.MaxResults), func() *pb.GetBrowsingStateRequest {
		return query.RequestForPlan(plan, strategyPlan.Enum, false, DefaultBucketConfig())
	})
	if err != nil {
		return nil, err
	}

	approx, recall, err := runVectorPlanRecallCase(ctx, session, query, vector, candidateIDs, plan)
	if err != nil {
		return nil, err
	}
	_ = approx

	indexSize := 0.0
	if rebuild != nil {
		indexSize = rebuild.IndexSizeMB
	}
	distMin, distMax := planDistanceBoundsCSV(plan)
	return []string{
		query.Model.Name,
		string(spec.Type),
		plan.Label(),
		planKForCSV(plan),
		distMin,
		distMax,
		FormatFloat(query.ActualSelectivity),
		FormatFloat(summary.TTFB),
		FormatFloat(summary.TTLB),
		FormatFloat(recall),
		FormatFloat(indexSize),
		r.opts.DatasetLabel(dataset),
		r.opts.DatasetSizeLabel(dataset),
	}, nil
}
