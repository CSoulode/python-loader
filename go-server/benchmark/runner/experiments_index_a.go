package runner

import (
	"context"
	"fmt"
	"path/filepath"

	benchindex "m3.dataloader/benchmark/index"
	pb "m3.dataloader/dataloader"
)

func (r *BenchRunner) RunExperiment5(ctx context.Context) error {
	rows := make([][]string, 0, len(r.opts.Datasets)*len(r.catalog.Models)*len(defaultSelectivities)*len(defaultKValues)*4)
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
		[]string{"dataset_label", "dataset_size", "model", "index_type", "k", "selectivity", "ttfb_ms", "ttlb_ms", "recall_at_k", "index_size_mb"},
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
	rows := make([][]string, 0, len(r.catalog.Models)*len(defaultSelectivities)*len(defaultKValues))
	for modelIndex, model := range r.catalog.Models {
		for selIndex, selectivity := range defaultSelectivities {
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
				row, err := r.runSingleIndexComparison(ctx, session, dataset, spec, rebuild[model.Name], query, vector, candidateIDs, k)
				if err != nil {
					return nil, err
				}
				rows = append(rows, row)
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
	k int32,
) ([]string, error) {
	plan := strategyForSelectivity(query.ActualSelectivity)
	summary, err := streamMedian(ctx, session, dataset, Experiment5, fmt.Sprintf("%s-%s-k%d", query.ID, spec.Type, k), func() *pb.GetBrowsingStateRequest {
		return query.Request(k, plan.Enum, false, DefaultBucketConfig())
	})
	if err != nil {
		return nil, err
	}

	approx, recall, err := runRecallCase(ctx, session, query, vector, candidateIDs, k)
	if err != nil {
		return nil, err
	}
	_ = approx

	indexSize := 0.0
	if rebuild != nil {
		indexSize = rebuild.IndexSizeMB
	}
	return []string{
		r.opts.DatasetLabel(dataset),
		r.opts.DatasetSizeLabel(dataset),
		query.Model.Name,
		string(spec.Type),
		fmt.Sprintf("%d", k),
		FormatFloat(query.ActualSelectivity),
		FormatFloat(summary.TTFB),
		FormatFloat(summary.TTLB),
		FormatFloat(recall),
		FormatFloat(indexSize),
	}, nil
}

func runRecallCase(
	ctx context.Context,
	session *ActiveSession,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	k int32,
) ([]Neighbor, float64, error) {
	var (
		approx []Neighbor
		exact  []Neighbor
		err    error
	)
	if len(candidateIDs) == 0 && query.TargetSelectivity >= 1.0 {
		approx, _, err = measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
			return SearchKNN(runCtx, session.VectorClient, query.Model.Name, vector, k)
		})
		if err != nil {
			return nil, 0, err
		}
		exact, err = ExactKNN(ctx, session.DB, query.Model, vector, int(k))
		if err != nil {
			return nil, 0, err
		}
		return approx, RecallAtK(approx, exact, int(k)), nil
	}
	if len(candidateIDs) == 0 {
		return nil, 0, nil
	}
	approx, _, err = measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
		return SearchFilteredKNN(runCtx, session.VectorClient, query.Model.Name, vector, k, candidateIDs)
	})
	if err != nil {
		return nil, 0, err
	}
	exact, err = ExactFilteredKNN(ctx, session.DB, query.Model, vector, candidateIDs, int(k))
	if err != nil {
		return nil, 0, err
	}
	return approx, RecallAtK(approx, exact, int(k)), nil
}
