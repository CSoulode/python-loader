package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lib/pq"

	benchindex "m3.dataloader/benchmark/index"
)

func (r *BenchRunner) RunExperiment6(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	selectivities := []float64{0.01, 0.05, 0.10, 0.30, 0.50}
	state := SelectivityBenchmarkState()
	rows := make([][]string, 0, len(r.opts.Datasets)*len(selectivities)*5)
	specs := []benchindex.Spec{
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeStrict},
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeRelaxed},
		{Type: benchindex.IVFFlat, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.IVFFlat, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeRelaxed},
	}
	for _, dataset := range r.opts.Datasets {
		for _, spec := range specs {
			err := r.withIndexedSession(ctx, dataset, spec, []BenchmarkModel{model}, func(session *ActiveSession, _ map[string]*benchindex.RebuildResult) error {
				manager := benchindex.NewManager(session.DB)
				for index, selectivity := range selectivities {
					query, err := BuildBenchmarkQuery(ctx, session.DB, state, model, selectivity, expQueryID("q", index), index)
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
					neighbors, latency, err := measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
						if len(candidateIDs) == 0 {
							return nil, nil
						}
						return SearchFilteredKNN(runCtx, session.VectorClient, model.Name, vector, 500, candidateIDs)
					})
					if err != nil {
						return err
					}
					exact, err := ExactFilteredKNN(ctx, session.DB, model, vector, candidateIDs, 500)
					if err != nil {
						return err
					}
					plan, usedIndex, err := manager.CaptureExplainWithSetup(
						ctx,
						iterativeExplainSetup(spec, 500),
						ApproxFilteredKNNQuery(model, spec.Precision),
						ApproxQueryArg(vector, spec.Precision),
						pq.Array(int32SliceToInts(candidateIDs)),
						500,
					)
					if err != nil {
						return err
					}
					if err := writeExplainPlan(r.paths.ExplainDir, dataset, spec, selectivity, plan); err != nil {
						return err
					}
					rows = append(rows, []string{
						r.opts.DatasetLabel(dataset),
						r.opts.DatasetSizeLabel(dataset),
						string(spec.Type),
						string(spec.IterativeMode),
						FormatFloat(query.ActualSelectivity),
						"500",
						fmt.Sprintf("%d", len(neighbors)),
						FormatFloat(RecallAtK(neighbors, exact, 500)),
						FormatFloat(latency),
						fmt.Sprintf("%t", usedIndex),
					})
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp6_iterative_scan.csv"),
		[]string{"dataset_label", "dataset_size", "index_type", "iterative_scan", "selectivity", "k", "returned_count", "recall_at_k", "latency_ms", "plan_used_index"},
		rows,
	)
}

func (r *BenchRunner) RunExperiment7(ctx context.Context) error {
	model, err := r.catalog.Model("siglip2")
	if err != nil {
		return err
	}
	queryState := DefaultRepresentativeStates()[0]
	specs := []benchindex.Spec{
		{Type: benchindex.HNSW, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.HNSW, Precision: benchindex.HalfPrecision, IterativeMode: benchindex.IterativeOff},
		{Type: benchindex.DiskANN, Precision: benchindex.FullPrecision, IterativeMode: benchindex.IterativeOff},
	}
	rows := make([][]string, 0, len(r.opts.Datasets)*len(specs)*len(defaultKValues))
	for _, dataset := range r.opts.Datasets {
		for _, spec := range specs {
			err := r.withIndexedSession(ctx, dataset, spec, []BenchmarkModel{model}, func(session *ActiveSession, rebuild map[string]*benchindex.RebuildResult) error {
				query, err := BuildBenchmarkQuery(ctx, session.DB, queryState, model, 1.0, "q01", 0)
				if err != nil {
					return err
				}
				vector, err := fetchReferenceVector(ctx, session, query)
				if err != nil {
					return err
				}
				for _, k := range defaultKValues {
					neighbors, latency, err := measureSearchMedian(ctx, func(runCtx context.Context) ([]Neighbor, error) {
						return SearchKNN(runCtx, session.VectorClient, model.Name, vector, k)
					})
					if err != nil {
						return err
					}
					exact, err := ExactKNN(ctx, session.DB, model, vector, int(k))
					if err != nil {
						return err
					}
					rebuildResult := rebuild[model.Name]
					rows = append(rows, []string{
						r.opts.DatasetLabel(dataset),
						r.opts.DatasetSizeLabel(dataset),
						string(spec.Type),
						precisionLabel(spec.Precision),
						fmt.Sprintf("%d", k),
						FormatFloat(rebuildResult.IndexSizeMB),
						FormatFloat(rebuildResult.BuildTimeS),
						FormatFloat(RecallAtK(neighbors, exact, int(k))),
						FormatFloat(latency),
					})
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp7_half_precision.csv"),
		[]string{"dataset_label", "dataset_size", "index_type", "precision", "k", "index_size_mb", "build_time_s", "recall_at_k", "latency_ms"},
		rows,
	)
}

func iterativeExplainSetup(spec benchindex.Spec, k int) []string {
	switch spec.Type {
	case benchindex.HNSW:
		setup := []string{fmt.Sprintf("SET LOCAL hnsw.ef_search = %d", k)}
		if spec.IterativeMode != benchindex.IterativeOff {
			setup = append(setup,
				fmt.Sprintf("SET LOCAL hnsw.iterative_scan = %s", spec.IterativeMode),
				"SET LOCAL hnsw.max_scan_tuples = 20000",
			)
		}
		return setup
	case benchindex.IVFFlat:
		setup := []string{"SET LOCAL ivfflat.probes = 14"}
		if spec.IterativeMode != benchindex.IterativeOff {
			setup = append(setup,
				fmt.Sprintf("SET LOCAL ivfflat.iterative_scan = %s", spec.IterativeMode),
			)
		}
		return setup
	default:
		return nil
	}
}

func writeExplainPlan(dir string, dataset DatasetID, spec benchindex.Spec, selectivity float64, plan string) error {
	filename := fmt.Sprintf("%s_%s_%s_sel%s.txt", dataset, spec.Type, spec.IterativeMode, selectivitySuffix(selectivity))
	return os.WriteFile(filepath.Join(dir, filename), []byte(plan), 0o644)
}

func selectivitySuffix(value float64) string {
	switch value {
	case 0.01:
		return "001"
	case 0.05:
		return "005"
	case 0.10:
		return "010"
	case 0.30:
		return "030"
	case 0.50:
		return "050"
	default:
		return "100"
	}
}
