package runner

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	benchindex "m3.dataloader/benchmark/index"
)

func (r *BenchRunner) withIndexedSession(
	ctx context.Context,
	dataset DatasetID,
	spec benchindex.Spec,
	models []BenchmarkModel,
	fn func(session *ActiveSession, rebuild map[string]*benchindex.RebuildResult) error,
) error {
	db, err := OpenDatasetDB(r.opts.DatasetDBURL(dataset))
	if err != nil {
		return err
	}
	manager := benchindex.NewManager(db)
	rebuild, err := r.rebuildIndexes(ctx, manager, dataset, spec, models)
	_ = db.Close()
	if err != nil {
		return err
	}

	session, err := StartSession(ctx, r.runtimeConfig(dataset, r.vectorEnvForSpec(spec)))
	if err != nil {
		return err
	}
	defer session.Close()
	return fn(session, rebuild)
}

func (r *BenchRunner) rebuildIndexes(
	ctx context.Context,
	manager *benchindex.Manager,
	dataset DatasetID,
	spec benchindex.Spec,
	models []BenchmarkModel,
) (map[string]*benchindex.RebuildResult, error) {
	results := make(map[string]*benchindex.RebuildResult, len(models))
	for _, model := range models {
		if spec.Precision == benchindex.HalfPrecision && !strings.EqualFold(model.Name, "siglip2") {
			continue
		}
		start := time.Now()
		log.Printf("BENCH_INDEX rebuild_start dataset=%s model=%s type=%s precision=%s", dataset, model.Name, spec.Type, spec.Precision)
		result, err := manager.RebuildIndex(ctx, benchmarkIndexModel(model), spec)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", dataset, model.Name, err)
		}
		log.Printf("BENCH_INDEX rebuild_done dataset=%s model=%s type=%s precision=%s elapsed_s=%.3f", dataset, model.Name, spec.Type, spec.Precision, time.Since(start).Seconds())
		results[model.Name] = result
		r.addIndexSnapshot(dataset, model, spec, result)
	}
	return results, nil
}

func benchmarkIndexModel(model BenchmarkModel) benchindex.Model {
	return benchindex.Model{
		Table:          model.Table,
		Dim:            model.Dim,
		DistanceMetric: model.DistanceMetric,
	}
}

func fetchReferenceVector(ctx context.Context, session *ActiveSession, query *BenchmarkQuery) ([]float32, error) {
	return FetchQueryVector(ctx, session.VectorClient, query.Model.Name, query.ReferenceObjectID)
}

func measureSearchMedian(
	ctx context.Context,
	run func(context.Context) ([]Neighbor, error),
) ([]Neighbor, float64, error) {
	latencies := make([]float64, 0, measuredRuns)
	var last []Neighbor
	for runIndex := 0; runIndex < warmupRuns+measuredRuns; runIndex++ {
		runCtx, cancel := withTimeout(ctx)
		start := time.Now()
		neighbors, err := run(runCtx)
		cancel()
		if err != nil {
			return nil, 0, err
		}
		if runIndex < warmupRuns {
			continue
		}
		last = neighbors
		latencies = append(latencies, time.Since(start).Seconds()*1000)
	}
	return last, Median(latencies), nil
}
