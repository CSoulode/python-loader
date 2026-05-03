package runner

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	benchindex "m3.dataloader/benchmark/index"
	pb "m3.dataloader/dataloader"
)

const benchmarkStreamBatchSize = 64
const benchmarkStreamK = 500

type jsdExperimentCase struct {
	Name     string
	Strategy pb.HybridStrategy
}

func (r *BenchRunner) RunExperiment3(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	rows := make([][]string, 0, len(r.opts.Datasets)*10*12)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSessionServerEnv(ctx, dataset, map[string]string{
			"STREAM_BATCH_SIZE": fmt.Sprintf("%d", benchmarkStreamBatchSize),
		}, func(session *ActiveSession) error {
			queries, err := buildMixedQueries(ctx, session.DB, model, 10)
			if err != nil {
				return err
			}
			for _, query := range queries {
				currentRows, err := r.runJSDCases(ctx, session, dataset, query)
				if err != nil {
					return err
				}
				rows = append(rows, currentRows...)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp3_ssb_convergence.csv"),
		[]string{"dataset_label", "dataset_size", "query_id", "strategy", "batch_idx", "elapsed_ms", "jsd", "cells_received", "cells_total"},
		rows,
	)
}

func (r *BenchRunner) runJSDCases(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
) ([][]string, error) {
	rows := make([][]string, 0, 16)
	for _, item := range jsdExperimentCases() {
		currentRows, err := r.runJSDCase(ctx, session, dataset, query, item)
		if err != nil {
			return nil, err
		}
		rows = append(rows, currentRows...)
	}
	return rows, nil
}

func jsdExperimentCases() []jsdExperimentCase {
	return []jsdExperimentCase{
		{Name: "post_filter", Strategy: pb.HybridStrategy_POST_FILTER},
		{Name: "pre_filter", Strategy: pb.HybridStrategy_PRE_FILTER},
		{Name: "hybrid", Strategy: pb.HybridStrategy_HYBRID},
	}
}

func (r *BenchRunner) runJSDCase(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
	item jsdExperimentCase,
) ([][]string, error) {
	request := query.Request(benchmarkStreamK, item.Strategy, false, DefaultBucketConfig())
	if err := invalidateBrowsingStateChainAll(ctx, session); err != nil {
		return nil, err
	}
	if _, err := executeMeasuredStream(ctx, session, dataset, Experiment3, query.ID+"-"+item.Name, -1, request); err != nil {
		return nil, err
	}
	if err := invalidateBrowsingStateChainAll(ctx, session); err != nil {
		return nil, err
	}
	result, err := executeMeasuredStream(ctx, session, dataset, Experiment3, query.ID+"-"+item.Name, 0, request)
	if err != nil {
		return nil, err
	}
	states, elapsed := BuildCellSeries(result.Items)
	points := BuildJSDSeries(states, elapsed, benchmarkStreamBatchSize, benchmarkStreamK)
	if len(points) == 0 {
		return nil, fmt.Errorf("no JSD points for query=%s strategy=%s", query.ID, item.Name)
	}
	return r.jsdRows(dataset, query.ID, item.Name, points), nil
}

func invalidateBrowsingStateChainAll(ctx context.Context, session *ActiveSession) error {
	_, err := phaseFInvalidate(ctx, session, phaseFInvalidationRequest{Scope: "all"})
	return err
}

func (r *BenchRunner) jsdRows(
	dataset DatasetID,
	queryID string,
	strategy string,
	points []JSDPoint,
) [][]string {
	rows := make([][]string, 0, len(points))
	for _, point := range points {
		rows = append(rows, r.jsdRow(dataset, queryID, strategy, point))
	}
	return rows
}

func (r *BenchRunner) jsdRow(
	dataset DatasetID,
	queryID string,
	strategy string,
	point JSDPoint,
) []string {
	return []string{
		r.opts.DatasetLabel(dataset), r.opts.DatasetSizeLabel(dataset), queryID, strategy,
		fmt.Sprintf("%d", point.BatchIdx), FormatFloat(point.ElapsedMS), FormatFloat(point.JSD),
		fmt.Sprintf("%d", point.CellsReceived), fmt.Sprintf("%d", point.CellsTotal),
	}
}

func (r *BenchRunner) RunExperiment4(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	rows := make([][]string, 0, len(r.opts.Datasets)*10)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			queries, err := buildMixedQueries(ctx, session.DB, model, 5)
			if err != nil {
				return err
			}
			for _, query := range queries {
				currentRows, err := r.measureCacheEffect(ctx, session, dataset, query)
				if err != nil {
					return err
				}
				rows = append(rows, currentRows...)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp4_cache_effect.csv"),
		[]string{"dataset_label", "dataset_size", "query_id", "selectivity", "strategy", "mode", "total_10_rebucket_ms", "avg_per_rebucket_ms"},
		rows,
	)
}

func (r *BenchRunner) measureCacheEffect(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
) ([][]string, error) {
	seedSummary, err := streamMedian(ctx, session, dataset, Experiment4, query.ID+"-seed", func() *pb.GetBrowsingStateRequest {
		return query.Request(500, pb.HybridStrategy_AUTO, false, DefaultBucketConfig())
	})
	if err != nil {
		return nil, err
	}
	cachedTotal, err := sumRequestLatency(ctx, session, dataset, Experiment4, query, true)
	if err != nil {
		return nil, err
	}
	uncachedTotal, err := sumRequestLatency(ctx, session, dataset, Experiment4, query, false)
	if err != nil {
		return nil, err
	}
	return [][]string{
		{
			r.opts.DatasetLabel(dataset),
			r.opts.DatasetSizeLabel(dataset),
			query.ID,
			FormatFloat(query.ActualSelectivity),
			strategyLabel(seedSummary.Strategy),
			"cached",
			FormatFloat(cachedTotal),
			FormatFloat(cachedTotal / float64(len(RebucketVariants()))),
		},
		{
			r.opts.DatasetLabel(dataset),
			r.opts.DatasetSizeLabel(dataset),
			query.ID,
			FormatFloat(query.ActualSelectivity),
			strategyLabel(seedSummary.Strategy),
			"uncached",
			FormatFloat(uncachedTotal),
			FormatFloat(uncachedTotal / float64(len(RebucketVariants()))),
		},
	}, nil
}

func sumRequestLatency(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	query *BenchmarkQuery,
	rebucketOnly bool,
) (float64, error) {
	total := 0.0
	for index, bucketCfg := range RebucketVariants() {
		result, err := executeMeasuredStream(ctx, session, dataset, experiment, fmt.Sprintf("%s-r%d", query.ID, index), index, query.Request(500, pb.HybridStrategy_AUTO, rebucketOnly, bucketCfg))
		if err != nil {
			return 0, err
		}
		total += result.Summary.TTLB
	}
	return total, nil
}

func buildMixedQueries(
	ctx context.Context,
	db *sql.DB,
	model BenchmarkModel,
	limit int,
) ([]*BenchmarkQuery, error) {
	return BuildQuerySet(
		ctx,
		db,
		MixedBenchmarkStates(),
		model,
		mixedTargetSelectivities,
		limit,
		minMixedCandidateCount,
		"q",
	)
}

func defaultRuntimeSpec() benchindex.Spec {
	return benchindex.Spec{
		Type:          benchindex.HNSW,
		Precision:     benchindex.FullPrecision,
		IterativeMode: benchindex.IterativeOff,
	}
}

func filterEvents(events []BenchEvent, name string) []BenchEvent {
	out := make([]BenchEvent, 0, len(events))
	for _, event := range events {
		if event.Event == name {
			out = append(out, event)
		}
	}
	return out
}

func strategyLabel(value string) string {
	if value == "" {
		return "auto"
	}
	switch value {
	case "post_filter":
		return value
	case "pre_filter":
		return value
	case "hybrid":
		return value
	default:
		return strings.ToLower(value)
	}
}
