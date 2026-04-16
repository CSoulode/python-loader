package runner

import (
	"context"
	"path/filepath"
)

func (r *BenchRunner) RunExperiment10(ctx context.Context) error {
	rows := make([][]string, 0, 80)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			variants, err := r.buildPhaseFVariants(ctx, session)
			if err != nil {
				return err
			}
			for _, plan := range buildPhaseFSessionPlans(variants) {
				currentRows, err := r.runPhaseFSession(ctx, session, dataset, Experiment10, plan)
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
		filepath.Join(r.paths.RawDir, "exp10_bs_cache_performance.csv"),
		[]string{
			"dataset_label",
			"dataset_size",
			"protocol",
			"session_type",
			"session_id",
			"request_idx",
			"request_kind",
			"bs_complexity",
			"vector_dim_count",
			"l0_hit",
			"l1_hit",
			"grpc_not_modified",
			"end_to_end_ms",
			"l0_latency_us",
			"l1_hit_latency_ms",
			"l1_miss_latency_ms",
			"l1_cache_entries",
			"l1_cache_memory_kb",
		},
		rows,
	)
}

func (r *BenchRunner) RunExperiment11(ctx context.Context) error {
	rows := make([][]string, 0, len(r.opts.Datasets)*3)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			variants, err := r.buildPhaseFVariants(ctx, session)
			if err != nil {
				return err
			}
			requests := buildPhaseFScenarioRequests(variants)
			outcomes, err := r.runPhaseFScenarioMatrix(ctx, session, dataset, requests)
			if err != nil {
				return err
			}
			for _, outcome := range outcomes {
				rows = append(rows, buildPhaseFExperiment11Row(r, dataset, outcome))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "exp11_conditional_requests.csv"),
		[]string{
			"dataset_label",
			"dataset_size",
			"protocol",
			"scenario",
			"total_requests",
			"not_modified_count",
			"fresh_response_count",
			"response_payload_bytes_saved",
			"invalidation_type",
			"entries_before",
			"entries_invalidated",
			"entries_survived",
			"survival_rate",
		},
		rows,
	)
}

func (r *BenchRunner) runPhaseFScenarioMatrix(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	requests []phaseFVariant,
) ([]phaseFScenarioOutcome, error) {
	ttlOutcome, err := r.runPhaseFTTLScenario(ctx, session, dataset, requests)
	if err != nil {
		return nil, err
	}
	allOutcome, err := r.runPhaseFInvalidateAllScenario(ctx, session, dataset, requests)
	if err != nil {
		return nil, err
	}
	dependencyOutcome, err := r.runPhaseFDependencyScenario(ctx, session, dataset, requests)
	if err != nil {
		return nil, err
	}
	return []phaseFScenarioOutcome{ttlOutcome, allOutcome, dependencyOutcome}, nil
}
