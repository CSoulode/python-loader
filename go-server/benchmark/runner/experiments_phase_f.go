package runner

import (
	"context"
	"fmt"
	"path/filepath"

	pb "m3.dataloader/dataloader"
)

const (
	phaseFMetadataTagA int32 = 101
	phaseFMetadataTagB int32 = 102
	phaseFMetadataTagC int32 = 103
	phaseFMetadataTagD int32 = 104
)

func (r *BenchRunner) RunExperiment10(ctx context.Context) error {
	rows := make([][]string, 0, len(r.opts.Datasets)*80)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			variants, err := r.buildPhaseFVariants(ctx, session)
			if err != nil {
				return err
			}
			for _, plan := range buildPhaseFSessionPlans(variants) {
				sessionRows, err := r.runPhaseFPerfSession(ctx, session, dataset, plan)
				if err != nil {
					return err
				}
				rows = append(rows, sessionRows...)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	header := []string{"session_type", "session_id", "request_idx", "bs_complexity", "vector_dim_count", "delta_kind", "lookup_path", "reused_fragments", "l0_hit", "l1_path_hit", "end_to_end_ms", "chain_nodes", "approx_memory_kb"}
	return WriteCSV(filepath.Join(r.paths.RawDir, "exp_f_chain_perf.csv"), header, rows)
}

func (r *BenchRunner) RunExperiment11(ctx context.Context) error {
	model, err := r.catalog.Default()
	if err != nil {
		return err
	}
	rows := make([][]string, 0, len(r.opts.Datasets)*4)
	for _, dataset := range r.opts.Datasets {
		err := r.withDefaultSession(ctx, dataset, func(session *ActiveSession) error {
			query, err := firstPhaseFQuery(ctx, session, model)
			if err != nil {
				return err
			}
			sessionRows, err := r.runPhaseFInvalidationSession(ctx, session, dataset, query, model)
			if err != nil {
				return err
			}
			rows = append(rows, sessionRows...)
			return nil
		})
		if err != nil {
			return err
		}
	}
	header := []string{"dataset_label", "dataset_size", "protocol", "scenario", "total_requests", "ancestor_etag_count", "not_modified_count", "fresh_response_count", "response_payload_bytes_saved", "invalidation_type", "dep_kind", "dep_key", "nodes_before", "nodes_invalidated", "nodes_survived", "survival_rate", "missed_invalidations"}
	return WriteCSV(filepath.Join(r.paths.RawDir, "exp_f_chain_invalidation.csv"), header, rows)
}

func (r *BenchRunner) runPhaseFPerfSession(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	plan phaseFSessionPlan,
) ([][]string, error) {
	client := newPhaseFBenchClient()
	localCache := newPhaseFLocalCache()
	if _, err := phaseFInvalidate(ctx, session, phaseFInvalidationRequest{Scope: "all"}); err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(plan.Requests))
	for index, item := range plan.Requests {
		if localCache.Has(item) {
			l0, err := client.L0Hit(item.key)
			if err != nil {
				return nil, err
			}
			rows = append(rows, phaseFPerfRow(phaseFPerfRowInput{
				Plan: plan, Index: index + 1, Item: item, Result: l0, Nodes: client.NodeCount(),
			}))
			continue
		}
		result, err := client.Execute(ctx, session, dataset, item)
		if err != nil {
			return nil, err
		}
		localCache.Put(item)
		rows = append(rows, phaseFPerfRow(phaseFPerfRowInput{
			Plan: plan, Index: index + 1, Item: item, Result: result, Nodes: client.NodeCount(),
		}))
	}
	return rows, nil
}

func (r *BenchRunner) runPhaseFInvalidationSession(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
	model BenchmarkModel,
) ([][]string, error) {
	client := newPhaseFBenchClient()
	for _, item := range phaseFInvalidationWarmups(query) {
		if _, err := client.Execute(ctx, session, dataset, item); err != nil {
			return nil, err
		}
	}
	beforeDep := client.NodeCount()
	single, err := client.RevalidateAll(ctx, session, dataset, 1, nil)
	if err != nil {
		return nil, err
	}
	multi, err := client.RevalidateAll(ctx, session, dataset, 4, nil)
	if err != nil {
		return nil, err
	}
	removedDep, err := phaseFInvalidate(ctx, session, phaseFInvalidationRequest{
		Scope: "dependency", Kind: "vector_model", ID: model.Name,
	})
	if err != nil {
		return nil, err
	}
	dep, err := client.RevalidateAll(ctx, session, dataset, 4, stringSet("vector-leaf"))
	if err != nil {
		return nil, err
	}
	allClient, err := warmPhaseFInvalidationClient(ctx, session, dataset, query)
	if err != nil {
		return nil, err
	}
	beforeAll := allClient.NodeCount()
	removedAll, err := phaseFInvalidate(ctx, session, phaseFInvalidationRequest{Scope: "all"})
	if err != nil {
		return nil, err
	}
	all, err := allClient.RevalidateAll(ctx, session, dataset, 4, allClient.keySet())
	if err != nil {
		return nil, err
	}
	return [][]string{
		phaseFInvalidationRow(newPhaseFInvalidationRowInput(r, dataset, "single_etag_revalidate_static", single, phaseFInvalidationCounts{Before: beforeDep})),
		phaseFInvalidationRow(newPhaseFInvalidationRowInput(r, dataset, "multi_ancestor_revalidate_static", multi, phaseFInvalidationCounts{Before: beforeDep})),
		phaseFInvalidationRow(phaseFInvalidationRowInput{
			Runner: r, Dataset: dataset, Scenario: "after_dependency_invalidate_static", Summary: dep,
			InvalidationType: "dependency", DepKind: "vector_model", DepKey: model.Name,
			Counts: phaseFInvalidationCounts{Before: beforeDep, Removed: removedDep},
		}),
		phaseFInvalidationRow(phaseFInvalidationRowInput{
			Runner: r, Dataset: dataset, Scenario: "after_invalidate_all_static", Summary: all,
			InvalidationType: "all", Counts: phaseFInvalidationCounts{Before: beforeAll, Removed: removedAll},
		}),
	}, nil
}

func warmPhaseFInvalidationClient(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	query *BenchmarkQuery,
) (*phaseFBenchClient, error) {
	client := newPhaseFBenchClient()
	for _, item := range phaseFInvalidationWarmups(query) {
		if _, err := client.Execute(ctx, session, dataset, item); err != nil {
			return nil, err
		}
	}
	return client, nil
}

type phaseFRequest struct {
	key            string
	parentKey      string
	tab            string
	complexity     string
	deltaKind      string
	vectorDimCount int
	req            *pb.GetBrowsingStateRequest
}

func phaseFInvalidationWarmups(query *BenchmarkQuery) []phaseFRequest {
	return []phaseFRequest{
		{key: "root", req: phaseFMetadataRequest(query)},
		{key: "metadata-tag-a", parentKey: "root", req: phaseFMetadataFilterRequest(query, phaseFMetadataTagA)},
		{key: "metadata-tag-b", parentKey: "metadata-tag-a", req: phaseFMetadataFilterRequest(query, phaseFMetadataTagB)},
		{key: "metadata-tag-c", parentKey: "metadata-tag-b", req: phaseFMetadataFilterRequest(query, phaseFMetadataTagC)},
		{key: "metadata-tag-d", parentKey: "metadata-tag-c", req: phaseFMetadataFilterRequest(query, phaseFMetadataTagD)},
		{key: "vector-leaf", parentKey: "root", req: query.Request(100, pb.HybridStrategy_AUTO, false, DefaultBucketConfig())},
	}
}

func phaseFMetadataRequest(query *BenchmarkQuery) *pb.GetBrowsingStateRequest {
	req := query.Request(100, pb.HybridStrategy_AUTO, false, nil)
	req.VectorDimension = nil
	req.VectorDimensions = nil
	req.VectorBucketId = nil
	req.VectorBucketIds = nil
	req.RebucketOnly = false
	return req
}

func phaseFMetadataFilterRequest(query *BenchmarkQuery, tagID int32) *pb.GetBrowsingStateRequest {
	req := phaseFMetadataRequest(query)
	req.Filters = append(req.Filters, &pb.AxisFilter{
		AxisFilterType: pb.AxisType_FILTER,
		Value:          tagID,
		ValueType:      pb.FilterValueType_TAG,
	})
	return req
}

func phaseFBucketConfig(count int32) *pb.BucketConfig {
	cfg := DefaultBucketConfig()
	cfg.Count = count
	return cfg
}

func firstPhaseFQuery(ctx context.Context, session *ActiveSession, model BenchmarkModel) (*BenchmarkQuery, error) {
	queries, err := buildMixedQueries(ctx, session.DB, model, 1)
	if err != nil {
		return nil, err
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("no phase F benchmark query available")
	}
	return queries[0], nil
}
