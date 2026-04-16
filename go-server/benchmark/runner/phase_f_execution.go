package runner

import (
	"context"
	"fmt"
	"time"
)

const phaseFProtocolGRPC = "grpc"

func (r *BenchRunner) runPhaseFSession(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	plan phaseFSessionPlan,
) ([][]string, error) {
	if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
		return nil, err
	}
	clearPhaseFSessionCaches(plan)

	rows := make([][]string, 0, len(plan.Requests))
	for index, request := range plan.Requests {
		row, err := r.executePhaseFPlannedRequest(
			ctx,
			session,
			dataset,
			experiment,
			plan,
			index,
			request,
		)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func clearPhaseFSessionCaches(plan phaseFSessionPlan) {
	seen := make(map[*phaseFL0Cache]bool)
	for _, request := range plan.Requests {
		if request.Cache == nil || seen[request.Cache] {
			continue
		}
		request.Cache.Clear()
		seen[request.Cache] = true
	}
}

func (r *BenchRunner) executePhaseFPlannedRequest(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	plan phaseFSessionPlan,
	index int,
	request phaseFPlannedRequest,
) ([]string, error) {
	l0Start := time.Now()
	if _, ok := request.Cache.Get(request.CacheState); ok {
		stats, err := ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
		if err != nil {
			return nil, err
		}
		return buildPhaseFExperiment10Row(
			r,
			dataset,
			plan,
			index,
			request,
			true,
			false,
			false,
			time.Since(l0Start),
			stats,
		), nil
	}

	before, err := ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
	if err != nil {
		return nil, err
	}
	stale, hasStale := request.Cache.GetStale(request.CacheState)
	result, err := executePhaseFNetworkRequest(
		ctx,
		session,
		dataset,
		experiment,
		plan,
		index,
		request,
		stale,
		hasStale,
	)
	if err != nil {
		return nil, err
	}
	after, err := ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
	if err != nil {
		return nil, err
	}
	if err := updatePhaseFL0Cache(request, stale, hasStale, result); err != nil {
		return nil, err
	}
	return buildPhaseFExperiment10NetworkRow(
		r,
		dataset,
		plan,
		index,
		request,
		before,
		after,
		result,
	), nil
}

func executePhaseFNetworkRequest(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	plan phaseFSessionPlan,
	index int,
	request phaseFPlannedRequest,
	stale phaseFL0StaleEntry,
	hasStale bool,
) (*ExecutedSSBRequest, error) {
	reqCtx, cancel := withTimeout(ctx)
	defer cancel()
	options := ExecuteRequestOptions{
		BenchID: phaseFBenchID(dataset, experiment, plan, index),
	}
	if hasStale {
		options.IfNoneMatch = stale.ETag
	}
	return ExecuteSSBRequestWithOptions(reqCtx, session.DataLoader, request.Request, options)
}

func updatePhaseFL0Cache(
	request phaseFPlannedRequest,
	stale phaseFL0StaleEntry,
	hasStale bool,
	result *ExecutedSSBRequest,
) error {
	if request.Cache == nil || result == nil {
		return nil
	}
	if result.NotModified {
		if hasStale {
			request.Cache.RefreshTTL(request.CacheState)
		}
		return nil
	}

	etag := metadataFirstValue(result.Headers, "etag")
	if etag == "" {
		return fmt.Errorf("phase F request missing etag header")
	}
	request.Cache.Put(request.CacheState, etag, result.PayloadBytes)
	return nil
}

func buildPhaseFExperiment10Row(
	r *BenchRunner,
	dataset DatasetID,
	plan phaseFSessionPlan,
	index int,
	request phaseFPlannedRequest,
	l0Hit bool,
	l1Hit bool,
	grpcNotModified bool,
	latency time.Duration,
	stats BrowsingStateCacheStats,
) []string {
	return []string{
		r.opts.DatasetLabel(dataset),
		r.opts.DatasetSizeLabel(dataset),
		phaseFProtocolGRPC,
		plan.Type,
		plan.ID,
		fmt.Sprintf("%d", index+1),
		request.Kind,
		request.Variant.Complexity,
		fmt.Sprintf("%d", request.Variant.VectorDimCount),
		formatBool(l0Hit),
		formatBool(l1Hit),
		formatBool(grpcNotModified),
		FormatFloat(durationToMS(latency)),
		FormatFloat(float64(latency.Microseconds())),
		"",
		"",
		fmt.Sprintf("%d", stats.Entries),
		FormatFloat(float64(stats.ApproxMemoryBytes) / 1024.0),
	}
}

func buildPhaseFExperiment10NetworkRow(
	r *BenchRunner,
	dataset DatasetID,
	plan phaseFSessionPlan,
	index int,
	request phaseFPlannedRequest,
	before BrowsingStateCacheStats,
	after BrowsingStateCacheStats,
	result *ExecutedSSBRequest,
) []string {
	row := buildPhaseFExperiment10Row(
		r,
		dataset,
		plan,
		index,
		request,
		false,
		after.Hits > before.Hits,
		after.GRPCNotModified > before.GRPCNotModified || result.NotModified,
		result.EndToEnd,
		after,
	)
	if row[9] == "true" {
		row[13] = ""
		row[14] = FormatFloat(durationToMS(result.EndToEnd))
		return row
	}
	row[15] = FormatFloat(durationToMS(result.EndToEnd))
	return row
}

func metadataFirstValue(headers map[string][]string, key string) string {
	values := headers[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func phaseFBenchID(
	dataset DatasetID,
	experiment ExperimentID,
	plan phaseFSessionPlan,
	index int,
) string {
	return benchRunID(dataset, experiment, fmt.Sprintf("%s-%s-r%02d", plan.Type, plan.ID, index+1), index)
}

func formatBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
