package runner

import (
	"context"
	"fmt"
	"time"
)

type phaseFScenarioOutcome struct {
	Name               string
	InvalidationType   string
	EntriesBefore      int
	EntriesInvalidated int
	EntriesSurvived    int
	TotalRequests      int
	NotModifiedCount   int
	FreshResponseCount int
	ResponseBytesSaved int
}

func buildPhaseFScenarioRequests(variants []phaseFVariant) []phaseFVariant {
	lookup := buildPhaseFVariantLookup(variants)
	return []phaseFVariant{
		lookup["LW-0d"],
		lookup["LW-1d"],
		lookup["GH-1d"],
		lookup["RH-2d"],
		lookup["HW-0d"],
		lookup["GH-2d"],
	}
}

func (r *BenchRunner) runPhaseFTTLScenario(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	requests []phaseFVariant,
) (phaseFScenarioOutcome, error) {
	cache, now := newPhaseFScenarioCache()
	if _, err := warmPhaseFScenario(ctx, session, dataset, requests, cache); err != nil {
		return phaseFScenarioOutcome{}, err
	}
	*now = (*now).Add(6 * time.Minute)
	return r.revalidatePhaseFScenario(ctx, session, dataset, "ttl_revalidate_static", "ttl", requests, cache)
}

func (r *BenchRunner) runPhaseFInvalidateAllScenario(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	requests []phaseFVariant,
) (phaseFScenarioOutcome, error) {
	cache, _ := newPhaseFScenarioCache()
	stats, err := warmPhaseFScenario(ctx, session, dataset, requests, cache)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	invalidated, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", "")
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	afterInvalidation, err := ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	return r.revalidateWithInvalidation(
		ctx,
		session,
		dataset,
		"after_invalidate_all_static",
		"all",
		requests,
		cache,
		stats.Entries,
		invalidated,
		afterInvalidation.Entries,
	)
}

func (r *BenchRunner) runPhaseFDependencyScenario(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	requests []phaseFVariant,
) (phaseFScenarioOutcome, error) {
	cache, _ := newPhaseFScenarioCache()
	stats, err := warmPhaseFScenario(ctx, session, dataset, requests, cache)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	invalidated, err := InvalidateBrowsingStateCache(
		ctx,
		session.Runtime.Ports.ServerHTTP,
		"dependency",
		"vector_model",
		"hsv",
	)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	afterInvalidation, err := ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	return r.revalidateWithInvalidation(
		ctx,
		session,
		dataset,
		"after_dependency_invalidate_static",
		"dependency",
		requests,
		cache,
		stats.Entries,
		invalidated,
		afterInvalidation.Entries,
	)
}

func newPhaseFScenarioCache() (*phaseFL0Cache, *time.Time) {
	cache := newPhaseFL0Cache()
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	return cache, &now
}

func warmPhaseFScenario(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	requests []phaseFVariant,
	cache *phaseFL0Cache,
) (BrowsingStateCacheStats, error) {
	if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
		return BrowsingStateCacheStats{}, err
	}
	cache.Clear()
	for index, request := range requests {
		if err := warmPhaseFRequest(ctx, session, dataset, request, cache, index); err != nil {
			return BrowsingStateCacheStats{}, err
		}
	}
	return ReadBrowsingStateCacheStats(ctx, session.Runtime.Ports.ServerHTTP)
}

func warmPhaseFRequest(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	request phaseFVariant,
	cache *phaseFL0Cache,
	index int,
) error {
	reqCtx, cancel := withTimeout(ctx)
	defer cancel()
	result, err := ExecuteSSBRequestWithOptions(
		reqCtx,
		session.DataLoader,
		clonePhaseFRequest(request.Request),
		ExecuteRequestOptions{BenchID: benchRunID(dataset, Experiment11, fmt.Sprintf("phasef11-warm-%02d", index), index)},
	)
	if err != nil {
		return err
	}
	etag := metadataFirstValue(result.Headers, "etag")
	if etag == "" {
		return fmt.Errorf("phase F warm request missing etag")
	}
	cache.Put(request.CacheState, etag, result.PayloadBytes)
	return nil
}

func (r *BenchRunner) revalidateWithInvalidation(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	name string,
	invalidationType string,
	requests []phaseFVariant,
	cache *phaseFL0Cache,
	entriesBefore int,
	entriesInvalidated int,
	entriesSurvived int,
) (phaseFScenarioOutcome, error) {
	outcome, err := r.revalidatePhaseFScenario(ctx, session, dataset, name, invalidationType, requests, cache)
	if err != nil {
		return phaseFScenarioOutcome{}, err
	}
	outcome.EntriesBefore = entriesBefore
	outcome.EntriesInvalidated = entriesInvalidated
	outcome.EntriesSurvived = entriesSurvived
	return outcome, nil
}

func (r *BenchRunner) revalidatePhaseFScenario(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	name string,
	invalidationType string,
	requests []phaseFVariant,
	cache *phaseFL0Cache,
) (phaseFScenarioOutcome, error) {
	outcome := phaseFScenarioOutcome{
		Name:               name,
		InvalidationType:   invalidationType,
		EntriesBefore:      -1,
		EntriesInvalidated: -1,
		EntriesSurvived:    -1,
	}
	for index, request := range requests {
		stale, ok := cache.GetStale(request.CacheState)
		if !ok {
			return phaseFScenarioOutcome{}, fmt.Errorf("missing stale cache entry for %s", request.ID)
		}
		result, err := revalidatePhaseFRequest(ctx, session, dataset, request, stale, index)
		if err != nil {
			return phaseFScenarioOutcome{}, err
		}
		outcome.TotalRequests++
		if result.NotModified {
			outcome.NotModifiedCount++
			outcome.ResponseBytesSaved += stale.PayloadBytes
			cache.RefreshTTL(request.CacheState)
			continue
		}
		outcome.FreshResponseCount++
		etag := metadataFirstValue(result.Headers, "etag")
		cache.Put(request.CacheState, etag, result.PayloadBytes)
	}
	return outcome, nil
}

func revalidatePhaseFRequest(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	request phaseFVariant,
	stale phaseFL0StaleEntry,
	index int,
) (*ExecutedSSBRequest, error) {
	reqCtx, cancel := withTimeout(ctx)
	defer cancel()
	return ExecuteSSBRequestWithOptions(
		reqCtx,
		session.DataLoader,
		clonePhaseFRequest(request.Request),
		ExecuteRequestOptions{
			BenchID:     benchRunID(dataset, Experiment11, fmt.Sprintf("phasef11-revalidate-%02d", index), index),
			IfNoneMatch: stale.ETag,
		},
	)
}

func buildPhaseFExperiment11Row(
	r *BenchRunner,
	dataset DatasetID,
	outcome phaseFScenarioOutcome,
) []string {
	return []string{
		r.opts.DatasetLabel(dataset),
		r.opts.DatasetSizeLabel(dataset),
		phaseFProtocolGRPC,
		outcome.Name,
		fmt.Sprintf("%d", outcome.TotalRequests),
		fmt.Sprintf("%d", outcome.NotModifiedCount),
		fmt.Sprintf("%d", outcome.FreshResponseCount),
		fmt.Sprintf("%d", outcome.ResponseBytesSaved),
		outcome.InvalidationType,
		formatOptionalInt(outcome.EntriesBefore),
		formatOptionalInt(outcome.EntriesInvalidated),
		formatOptionalInt(outcome.EntriesSurvived),
		formatOptionalRatio(outcome),
	}
}

func formatOptionalInt(value int) string {
	if value < 0 {
		return ""
	}
	return fmt.Sprintf("%d", value)
}

func formatOptionalRatio(outcome phaseFScenarioOutcome) string {
	if outcome.EntriesBefore < 0 {
		return ""
	}
	return FormatFloat(float64(outcome.EntriesSurvived) / float64(outcome.EntriesBefore))
}
