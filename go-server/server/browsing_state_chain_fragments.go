package main

import (
	"context"
	"fmt"
	"sync"

	pb "m3.dataloader/dataloader"
)

type browsingStateContextKey struct{}

type browsingStateFragmentCollector struct {
	mu        sync.Mutex
	fragments browsingStateFragments
}

func newBrowsingStateFragmentCollector() *browsingStateFragmentCollector {
	return &browsingStateFragmentCollector{
		fragments: browsingStateFragments{VectorDims: make(map[string]browsingStateVectorFragment)},
	}
}

func contextWithBrowsingStateLookup(ctx context.Context, lookup *browsingStateChainLookup) context.Context {
	if lookup == nil {
		return ctx
	}
	return context.WithValue(ctx, browsingStateContextKey{}, lookup)
}

func browsingStateLookupFromContext(ctx context.Context) *browsingStateChainLookup {
	lookup, _ := ctx.Value(browsingStateContextKey{}).(*browsingStateChainLookup)
	return lookup
}

func recordBrowsingStateCandidates(ctx context.Context, ids []int32) {
	lookup := browsingStateLookupFromContext(ctx)
	if lookup == nil || lookup.Collector == nil || len(ids) == 0 {
		return
	}
	lookup.Collector.mu.Lock()
	lookup.Collector.fragments.Candidates = append([]int32(nil), ids...)
	lookup.Collector.mu.Unlock()
}

func reusableBrowsingStateCandidates(ctx context.Context) ([]int32, bool) {
	lookup := browsingStateLookupFromContext(ctx)
	if lookup == nil || lookup.Fragments == nil {
		return nil, false
	}
	if !deltaCanReuseCandidates(lookup.Delta.Kind) || len(lookup.Fragments.Candidates) == 0 {
		return nil, false
	}
	appendBrowsingStateReusable(lookup, "candidates")
	return append([]int32(nil), lookup.Fragments.Candidates...), true
}

func browsingStateCandidateRestriction(ctx context.Context) ([]int, bool) {
	lookup := browsingStateLookupFromContext(ctx)
	if lookup == nil || lookup.Fragments == nil {
		return nil, false
	}
	if lookup.Delta.Kind != DeltaAddFilter || len(lookup.Fragments.Candidates) == 0 {
		return nil, false
	}
	appendBrowsingStateReusable(lookup, "candidates")
	return int32CandidatesToInt(lookup.Fragments.Candidates), true
}

func int32CandidatesToInt(ids []int32) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, int(id))
	}
	return out
}

func recordBrowsingStateVectorFragment(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
	result searchResult,
	bucketed *vectorDimensionResult,
	strategy HybridStrategy,
) {
	lookup := browsingStateLookupFromContext(ctx)
	if lookup == nil || lookup.Collector == nil || cfg == nil {
		return
	}
	key := browsingStateVectorFragmentKey(cfg, refHash, filterHash, result.Kind)
	fragment := browsingStateVectorFragment{
		RefHash:      refHash,
		FilterHash:   filterHash,
		Signature:    key,
		SearchResult: cloneSearchResult(result),
		Result:       cloneVectorDimensionResult(bucketed),
		Strategy:     strategy,
		Kind:         result.Kind,
	}
	lookup.Collector.mu.Lock()
	lookup.Collector.fragments.VectorDims[key] = fragment
	lookup.Collector.mu.Unlock()
}

func reusableBrowsingStateVectorFragment(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
	kind SearchKind,
) (browsingStateVectorFragment, bool) {
	lookup := browsingStateLookupFromContext(ctx)
	if lookup == nil || lookup.Fragments == nil {
		return browsingStateVectorFragment{}, false
	}
	if !deltaCanReuseVectorFragments(lookup.Delta.Kind) {
		return browsingStateVectorFragment{}, false
	}
	key := browsingStateVectorFragmentKey(cfg, refHash, filterHash, kind)
	fragment, ok := lookup.Fragments.VectorDims[key]
	if !ok {
		return browsingStateVectorFragment{}, false
	}
	appendBrowsingStateReusable(lookup, "knn_refs", "buckets")
	return cloneBrowsingStateVectorFragment(fragment), true
}

func markBrowsingStateCellGridReuse(lookup *browsingStateChainLookup) {
	if lookup == nil {
		return
	}
	if lookup.Collector != nil {
		lookup.Collector.mu.Lock()
		defer lookup.Collector.mu.Unlock()
	}
	lookup.Reusable = []string{"cellgrid"}
}

func markBrowsingStateVectorFragmentReuse(ctx context.Context) {
	appendBrowsingStateReusable(browsingStateLookupFromContext(ctx), "knn_refs", "buckets")
}

func appendBrowsingStateReusable(lookup *browsingStateChainLookup, stages ...string) {
	if lookup == nil {
		return
	}
	if lookup.Collector != nil {
		lookup.Collector.mu.Lock()
		defer lookup.Collector.mu.Unlock()
	}
	for _, stage := range stages {
		lookup.Reusable = appendUniqueString(lookup.Reusable, stage)
	}
}

func finalizeBrowsingStateObservation(lookup *browsingStateChainLookup) {
	if lookup == nil || lookup.Mode != chainLookupAncestorReuse || len(lookup.Reusable) > 0 {
		return
	}
	lookup.Mode = chainLookupColdMiss
	lookup.CachePath = string(chainLookupColdMiss)
}

func browsingStateVectorFragmentKey(cfg *pb.VectorSearchDimension, refHash uint64, filterHash uint64, kind SearchKind) string {
	key := newCacheKey(newCacheQueryFromConfig(cfg, refHash, filterHash))
	key.Kind = kind
	return fmt.Sprintf("%s|%d|%d|%s|%t|%d|%d", key.ModelName, key.RefHash, key.FilterHash, key.Kind.String(), key.IsRange, key.MinBits, key.MaxBits)
}

func mergeLookupFragments(lookup browsingStateChainLookup) *browsingStateFragments {
	merged := cloneBrowsingStateFragments(lookup.Fragments)
	if lookup.Collector == nil {
		return merged
	}
	lookup.Collector.mu.Lock()
	defer lookup.Collector.mu.Unlock()
	return mergeBrowsingStateFragments(merged, &lookup.Collector.fragments)
}

func mergeBrowsingStateFragments(left *browsingStateFragments, right *browsingStateFragments) *browsingStateFragments {
	if left == nil {
		left = &browsingStateFragments{VectorDims: make(map[string]browsingStateVectorFragment)}
	}
	if right == nil {
		return left
	}
	if len(right.Candidates) > 0 {
		left.Candidates = append([]int32(nil), right.Candidates...)
	}
	if left.VectorDims == nil {
		left.VectorDims = make(map[string]browsingStateVectorFragment)
	}
	for key, fragment := range right.VectorDims {
		left.VectorDims[key] = cloneBrowsingStateVectorFragment(fragment)
	}
	return left
}

func cloneBrowsingStateFragments(src *browsingStateFragments) *browsingStateFragments {
	if src == nil {
		return nil
	}
	cloned := &browsingStateFragments{Candidates: append([]int32(nil), src.Candidates...)}
	if len(src.VectorDims) > 0 {
		cloned.VectorDims = make(map[string]browsingStateVectorFragment, len(src.VectorDims))
		for key, fragment := range src.VectorDims {
			cloned.VectorDims[key] = cloneBrowsingStateVectorFragment(fragment)
		}
	}
	return cloned
}

func cloneBrowsingStateVectorFragment(src browsingStateVectorFragment) browsingStateVectorFragment {
	src.SearchResult = cloneSearchResult(src.SearchResult)
	src.Result = cloneVectorDimensionResult(src.Result)
	return src
}

func cloneVectorDimensionResult(src *vectorDimensionResult) *vectorDimensionResult {
	if src == nil {
		return nil
	}
	cloned := &vectorDimensionResult{
		AxisType:    src.AxisType,
		ParsedAxis:  src.ParsedAxis,
		AxisSQL:     src.AxisSQL,
		BucketInfos: cloneBucketInfos(src.BucketInfos),
	}
	if len(src.ParsedAxis.Ids) > 0 {
		cloned.ParsedAxis.Ids = make(map[int]int, len(src.ParsedAxis.Ids))
		for key, value := range src.ParsedAxis.Ids {
			cloned.ParsedAxis.Ids[key] = value
		}
	}
	if len(src.ObjectIDsByBucket) > 0 {
		cloned.ObjectIDsByBucket = make(map[int][]int, len(src.ObjectIDsByBucket))
		for key, ids := range src.ObjectIDsByBucket {
			cloned.ObjectIDsByBucket[key] = append([]int(nil), ids...)
		}
	}
	return cloned
}

func deltaCanReuseCandidates(kind DeltaKind) bool {
	switch kind {
	case DeltaAddVectorDim, DeltaRemoveVectorDim, DeltaChangeAxisNonVector, DeltaRebucketOnly, DeltaChangeForcedStrategy:
		return true
	default:
		return false
	}
}

func deltaCanReuseVectorFragments(kind DeltaKind) bool {
	switch kind {
	case DeltaAddFilter, DeltaAddVectorDim, DeltaRemoveVectorDim, DeltaChangeAxisNonVector, DeltaRebucketOnly, DeltaChangeForcedStrategy:
		return true
	default:
		return false
	}
}

func appendUniqueString(items []string, item string) []string {
	for _, existing := range items {
		if existing == item {
			return items
		}
	}
	return append(items, item)
}
