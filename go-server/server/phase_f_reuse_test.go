package main

import (
	"context"
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
)

func TestPhaseFDeltaInferenceCoversSafeReuseTable(t *testing.T) {
	parent := phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"})
	addFilter := phaseFSnapshot("axis-a", "filter-b", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"})
	addFilter.FilterCount = parent.FilterCount + 1
	testCases := []struct {
		name   string
		target *BrowsingStateSnapshot
		want   DeltaKind
	}{
		{"add_filter", addFilter, DeltaAddFilter},
		{"remove_filter", phaseFSnapshot("axis-a", "", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"}), DeltaRemoveFilter},
		{"change_axis", phaseFSnapshot("axis-b", "filter-a", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"}), DeltaChangeAxisNonVector},
		{"add_vector", phaseFSnapshot("axis-a", "filter-a", "vector-b", "bucket-a", "auto", map[string]string{"x": "vx", "y": "vy"}), DeltaAddVectorDim},
		{"remove_vector", phaseFSnapshot("axis-a", "filter-a", "vector-c", "bucket-a", "auto", nil), DeltaRemoveVectorDim},
		{"rebucket", phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-b", "auto", map[string]string{"x": "vx"}), DeltaRebucketOnly},
		{"strategy", phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "prefilter", map[string]string{"x": "vx"}), DeltaChangeForcedStrategy},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := inferTransitionDelta(parent, tc.target, TransitionDelta{Kind: DeltaRoot})
			if got.Kind != tc.want {
				t.Fatalf("delta = %s, want %s", got.Kind, tc.want)
			}
		})
	}
}

func TestPhaseFLookupReusesFragmentsForSafeDeltas(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parentSnapshot := phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"})
	parent := browsingStateChainLookup{TargetID: "parent", Snapshot: parentSnapshot}
	parent.Fragments = phaseFFragments()
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	target := browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Snapshot: phaseFSnapshot("axis-a", "filter-b", "vector-a", "bucket-a", "auto", map[string]string{"x": "vx"}),
	}
	target.Snapshot.FilterCount = parentSnapshot.FilterCount + 1
	target.Snapshot.FilterItems = append(target.Snapshot.FilterItems, parentSnapshot.FilterItems...)
	lookup := chain.Lookup(target)
	if lookup.Mode != chainLookupAncestorReuse {
		t.Fatalf("mode = %s, want ancestor_reuse", lookup.Mode)
	}
	if len(lookup.Fragments.Candidates) != 3 {
		t.Fatalf("candidate restriction ids = %d, want 3", len(lookup.Fragments.Candidates))
	}
	if len(lookup.Fragments.VectorDims) != 0 {
		t.Fatalf("vector fragments = %d, want 0", len(lookup.Fragments.VectorDims))
	}
}

func TestPhaseFLookupRejectsAddFilterReplacementReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parentSnapshot := phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", nil)
	parent := browsingStateChainLookup{TargetID: "parent", Snapshot: parentSnapshot}
	parent.Fragments = phaseFFragments()
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	target := browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Snapshot: phaseFSnapshot("axis-a", "filter-b", "vector-a", "bucket-a", "auto", nil),
	}
	target.Snapshot.FilterCount = parentSnapshot.FilterCount + 1
	target.Snapshot.FilterItems = []string{"filter-b", "filter-c"}

	lookup := chain.Lookup(target)
	if lookup.Mode != chainLookupColdMiss {
		t.Fatalf("mode = %s, want cold_miss", lookup.Mode)
	}
}

func TestPhaseFChangeAxisCandidatesRequireSameAxisDomain(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parentSnapshot := phaseFSnapshot("axis-x", "filter-a", "vector-a", "bucket-a", "auto", nil)
	parentSnapshot.AxisDomainKey = "tagset=1"
	parent := browsingStateChainLookup{TargetID: "parent", Snapshot: parentSnapshot}
	parent.Fragments = phaseFFragments()
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	target := browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Snapshot: phaseFSnapshot("axis-y", "filter-a", "vector-a", "bucket-a", "auto", nil),
	}
	target.Snapshot.AxisDomainKey = "tagset=2"

	lookup := chain.Lookup(target)
	if len(lookup.Fragments.Candidates) != 0 {
		t.Fatalf("candidate fragments = %v, want none", lookup.Fragments.Candidates)
	}
}

func TestPhaseFLookupRejectsRemoveFilterReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := browsingStateChainLookup{
		TargetID: "parent",
		Snapshot: phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", nil),
	}
	parent.Fragments = phaseFFragments()
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	lookup := chain.Lookup(browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Snapshot: phaseFSnapshot("axis-a", "", "vector-a", "bucket-a", "auto", nil),
	})
	if lookup.Mode != chainLookupColdMiss {
		t.Fatalf("mode = %s, want cold_miss", lookup.Mode)
	}
}

func TestPhaseFLookupRejectsForcedStrategyReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := browsingStateChainLookup{
		TargetID:  "parent",
		Snapshot:  phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", nil),
		Fragments: phaseFFragments(),
	}
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	lookup := chain.Lookup(browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Snapshot: phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "pre_filter", nil),
	})
	if lookup.Mode != chainLookupColdMiss {
		t.Fatalf("mode = %s, want cold_miss", lookup.Mode)
	}
	if lookup.FallbackReason != "forced_strategy_changes_vector_semantics" {
		t.Fatalf("fallback reason = %q", lookup.FallbackReason)
	}
}

func TestPhaseFRequestedRebucketDoesNotMaskForcedStrategyChange(t *testing.T) {
	parent := phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-a", "auto", nil)
	target := phaseFSnapshot("axis-a", "filter-a", "vector-a", "bucket-b", "pre_filter", nil)

	got := inferTransitionDelta(parent, target, TransitionDelta{Kind: DeltaRebucketOnly})
	if got.Kind != DeltaChangeForcedStrategy {
		t.Fatalf("delta = %s, want %s", got.Kind, DeltaChangeForcedStrategy)
	}
}

func TestPhaseFLookupRejectsRebucketForcedStrategyReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := browsingStateChainLookup{
		TargetID: "parent",
		Snapshot: phaseFSnapshot(
			"axis-a",
			"filter-a",
			"vector-a",
			"bucket-a",
			"auto",
			map[string]string{"x": "vx"},
		),
		Fragments: phaseFFragments(),
	}
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})

	lookup := chain.Lookup(browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		Delta:    TransitionDelta{Kind: DeltaRebucketOnly},
		Snapshot: phaseFSnapshot(
			"axis-a",
			"filter-a",
			"vector-a",
			"bucket-b",
			"pre_filter",
			map[string]string{"x": "vx"},
		),
	})
	if lookup.Mode != chainLookupColdMiss {
		t.Fatalf("mode = %s, want cold_miss", lookup.Mode)
	}
	if lookup.FallbackReason != "forced_strategy_changes_vector_semantics" {
		t.Fatalf("fallback reason = %q", lookup.FallbackReason)
	}
}

func TestPhaseFRemoveFilterResetsDependencyLineage(t *testing.T) {
	dep := browsingStateDependency{Kind: browsingStateDepTag, ID: "101"}
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(
		browsingStateChainLookup{TargetID: "parent", Dependencies: []browsingStateDependency{dep}, NodeDependencies: []browsingStateDependency{dep}},
		&bsCellGrid{HTTPBody: []byte("parent")},
	)
	chain.PublishNode(
		browsingStateChainLookup{TargetID: "target", ParentID: "parent", Delta: TransitionDelta{Kind: DeltaRemoveFilter}},
		&bsCellGrid{HTTPBody: []byte("target")},
	)

	if removed := chain.InvalidateDependency(dep); removed != 1 {
		t.Fatalf("removed = %d, want only parent", removed)
	}
	if got := chain.NodeCount(); got != 1 {
		t.Fatalf("node count = %d, want target survivor", got)
	}
}

func TestPhaseFAddFilterUsesCandidatesOnlyAsRestriction(t *testing.T) {
	lookup := &browsingStateChainLookup{
		Delta:     TransitionDelta{Kind: DeltaAddFilter},
		Fragments: phaseFFragments(),
	}
	ctx := contextWithBrowsingStateLookup(context.Background(), lookup)
	if _, ok := reusableBrowsingStateCandidates(ctx); ok {
		t.Fatalf("add-filter must not directly reuse parent candidates")
	}
	ids, ok := browsingStateCandidateRestriction(ctx)
	if !ok || len(ids) != 3 {
		t.Fatalf("candidate restriction = %v %v, want 3 ids", ids, ok)
	}
	if len(lookup.Reusable) != 1 || lookup.Reusable[0] != "candidates" {
		t.Fatalf("reusable = %v, want candidates", lookup.Reusable)
	}
}

func TestPhaseFVectorFragmentReuseRecordsObservableStages(t *testing.T) {
	cfg := &pb.VectorSearchDimension{ModelName: "siglip"}
	key := browsingStateVectorFragmentKey(cfg, 10, 20, searchKindGlobalKNN)
	lookup := &browsingStateChainLookup{
		Delta: TransitionDelta{Kind: DeltaRebucketOnly},
		Fragments: &browsingStateFragments{
			VectorDims: map[string]browsingStateVectorFragment{
				key: {Signature: key, SearchResult: searchResult{RawNeighbors: []Neighbor{{ObjectID: 1}}}},
			},
		},
	}
	ctx := contextWithBrowsingStateLookup(context.Background(), lookup)

	_, ok := reusableBrowsingStateVectorFragment(ctx, cfg, 10, 20, searchKindGlobalKNN)
	if !ok {
		t.Fatalf("expected vector fragment reuse")
	}
	if len(lookup.Reusable) != 2 || lookup.Reusable[0] != "knn_refs" || lookup.Reusable[1] != "buckets" {
		t.Fatalf("reusable = %v, want knn_refs,buckets", lookup.Reusable)
	}
}

func TestPhaseFAncestorObservationWithoutConsumedFragmentsIsColdMiss(t *testing.T) {
	lookup := &browsingStateChainLookup{
		Mode:      chainLookupAncestorReuse,
		CachePath: string(chainLookupAncestorReuse),
	}

	finalizeBrowsingStateObservation(lookup)

	if lookup.CachePath != string(chainLookupColdMiss) {
		t.Fatalf("cache path = %s, want cold_miss", lookup.CachePath)
	}
}

func TestPhaseFInflightFollowerReceivesPublishedNode(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	leader, _ := chain.StartInflight("same-target")
	if !leader {
		t.Fatalf("first caller should be leader")
	}
	follower, wait := chain.StartInflight("same-target")
	if follower {
		t.Fatalf("second caller should wait")
	}
	node := &StateNode{ID: "target", CellGrid: &bsCellGrid{HTTPBody: []byte("ok")}}
	go chain.FinishInflight("same-target", node, nil)
	got, err := wait()
	if err != nil {
		t.Fatalf("wait error: %v", err)
	}
	if got == nil || got.ID != "target" {
		t.Fatalf("node = %#v, want target", got)
	}
}

func phaseFSnapshot(axis string, filter string, vector string, bucket string, strategy string, dims map[string]string) *BrowsingStateSnapshot {
	deps := []browsingStateDependency{{Kind: browsingStateDepTagset, ID: "1"}}
	if filter == "" {
		deps = nil
	}
	return &BrowsingStateSnapshot{
		AxisKey:         axis,
		AxisDomainKey:   axis,
		FilterKey:       filter,
		FilterCount:     len(deps),
		FilterItems:     phaseFFilterItems(filter),
		MetadataKey:     axis + "|" + filter,
		VectorSearchKey: vector,
		VectorBucketKey: bucket,
		StrategyKey:     strategy,
		VectorDims:      dims,
		Dependencies:    deps,
	}
}

func phaseFFilterItems(filter string) []string {
	if filter == "" {
		return nil
	}
	return []string{filter}
}

func phaseFFragments() *browsingStateFragments {
	return &browsingStateFragments{
		Candidates: []int32{1, 2, 3},
		VectorDims: map[string]browsingStateVectorFragment{
			"sig": {Signature: "sig", SearchResult: searchResult{RawNeighbors: []Neighbor{{ObjectID: 1}}}},
		},
	}
}
