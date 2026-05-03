package runner

import (
	"strconv"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

func TestPhaseFInvalidationRowUsesKeyLevelMisses(t *testing.T) {
	summary := phaseFInvalidationSummary{
		Total:               6,
		AncestorCount:       4,
		NotModified:         5,
		Fresh:               1,
		PayloadSaved:        123,
		MissedInvalidations: 1,
	}
	counts := phaseFInvalidationCounts{Before: 6, Removed: 1}
	catalog, err := ParseDatasetCatalog("pilot_94k:94346:BENCH_DB_URL_PILOT_94K")
	if err != nil {
		t.Fatalf("dataset catalog: %v", err)
	}
	runner := &BenchRunner{opts: Options{DatasetCatalog: catalog}}

	row := phaseFInvalidationRow(phaseFInvalidationRowInput{
		Runner: runner, Dataset: DatasetPilot94, Scenario: "after_dependency_invalidate_static",
		Summary: summary, InvalidationType: "dependency", DepKind: "vector_model",
		DepKey: "siglip2", Counts: counts,
	})

	if got := row[len(row)-1]; got != "1" {
		t.Fatalf("missed_invalidations = %s, want 1", got)
	}
	if got := row[len(row)-2]; got != "0.833333" {
		t.Fatalf("survival_rate = %s, want 0.833333", got)
	}
}

func TestPhaseFBenchClientKeySet(t *testing.T) {
	client := newPhaseFBenchClient()
	client.keyToID["b"] = "state-b"
	client.keyToID["a"] = "state-a"

	set := client.keySet()

	if _, ok := set["a"]; !ok {
		t.Fatalf("key set missing a")
	}
	if _, ok := set["b"]; !ok {
		t.Fatalf("key set missing b")
	}
}

func TestPhaseFBenchResultUsesObservedServerMetadata(t *testing.T) {
	header := metadata.Pairs(
		grpcCachePath, "cold_miss",
		grpcDeltaKind, "Root",
		grpcReusedFragments, "candidates",
	)
	trailer := metadata.Pairs(
		grpcCachePath, "ancestor_reuse",
		grpcDeltaKind, "AddVectorDim",
		grpcReusedFragments, "knn_refs,buckets",
	)

	result, err := phaseFBenchResultFromMetadata(header, trailer, 12.5)
	if err != nil {
		t.Fatalf("metadata result: %v", err)
	}
	if result.DeltaKind != "AddVectorDim" {
		t.Fatalf("delta = %s, want AddVectorDim", result.DeltaKind)
	}
	if result.Reusable != "knn_refs,buckets" {
		t.Fatalf("reused = %s, want knn_refs,buckets", result.Reusable)
	}
}

func TestPhaseFBenchResultRequiresObservationMetadata(t *testing.T) {
	_, err := phaseFBenchResultFromMetadata(metadata.Pairs(grpcCachePath, "cold_miss"), nil, 1)
	if err == nil {
		t.Fatalf("expected missing metadata error")
	}
}

func TestPhaseFBenchResultAcceptsEmptyReusedFragments(t *testing.T) {
	md := metadata.Pairs(
		grpcCachePath, "cold_miss",
		grpcDeltaKind, "Root",
		grpcReusedFragments, "",
	)
	result, err := phaseFBenchResultFromMetadata(md, nil, 1)
	if err != nil {
		t.Fatalf("metadata result: %v", err)
	}
	if result.Reusable != "" {
		t.Fatalf("reused = %q, want empty", result.Reusable)
	}
}

func TestPhaseFSessionPlansCoverRequiredV2Scenarios(t *testing.T) {
	plans := buildPhaseFSessionPlans(fakePhaseFVariants())

	if len(plans) != 4 {
		t.Fatalf("plans = %d, want 4", len(plans))
	}
	assertPhaseFPlan(t, plans, phaseFPlanExpectation{SessionType: "linear_explore", DeltaKind: "AddFilter"})
	assertPhaseFPlan(t, plans, phaseFPlanExpectation{SessionType: "rebucket_tuning", DeltaKind: "RebucketOnly"})
	assertPhaseFBacktrackHasL0Candidate(t, plans)
	assertPhaseFMultiTabHasSharedL1Candidate(t, plans)
}

type phaseFPlanExpectation struct {
	SessionType string
	DeltaKind   string
}

func assertPhaseFPlan(t *testing.T, plans []phaseFSessionPlan, expectation phaseFPlanExpectation) {
	t.Helper()
	for _, plan := range plans {
		if plan.Type != expectation.SessionType {
			continue
		}
		for _, request := range plan.Requests {
			if request.deltaKind == expectation.DeltaKind {
				return
			}
		}
	}
	t.Fatalf("session %s missing delta %s", expectation.SessionType, expectation.DeltaKind)
}

func assertPhaseFBacktrackHasL0Candidate(t *testing.T, plans []phaseFSessionPlan) {
	t.Helper()
	seen := make(map[string]bool)
	for _, request := range phaseFPlanByType(plans, "backtrack_heavy").Requests {
		if seen[request.key] {
			return
		}
		seen[request.key] = true
	}
	t.Fatalf("backtrack_heavy has no repeated local key")
}

func assertPhaseFMultiTabHasSharedL1Candidate(t *testing.T, plans []phaseFSessionPlan) {
	t.Helper()
	byKey := make(map[string]map[string]bool)
	for _, request := range phaseFPlanByType(plans, "multi_tab_shared").Requests {
		if byKey[request.key] == nil {
			byKey[request.key] = make(map[string]bool)
		}
		byKey[request.key][request.tab] = true
	}
	for _, tabs := range byKey {
		if tabs["tab-a"] && tabs["tab-b"] {
			return
		}
	}
	t.Fatalf("multi_tab_shared has no key shared across tabs")
}

func phaseFPlanByType(plans []phaseFSessionPlan, sessionType string) phaseFSessionPlan {
	for _, plan := range plans {
		if plan.Type == sessionType {
			return plan
		}
	}
	return phaseFSessionPlan{}
}

func fakePhaseFVariants() []phaseFVariant {
	variants := make([]phaseFVariant, 0, len(orderedPhaseFVariantIDs()))
	for _, key := range orderedPhaseFVariantIDs() {
		variants = append(variants, fakePhaseFVariant(key))
	}
	return variants
}

func fakePhaseFVariant(key string) phaseFVariant {
	complexity, dimCount := fakePhaseFVariantParts(key)
	return phaseFVariant{
		ID:             key,
		Complexity:     complexity,
		VectorDimCount: dimCount,
		Request:        fakePhaseFRequest(dimCount),
	}
}

func fakePhaseFVariantParts(key string) (string, int) {
	parts := strings.Split(key, "-")
	dimCount, _ := strconv.Atoi(strings.TrimSuffix(parts[1], "d"))
	return parts[0], dimCount
}

func fakePhaseFRequest(dimCount int) *pb.GetBrowsingStateRequest {
	request := &pb.GetBrowsingStateRequest{Filters: []*pb.AxisFilter{{Value: 1, ValueType: pb.FilterValueType_TAGSET}}}
	if dimCount == 0 {
		return request
	}
	request.VectorDimension = &pb.VectorSearchDimension{BucketCfg: DefaultBucketConfig()}
	return request
}
