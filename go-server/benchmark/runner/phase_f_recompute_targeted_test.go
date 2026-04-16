package runner

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

const phaseFInvalidateAllTargetedRounds = 4

func TestPhaseFGH2DInvalidateAllPreservesCanonicalETag(t *testing.T) {
	runPhaseFSingleRequestInvalidateAllStability(t, "GH-2d", 1)
}

func TestPhaseFRH2DInvalidateAllPreservesCanonicalETag(t *testing.T) {
	runPhaseFSingleRequestInvalidateAllStability(
		t,
		"RH-2d",
		phaseFInvalidateAllTargetedRounds,
	)
}

func TestPhaseFHW0DInvalidateAllPreservesCanonicalETag(t *testing.T) {
	runPhaseFSingleRequestInvalidateAllStability(t, "HW-0d", 1)
}

func runPhaseFSingleRequestInvalidateAllStability(t *testing.T, requestID string, rounds int) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv(phaseFRecomputeDatasetEnv))
	if databaseURL == "" {
		t.Skipf("%s is not set", phaseFRecomputeDatasetEnv)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := newPhaseFRecomputeTestRunner(t, databaseURL)
	session, err := runner.startDefaultSession(ctx, DatasetPilot94, nil)
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	defer session.Close()

	variants, err := runner.buildPhaseFVariants(ctx, session)
	if err != nil {
		t.Fatalf("build variants: %v", err)
	}
	request, err := findPhaseFScenarioRequest(variants, requestID)
	if err != nil {
		t.Fatal(err)
	}

	for round := 0; round < rounds; round++ {
		if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
			t.Fatalf("round %d invalidate before warm: %v", round, err)
		}
		warmBenchID := benchRunID(
			DatasetPilot94,
			Experiment11,
			fmt.Sprintf("etag-warm-%s-%02d", requestID, round),
			round,
		)
		warm, err := ExecuteSSBRequestWithOptions(
			ctx,
			session.DataLoader,
			clonePhaseFRequest(request.Request),
			ExecuteRequestOptions{
				BenchID: warmBenchID,
			},
		)
		if err != nil {
			t.Fatalf("round %d warm request %s: %v", round, requestID, err)
		}
		warmEvents, err := ReadBenchEventsUntil(
			session.Runtime.ServerLogPath,
			warmBenchID,
			func(events []BenchEvent) bool { return benchEventsReady(events, len(warm.Responses)) },
		)
		if err != nil {
			t.Fatalf("round %d read warm events %s: %v", round, requestID, err)
		}
		warmETag := metadataFirstValue(warm.Headers, "etag")
		if warmETag == "" {
			t.Fatalf("round %d warm request %s missing etag", round, requestID)
		}

		if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
			t.Fatalf("round %d invalidate before revalidate: %v", round, err)
		}
		revalidateBenchID := benchRunID(
			DatasetPilot94,
			Experiment11,
			fmt.Sprintf("etag-revalidate-%s-%02d", requestID, round),
			round,
		)
		revalidated, err := ExecuteSSBRequestWithOptions(
			ctx,
			session.DataLoader,
			clonePhaseFRequest(request.Request),
			ExecuteRequestOptions{
				BenchID:     revalidateBenchID,
				IfNoneMatch: warmETag,
			},
		)
		if err != nil {
			t.Fatalf("round %d revalidate request %s: %v", round, requestID, err)
		}
		revalidateEvents, err := ReadBenchEventsUntil(
			session.Runtime.ServerLogPath,
			revalidateBenchID,
			func(events []BenchEvent) bool { return benchEventsReady(events, len(revalidated.Responses)) },
		)
		if err != nil {
			t.Fatalf("round %d read revalidate events %s: %v", round, requestID, err)
		}
		if revalidated.NotModified {
			continue
		}

		t.Fatalf(
			"round %d %s unstable warm_etag=%s fresh_etag=%s canonical_warm=%v canonical_fresh=%v bucket_warm=%s bucket_fresh=%s warm_events=%s fresh_events=%s",
			round,
			requestID,
			warmETag,
			metadataFirstValue(revalidated.Headers, "etag"),
			responseCanonicalSignature(warm),
			responseCanonicalSignature(revalidated),
			responseBucketSignature(warm),
			responseBucketSignature(revalidated),
			phaseFBenchEventDigest(warmEvents),
			phaseFBenchEventDigest(revalidateEvents),
		)
	}
}

func phaseFBenchEventDigest(events []BenchEvent) string {
	if len(events) == 0 {
		return "<none>"
	}

	parts := make([]string, 0, len(events))
	for _, event := range events {
		switch event.Event {
		case "strategy_selected":
			parts = append(
				parts,
				fmt.Sprintf(
					"strategy(model=%s,strategy=%s,forced=%s,est=%d,total=%d)",
					event.ModelName,
					event.Strategy,
					event.ForcedStrategy,
					event.EstimatedFilteredCount,
					event.TotalMediaCount,
				),
			)
		case "vector_cache_hit", "vector_cache_miss", "vector_cache_put", "vector_search_done", "hybrid_intersection_done":
			parts = append(
				parts,
				fmt.Sprintf(
					"%s(model=%s,kind=%s,result=%d,input=%d,candidates=%d)",
					event.Event,
					event.ModelName,
					event.SearchKind,
					event.ResultCount,
					event.InputCount,
					event.CandidateCount,
				),
			)
		}
	}
	if len(parts) == 0 {
		return "<no-vector-events>"
	}
	return strings.Join(parts, "; ")
}

func findPhaseFScenarioRequest(variants []phaseFVariant, requestID string) (phaseFVariant, error) {
	for _, request := range buildPhaseFScenarioRequests(variants) {
		if request.ID == requestID {
			return request, nil
		}
	}
	return phaseFVariant{}, fmt.Errorf("phase F scenario request %q not found", requestID)
}
