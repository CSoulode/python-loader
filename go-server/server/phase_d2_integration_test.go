package main

import (
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
)

func phaseD2TagFilterRequest(
	tagID int32,
	forced pb.HybridStrategy,
) *pb.GetBrowsingStateRequest {
	req := phaseD1TagFilterRequest(tagID, 1.0, pb.BucketStrategy_EQUAL_WIDTH)
	req.HybridStrategy = forced
	return req
}

func TestPhaseD2ForcedStrategiesMatchDocumentedSemantics(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	t.Run("post filter", func(t *testing.T) {
		env := setupPhaseATestEnv(t, dbURL)
		defer env.cleanup()

		responses := collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_POST_FILTER))
		assertStateCellsByXY(t, responses, map[string]cellExpectation{
			"2:1": {count: 1, representativeID: 2},
		})
		if env.vectorKV.KNNCallCount() != 1 || env.vectorKV.FilteredKNNCallCount() != 0 {
			t.Fatalf("unexpected calls: knn=%d filtered=%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount())
		}
	})

	t.Run("pre filter", func(t *testing.T) {
		env := setupPhaseATestEnv(t, dbURL)
		defer env.cleanup()

		responses := collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_PRE_FILTER))
		assertStateCellsByXY(t, responses, map[string]cellExpectation{
			"2:1": {count: 1, representativeID: 2},
			"2:2": {count: 1, representativeID: 3},
		})
		if env.vectorKV.KNNCallCount() != 0 || env.vectorKV.FilteredKNNCallCount() != 1 {
			t.Fatalf("unexpected calls: knn=%d filtered=%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount())
		}
	})

	t.Run("hybrid", func(t *testing.T) {
		env := setupPhaseATestEnv(t, dbURL)
		defer env.cleanup()

		responses := collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_HYBRID))
		assertStateCellsByXY(t, responses, map[string]cellExpectation{
			"2:1": {count: 1, representativeID: 2},
		})
		if env.vectorKV.KNNCallCount() != 1 || env.vectorKV.FilteredKNNCallCount() != 0 {
			t.Fatalf("unexpected calls: knn=%d filtered=%d", env.vectorKV.KNNCallCount(), env.vectorKV.FilteredKNNCallCount())
		}
	})
}

func TestPhaseD2HybridReusesGlobalCacheOnly(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	t.Run("reuses global cache", func(t *testing.T) {
		env := setupPhaseATestEnv(t, dbURL)
		defer env.cleanup()

		collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_POST_FILTER))
		if env.vectorKV.KNNCallCount() != 1 {
			t.Fatalf("expected initial global search, got knn=%d", env.vectorKV.KNNCallCount())
		}

		collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_HYBRID))
		if env.vectorKV.KNNCallCount() != 1 {
			t.Fatalf("hybrid should reuse global cache: knn=%d", env.vectorKV.KNNCallCount())
		}
		if env.vectorKV.FilteredKNNCallCount() != 0 {
			t.Fatalf("hybrid should not call FilteredKNN: filtered=%d", env.vectorKV.FilteredKNNCallCount())
		}
	})

	t.Run("does not reuse filtered cache", func(t *testing.T) {
		env := setupPhaseATestEnv(t, dbURL)
		defer env.cleanup()

		collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_PRE_FILTER))
		if env.vectorKV.FilteredKNNCallCount() != 1 {
			t.Fatalf("expected initial filtered search, got filtered=%d", env.vectorKV.FilteredKNNCallCount())
		}

		collectBrowsingStateResponses(t, env.grpcClient, phaseD2TagFilterRequest(102, pb.HybridStrategy_HYBRID))
		if env.vectorKV.KNNCallCount() != 1 {
			t.Fatalf("hybrid should trigger fresh KNN when cache is not global: knn=%d", env.vectorKV.KNNCallCount())
		}
	})
}
