package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
)

const (
	phaseFRecomputeDatasetEnv = "BENCH_DB_URL_PILOT_94K"
	phaseFRecomputeReportPath = "/tmp/bs-etag-repro.txt"
)

func TestPhaseFInvalidateAllPreservesCanonicalETag(t *testing.T) {
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
	requests := buildPhaseFScenarioRequests(variants)

	if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
		t.Fatalf("invalidate before warm: %v", err)
	}

	type requestSnapshot struct {
		ID         string
		WarmETag   string
		WarmResult *ExecutedSSBRequest
	}
	snapshots := make([]requestSnapshot, 0, len(requests))
	for index, request := range requests {
		result, execErr := ExecuteSSBRequestWithOptions(
			ctx,
			session.DataLoader,
			clonePhaseFRequest(request.Request),
			ExecuteRequestOptions{BenchID: benchRunID(DatasetPilot94, Experiment11, fmt.Sprintf("etag-warm-%02d", index), index)},
		)
		if execErr != nil {
			t.Fatalf("warm request %s: %v", request.ID, execErr)
		}
		etag := metadataFirstValue(result.Headers, "etag")
		if etag == "" {
			t.Fatalf("warm request %s missing etag", request.ID)
		}
		snapshots = append(snapshots, requestSnapshot{
			ID:         request.ID,
			WarmETag:   etag,
			WarmResult: result,
		})
	}

	if _, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", ""); err != nil {
		t.Fatalf("invalidate before revalidate: %v", err)
	}

	reportLines := []string{
		fmt.Sprintf("dataset=%s", DatasetPilot94),
		fmt.Sprintf("requests=%d", len(snapshots)),
	}
	unstable := make([]string, 0, len(snapshots))
	for index, snapshot := range snapshots {
		result, execErr := ExecuteSSBRequestWithOptions(
			ctx,
			session.DataLoader,
			clonePhaseFRequest(requests[index].Request),
			ExecuteRequestOptions{
				BenchID:     benchRunID(DatasetPilot94, Experiment11, fmt.Sprintf("etag-revalidate-%02d", index), index),
				IfNoneMatch: snapshot.WarmETag,
			},
		)
		if execErr != nil {
			t.Fatalf("revalidate request %s: %v", snapshot.ID, execErr)
		}
		if result.NotModified {
			reportLines = append(reportLines, fmt.Sprintf("%s stable not_modified=true", snapshot.ID))
			continue
		}

		unstable = append(unstable, snapshot.ID)
		reportLines = append(reportLines,
			fmt.Sprintf("%s stable not_modified=false warm_etag=%s fresh_etag=%s", snapshot.ID, snapshot.WarmETag, metadataFirstValue(result.Headers, "etag")),
			fmt.Sprintf("  warm_seq=%v", responseSequenceSignature(snapshot.WarmResult)),
			fmt.Sprintf("  fresh_seq=%v", responseSequenceSignature(result)),
			fmt.Sprintf("  warm_canonical=%v", responseCanonicalSignature(snapshot.WarmResult)),
			fmt.Sprintf("  fresh_canonical=%v", responseCanonicalSignature(result)),
			fmt.Sprintf("  warm_bucket=%s", responseBucketSignature(snapshot.WarmResult)),
			fmt.Sprintf("  fresh_bucket=%s", responseBucketSignature(result)),
		)
	}

	if writeErr := os.WriteFile(phaseFRecomputeReportPath, []byte(strings.Join(reportLines, "\n")+"\n"), 0o644); writeErr != nil {
		t.Fatalf("write report: %v", writeErr)
	}
	if len(unstable) > 0 {
		t.Fatalf("unstable invalidate-all etags for %v; see %s", unstable, phaseFRecomputeReportPath)
	}
}

func newPhaseFRecomputeTestRunner(t *testing.T, databaseURL string) *BenchRunner {
	t.Helper()

	root := benchmarkTestWorkspaceRoot(t)
	envFile := filepath.Join(root, "python-loader", "go-server", ".env")
	modelsFile := filepath.Join(root, "vectorkv", "config", "models.json")
	tempRoot := filepath.Join(t.TempDir(), "phase-f-recompute")
	paths := NewOutputPaths(tempRoot)
	if err := ensureOutputTree(paths); err != nil {
		t.Fatalf("ensure output tree: %v", err)
	}
	if err := BuildBinaries(context.Background(), root, paths); err != nil {
		t.Fatalf("build binaries: %v", err)
	}

	baseEnv, err := ParseDotEnv(envFile)
	if err != nil {
		t.Fatalf("parse env: %v", err)
	}
	catalog, err := LoadModelCatalog(modelsFile)
	if err != nil {
		t.Fatalf("load model catalog: %v", err)
	}
	datasetCatalog, err := ParseDatasetCatalog("pilot_94k:94346:" + phaseFRecomputeDatasetEnv)
	if err != nil {
		t.Fatalf("parse dataset catalog: %v", err)
	}
	if err := os.Setenv(phaseFRecomputeDatasetEnv, databaseURL); err != nil {
		t.Fatalf("set env %s: %v", phaseFRecomputeDatasetEnv, err)
	}

	return &BenchRunner{
		opts: Options{
			RootDir:          root,
			OutputRoot:       tempRoot,
			GoServerEnvFile:  envFile,
			VectorModelsFile: modelsFile,
			Datasets:         []DatasetID{DatasetPilot94},
			Experiments:      []ExperimentID{Experiment11},
			DatasetCatalog:   datasetCatalog,
		},
		paths:   paths,
		baseEnv: baseEnv,
		catalog: catalog,
	}
}

func benchmarkTestWorkspaceRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	start := filepath.Dir(file)
	root, err := DetectWorkspaceRoot(start)
	if err != nil {
		t.Fatalf("detect workspace root: %v", err)
	}
	return root
}

func responseSequenceSignature(result *ExecutedSSBRequest) []string {
	if result == nil {
		return nil
	}
	signature := make([]string, 0, len(result.Responses))
	for _, item := range result.Responses {
		signature = append(signature, responseCellSignature(item.Response, false))
	}
	return signature
}

func responseCanonicalSignature(result *ExecutedSSBRequest) []string {
	if result == nil {
		return nil
	}
	signature := make([]string, 0, len(result.Responses))
	for _, item := range result.Responses {
		signature = append(signature, responseCellSignature(item.Response, true))
	}
	sort.Strings(signature)
	return signature
}

func responseCellSignature(resp *pb.BrowsingStateResponse, sortObjects bool) string {
	if resp == nil {
		return "<nil>"
	}
	objectIDs := make([]int32, 0, len(resp.GetCubeObjects()))
	for _, cubeObject := range resp.GetCubeObjects() {
		objectIDs = append(objectIDs, cubeObject.GetId())
	}
	if sortObjects {
		slices.Sort(objectIDs)
	}
	return fmt.Sprintf("%d:%d:%d:%d:%v", resp.GetX(), resp.GetY(), resp.GetZ(), resp.GetCount(), objectIDs)
}

func responseBucketSignature(result *ExecutedSSBRequest) string {
	if result == nil || len(result.Responses) == 0 || result.Responses[0].Response == nil {
		return ""
	}
	first := result.Responses[0].Response
	items := make([]string, 0)
	for _, info := range first.GetBucketInfos() {
		items = append(items, bucketInfoSignature("legacy", info))
	}
	axisKeys := make([]string, 0, len(first.GetAxisBucketInfos()))
	for axisKey := range first.GetAxisBucketInfos() {
		axisKeys = append(axisKeys, axisKey)
	}
	sort.Strings(axisKeys)
	for _, axisKey := range axisKeys {
		for _, info := range first.GetAxisBucketInfos()[axisKey].GetItems() {
			items = append(items, bucketInfoSignature(axisKey, info))
		}
	}
	return strings.Join(items, "|")
}

func bucketInfoSignature(axis string, info *pb.BucketInfo) string {
	if info == nil {
		return axis + ":nil"
	}
	return fmt.Sprintf(
		"%s:%d:%f:%f:%s",
		axis,
		info.GetBucketId(),
		info.GetLowerBound(),
		info.GetUpperBound(),
		info.GetLabel(),
	)
}
