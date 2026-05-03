package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
)

func TestPhaseF1GRPCFullHitSkipsVectorKV(t *testing.T) {
	env := setupPhaseFTestEnv(t)
	defer env.cleanup()

	req := phaseFVectorStateRequest()
	first := collectBrowsingStateResponses(t, env.grpcClient, req)
	assertBucketInfosOnFirstResponse(t, first, 2)
	seedGet := env.vectorKV.GetCallCount()
	seedSearch := totalVectorSearchCalls(env)
	seedList := env.vectorKV.ListModelsCallCount()

	second := collectBrowsingStateResponses(t, env.grpcClient, req)
	assertBucketInfosOnFirstResponse(t, second, 2)
	if env.vectorKV.GetCallCount() != seedGet || totalVectorSearchCalls(env) != seedSearch || env.vectorKV.ListModelsCallCount() != seedList {
		t.Fatalf("full hit should skip vectorkv: get=%d/%d search=%d/%d list=%d/%d", env.vectorKV.GetCallCount(), seedGet, totalVectorSearchCalls(env), seedSearch, env.vectorKV.ListModelsCallCount(), seedList)
	}
	if got := env.server.ensureBrowsingStateChain().Snapshot()["full_hits"]; got == 0 {
		t.Fatalf("full hit counter = %d, want > 0", got)
	}
}

func TestPhaseF1HTTPFullHitSkipsVectorKV(t *testing.T) {
	env := setupPhaseFTestEnv(t)
	defer env.cleanup()

	rawURL := env.httpServer.URL + "/?" + phaseFHTTPVectorParams().Encode()
	var first compatBrowsingStateEnvelope
	resp1 := httpGetJSONWithHeaders(t, rawURL, &first)
	if got := resp1.Header.Get("X-Browsing-Cache-Path"); got != string(chainLookupColdMiss) {
		t.Fatalf("first cache path = %q, want cold_miss", got)
	}
	seedGet := env.vectorKV.GetCallCount()
	seedSearch := totalVectorSearchCalls(env)
	seedList := env.vectorKV.ListModelsCallCount()

	var second compatBrowsingStateEnvelope
	resp2 := httpGetJSONWithHeaders(t, rawURL, &second)
	if got := resp2.Header.Get("X-Browsing-Cache-Path"); got != string(chainLookupFullHit) {
		t.Fatalf("second cache path = %q, want full_hit", got)
	}
	if env.vectorKV.GetCallCount() != seedGet || totalVectorSearchCalls(env) != seedSearch || env.vectorKV.ListModelsCallCount() != seedList {
		t.Fatalf("http full hit should skip vectorkv: get=%d/%d search=%d/%d list=%d/%d", env.vectorKV.GetCallCount(), seedGet, totalVectorSearchCalls(env), seedSearch, env.vectorKV.ListModelsCallCount(), seedList)
	}
}

func TestPhaseF1AllModeBypassesStateChain(t *testing.T) {
	env := setupPhaseFTestEnv(t)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{All: "[]"})
	assertAllCubeObjectIDs(t, responses, []int32{1, 2, 3})
	if got := env.server.ensureBrowsingStateChain().NodeCount(); got != 0 {
		t.Fatalf("chain nodes = %d, want 0 for all mode", got)
	}
}

func setupPhaseFTestEnv(t *testing.T) *phaseATestEnv {
	t.Helper()
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}
	return setupPhaseATestEnv(t, dbURL)
}

func phaseFVectorStateRequest() *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET}},
		VectorDimension: &pb.VectorSearchDimension{
			ModelName:  "siglip2",
			Reference:  &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			BucketCfg:  &pb.BucketConfig{Strategy: pb.BucketStrategy_EQUAL_WIDTH, Count: 2, DistMin: 0, DistMax: 0.5},
			MaxResults: 2,
			Axis:       pb.AxisType_Y_AXIS,
		},
	}
}

func phaseFHTTPVectorParams() url.Values {
	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":0.5,"maxResults":2}`)
	return params
}

func httpGetJSONWithHeaders(t *testing.T, rawURL string, target any) *http.Response {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatalf("http.Get(%s): %v", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http status = %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	return resp
}
