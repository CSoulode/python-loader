package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
)

func phaseE1VectorDimension(model string, objectID int32, axis pb.AxisType) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: model,
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: objectID}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  1,
		},
		MaxResults: 3,
		Axis:       axis,
	}
}

func assertAxisBucketInfosOnFirstResponse(t *testing.T, responses []*pb.BrowsingStateResponse, axes ...string) {
	t.Helper()

	if len(responses) == 0 {
		t.Fatal("got no responses")
	}
	got := responses[0].GetAxisBucketInfos()
	if len(got) != len(axes) {
		t.Fatalf("axis bucket infos = %d, want %d", len(got), len(axes))
	}
	if len(responses[0].GetBucketInfos()) != 0 {
		t.Fatalf("legacy bucket infos should be empty for new protocol, got %d", len(responses[0].GetBucketInfos()))
	}
	for _, axisKey := range axes {
		if len(got[axisKey].GetItems()) != 2 {
			t.Fatalf("axis %q bucket infos = %d, want 2", axisKey, len(got[axisKey].GetItems()))
		}
	}
}

func assertCompatStateCellsByXY(t *testing.T, responses []compatBrowsingStateResponse, expected map[string]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d compat responses, want %d", len(responses), len(expected))
	}
	for _, response := range responses {
		key := fmt.Sprintf("%d:%d", response.X, response.Y)
		want, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected compat cell %s", key)
		}
		if response.Count != want.count {
			t.Fatalf("compat cell %s count=%d want=%d", key, response.Count, want.count)
		}
		if got := response.CubeObjects[0].Id; got != want.representativeID {
			t.Fatalf("compat cell %s representative=%d want=%d", key, got, want.representativeID)
		}
	}
}

func TestPhaseE1DualVectorAxesAllGRPCVariants(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	req := &pb.GetBrowsingStateRequest{
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("siglip2", 1, pb.AxisType_X_AXIS),
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Y_AXIS),
		},
	}

	for _, rpc := range allBrowsingStateRPCs(env.grpcClient) {
		t.Run(rpc.name, func(t *testing.T) {
			responses := collectBrowsingStateResponsesWithCall(t, rpc.call, req)
			assertAxisBucketInfosOnFirstResponse(t, responses, "x", "y")
			assertStateCellsByXY(t, responses, map[string]cellExpectation{
				"1:1": {count: 1, representativeID: 1},
				"1:2": {count: 1, representativeID: 2},
				"2:1": {count: 1, representativeID: 3},
			})
		})
	}
}

func TestPhaseE1HTTPDualVectorEnvelope(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	payload, err := json.Marshal([]map[string]any{
		{"model": "siglip2", "objectId": 1, "axis": "x", "bucketCount": 2, "bucketStrategy": "equal_width", "distMin": 0, "distMax": 1, "maxResults": 3},
		{"model": "hsv", "objectId": 1, "axis": "y", "bucketCount": 2, "bucketStrategy": "equal_width", "distMin": 0, "distMax": 1, "maxResults": 3},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	params := url.Values{}
	params.Set("vectorDimensions", string(payload))

	var response compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &response)
	if len(response.AxisBucketInfos) != 2 {
		t.Fatalf("axisBucketInfos = %d, want 2", len(response.AxisBucketInfos))
	}
	if len(response.BucketInfos) != 0 {
		t.Fatalf("legacy bucketInfos should be empty, got %d", len(response.BucketInfos))
	}
	assertCompatStateCellsByXY(t, response.Cells, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"1:2": {count: 1, representativeID: 2},
		"2:1": {count: 1, representativeID: 3},
	})
}

func TestPhaseE1MixedKNNAndRangeAxes(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	rangeDim := phaseE1VectorDimension("siglip2", 1, pb.AxisType_X_AXIS)
	rangeDim.DistanceRange = &pb.DistanceRange{MinDistance: 0, MaxDistance: 0.5}
	rangeDim.RangeSemantics = pb.RangeSemantics_DISTANCE
	rangeDim.MaxResults = 2

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		VectorDimensions: []*pb.VectorSearchDimension{
			rangeDim,
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Y_AXIS),
		},
	})
	assertAxisBucketInfosOnFirstResponse(t, responses, "x", "y")
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"2:2": {count: 1, representativeID: 2},
	})
	if env.vectorKV.RangeSearchCallCount() != 1 || env.vectorKV.KNNCallCount() != 1 {
		t.Fatalf("unexpected calls: range=%d knn=%d", env.vectorKV.RangeSearchCallCount(), env.vectorKV.KNNCallCount())
	}
}

func TestPhaseE1HTTPMediaThumbnailRedirect(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	httpMux := http.NewServeMux()
	httpMux.HandleFunc(
		"/api/media/{id}/thumbnail",
		GetMetaDataCubeCompatMediaThumbnailHandler(env.db),
	)
	httpServer := httptest.NewServer(httpMux)
	defer httpServer.Close()

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(httpServer.URL + "/api/media/1/thumbnail")
	if err != nil {
		t.Fatalf("get media thumbnail: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusTemporaryRedirect)
	}
	if location := resp.Header.Get("Location"); location != "/thumb-1.jpg" {
		t.Fatalf("location = %q, want %q", location, "/thumb-1.jpg")
	}
}

func TestPhaseE1AllModeUsesVectorBucketIDs(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		All: "[]",
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("siglip2", 1, pb.AxisType_X_AXIS),
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Y_AXIS),
		},
		VectorBucketIds: map[string]int32{"x": 0, "y": 0},
	})
	assertAllCubeObjectIDs(t, responses, []int32{1})
}

func TestPhaseE1VectorFilterAndVectorDimensionsCoexist(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		VectorFilter: &pb.VectorFilterConfig{
			ModelName: "siglip2",
			Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
			K:         2,
		},
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Y_AXIS),
		},
	})
	// object 1 survives in bucket 0, object 2 survives in bucket 1 after vector_filter narrows to {1,2}
	assertAxisBucketInfosOnFirstResponse(t, responses, "y")
	assertStateCellsByXY(t, responses, map[string]cellExpectation{
		"1:1": {count: 1, representativeID: 1},
		"2:2": {count: 1, representativeID: 2},
	})
}

func TestPhaseE1GRPC3DMixedAxes(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("siglip2", 1, pb.AxisType_Y_AXIS),
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Z_AXIS),
		},
	})
	assertAxisBucketInfosOnFirstResponse(t, responses, "y", "z")
	assertStateCellsByXYZ(t, responses, map[string]cellExpectation{
		"1:1:1": {count: 1, representativeID: 1},
		"2:1:2": {count: 1, representativeID: 2},
		"2:2:1": {count: 1, representativeID: 3},
	})
}

func TestPhaseE1GRPC3DAllVectorAxes(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
		VectorDimensions: []*pb.VectorSearchDimension{
			phaseE1VectorDimension("siglip2", 1, pb.AxisType_X_AXIS),
			phaseE1VectorDimension("siglip2", 2, pb.AxisType_Y_AXIS),
			phaseE1VectorDimension("hsv", 1, pb.AxisType_Z_AXIS),
		},
	})
	assertAxisBucketInfosOnFirstResponse(t, responses, "x", "y", "z")
	assertStateCellsByXYZ(t, responses, map[string]cellExpectation{
		"1:2:1": {count: 1, representativeID: 1},
		"1:1:2": {count: 1, representativeID: 2},
		"2:1:1": {count: 1, representativeID: 3},
	})
}

func TestPhaseE1HTTPRebucketOnlyRequiresAllDimsHit(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	seedPayload := `[{"model":"siglip2","objectId":1,"axis":"x","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":1,"maxResults":3},{"model":"hsv","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":1,"maxResults":3}]`
	rebucketPayload := `[{"model":"siglip2","objectId":1,"axis":"x","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":3},{"model":"hsv","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":3}]`

	seedParams := url.Values{}
	seedParams.Set("vectorDimensions", seedPayload)
	var seeded compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+seedParams.Encode(), &seeded)

	seedGet := env.vectorKV.GetCallCount()
	seedKNN := env.vectorKV.KNNCallCount()
	seedList := env.vectorKV.ListModelsCallCount()

	rebucketParams := url.Values{}
	rebucketParams.Set("vectorDimensions", rebucketPayload)
	rebucketParams.Set("rebucketOnly", "true")
	var rebucketed compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+rebucketParams.Encode(), &rebucketed)
	if env.vectorKV.GetCallCount() != seedGet || env.vectorKV.KNNCallCount() != seedKNN || env.vectorKV.ListModelsCallCount() != seedList {
		t.Fatalf(
			"rebucketOnly should reuse cached results: get=%d/%d knn=%d/%d list=%d/%d",
			env.vectorKV.GetCallCount(), seedGet,
			env.vectorKV.KNNCallCount(), seedKNN,
			env.vectorKV.ListModelsCallCount(), seedList,
		)
	}

	missParams := url.Values{}
	missParams.Set("vectorDimensions", `[{"model":"siglip2","objectId":1,"axis":"x","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":3},{"model":"hsv","objectId":2,"axis":"y","bucketCount":2,"bucketStrategy":"equal_depth","distMin":0,"distMax":1,"maxResults":3}]`)
	missParams.Set("rebucketOnly", "true")
	resp, err := http.Get(env.httpServer.URL + "/?" + missParams.Encode())
	if err != nil {
		t.Fatalf("http.Get(rebucket miss): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("rebucket miss status = %d, want 409", resp.StatusCode)
	}
}
