package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"

	pb "m3.dataloader/dataloader"
)

type browsingStateRPC struct {
	name string
	call func(context.Context, *pb.GetBrowsingStateRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.BrowsingStateResponse], error)
}

func allBrowsingStateRPCs(client pb.DataLoaderClient) []browsingStateRPC {
	return []browsingStateRPC{
		{name: "GetBrowsingState", call: client.GetBrowsingState},
		{name: "GetBrowsingState2", call: client.GetBrowsingState2},
		{name: "GetBrowsingStateNonDistinctBranchesIncrementalGrouping", call: client.GetBrowsingStateNonDistinctBranchesIncrementalGrouping},
		{name: "GetBrowsingStateNonDistinctBranchesSingles", call: client.GetBrowsingStateNonDistinctBranchesSingles},
		{name: "GetBrowsingStateDistinctBranchesFull", call: client.GetBrowsingStateDistinctBranchesFull},
		{name: "GetBrowsingStateDistinctBranchesIncrementalGrouping", call: client.GetBrowsingStateDistinctBranchesIncrementalGrouping},
		{name: "GetBrowsingStateNonDistinctBranchesDeduplicatedSingles", call: client.GetBrowsingStateNonDistinctBranchesDeduplicatedSingles},
		{name: "GetBrowsingStateNonDistinctBranchesFull", call: client.GetBrowsingStateNonDistinctBranchesFull},
	}
}

func collectBrowsingStateResponsesWithCall(
	t *testing.T,
	call func(context.Context, *pb.GetBrowsingStateRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.BrowsingStateResponse], error),
	req *pb.GetBrowsingStateRequest,
) []*pb.BrowsingStateResponse {
	t.Helper()

	stream, err := call(context.Background(), req)
	if err != nil {
		t.Fatalf("start stream: %v", err)
	}

	var responses []*pb.BrowsingStateResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("stream recv: %v", err)
		}
		responses = append(responses, resp)
	}
	return responses
}

func assertBucketInfosOnFirstResponse(t *testing.T, responses []*pb.BrowsingStateResponse, want int) {
	t.Helper()

	if len(responses) == 0 {
		t.Fatal("got no responses")
	}
	if len(responses[0].GetBucketInfos()) != want {
		t.Fatalf("first response bucket infos = %d, want %d", len(responses[0].GetBucketInfos()), want)
	}
}

func assertStateCellsByXYZ(t *testing.T, responses []*pb.BrowsingStateResponse, expected map[string]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d responses, want %d", len(responses), len(expected))
	}
	for _, response := range responses {
		key := fmt.Sprintf("%d:%d:%d", response.GetX(), response.GetY(), response.GetZ())
		want, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected cell %s", key)
		}
		if response.GetCount() != want.count {
			t.Fatalf("cell %s count=%d want=%d", key, response.GetCount(), want.count)
		}
		if got := response.GetCubeObjects()[0].GetId(); got != want.representativeID {
			t.Fatalf("cell %s representative=%d want=%d", key, got, want.representativeID)
		}
	}
}

func assertCompatStateCellsByXYZ(t *testing.T, responses []compatBrowsingStateResponse, expected map[string]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d compat responses, want %d", len(responses), len(expected))
	}
	for _, response := range responses {
		key := fmt.Sprintf("%d:%d:%d", response.X, response.Y, response.Z)
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

func phaseBTestVectorDimension() *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  0.5,
		},
		MaxResults: 2,
		Axis:       pb.AxisType_Y_AXIS,
	}
}

func TestPhaseBVectorDimensionAllGRPCVariants2DAnd3D(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	req2D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		VectorDimension: phaseBTestVectorDimension(),
	}
	req3D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_Z_AXIS, Value: 2, ValueType: pb.FilterValueType_TAGSET},
		},
		VectorDimension: phaseBTestVectorDimension(),
	}

	for _, rpc := range allBrowsingStateRPCs(env.grpcClient) {
		t.Run(rpc.name+"/2d", func(t *testing.T) {
			responses := collectBrowsingStateResponsesWithCall(t, rpc.call, req2D)
			assertBucketInfosOnFirstResponse(t, responses, 2)
			assertStateCellsByXY(t, responses, map[string]cellExpectation{
				"1:1": {count: 1, representativeID: 1},
				"2:2": {count: 1, representativeID: 2},
			})
		})

		t.Run(rpc.name+"/3d", func(t *testing.T) {
			responses := collectBrowsingStateResponsesWithCall(t, rpc.call, req3D)
			assertBucketInfosOnFirstResponse(t, responses, 2)
			assertStateCellsByXYZ(t, responses, map[string]cellExpectation{
				"1:1:1": {count: 1, representativeID: 1},
				"2:2:2": {count: 1, representativeID: 2},
			})
		})
	}
}

func TestPhaseBVectorDimensionHTTP3DEnvelope(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("zAxis", `{"type":"tagset","id":2}`)
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":0.5,"maxResults":2}`)

	var response compatBrowsingStateEnvelope
	httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &response)
	if len(response.BucketInfos) != 2 {
		t.Fatalf("bucket infos = %d, want 2", len(response.BucketInfos))
	}
	assertCompatStateCellsByXYZ(t, response.Cells, map[string]cellExpectation{
		"1:1:1": {count: 1, representativeID: 1},
		"2:2:2": {count: 1, representativeID: 2},
	})
}
