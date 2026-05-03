package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

func TestPhaseFSafeDeltasMatchColdRecompute(t *testing.T) {
	env := setupPhaseFTestEnv(t)
	defer env.cleanup()

	for _, tc := range phaseFDeltaCases() {
		t.Run(tc.name, func(t *testing.T) {
			assertPhaseFDeltaMatchesCold(t, env, tc.parent, tc.target, tc.cachePath)
		})
	}
}

type phaseFDeltaCase struct {
	name      string
	parent    *pb.GetBrowsingStateRequest
	target    *pb.GetBrowsingStateRequest
	cachePath string
}

func phaseFDeltaCases() []phaseFDeltaCase {
	base := phaseFVectorStateRequest()
	return []phaseFDeltaCase{
		{"add_filter", phaseFPreFilterRequest(base), phaseFAddTagFilterRequest(base), string(chainLookupAncestorReuse)},
		{"remove_filter", phaseFAddTagFilterRequest(base), phaseFPreFilterRequest(base), string(chainLookupColdMiss)},
		{"add_vector_dim", base, phaseFMultiVectorStateRequest(), string(chainLookupAncestorReuse)},
		{"remove_vector_dim", phaseFMultiVectorStateRequest(), base, string(chainLookupAncestorReuse)},
		{"change_axis_non_vector", base, phaseFChangedAxisRequest(base), string(chainLookupAncestorReuse)},
		{"rebucket_only", base, phaseFRebucketRequest(base), string(chainLookupAncestorReuse)},
		{"change_forced_strategy", base, phaseFForcedStrategyRequest(base), string(chainLookupColdMiss)},
	}
}

func assertPhaseFDeltaMatchesCold(
	t *testing.T,
	env *phaseATestEnv,
	parent *pb.GetBrowsingStateRequest,
	target *pb.GetBrowsingStateRequest,
	wantPath string,
) {
	t.Helper()
	env.server.ensureBrowsingStateChain().InvalidateAll()
	_, _, parentTrailer := collectBrowsingStateWithMD(t, env.grpcClient, parent, nil)
	parentID := phaseFStateIDFromMD(parentTrailer)
	md := metadata.Pairs(browsingStateParentHeaderID, parentID)
	reused, header, _ := collectBrowsingStateWithMD(t, env.grpcClient, target, md)
	if got := firstMetadataValue(header, browsingStateHeaderCache); got != StateID(wantPath) {
		t.Fatalf("cache path = %q, want %q", got, wantPath)
	}

	env.server.ensureBrowsingStateChain().InvalidateAll()
	cold, _, _ := collectBrowsingStateWithMD(t, env.grpcClient, target, nil)
	assertCanonicalResponsesEqual(t, reused, cold)
}

func collectBrowsingStateWithMD(
	t *testing.T,
	client pb.DataLoaderClient,
	req *pb.GetBrowsingStateRequest,
	md metadata.MD,
) ([]*pb.BrowsingStateResponse, metadata.MD, metadata.MD) {
	t.Helper()
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	var header metadata.MD
	var trailer metadata.MD
	stream, err := client.GetBrowsingState2(ctx, req, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		t.Fatalf("GetBrowsingState2: %v", err)
	}
	responses := recvBrowsingStateStream(t, stream)
	return responses, header, trailer
}

func recvBrowsingStateStream(
	t *testing.T,
	stream pb.DataLoader_GetBrowsingState2Client,
) []*pb.BrowsingStateResponse {
	t.Helper()
	var responses []*pb.BrowsingStateResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return responses
		}
		if err != nil {
			t.Fatalf("stream recv: %v", err)
		}
		responses = append(responses, resp)
	}
}

func phaseFStateIDFromMD(md metadata.MD) string {
	id := firstMetadataValue(md, browsingStateHeaderID)
	return string(id)
}

func phaseFPreFilterRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	cloned := clonePhaseFTestRequest(req)
	cloned.HybridStrategy = pb.HybridStrategy_PRE_FILTER
	return cloned
}

func phaseFAddTagFilterRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	cloned := phaseFPreFilterRequest(req)
	cloned.Filters = append(cloned.Filters, &pb.AxisFilter{
		AxisFilterType: pb.AxisType_FILTER,
		Value:          102,
		ValueType:      pb.FilterValueType_TAG,
	})
	return cloned
}

func phaseFMultiVectorStateRequest() *pb.GetBrowsingStateRequest {
	base := clonePhaseFTestRequest(phaseFVectorStateRequest())
	second := cloneVectorSearchConfig(base.GetVectorDimension())
	second.Axis = pb.AxisType_Z_AXIS
	second.Reference = &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 2}}
	base.VectorDimensions = []*pb.VectorSearchDimension{base.GetVectorDimension(), second}
	base.VectorDimension = nil
	return base
}

func phaseFChangedAxisRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	cloned := clonePhaseFTestRequest(req)
	cloned.Filters = []*pb.AxisFilter{{
		AxisFilterType: pb.AxisType_Z_AXIS,
		Value:          1,
		ValueType:      pb.FilterValueType_TAGSET,
	}}
	return cloned
}

func phaseFRebucketRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	cloned := clonePhaseFTestRequest(req)
	cloned.RebucketOnly = true
	cloned.GetVectorDimension().BucketCfg.Count = 3
	return cloned
}

func phaseFForcedStrategyRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	cloned := clonePhaseFTestRequest(req)
	cloned.HybridStrategy = pb.HybridStrategy_POST_FILTER
	return cloned
}

func clonePhaseFTestRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	return proto.Clone(req).(*pb.GetBrowsingStateRequest)
}

func canonicalPhaseFResponses(t *testing.T, responses []*pb.BrowsingStateResponse) []byte {
	t.Helper()
	out := make([]byte, 0)
	for _, response := range responses {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		out = append(out, size[:]...)
		out = append(out, payload...)
	}
	return out
}

func assertCanonicalResponsesEqual(
	t *testing.T,
	left []*pb.BrowsingStateResponse,
	right []*pb.BrowsingStateResponse,
) {
	t.Helper()
	if !bytes.Equal(canonicalPhaseFResponses(t, left), canonicalPhaseFResponses(t, right)) {
		t.Fatalf("ancestor result differs from cold recompute")
	}
}
