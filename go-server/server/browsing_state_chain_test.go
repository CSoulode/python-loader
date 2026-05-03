package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

func TestBrowsingStateChainFullHit(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	prepared := browsingStateChainLookup{TargetID: "state-a", Delta: TransitionDelta{Kind: DeltaRoot}}
	chain.PublishNode(prepared, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 2}}})

	lookup := chain.Lookup(prepared)
	if lookup.Mode != chainLookupFullHit {
		t.Fatalf("lookup mode = %s, want %s", lookup.Mode, chainLookupFullHit)
	}
	if lookup.Node == nil || len(lookup.Node.CellGrid.Responses) != 1 {
		t.Fatalf("full hit node was not replayable")
	}
}

func TestBrowsingStateChainRebucketAncestorReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := browsingStateChainLookup{TargetID: "parent", ReuseKey: "same", Delta: TransitionDelta{Kind: DeltaRoot}}
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 2}}})

	target := browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		ReuseKey: "same",
		Delta:    TransitionDelta{Kind: DeltaRebucketOnly},
	}
	lookup := chain.Lookup(target)
	if lookup.Mode != chainLookupAncestorReuse {
		t.Fatalf("lookup mode = %s, want %s", lookup.Mode, chainLookupAncestorReuse)
	}
	if lookup.Ancestor == nil || lookup.Ancestor.ID != "parent" {
		t.Fatalf("ancestor = %#v, want parent", lookup.Ancestor)
	}
}

func TestBrowsingStateChainRejectsUnsafeAncestorReuse(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := browsingStateChainLookup{TargetID: "parent", ReuseKey: "base-a"}
	chain.PublishNode(parent, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})
	lookup := chain.Lookup(browsingStateChainLookup{
		TargetID: "target",
		ParentID: "parent",
		ReuseKey: "base-b",
		Delta:    TransitionDelta{Kind: DeltaRebucketOnly},
	})
	if lookup.Mode != chainLookupColdMiss {
		t.Fatalf("lookup mode = %s, want %s", lookup.Mode, chainLookupColdMiss)
	}
}

func TestGetBrowsingState2PhaseFFullHitReplay(t *testing.T) {
	req := &pb.GetBrowsingStateRequest{Filters: []*pb.AxisFilter{{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAG}}}
	targetID, err := computeGRPCBrowsingStateID(req)
	if err != nil {
		t.Fatalf("compute state id: %v", err)
	}
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: targetID}, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 7}}})

	stream := &phaseFTestStream{ctx: context.Background()}
	server := &DataLoaderServer{browsingChain: chain}
	if err := server.GetBrowsingState2(req, stream); err != nil {
		t.Fatalf("GetBrowsingState2 full hit: %v", err)
	}
	if len(stream.sent) != 1 || stream.sent[0].GetCount() != 7 {
		t.Fatalf("sent = %#v, want cached response", stream.sent)
	}
	if got := stream.header.Get(browsingStateHeaderCache); len(got) != 1 || got[0] != string(chainLookupFullHit) {
		t.Fatalf("cache header = %v, want full_hit", got)
	}
	if got := stream.header.Get(browsingStateHeaderReusedFragments); len(got) != 1 || got[0] != "cellgrid" {
		t.Fatalf("reused header = %v, want cellgrid", got)
	}
	if got := stream.trailer.Get(browsingStateHeaderReusedFragments); len(got) != 1 || got[0] != "cellgrid" {
		t.Fatalf("reused trailer = %v, want cellgrid", got)
	}
}

func TestCompatCellPhaseFHTTPFullHitReplay(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/cell?xAxis=%7B%7D", nil)
	targetID := computeHTTPBrowsingStateID(request)
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: targetID}, &bsCellGrid{HTTPBody: []byte("[{\"x\":1}]\n")})

	server := &DataLoaderServer{db: &sql.DB{}, browsingChain: chain}
	recorder := httptest.NewRecorder()
	GetMetaDataCubeCompatCellHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if body := recorder.Body.String(); body != "[{\"x\":1}]\n" {
		t.Fatalf("body = %q, want cached body", body)
	}
	if got := recorder.Header().Get("X-Browsing-State-Cache"); got != string(chainLookupFullHit) {
		t.Fatalf("cache header = %q, want full_hit", got)
	}
	if got := recorder.Header().Get(browsingStateHeaderReusedFragments); got != "cellgrid" {
		t.Fatalf("reused fragments = %q, want cellgrid", got)
	}
}

func TestBrowsingStateChainHTTPReplayRequiresHTTPBody(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: "shared"}, &bsCellGrid{
		Responses:        []*pb.BrowsingStateResponse{{X: 1}},
		HasGRPCResponses: true,
	})
	lookup := chain.Lookup(browsingStateChainLookup{TargetID: "shared"})
	server := &DataLoaderServer{browsingChain: chain}

	if server.replayBrowsingStateChainHTTP(httptest.NewRecorder(), &lookup) {
		t.Fatalf("HTTP replay succeeded without HTTP body")
	}
}

func TestBrowsingStateChainPublishMergesProtocolPayloads(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	node := chain.PublishNode(browsingStateChainLookup{TargetID: "shared"}, &bsCellGrid{
		Responses:        []*pb.BrowsingStateResponse{{X: 1}},
		HasGRPCResponses: true,
	})
	merged := chain.PublishNode(browsingStateChainLookup{TargetID: "shared"}, &bsCellGrid{
		HTTPBody:    []byte("[{\"x\":1}]\n"),
		HasHTTPBody: true,
	})

	if merged != node {
		t.Fatalf("merged node pointer changed")
	}
	if !cellGridHasGRPCResponses(node.CellGrid) || !cellGridHasHTTPBody(node.CellGrid) {
		t.Fatalf("cell grid protocols grpc=%v http=%v", cellGridHasGRPCResponses(node.CellGrid), cellGridHasHTTPBody(node.CellGrid))
	}
}

func TestBrowsingStateChainAllInvalidatesNodes(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: "a"}, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1}}})
	chain.PublishNode(browsingStateChainLookup{TargetID: "b"}, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 2}}})

	if removed := chain.InvalidateAll(); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got := chain.NodeCount(); got != 0 {
		t.Fatalf("node count = %d, want 0", got)
	}
}

func TestBrowsingStateChainMerkleETagIncludesParent(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	grid := &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 2}}}
	parent := chain.PublishNode(browsingStateChainLookup{TargetID: "parent"}, grid)
	child := chain.PublishNode(browsingStateChainLookup{TargetID: "child", ParentID: "parent"}, grid)

	if parent.ETag == "" || child.ETag == "" {
		t.Fatalf("etags should be populated: parent=%q child=%q", parent.ETag, child.ETag)
	}
	if parent.ETag == child.ETag {
		t.Fatalf("child etag should include parent lineage")
	}
}

func TestBrowsingStateChainHTTPNotModified(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/cell?xAxis=%7B%7D", nil)
	targetID := computeHTTPBrowsingStateID(request)
	chain := newBrowsingStateChain(8, time.Minute)
	node := chain.PublishNode(browsingStateChainLookup{TargetID: targetID}, &bsCellGrid{HTTPBody: []byte("[{\"x\":1}]\n")})

	conditional := httptest.NewRequest(http.MethodGet, "/api/cell?xAxis=%7B%7D", nil)
	conditional.Header.Set("If-None-Match", node.ETag)
	recorder := httptest.NewRecorder()
	server := &DataLoaderServer{db: &sql.DB{}, browsingChain: chain}
	GetMetaDataCubeCompatCellHandler(server).ServeHTTP(recorder, conditional)

	if recorder.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", recorder.Body.String())
	}
}

func TestBrowsingStateChainDependencyInvalidatesIntroducerSubtree(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	base := []browsingStateDependency{{Kind: browsingStateDepTagset, ID: "1"}}
	vector := browsingStateDependency{Kind: browsingStateDepVectorModel, ID: "hsv"}
	chain.PublishNode(browsingStateChainLookup{TargetID: "root", Dependencies: base, NodeDependencies: base}, &bsCellGrid{HTTPBody: []byte("root")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "child", ParentID: "root", Dependencies: append(base, vector), NodeDependencies: append(base, vector)}, &bsCellGrid{HTTPBody: []byte("child")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "sibling", Dependencies: []browsingStateDependency{{Kind: browsingStateDepTagset, ID: "2"}}, NodeDependencies: []browsingStateDependency{{Kind: browsingStateDepTagset, ID: "2"}}}, &bsCellGrid{HTTPBody: []byte("sibling")})

	if removed := chain.InvalidateDependency(vector); removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if got := chain.NodeCount(); got != 2 {
		t.Fatalf("node count = %d, want root and sibling", got)
	}
}

func TestGetBrowsingState2PhaseFNotModifiedReplay(t *testing.T) {
	req := &pb.GetBrowsingStateRequest{Filters: []*pb.AxisFilter{{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAG}}}
	targetID, err := computeGRPCBrowsingStateID(req)
	if err != nil {
		t.Fatalf("compute state id: %v", err)
	}
	chain := newBrowsingStateChain(8, time.Minute)
	node := chain.PublishNode(browsingStateChainLookup{TargetID: targetID}, &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 7}}})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(browsingStateGRPCIfNoneMatch, node.ETag))
	stream := &phaseFTestStream{ctx: ctx}

	server := &DataLoaderServer{browsingChain: chain}
	if err := server.GetBrowsingState2(req, stream); err != nil {
		t.Fatalf("GetBrowsingState2 not modified: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent = %#v, want empty stream", stream.sent)
	}
	if got := stream.header.Get(browsingStateGRPCNotModified); len(got) != 1 || got[0] != "true" {
		t.Fatalf("not modified header = %v, want true", got)
	}
}

func TestBrowsingStateChainGRPCTrailerUsesFinalObservationMetadata(t *testing.T) {
	stream := &phaseFTestStream{ctx: context.Background()}
	lookup := &browsingStateChainLookup{
		TargetID:  "target",
		CachePath: string(chainLookupAncestorReuse),
		Delta:     TransitionDelta{Kind: DeltaAddFilter},
		Reusable:  []string{"candidates"},
	}
	node := &StateNode{ETag: `"etag"`}

	sendBrowsingStateChainGRPCTrailer(stream, lookup, node)

	if got := stream.trailer.Get(browsingStateHeaderCache); len(got) != 1 || got[0] != string(chainLookupAncestorReuse) {
		t.Fatalf("cache trailer = %v, want ancestor_reuse", got)
	}
	if got := stream.trailer.Get(browsingStateHeaderReusedFragments); len(got) != 1 || got[0] != "candidates" {
		t.Fatalf("reused trailer = %v, want candidates", got)
	}
}

type phaseFTestStream struct {
	ctx     context.Context
	header  metadata.MD
	trailer metadata.MD
	sent    []*pb.BrowsingStateResponse
}

func (s *phaseFTestStream) Send(resp *pb.BrowsingStateResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func (s *phaseFTestStream) Context() context.Context {
	return s.ctx
}

func (s *phaseFTestStream) SetHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *phaseFTestStream) SendHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *phaseFTestStream) SetTrailer(md metadata.MD) {
	s.trailer = metadata.Join(s.trailer, md)
}

func (s *phaseFTestStream) SendMsg(any) error { return nil }

func (s *phaseFTestStream) RecvMsg(any) error { return nil }
