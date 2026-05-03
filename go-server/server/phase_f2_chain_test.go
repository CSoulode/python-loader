package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

func TestPhaseF2HTTPAndGRPCShareCanonicalStateID(t *testing.T) {
	grpcReq := phaseF2CanonicalGRPCRequest()
	grpcID, err := computeGRPCBrowsingStateID(grpcReq)
	if err != nil {
		t.Fatalf("compute grpc state id: %v", err)
	}
	httpReq := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery(), nil)
	httpID := computeHTTPBrowsingStateID(httpReq)

	if httpID != grpcID {
		t.Fatalf("http state id = %s, grpc state id = %s", httpID, grpcID)
	}
}

func TestPhaseF2HTTPAndGRPCShareForcedStrategyStateID(t *testing.T) {
	grpcReq := phaseF2CanonicalGRPCRequest()
	grpcReq.HybridStrategy = pb.HybridStrategy_PRE_FILTER
	grpcID, err := computeGRPCBrowsingStateID(grpcReq)
	if err != nil {
		t.Fatalf("compute grpc state id: %v", err)
	}
	httpReq := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery()+"&hybridStrategy=pre_filter", nil)
	httpID := computeHTTPBrowsingStateID(httpReq)

	if httpID != grpcID {
		t.Fatalf("http state id = %s, grpc state id = %s", httpID, grpcID)
	}
}

func TestPhaseF2HTTPHybridStrategyChangesStateID(t *testing.T) {
	base := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery(), nil)
	pre := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery()+"&hybridStrategy=pre_filter", nil)
	post := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery()+"&hybridStrategy=post_filter", nil)

	if computeHTTPBrowsingStateID(base) == computeHTTPBrowsingStateID(pre) {
		t.Fatalf("base and pre_filter state ids must differ")
	}
	if computeHTTPBrowsingStateID(pre) == computeHTTPBrowsingStateID(post) {
		t.Fatalf("pre_filter and post_filter state ids must differ")
	}
}

func TestPhaseF2HTTPHybridStrategyAutoMatchesEmptyStateID(t *testing.T) {
	base := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery(), nil)
	auto := httptest.NewRequest(http.MethodGet, "/api/cell?"+phaseF2CanonicalHTTPQuery()+"&hybridStrategy=auto", nil)

	if computeHTTPBrowsingStateID(base) != computeHTTPBrowsingStateID(auto) {
		t.Fatalf("empty and auto hybrid strategy should share state id")
	}
}

func TestPhaseF2HTTPRangeFilterCanonicalizesIDRangePairs(t *testing.T) {
	first := phaseF2RangeFilterRequest(`[{"type":"numrange","ids":[2,1],"ranges":[["20","30"],["10","15"]]}]`)
	second := phaseF2RangeFilterRequest(`[{"type":"numrange","ids":[1,2],"ranges":[["10","15"],["20","30"]]}]`)

	firstID, err := computeHTTPBrowsingStateIDStrict(first, false)
	if err != nil {
		t.Fatalf("first state id: %v", err)
	}
	secondID, err := computeHTTPBrowsingStateIDStrict(second, false)
	if err != nil {
		t.Fatalf("second state id: %v", err)
	}
	if firstID != secondID {
		t.Fatalf("state ids differ for equivalent range pairs: %s != %s", firstID, secondID)
	}
}

func TestPhaseF2HTTPRangeFilterStateIDPreservesPairBinding(t *testing.T) {
	base := phaseF2RangeFilterRequest(`[{"type":"numrange","ids":[1,2],"ranges":[["10","15"],["20","30"]]}]`)
	rebound := phaseF2RangeFilterRequest(`[{"type":"numrange","ids":[1,2],"ranges":[["20","30"],["10","15"]]}]`)

	baseID, err := computeHTTPBrowsingStateIDStrict(base, false)
	if err != nil {
		t.Fatalf("base state id: %v", err)
	}
	reboundID, err := computeHTTPBrowsingStateIDStrict(rebound, false)
	if err != nil {
		t.Fatalf("rebound state id: %v", err)
	}
	if baseID == reboundID {
		t.Fatalf("state ids must differ when id/range bindings differ")
	}
}

func TestPhaseF2HTTPRangeFilterRejectsMalformedBounds(t *testing.T) {
	request := phaseF2RangeFilterRequest(`[{"type":"numrange","ids":[1,2],"ranges":[["10","15"]]}]`)

	if _, err := computeHTTPBrowsingStateIDStrict(request, false); err == nil {
		t.Fatalf("expected malformed range filter to fail")
	}
}

func TestPhaseF2MergedProtocolPayloadRecomputesETagOrderIndependently(t *testing.T) {
	lookup := browsingStateChainLookup{TargetID: "same", Delta: TransitionDelta{Kind: DeltaAddFilter}}
	httpGrid := &bsCellGrid{HTTPBody: []byte("[{\"x\":1}]\n")}
	grpcGrid := &bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 1}}}

	httpFirst := newBrowsingStateChain(8, time.Minute)
	httpFirst.PublishNode(lookup, httpGrid)
	httpFirstNode := httpFirst.PublishNode(lookup, grpcGrid)

	grpcFirst := newBrowsingStateChain(8, time.Minute)
	grpcFirst.PublishNode(lookup, grpcGrid)
	grpcFirstNode := grpcFirst.PublishNode(lookup, httpGrid)

	if httpFirstNode.ETag != grpcFirstNode.ETag {
		t.Fatalf("etag order mismatch: http-first=%s grpc-first=%s", httpFirstNode.ETag, grpcFirstNode.ETag)
	}
}

func phaseF2RangeFilterRequest(filters string) *http.Request {
	query := url.Values{}
	query.Set("xAxis", `{"type":"tagset","id":1}`)
	query.Set("filters", filters)
	return httptest.NewRequest(http.MethodGet, "/api/cell?"+query.Encode(), nil)
}

func TestPhaseF2HTTPAncestorETagHeadersUseFreshTarget(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	parent := chain.PublishNode(
		browsingStateChainLookup{TargetID: "parent", Snapshot: phaseFSnapshot("a", "f", "v", "b", "auto", nil)},
		&bsCellGrid{HTTPBody: []byte("[{\"x\":1}]\n")},
	)
	lookup := browsingStateChainLookup{
		TargetID:      "target",
		ParentID:      "parent",
		Delta:         TransitionDelta{Kind: DeltaAddFilter},
		Snapshot:      phaseFSnapshot("a", "f2", "v", "b", "auto", nil),
		AncestorETags: []string{parent.ETag},
	}
	lookup.Ancestor = parent
	lookup.AncestorETagMatch = true
	lookup.MatchedAncestorID = parent.ID
	lookup.MatchedAncestorTag = parent.ETag
	lookup.Node = chain.PublishNode(lookup, &bsCellGrid{HTTPBody: []byte("[{\"x\":2}]\n")})
	recorder := httptest.NewRecorder()

	writeBrowsingStateHTTPETagHeaders(recorder, &lookup)
	if got := recorder.Header().Get(browsingStateHeaderAncestorID); got != "parent" {
		t.Fatalf("ancestor id header = %q, want parent", got)
	}
	if got := recorder.Header().Get(browsingStateHeaderAncestorETag); got != parent.ETag {
		t.Fatalf("ancestor etag header = %q, want %q", got, parent.ETag)
	}
	if got := recorder.Header().Get("ETag"); got == "" || got == parent.ETag {
		t.Fatalf("target etag = %q, want fresh lineage etag", got)
	}
}

func TestPhaseF2HTTPFullHitETagNotModified(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/cell?xAxis=%7B%7D", nil)
	targetID := computeHTTPBrowsingStateID(request)
	chain := newBrowsingStateChain(8, time.Minute)
	node := chain.PublishNode(
		browsingStateChainLookup{TargetID: targetID},
		&bsCellGrid{HTTPBody: []byte("[{\"x\":1}]\n")},
	)

	request.Header.Set("If-None-Match", node.ETag)
	server := &DataLoaderServer{db: &sql.DB{}, browsingChain: chain}
	recorder := httptest.NewRecorder()
	GetMetaDataCubeCompatCellHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", recorder.Body.Len())
	}
	if got := recorder.Header().Get("ETag"); got != node.ETag {
		t.Fatalf("etag = %q, want %q", got, node.ETag)
	}
}

func TestPhaseF2GRPCFullHitETagNotModified(t *testing.T) {
	req := &pb.GetBrowsingStateRequest{Filters: []*pb.AxisFilter{{Value: 1, ValueType: pb.FilterValueType_TAG}}}
	targetID, err := computeGRPCBrowsingStateID(req)
	if err != nil {
		t.Fatalf("compute state id: %v", err)
	}
	chain := newBrowsingStateChain(8, time.Minute)
	node := chain.PublishNode(
		browsingStateChainLookup{TargetID: targetID},
		&bsCellGrid{Responses: []*pb.BrowsingStateResponse{{X: 1, Count: 7}}},
	)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(browsingStateGRPCIfNoneMatch, node.ETag))
	stream := &phaseFTestStream{ctx: ctx}
	server := &DataLoaderServer{browsingChain: chain}

	if err := server.GetBrowsingState2(req, stream); err != nil {
		t.Fatalf("GetBrowsingState2: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent responses = %d, want 0", len(stream.sent))
	}
	if got := stream.header.Get(browsingStateGRPCNotModified); len(got) != 1 || got[0] != "true" {
		t.Fatalf("not-modified header = %v, want true", got)
	}
}

func TestPhaseF2InvalidateSubtreePreservesSiblings(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: "root"}, &bsCellGrid{HTTPBody: []byte("root")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "child", ParentID: "root"}, &bsCellGrid{HTTPBody: []byte("child")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "grandchild", ParentID: "child"}, &bsCellGrid{HTTPBody: []byte("grandchild")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "sibling", ParentID: "root"}, &bsCellGrid{HTTPBody: []byte("sibling")})

	if removed := chain.InvalidateSubtree("child"); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got := chain.NodeCount(); got != 2 {
		t.Fatalf("node count = %d, want 2", got)
	}
}

func TestPhaseF2InvalidateDependencyRemovesIntroducerSubtree(t *testing.T) {
	dep := browsingStateDependency{Kind: browsingStateDepVectorModel, ID: "siglip2"}
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: "root"}, &bsCellGrid{HTTPBody: []byte("root")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "introducer", ParentID: "root", NodeDependencies: []browsingStateDependency{dep}}, &bsCellGrid{HTTPBody: []byte("a")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "descendant", ParentID: "introducer"}, &bsCellGrid{HTTPBody: []byte("b")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "sibling", ParentID: "root"}, &bsCellGrid{HTTPBody: []byte("c")})

	if removed := chain.InvalidateDependency(dep); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got := chain.NodeCount(); got != 2 {
		t.Fatalf("node count = %d, want root+sibling", got)
	}
}

func TestPhaseF2SixNodeDependencyInvalidationMeetsSurvivalTarget(t *testing.T) {
	dep := browsingStateDependency{Kind: browsingStateDepVectorModel, ID: "siglip2"}
	chain := newBrowsingStateChain(8, time.Minute)
	for _, node := range phaseFSixNodeInvalidationDAG(dep) {
		chain.PublishNode(node, &bsCellGrid{HTTPBody: []byte(string(node.TargetID))})
	}

	if removed := chain.InvalidateDependency(dep); removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if got := chain.NodeCount(); got != 5 {
		t.Fatalf("surviving nodes = %d, want 5", got)
	}
}

func TestPhaseF2MutationInvalidatesTagAndTagsetStates(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{
		TargetID:         "tag-state",
		NodeDependencies: []browsingStateDependency{{Kind: browsingStateDepTag, ID: "101"}},
	}, &bsCellGrid{HTTPBody: []byte("tag")})
	chain.PublishNode(browsingStateChainLookup{
		TargetID:         "tagset-state",
		NodeDependencies: []browsingStateDependency{{Kind: browsingStateDepTagset, ID: "1"}},
	}, &bsCellGrid{HTTPBody: []byte("tagset")})
	server := &DataLoaderServer{browsingChain: chain}

	if removed := server.invalidateBrowsingStateTagAndTagset(101, 1); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got := chain.NodeCount(); got != 0 {
		t.Fatalf("node count = %d, want 0", got)
	}
}

func TestPhaseF2MutationInvalidatesNodeAndParentStates(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{
		TargetID:         "node-state",
		NodeDependencies: []browsingStateDependency{{Kind: browsingStateDepNode, ID: "10"}},
	}, &bsCellGrid{HTTPBody: []byte("node")})
	chain.PublishNode(browsingStateChainLookup{
		TargetID:         "parent-state",
		NodeDependencies: []browsingStateDependency{{Kind: browsingStateDepNode, ID: "3"}},
	}, &bsCellGrid{HTTPBody: []byte("parent")})
	server := &DataLoaderServer{browsingChain: chain}

	if removed := server.invalidateBrowsingStateNode(&pb.Node{Id: 10, ParentNodeId: 3}); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
}

func phaseFSixNodeInvalidationDAG(dep browsingStateDependency) []browsingStateChainLookup {
	return []browsingStateChainLookup{
		{TargetID: "root"},
		{TargetID: "metadata-a", ParentID: "root"},
		{TargetID: "metadata-b", ParentID: "metadata-a"},
		{TargetID: "metadata-c", ParentID: "metadata-b"},
		{TargetID: "metadata-d", ParentID: "metadata-c"},
		{TargetID: "vector-leaf", ParentID: "root", NodeDependencies: []browsingStateDependency{dep}},
	}
}

func TestPhaseF2DebugInvalidationEndpoint(t *testing.T) {
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{TargetID: "root"}, &bsCellGrid{HTTPBody: []byte("root")})
	chain.PublishNode(browsingStateChainLookup{TargetID: "child", ParentID: "root"}, &bsCellGrid{HTTPBody: []byte("child")})
	server := &DataLoaderServer{browsingChain: chain}
	body := bytes.NewBufferString(`{"subtree":"child"}`)
	request := httptest.NewRequest(http.MethodPost, "/debug/cache/browsing-state/invalidate", body)
	recorder := httptest.NewRecorder()

	GetBrowsingStateChainInvalidationHandler(server).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response browsingStateInvalidationResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Removed != 1 {
		t.Fatalf("removed = %d, want 1", response.Removed)
	}
}

func phaseF2CanonicalGRPCRequest() *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_FILTER, Value: 102, ValueType: pb.FilterValueType_TAG},
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_FILTER, Value: 101, ValueType: pb.FilterValueType_TAG},
		},
		VectorDimension: phaseF2CanonicalVectorDimension(),
	}
}

func phaseF2CanonicalVectorDimension() *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{
		ModelName: "siglip2",
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_ObjectId{ObjectId: 1},
		},
		BucketCfg: &pb.BucketConfig{
			Strategy: pb.BucketStrategy_EQUAL_WIDTH,
			Count:    2,
			DistMin:  0,
			DistMax:  1,
		},
		MaxResults: 10,
		Axis:       pb.AxisType_Y_AXIS,
	}
}

func phaseF2CanonicalHTTPQuery() string {
	params := url.Values{}
	params.Set("xAxis", `{"type":"tagset","id":1}`)
	params.Set("filters", `[{"type":"tag","ids":[102,101]}]`)
	params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":1,"maxResults":10}`)
	return params.Encode()
}
