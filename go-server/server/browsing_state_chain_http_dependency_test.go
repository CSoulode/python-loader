package main

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
)

func TestHTTPVectorFilterDependencyIncludesModel(t *testing.T) {
	req := httptest.NewRequest("GET", httpVectorFilterURL("siglip2"), nil)
	deps := extractHTTPBrowsingStateDependencies(req)

	if !dependencyListContains(deps, browsingStateDependency{Kind: browsingStateDepVectorModel, ID: "siglip2"}) {
		t.Fatalf("deps = %+v, want vector_model siglip2", deps)
	}
}

func TestHTTPVectorFilterDependencyInvalidatesNode(t *testing.T) {
	req := httptest.NewRequest("GET", httpVectorFilterURL("siglip2"), nil)
	deps := extractHTTPBrowsingStateDependencies(req)
	chain := newBrowsingStateChain(8, time.Minute)
	chain.PublishNode(browsingStateChainLookup{
		TargetID:         "vector-filter-state",
		Dependencies:     deps,
		NodeDependencies: deps,
	}, &bsCellGrid{HTTPBody: []byte("ok")})

	removed := chain.InvalidateDependency(browsingStateDependency{
		Kind: browsingStateDepVectorModel,
		ID:   "siglip2",
	})
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if count := chain.NodeCount(); count != 0 {
		t.Fatalf("node count = %d, want 0", count)
	}
}

func TestHTTPTagFilterDependencyUsesTagKind(t *testing.T) {
	query := url.Values{}
	query.Set("filters", `[{"type":"tag","ids":[101]}]`)
	req := httptest.NewRequest("GET", "/api/cell/?"+query.Encode(), nil)
	deps := extractHTTPBrowsingStateDependencies(req)

	tag := browsingStateDependency{Kind: browsingStateDepTag, ID: "101"}
	tagset := browsingStateDependency{Kind: browsingStateDepTagset, ID: "101"}
	if !dependencyListContains(deps, tag) {
		t.Fatalf("deps = %+v, want tag dependency", deps)
	}
	if dependencyListContains(deps, tagset) {
		t.Fatalf("deps = %+v, must not treat tag id as tagset id", deps)
	}
}

func TestGRPCTagFilterDependencyUsesTagKind(t *testing.T) {
	deps := extractGRPCBrowsingStateDependencies(&pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{Value: 101, ValueType: pb.FilterValueType_TAG}},
	})

	if !dependencyListContains(deps, browsingStateDependency{Kind: browsingStateDepTag, ID: "101"}) {
		t.Fatalf("deps = %+v, want tag dependency", deps)
	}
}

func httpVectorFilterURL(model string) string {
	query := url.Values{}
	query.Set("xAxis", `{"type":"tagset","id":1}`)
	query.Set("vectorFilter", `{"model":"`+model+`","objectId":1,"k":2}`)
	return "/api/cell/?" + query.Encode()
}
