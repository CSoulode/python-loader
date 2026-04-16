package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func TestGetBrowsingStateReturnsCachedHitWithoutDB(t *testing.T) {
	req := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          11,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
	}
	prepared, err := prepareBrowsingStateRequest(req)
	if err != nil {
		t.Fatalf("prepare request: %v", err)
	}
	key, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, prepared)
	if err != nil {
		t.Fatalf("compute key: %v", err)
	}

	server := &DataLoaderServer{bsCache: newBrowsingStateCache(4, time.Minute)}
	server.bsCache.PutEntry(key, testBSCacheValue(), "etag-hit", nil)

	stream := newTestBrowsingStateStream()
	if err := server.GetBrowsingState(req, stream); err != nil {
		t.Fatalf("GetBrowsingState returned error: %v", err)
	}
	if len(stream.responses) != 1 {
		t.Fatalf("responses = %d, want 1", len(stream.responses))
	}
	if got := stream.responses[0].GetBucketInfos(); len(got) != 1 {
		t.Fatalf("bucket infos = %d, want 1", len(got))
	}
	if got := stream.header.Get(bsGRPCETagKey); len(got) != 1 || got[0] != "etag-hit" {
		t.Fatalf("etag header = %+v, want %q", got, "etag-hit")
	}
}

func TestGetBrowsingStateReturnsNotModifiedOnCachedETagMatch(t *testing.T) {
	req := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          11,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
	}
	prepared, err := prepareBrowsingStateRequest(req)
	if err != nil {
		t.Fatalf("prepare request: %v", err)
	}
	key, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, prepared)
	if err != nil {
		t.Fatalf("compute key: %v", err)
	}

	server := &DataLoaderServer{bsCache: newBrowsingStateCache(4, time.Minute)}
	server.bsCache.PutEntry(key, testBSCacheValue(), "etag-hit", nil)

	stream := newTestBrowsingStateStream()
	stream.ctx = metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(bsGRPCIfNoneMatchKey, `"etag-hit"`),
	)
	if err := server.GetBrowsingState(req, stream); err != nil {
		t.Fatalf("GetBrowsingState returned error: %v", err)
	}
	if len(stream.responses) != 0 {
		t.Fatalf("responses = %d, want 0", len(stream.responses))
	}
	if got := stream.header.Get(bsGRPCNotModifiedKey); len(got) != 1 || got[0] != bsNotModifiedTrueValue {
		t.Fatalf("bs-not-modified = %+v, want %q", got, bsNotModifiedTrueValue)
	}
}

func TestCompatCellHandlerReturnsCachedHitWithoutDB(t *testing.T) {
	server := &DataLoaderServer{
		db:      &sql.DB{},
		bsCache: newBrowsingStateCache(4, time.Minute),
	}
	prepared := &preparedBrowsingStateRequest{
		AxisX: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		AxisY: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		AxisZ: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
	}
	key, err := computeBSCacheKey(bsCacheNamespaceHTTPCompatCell, prepared)
	if err != nil {
		t.Fatalf("compute key: %v", err)
	}
	server.bsCache.PutEntry(key, bsCacheValue{
		Cells: []cachedBrowsingStateCell{{
			X:     1,
			Y:     1,
			Z:     1,
			Count: 2,
			CubeObjects: []cachedCubeObject{{
				ID:           13,
				FileURI:      "/media/13",
				ThumbnailURI: "/thumb/13",
			}},
		}},
	}, "etag-http", nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/cell", nil)
	GetMetaDataCubeCompatCellHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var got []compatBrowsingStateResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(got) != 1 || got[0].CubeObjects[0].Id != 13 {
		t.Fatalf("unexpected response body: %+v", got)
	}
	if got := recorder.Header().Get(bsHTTPETagHeaderKey); got != `"etag-http"` {
		t.Fatalf("etag header = %q, want %q", got, `"etag-http"`)
	}
}

func TestCompatCellHandlerReturns304WhenIfNoneMatchMatchesCache(t *testing.T) {
	server := &DataLoaderServer{
		db:      &sql.DB{},
		bsCache: newBrowsingStateCache(4, time.Minute),
	}
	prepared := &preparedBrowsingStateRequest{
		AxisX: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		AxisY: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
		AxisZ: qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}},
	}
	key, err := computeBSCacheKey(bsCacheNamespaceHTTPCompatCell, prepared)
	if err != nil {
		t.Fatalf("compute key: %v", err)
	}
	server.bsCache.PutEntry(key, testBSCacheValue(), "etag-http", nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/cell", nil)
	request.Header.Set(bsHTTPIfNoneMatchKey, `"etag-http"`)
	GetMetaDataCubeCompatCellHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", recorder.Code)
	}
	if body := strings.TrimSpace(recorder.Body.String()); body != "" {
		t.Fatalf("expected empty body, got %q", body)
	}
}

func TestBrowsingStateCacheInvalidateHandler(t *testing.T) {
	server := &DataLoaderServer{bsCache: newBrowsingStateCache(4, time.Minute)}
	server.bsCache.PutEntry(
		bsCacheStorageKey{Namespace: "grpc:GetBrowsingState", Canonical: "a"},
		testBSCacheValue(),
		"etag-a",
		[]bsCacheDependency{{Kind: bsDependencyKindTagset, Key: "1"}},
	)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/debug/cache/browsing-state/invalidate",
		strings.NewReader(`{"scope":"dependency","kind":"tagset","key":"1"}`),
	)
	GetBrowsingStateCacheInvalidateHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response bsInvalidateResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Invalidated != 1 {
		t.Fatalf("invalidated = %d, want 1", response.Invalidated)
	}
}

type testBrowsingStateStream struct {
	ctx       context.Context
	header    metadata.MD
	responses []*pb.BrowsingStateResponse
}

func newTestBrowsingStateStream() *testBrowsingStateStream {
	return &testBrowsingStateStream{ctx: context.Background()}
}

func (s *testBrowsingStateStream) SetHeader(md metadata.MD) error {
	s.header = md.Copy()
	return nil
}

func (s *testBrowsingStateStream) SendHeader(md metadata.MD) error {
	s.header = md.Copy()
	return nil
}

func (s *testBrowsingStateStream) SetTrailer(metadata.MD) {}

func (s *testBrowsingStateStream) Context() context.Context { return s.ctx }

func (s *testBrowsingStateStream) SendMsg(any) error { return nil }

func (s *testBrowsingStateStream) RecvMsg(any) error { return nil }

func (s *testBrowsingStateStream) Send(resp *pb.BrowsingStateResponse) error {
	s.responses = append(s.responses, proto.Clone(resp).(*pb.BrowsingStateResponse))
	return nil
}
