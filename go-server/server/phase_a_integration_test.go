package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

const phaseATestDatabaseEnv = "PHASE_A_TEST_DATABASE_URL"

type fakeVectorKVServer struct {
	kvstorev1.UnimplementedVectorKVServer
	getCalls         int32
	knnCalls         int32
	filteredKNNCalls int32
	listModelsCalls  int32
	annIndex         string
	iterativeScan    bool
}

func (s *fakeVectorKVServer) Get(_ context.Context, req *kvstorev1.GetRequest) (*kvstorev1.GetResponse, error) {
	atomic.AddInt32(&s.getCalls, 1)
	if req.GetModel() != "siglip2" {
		return nil, fmt.Errorf("unexpected model %q", req.GetModel())
	}
	if req.GetId() != 1 {
		return nil, fmt.Errorf("unexpected object id %d", req.GetId())
	}
	return &kvstorev1.GetResponse{
		Vector: &kvstorev1.Vector{Values: []float32{1, 0, 0}},
	}, nil
}

func (s *fakeVectorKVServer) KNN(_ context.Context, req *kvstorev1.KNNRequest) (*kvstorev1.KNNResponse, error) {
	atomic.AddInt32(&s.knnCalls, 1)
	if req.GetModel() != "siglip2" {
		return nil, fmt.Errorf("unexpected model %q", req.GetModel())
	}
	if req.GetK() <= 0 {
		return nil, fmt.Errorf("unexpected k %d", req.GetK())
	}

	all := []*kvstorev1.Neighbor{
		{Id: 1, Distance: 0},
		{Id: 2, Distance: 0.25},
		{Id: 3, Distance: 0.75},
	}
	filtered := make([]*kvstorev1.Neighbor, 0, len(all))
	for _, neighbor := range all {
		if req.GetMaxDistance() > 0 && neighbor.GetDistance() > req.GetMaxDistance() {
			continue
		}
		filtered = append(filtered, neighbor)
		if int32(len(filtered)) == req.GetK() {
			break
		}
	}
	return &kvstorev1.KNNResponse{Neighbors: filtered}, nil
}

func (s *fakeVectorKVServer) FilteredKNN(_ context.Context, req *kvstorev1.FilteredKNNRequest) (*kvstorev1.KNNResponse, error) {
	atomic.AddInt32(&s.filteredKNNCalls, 1)
	if req.GetModel() != "siglip2" {
		return nil, fmt.Errorf("unexpected model %q", req.GetModel())
	}
	if req.GetK() <= 0 {
		return nil, fmt.Errorf("unexpected k %d", req.GetK())
	}

	allowed := make(map[int32]struct{}, len(req.GetCandidateIds()))
	for _, candidateID := range req.GetCandidateIds() {
		allowed[candidateID] = struct{}{}
	}
	all := []*kvstorev1.Neighbor{
		{Id: 1, Distance: 0},
		{Id: 2, Distance: 0.25},
		{Id: 3, Distance: 0.75},
	}
	filtered := make([]*kvstorev1.Neighbor, 0, len(all))
	for _, neighbor := range all {
		if _, ok := allowed[neighbor.GetId()]; !ok {
			continue
		}
		filtered = append(filtered, neighbor)
		if int32(len(filtered)) == req.GetK() {
			break
		}
	}
	if len(filtered) == 0 {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &kvstorev1.KNNResponse{Neighbors: filtered}, nil
}

func (s *fakeVectorKVServer) ListModels(_ context.Context, _ *kvstorev1.ListModelsRequest) (*kvstorev1.ListModelsResponse, error) {
	atomic.AddInt32(&s.listModelsCalls, 1)
	annIndex := s.annIndex
	if annIndex == "" {
		annIndex = "hnsw"
	}
	return &kvstorev1.ListModelsResponse{
		DefaultModel: "siglip2",
		Models: []*kvstorev1.ModelInfo{{
			Name:                   "siglip2",
			Dim:                    3,
			TagsetName:             "SigLIP2",
			DistanceMetric:         "cosine",
			AnnIndex:               annIndex,
			IterativeScanAvailable: s.iterativeScan,
		}},
	}, nil
}

func (s *fakeVectorKVServer) GetCallCount() int32 {
	return atomic.LoadInt32(&s.getCalls)
}

func (s *fakeVectorKVServer) KNNCallCount() int32 {
	return atomic.LoadInt32(&s.knnCalls)
}

func (s *fakeVectorKVServer) ListModelsCallCount() int32 {
	return atomic.LoadInt32(&s.listModelsCalls)
}

func (s *fakeVectorKVServer) FilteredKNNCallCount() int32 {
	return atomic.LoadInt32(&s.filteredKNNCalls)
}

type phaseATestEnv struct {
	db         *sql.DB
	server     *DataLoaderServer
	vectorKV   *fakeVectorKVServer
	grpcClient pb.DataLoaderClient
	httpServer *httptest.Server
	cleanup    func()
}

func TestPhaseAEndToEndGRPCAndHTTP(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	t.Run("grpc regression without vector filter", func(t *testing.T) {
		responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
			Filters: []*pb.AxisFilter{{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			}},
		})
		assertStateCells(t, responses, map[int32]cellExpectation{
			1: {count: 1, representativeID: 1},
			2: {count: 2, representativeID: 3},
		})
	})

	t.Run("grpc vector filter narrows state results", func(t *testing.T) {
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
		})
		assertStateCells(t, responses, map[int32]cellExpectation{
			1: {count: 1, representativeID: 1},
			2: {count: 1, representativeID: 2},
		})
	})

	t.Run("grpc all regression without vector filter", func(t *testing.T) {
		responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
			Filters: []*pb.AxisFilter{{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			}},
			All: "[]",
		})
		assertAllCubeObjectIDs(t, responses, []int32{1, 2, 3})
	})

	t.Run("grpc all vector filter narrows media list", func(t *testing.T) {
		responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
			Filters: []*pb.AxisFilter{{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			}},
			All: "[]",
			VectorFilter: &pb.VectorFilterConfig{
				ModelName: "siglip2",
				Reference: &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: 1}},
				K:         2,
			},
		})
		assertAllCubeObjectIDs(t, responses, []int32{1, 2})
	})

	t.Run("http regression without vector filter", func(t *testing.T) {
		var responses []compatBrowsingStateResponse
		httpGetJSON(t, env.httpServer.URL+"/?xAxis=%7B%22type%22%3A%22tagset%22,%22id%22%3A1%7D", &responses)
		assertCompatStateCells(t, responses, map[int32]cellExpectation{
			1: {count: 1, representativeID: 1},
			2: {count: 2, representativeID: 3},
		})
	})

	t.Run("http vector filter narrows state results", func(t *testing.T) {
		params := url.Values{}
		params.Set("xAxis", `{"type":"tagset","id":1}`)
		params.Set("vectorFilter", `{"model":"siglip2","objectId":1,"k":2,"maxDistance":0}`)

		var responses []compatBrowsingStateResponse
		httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &responses)
		assertCompatStateCells(t, responses, map[int32]cellExpectation{
			1: {count: 1, representativeID: 1},
			2: {count: 1, representativeID: 2},
		})
	})

	t.Run("http all regression without vector filter", func(t *testing.T) {
		params := url.Values{}
		params.Set("xAxis", `{"type":"tagset","id":1}`)
		params.Set("all", "[]")

		var objects []compatCubeObject
		httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &objects)
		assertCompatObjectIDs(t, objects, []int32{1, 2, 3})
	})

	t.Run("http all vector filter narrows media list", func(t *testing.T) {
		params := url.Values{}
		params.Set("xAxis", `{"type":"tagset","id":1}`)
		params.Set("all", "[]")
		params.Set("vectorFilter", `{"model":"siglip2","objectId":1,"k":2,"maxDistance":0}`)

		var objects []compatCubeObject
		httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &objects)
		assertCompatObjectIDs(t, objects, []int32{1, 2})
	})
}

func TestPhaseBVectorDimensionEndToEndGRPCAndHTTP(t *testing.T) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		t.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(t, dbURL)
	defer env.cleanup()

	vectorDimension := &pb.VectorSearchDimension{
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

	t.Run("grpc state returns bucket infos on first response", func(t *testing.T) {
		responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
			Filters: []*pb.AxisFilter{{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			}},
			VectorDimension: vectorDimension,
		})

		if len(responses) != 2 {
			t.Fatalf("got %d responses, want 2", len(responses))
		}
		if len(responses[0].GetBucketInfos()) != 2 {
			t.Fatalf("first response bucket infos = %d, want 2", len(responses[0].GetBucketInfos()))
		}
		assertStateCellsByXY(t, responses, map[string]cellExpectation{
			"1:1": {count: 1, representativeID: 1},
			"2:2": {count: 1, representativeID: 2},
		})
	})

	t.Run("grpc all returns selected bucket contents", func(t *testing.T) {
		bucketID := int32(1)
		responses := collectBrowsingStateResponses(t, env.grpcClient, &pb.GetBrowsingStateRequest{
			Filters: []*pb.AxisFilter{{
				AxisFilterType: pb.AxisType_X_AXIS,
				Value:          1,
				ValueType:      pb.FilterValueType_TAGSET,
			}},
			All:             "[]",
			VectorDimension: vectorDimension,
			VectorBucketId:  &bucketID,
		})
		assertAllCubeObjectIDs(t, responses, []int32{2})
	})

	t.Run("http state returns envelope with bucket infos", func(t *testing.T) {
		params := url.Values{}
		params.Set("xAxis", `{"type":"tagset","id":1}`)
		params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":0.5,"maxResults":2}`)

		var response compatBrowsingStateEnvelope
		httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &response)
		if len(response.BucketInfos) != 2 {
			t.Fatalf("bucket infos = %d, want 2", len(response.BucketInfos))
		}
		assertCompatStateCells(t, response.Cells, map[int32]cellExpectation{
			1: {count: 1, representativeID: 1},
			2: {count: 1, representativeID: 2},
		})
	})

	t.Run("http all returns selected bucket contents", func(t *testing.T) {
		params := url.Values{}
		params.Set("xAxis", `{"type":"tagset","id":1}`)
		params.Set("all", "[]")
		params.Set("vectorDimension", `{"model":"siglip2","objectId":1,"axis":"y","bucketCount":2,"bucketStrategy":"equal_width","distMin":0,"distMax":0.5,"maxResults":2}`)
		params.Set("vectorBucketId", "0")

		var objects []compatCubeObject
		httpGetJSON(t, env.httpServer.URL+"/?"+params.Encode(), &objects)
		assertCompatObjectIDs(t, objects, []int32{1})
	})
}

func setupPhaseATestEnv(tb testing.TB, dbURL string) *phaseATestEnv {
	tb.Helper()

	adminDB := openTestDB(tb, adminDatabaseURL(tb, dbURL))
	dbName := fmt.Sprintf("phase_a_%d_%d", time.Now().UnixNano(), rand.Intn(1000))
	if _, err := adminDB.Exec(`CREATE DATABASE "` + dbName + `"`); err != nil {
		tb.Fatalf("create test database: %v", err)
	}

	testDBURL := replaceDatabaseName(tb, dbURL, dbName)
	testDB := openTestDB(tb, testDBURL)
	setupPhaseATestDatabase(tb, testDB)

	vectorAddr, vectorKV, stopVector := startFakeVectorKV(tb)
	resolver, conn, err := newVectorFilterResolverFromAddress(context.Background(), vectorAddr)
	if err != nil {
		tb.Fatalf("newVectorFilterResolverFromAddress: %v", err)
	}

	server := &DataLoaderServer{
		db:            testDB,
		vectorFilters: resolver,
		vectorConn:    conn,
	}

	grpcClient, stopGRPC := startDataLoaderGRPC(tb, server)
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/", GetMetaDataCubeCompatCellHandler(server))
	httpMux.HandleFunc("/api/vector/models", GetVectorModelsHandler(server))
	httpServer := httptest.NewServer(httpMux)

	cleanup := func() {
		httpServer.Close()
		stopGRPC()
		stopVector()
		server.Close()
		dropTestDatabase(tb, adminDB, dbName)
		adminDB.Close()
	}

	return &phaseATestEnv{
		db:         testDB,
		server:     server,
		vectorKV:   vectorKV,
		grpcClient: grpcClient,
		httpServer: httpServer,
		cleanup:    cleanup,
	}
}

func openTestDB(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		tb.Fatalf("sql.Open(%q): %v", dsn, err)
	}
	if err := db.Ping(); err != nil {
		tb.Fatalf("db.Ping(%q): %v", dsn, err)
	}
	return db
}

func adminDatabaseURL(tb testing.TB, dbURL string) string {
	tb.Helper()
	return replaceDatabaseName(tb, dbURL, "postgres")
}

func replaceDatabaseName(tb testing.TB, rawURL string, dbName string) string {
	tb.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		tb.Fatalf("parse database url: %v", err)
	}
	parsed.Path = "/" + dbName
	return parsed.String()
}

func dropTestDatabase(tb testing.TB, adminDB *sql.DB, dbName string) {
	tb.Helper()

	if _, err := adminDB.Exec(`
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = $1 AND pid <> pg_backend_pid()
`, dbName); err != nil {
		tb.Fatalf("terminate test database connections: %v", err)
	}
	if _, err := adminDB.Exec(`DROP DATABASE "` + dbName + `"`); err != nil {
		tb.Fatalf("drop test database: %v", err)
	}
}

func setupPhaseATestDatabase(tb testing.TB, db *sql.DB) {
	tb.Helper()

	statements := []string{
		`CREATE TABLE public.medias (id integer PRIMARY KEY, file_uri text NOT NULL, file_type integer NOT NULL, thumbnail_uri text)`,
		`CREATE TABLE public.tag_types (id integer PRIMARY KEY, description text NOT NULL)`,
		`CREATE TABLE public.tagsets (id integer PRIMARY KEY, name text NOT NULL UNIQUE, tagtype_id integer NOT NULL)`,
		`CREATE TABLE public.tags (id integer PRIMARY KEY, tagtype_id integer NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.alphanumerical_tags (id integer PRIMARY KEY, name text NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.numerical_tags (id integer PRIMARY KEY, name integer NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.date_tags (id integer PRIMARY KEY, name date NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.time_tags (id integer PRIMARY KEY, name time NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.timestamp_tags (id integer PRIMARY KEY, name timestamp NOT NULL, tagset_id integer NOT NULL)`,
		`CREATE TABLE public.taggings (object_id integer NOT NULL, tag_id integer NOT NULL, PRIMARY KEY (object_id, tag_id))`,
		`CREATE TABLE public.hierarchies (id integer PRIMARY KEY, name text NOT NULL, tagset_id integer NOT NULL, rootnode_id integer NULL)`,
		`CREATE TABLE public.nodes (id integer PRIMARY KEY, tag_id integer NOT NULL, hierarchy_id integer NOT NULL, parentnode_id integer NULL)`,
		`INSERT INTO public.tag_types (id, description) VALUES (1, 'alpha'), (2, 'timestamp')`,
		`INSERT INTO public.tagsets (id, name, tagtype_id) VALUES (1, 'Animals', 1), (2, 'Timestamp UTC', 2)`,
		`INSERT INTO public.medias (id, file_uri, file_type, thumbnail_uri) VALUES (1, 'file-1.jpg', 1, 'thumb-1.jpg'), (2, 'file-2.jpg', 1, 'thumb-2.jpg'), (3, 'file-3.jpg', 1, 'thumb-3.jpg')`,
		`INSERT INTO public.tags (id, tagtype_id, tagset_id) VALUES (101, 1, 1), (102, 1, 1), (201, 2, 2), (202, 2, 2), (203, 2, 2)`,
		`INSERT INTO public.alphanumerical_tags (id, name, tagset_id) VALUES (101, 'Cat', 1), (102, 'Dog', 1)`,
		`INSERT INTO public.timestamp_tags (id, name, tagset_id) VALUES (201, '2020-01-01 00:00:00', 2), (202, '2020-01-01 00:01:00', 2), (203, '2020-01-01 00:02:00', 2)`,
		`INSERT INTO public.taggings (object_id, tag_id) VALUES (1, 101), (2, 102), (3, 102), (1, 201), (2, 202), (3, 203)`,
		`CREATE MATERIALIZED VIEW public.tagsets_taggings AS SELECT t.tagset_id, r.tag_id, r.object_id FROM public.tags t JOIN public.taggings r ON r.tag_id = t.id`,
		`CREATE MATERIALIZED VIEW public.nodes_taggings AS SELECT NULL::integer AS parentnode_id, NULL::integer AS node_id, NULL::integer AS tag_id, NULL::integer AS object_id WHERE false`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			tb.Fatalf("setup statement failed: %v\n%s", err, stmt)
		}
	}
}

func startFakeVectorKV(tb testing.TB) (string, *fakeVectorKVServer, func()) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen vectorkv: %v", err)
	}

	srv := grpc.NewServer()
	handler := &fakeVectorKVServer{}
	kvstorev1.RegisterVectorKVServer(srv, handler)
	go func() {
		if err := srv.Serve(listener); err != nil && !strings.Contains(err.Error(), "closed network connection") {
			panic(err)
		}
	}()

	return listener.Addr().String(), handler, func() {
		srv.Stop()
		_ = listener.Close()
	}
}

func startDataLoaderGRPC(tb testing.TB, server *DataLoaderServer) (pb.DataLoaderClient, func()) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen dataloader: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterDataLoaderServer(srv, server)
	go func() {
		if err := srv.Serve(listener); err != nil && !strings.Contains(err.Error(), "closed network connection") {
			panic(err)
		}
	}()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("dial dataloader: %v", err)
	}

	return pb.NewDataLoaderClient(conn), func() {
		_ = conn.Close()
		srv.Stop()
		_ = listener.Close()
	}
}

func collectBrowsingStateResponses(t *testing.T, client pb.DataLoaderClient, req *pb.GetBrowsingStateRequest) []*pb.BrowsingStateResponse {
	t.Helper()

	stream, err := client.GetBrowsingState2(context.Background(), req)
	if err != nil {
		t.Fatalf("GetBrowsingState2: %v", err)
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

type cellExpectation struct {
	count            int32
	representativeID int32
}

func assertStateCells(t *testing.T, responses []*pb.BrowsingStateResponse, expected map[int32]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d responses, want %d", len(responses), len(expected))
	}

	for _, response := range responses {
		want, ok := expected[response.GetX()]
		if !ok {
			t.Fatalf("unexpected cell x=%d", response.GetX())
		}
		if response.GetCount() != want.count {
			t.Fatalf("cell x=%d count=%d want=%d", response.GetX(), response.GetCount(), want.count)
		}
		if got := response.GetCubeObjects()[0].GetId(); got != want.representativeID {
			t.Fatalf("cell x=%d representative=%d want=%d", response.GetX(), got, want.representativeID)
		}
	}
}

func assertStateCellsByXY(t *testing.T, responses []*pb.BrowsingStateResponse, expected map[string]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d responses, want %d", len(responses), len(expected))
	}
	for _, response := range responses {
		key := fmt.Sprintf("%d:%d", response.GetX(), response.GetY())
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

func assertAllCubeObjectIDs(t *testing.T, responses []*pb.BrowsingStateResponse, expected []int32) {
	t.Helper()

	if len(responses) != 1 {
		t.Fatalf("got %d all responses, want 1", len(responses))
	}
	ids := make([]int32, 0, len(responses[0].GetCubeObjects()))
	for _, obj := range responses[0].GetCubeObjects() {
		ids = append(ids, obj.GetId())
	}
	if !slices.Equal(ids, expected) {
		t.Fatalf("all ids = %v, want %v", ids, expected)
	}
}

func httpGetJSON(t *testing.T, rawURL string, target any) {
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
}

func assertCompatStateCells(t *testing.T, responses []compatBrowsingStateResponse, expected map[int32]cellExpectation) {
	t.Helper()

	if len(responses) != len(expected) {
		t.Fatalf("got %d compat responses, want %d", len(responses), len(expected))
	}
	for _, response := range responses {
		want, ok := expected[response.X]
		if !ok {
			t.Fatalf("unexpected compat cell x=%d", response.X)
		}
		if response.Count != want.count {
			t.Fatalf("compat cell x=%d count=%d want=%d", response.X, response.Count, want.count)
		}
		if got := response.CubeObjects[0].Id; got != want.representativeID {
			t.Fatalf("compat cell x=%d representative=%d want=%d", response.X, got, want.representativeID)
		}
	}
}

func assertCompatObjectIDs(t *testing.T, objects []compatCubeObject, expected []int32) {
	t.Helper()

	ids := make([]int32, 0, len(objects))
	for _, object := range objects {
		ids = append(ids, object.Id)
	}
	if !slices.Equal(ids, expected) {
		t.Fatalf("compat object ids = %v, want %v", ids, expected)
	}
}
