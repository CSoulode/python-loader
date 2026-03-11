package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

const vectorDialTimeout = 5 * time.Second

type vectorSearchClient interface {
	Get(context.Context, *kvstorev1.GetRequest, ...grpc.CallOption) (*kvstorev1.GetResponse, error)
	KNN(context.Context, *kvstorev1.KNNRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error)
}

type vectorFilterResolver struct {
	client vectorSearchClient
}

type compatVectorFilter struct {
	Model       string  `json:"model"`
	ObjectID    int32   `json:"objectId"`
	K           int32   `json:"k"`
	MaxDistance float32 `json:"maxDistance"`
}

func newVectorFilterResolver(client vectorSearchClient) *vectorFilterResolver {
	return &vectorFilterResolver{client: client}
}

func newVectorFilterResolverFromAddress(ctx context.Context, addr string) (*vectorFilterResolver, *grpc.ClientConn, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "vectorkv address is required")
	}

	dialCtx, cancel := context.WithTimeout(ctx, vectorDialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "connect vectorkv: %v", err)
	}

	return newVectorFilterResolver(kvstorev1.NewVectorKVClient(conn)), conn, nil
}

func rejectAxisLevelVectorFilters(filters []*pb.AxisFilter) error {
	for _, filter := range filters {
		if filter.GetValueType() != pb.FilterValueType_VECTOR_FILTER {
			continue
		}
		return status.Error(codes.InvalidArgument, "VECTOR_FILTER must use request.vector_filter, not filters[]")
	}
	return nil
}

func validateVectorFilterConfig(cfg *pb.VectorFilterConfig) error {
	if cfg == nil {
		return nil
	}
	if strings.TrimSpace(cfg.GetModelName()) == "" {
		return status.Error(codes.InvalidArgument, "vector_filter.model_name is required")
	}
	if cfg.GetReference() == nil {
		return status.Error(codes.InvalidArgument, "vector_filter.reference is required")
	}
	if cfg.GetK() <= 0 {
		return status.Error(codes.InvalidArgument, "vector_filter.k must be > 0")
	}
	if cfg.GetMaxDistance() < 0 {
		return status.Error(codes.InvalidArgument, "vector_filter.max_distance must be >= 0")
	}
	return nil
}

func (r *vectorFilterResolver) ResolveObjectIDs(ctx context.Context, cfg *pb.VectorFilterConfig) ([]int, error) {
	if err := validateVectorFilterConfig(cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	if r == nil || r.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "vector filter resolver is not configured")
	}

	queryVector, err := r.resolveQueryVector(ctx, cfg)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.KNN(ctx, &kvstorev1.KNNRequest{
		Query:       &kvstorev1.Vector{Values: queryVector},
		K:           cfg.GetK(),
		Model:       strings.TrimSpace(cfg.GetModelName()),
		MaxDistance: cfg.GetMaxDistance(),
	})
	if status.Code(err) == codes.NotFound {
		return []int{}, nil
	}
	if err != nil {
		return nil, wrapVectorKVError("vector_filter search", err)
	}

	return sortedNeighborIDs(resp.GetNeighbors()), nil
}

func (r *vectorFilterResolver) resolveQueryVector(ctx context.Context, cfg *pb.VectorFilterConfig) ([]float32, error) {
	switch ref := cfg.GetReference().GetRef().(type) {
	case *pb.VectorReference_ObjectId:
		if ref.ObjectId <= 0 {
			return nil, status.Error(codes.InvalidArgument, "vector_filter.reference.object_id must be > 0")
		}
		resp, err := r.client.Get(ctx, &kvstorev1.GetRequest{
			Id:    ref.ObjectId,
			Model: strings.TrimSpace(cfg.GetModelName()),
		})
		if err != nil {
			return nil, wrapVectorKVError("vector_filter reference lookup", err)
		}
		return append([]float32(nil), resp.GetVector().GetValues()...), nil
	case *pb.VectorReference_RawEmbedding:
		values := ref.RawEmbedding.GetValues()
		if len(values) == 0 {
			return nil, status.Error(codes.InvalidArgument, "vector_filter.reference.raw_embedding must not be empty")
		}
		return append([]float32(nil), values...), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "vector_filter.reference must set object_id or raw_embedding")
	}
}

func parseCompatVectorFilter(raw string) (*pb.VectorFilterConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var parsed compatVectorFilter
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorFilter JSON: %v", err)
	}

	return &pb.VectorFilterConfig{
		ModelName: strings.TrimSpace(parsed.Model),
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_ObjectId{ObjectId: parsed.ObjectID},
		},
		K:           parsed.K,
		MaxDistance: parsed.MaxDistance,
	}, nil
}

func mapVectorFilterHTTPStatus(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusBadGateway
	default:
		return http.StatusBadGateway
	}
}

func vectorFilterHTTPMessage(err error) string {
	if status.Code(err) == codes.Unknown {
		return err.Error()
	}
	return status.Convert(err).Message()
}

func wrapVectorKVError(prefix string, err error) error {
	code := status.Code(err)
	msg := status.Convert(err).Message()

	switch code {
	case codes.InvalidArgument, codes.NotFound, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return status.Errorf(code, "%s: %s", prefix, msg)
	default:
		return status.Errorf(codes.Internal, "%s: %v", prefix, err)
	}
}

func sortedNeighborIDs(neighbors []*kvstorev1.Neighbor) []int {
	seen := make(map[int]struct{}, len(neighbors))
	ids := make([]int, 0, len(neighbors))
	for _, neighbor := range neighbors {
		id := int(neighbor.GetId())
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (s *DataLoaderServer) parseBrowsingStateRequest(
	ctx context.Context,
	req *pb.GetBrowsingStateRequest,
) ([]string, qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	if err := rejectAxisLevelVectorFilters(req.GetFilters()); err != nil {
		return nil, qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if err != nil {
		return nil, qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	vectorIDs, active, err := s.resolveRequestVectorFilter(ctx, req)
	if err != nil {
		return nil, qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	filters = appendVectorObjectIDFilter(filters, vectorIDs, active)
	if active && !containsAxisOrder(axisOrder, "filter") {
		axisOrder = append(axisOrder, "filter")
	}

	return axisOrder, axisX, axisY, axisZ, filters, nil
}

func (s *DataLoaderServer) resolveRequestVectorFilter(
	ctx context.Context,
	req *pb.GetBrowsingStateRequest,
) ([]int, bool, error) {
	if req.GetVectorFilter() == nil {
		return nil, false, nil
	}
	if strings.TrimSpace(req.GetTimeline()) != "" {
		return nil, false, status.Error(codes.InvalidArgument, "vector_filter is not supported with timeline requests")
	}

	ids, err := s.vectorFilters.ResolveObjectIDs(ctx, req.GetVectorFilter())
	if err != nil {
		return nil, false, err
	}
	return ids, true, nil
}

func appendVectorObjectIDFilter(filters []qg.ParsedFilter, ids []int, active bool) []qg.ParsedFilter {
	if !active {
		return filters
	}

	next := make([]qg.ParsedFilter, 0, len(filters)+1)
	next = append(next, filters...)
	next = append(next, qg.ParsedFilter{Type: "objectid", Ids: ids})
	return next
}

func containsAxisOrder(axisOrder []string, target string) bool {
	for _, item := range axisOrder {
		if item == target {
			return true
		}
	}
	return false
}
