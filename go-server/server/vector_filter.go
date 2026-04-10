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
	FilteredKNN(context.Context, *kvstorev1.FilteredKNNRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error)
	RangeSearch(context.Context, *kvstorev1.RangeSearchRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error)
	FilteredRangeSearch(context.Context, *kvstorev1.FilteredRangeSearchRequest, ...grpc.CallOption) (*kvstorev1.KNNResponse, error)
	ListModels(context.Context, *kvstorev1.ListModelsRequest, ...grpc.CallOption) (*kvstorev1.ListModelsResponse, error)
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
	if err := validateVectorReference(cfg.GetReference(), "vector_filter.reference"); err != nil {
		return err
	}
	if cfg.GetK() <= 0 {
		return status.Error(codes.InvalidArgument, "vector_filter.k must be > 0")
	}
	if cfg.GetMaxDistance() < 0 {
		return status.Error(codes.InvalidArgument, "vector_filter.max_distance must be >= 0")
	}
	return nil
}

func validateVectorReference(ref *pb.VectorReference, fieldName string) error {
	switch value := ref.GetRef().(type) {
	case *pb.VectorReference_ObjectId:
		if value.ObjectId <= 0 {
			return status.Errorf(codes.InvalidArgument, "%s.object_id must be > 0", fieldName)
		}
	case *pb.VectorReference_RawEmbedding:
		if len(value.RawEmbedding.GetValues()) == 0 {
			return status.Errorf(codes.InvalidArgument, "%s.raw_embedding must not be empty", fieldName)
		}
	default:
		return status.Errorf(codes.InvalidArgument, "%s must set object_id or raw_embedding", fieldName)
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
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
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

type browsingStateRequestPlan struct {
	AxisOrder          []string
	AxisX              qg.ParsedAxis
	AxisY              qg.ParsedAxis
	AxisZ              qg.ParsedAxis
	Filters            []qg.ParsedFilter
	AxisSubqueries     map[string]string
	BucketInfos        []*pb.BucketInfo
	AxisBucketInfos    map[string][]*pb.BucketInfo
	UseAxisBucketInfos bool
}

func (s *DataLoaderServer) parseBrowsingStateRequest(
	ctx context.Context,
	req *pb.GetBrowsingStateRequest,
) (*browsingStateRequestPlan, error) {
	if err := rejectAxisLevelVectorFilters(req.GetFilters()); err != nil {
		return nil, err
	}
	forcedStrategy, err := strategyFromProto(req.GetHybridStrategy())
	if err != nil {
		return nil, err
	}

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if err != nil {
		return nil, err
	}
	merged, err := mergeVectorDimensions(req)
	if err != nil {
		return nil, err
	}
	if err := validateVectorFilterAgainstDimensions(req.GetVectorFilter(), merged.Dims); err != nil {
		return nil, err
	}

	vectorIDs, active, err := s.resolveRequestVectorFilter(ctx, req)
	if err != nil {
		return nil, err
	}

	filters = appendVectorObjectIDFilter(filters, vectorIDs, active)
	if active && !containsAxisOrder(axisOrder, "filter") {
		axisOrder = append(axisOrder, "filter")
	}

	plan := &browsingStateRequestPlan{
		AxisOrder: axisOrder,
		AxisX:     axisX,
		AxisY:     axisY,
		AxisZ:     axisZ,
		Filters:   filters,
	}
	return s.applyVectorDimensionsToPlan(
		ctx,
		plan,
		merged,
		strings.TrimSpace(req.GetAll()) != "",
		strings.TrimSpace(req.GetTimeline()) != "",
		req.GetRebucketOnly(),
		forcedStrategy,
	)
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

func (s *DataLoaderServer) applyVectorDimensionToPlan(
	ctx context.Context,
	plan *browsingStateRequestPlan,
	vectorDimension *pb.VectorSearchDimension,
	vectorBucketID *int32,
	allDefined bool,
	timelineDefined bool,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) (*browsingStateRequestPlan, error) {
	merged := mergedVectorDimensions{}
	if vectorDimension != nil {
		merged.Dims = []*pb.VectorSearchDimension{vectorDimension}
	}
	if vectorBucketID != nil {
		if vectorDimension == nil {
			return nil, status.Error(codes.InvalidArgument, "vector_bucket_id requires vector_dimension")
		}
		axisKey := axisToken(vectorDimension.GetAxis())
		if axisKey == "" {
			return nil, status.Error(codes.InvalidArgument, "vector_dimension.axis must be X_AXIS, Y_AXIS, or Z_AXIS")
		}
		merged.BucketIDs = map[string]int32{axisKey: *vectorBucketID}
	}
	return s.applyVectorDimensionsToPlan(
		ctx,
		plan,
		merged,
		allDefined,
		timelineDefined,
		rebucketOnly,
		forcedStrategy,
	)
}

func validateVectorDimensionRequestUsage(
	vectorDimension *pb.VectorSearchDimension,
	vectorBucketID *int32,
	allDefined bool,
	timelineDefined bool,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) error {
	if vectorDimension == nil {
		if vectorBucketID != nil {
			return status.Error(codes.InvalidArgument, "vector_bucket_id requires vector_dimension")
		}
		if rebucketOnly {
			return status.Error(codes.InvalidArgument, "rebucket_only requires vector_dimension")
		}
		if forcedStrategy != Auto {
			return status.Error(codes.InvalidArgument, "hybrid_strategy requires vector_dimension")
		}
		return nil
	}
	if timelineDefined {
		return status.Error(codes.InvalidArgument, "vector_dimension is not supported with timeline requests")
	}
	if rebucketOnly && allDefined {
		return status.Error(codes.InvalidArgument, "rebucket_only is not supported with all requests")
	}
	if allDefined {
		if vectorBucketID == nil {
			return status.Error(codes.InvalidArgument, "vector_bucket_id must be set when all and vector_dimension are both provided")
		}
		if *vectorBucketID < 0 {
			return status.Error(codes.InvalidArgument, "vector_bucket_id must be >= 0")
		}
		return nil
	}
	if vectorBucketID != nil {
		return status.Error(codes.InvalidArgument, "vector_bucket_id is only valid when all and vector_dimension are both provided")
	}
	return nil
}

func axisToken(axisType pb.AxisType) string {
	switch axisType {
	case pb.AxisType_X_AXIS:
		return "x"
	case pb.AxisType_Y_AXIS:
		return "y"
	case pb.AxisType_Z_AXIS:
		return "z"
	default:
		return ""
	}
}

func selectAxisByToken(plan *browsingStateRequestPlan, axisKey string) qg.ParsedAxis {
	switch axisKey {
	case "x":
		return plan.AxisX
	case "y":
		return plan.AxisY
	case "z":
		return plan.AxisZ
	default:
		return qg.ParsedAxis{}
	}
}

func setAxisByToken(plan *browsingStateRequestPlan, axisKey string, axis qg.ParsedAxis) {
	switch axisKey {
	case "x":
		plan.AxisX = axis
	case "y":
		plan.AxisY = axis
	case "z":
		plan.AxisZ = axis
	}
}

func ensureAxisOrder(axisOrder []string, target string) []string {
	if containsAxisOrder(axisOrder, target) {
		return axisOrder
	}

	next := make([]string, 0, len(axisOrder)+1)
	inserted := false
	for _, item := range axisOrder {
		if item == "filter" && !inserted {
			next = append(next, target)
			inserted = true
		}
		next = append(next, item)
	}
	if !inserted {
		next = append(next, target)
	}
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
