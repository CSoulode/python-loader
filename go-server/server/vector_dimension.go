package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

const (
	defaultCosineDistanceMax = 2.0
	defaultL2DistanceSpan    = 1.0
)

type vectorDimensionBucket struct {
	ID         int
	LowerBound float64
	UpperBound float64
}

type bucketedVectorResult struct {
	ObjectID int
	BucketID int
	Distance float64
}

type bucketConfig struct {
	Strategy    pb.BucketStrategy
	Count       int
	DistanceMin float64
	DistanceMax float64
}

type vectorDimensionResult struct {
	AxisType          pb.AxisType
	ParsedAxis        qg.ParsedAxis
	AxisSQL           string
	BucketInfos       []*pb.BucketInfo
	ObjectIDsByBucket map[int][]int
}

type compatVectorDimension struct {
	Model          string  `json:"model"`
	ObjectID       int32   `json:"objectId"`
	Axis           string  `json:"axis"`
	BucketCount    int32   `json:"bucketCount"`
	BucketStrategy string  `json:"bucketStrategy"`
	DistMin        float32 `json:"distMin"`
	DistMax        float32 `json:"distMax"`
	MaxResults     int32   `json:"maxResults"`
	K              int32   `json:"k"`
}

func validateVectorDimensionConfig(cfg *pb.VectorSearchDimension) error {
	if cfg == nil {
		return nil
	}
	if strings.TrimSpace(cfg.GetModelName()) == "" {
		return status.Error(codes.InvalidArgument, "vector_dimension.model_name is required")
	}
	if cfg.GetReference() == nil {
		return status.Error(codes.InvalidArgument, "vector_dimension.reference is required")
	}
	if cfg.GetMaxResults() <= 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.max_results must be > 0")
	}
	if cfg.GetBucketCfg() == nil {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg is required")
	}
	if cfg.GetBucketCfg().GetCount() <= 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.count must be > 0")
	}
	if cfg.GetBucketCfg().GetDistMin() < 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_min must be >= 0")
	}
	if cfg.GetBucketCfg().GetDistMax() < 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_max must be >= 0")
	}
	if cfg.GetBucketCfg().GetDistMax() > 0 && cfg.GetBucketCfg().GetDistMax() <= cfg.GetBucketCfg().GetDistMin() {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.dist_max must be greater than dist_min")
	}
	if len(cfg.GetBucketCfg().GetCustomBreaks()) > 0 {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks is not supported in phase B")
	}
	switch cfg.GetAxis() {
	case pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS, pb.AxisType_Z_AXIS:
	default:
		return status.Error(codes.InvalidArgument, "vector_dimension.axis must be X_AXIS, Y_AXIS, or Z_AXIS")
	}
	if cfg.GetBucketCfg().GetStrategy() != pb.BucketStrategy_EQUAL_WIDTH {
		return status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.strategy only supports EQUAL_WIDTH in phase B")
	}
	return nil
}

func (s *DataLoaderServer) resolveModelInfo(ctx context.Context, modelName string) (*kvstorev1.ModelInfo, error) {
	if s == nil || s.vectorFilters == nil || s.vectorFilters.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "vector filter resolver is not configured")
	}

	resp, err := s.vectorFilters.client.ListModels(ctx, &kvstorev1.ListModelsRequest{})
	if err != nil {
		return nil, wrapVectorKVError("vector_dimension models", err)
	}

	want := strings.TrimSpace(modelName)
	for _, model := range resp.GetModels() {
		if strings.TrimSpace(model.GetName()) == want {
			return model, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "vector_dimension model %q not found", want)
}

func (s *DataLoaderServer) handleVectorDimension(
	ctx context.Context,
	cfg *pb.VectorSearchDimension,
) (*vectorDimensionResult, error) {
	if err := validateVectorDimensionConfig(cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}

	modelInfo, err := s.resolveModelInfo(ctx, cfg.GetModelName())
	if err != nil {
		return nil, err
	}

	queryVector, err := s.vectorFilters.resolveQueryVector(ctx, &pb.VectorFilterConfig{
		ModelName: cfg.GetModelName(),
		Reference: cfg.GetReference(),
		K:         cfg.GetMaxResults(),
	})
	if err != nil {
		return nil, err
	}

	resp, err := s.vectorFilters.client.KNN(ctx, &kvstorev1.KNNRequest{
		Query: &kvstorev1.Vector{Values: queryVector},
		K:     cfg.GetMaxResults(),
		Model: strings.TrimSpace(cfg.GetModelName()),
	})
	if err != nil {
		return nil, wrapVectorKVError("vector_dimension search", err)
	}

	localCfg := bucketConfigFromProto(cfg.GetBucketCfg())
	localCfg = applyMetricDefaults(localCfg, modelInfo.GetDistanceMetric(), resp.GetNeighbors())

	filtered := filterNeighborsByDistance(resp.GetNeighbors(), localCfg.DistanceMin, localCfg.DistanceMax)
	boundaries, err := computeBucketBoundaries(localCfg, filtered)
	if err != nil {
		return nil, err
	}

	buckets := buildVectorBuckets(boundaries)
	bucketInfos := buildBucketInfos(buckets, modelInfo.GetDistanceMetric())
	bucketedResults, idsByBucket := assignBuckets(filtered, boundaries)

	return &vectorDimensionResult{
		AxisType: cfg.GetAxis(),
		ParsedAxis: qg.ParsedAxis{
			Type: "vector",
			Id:   unsetAxisId,
			Ids:  buildVectorAxisIDs(buckets),
		},
		AxisSQL:           buildVectorAxisSQL(bucketedResults),
		BucketInfos:       bucketInfos,
		ObjectIDsByBucket: idsByBucket,
	}, nil
}

func bucketConfigFromProto(cfg *pb.BucketConfig) bucketConfig {
	if cfg == nil {
		return bucketConfig{}
	}
	return bucketConfig{
		Strategy:    cfg.GetStrategy(),
		Count:       int(cfg.GetCount()),
		DistanceMin: float64(cfg.GetDistMin()),
		DistanceMax: float64(cfg.GetDistMax()),
	}
}

func applyMetricDefaults(cfg bucketConfig, metric string, neighbors []*kvstorev1.Neighbor) bucketConfig {
	if cfg.DistanceMax > 0 {
		return cfg
	}

	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "cosine":
		cfg.DistanceMax = defaultCosineDistanceMax
	case "l2":
		maxDistance := maxNeighborDistance(neighbors)
		if maxDistance <= cfg.DistanceMin {
			cfg.DistanceMax = cfg.DistanceMin + defaultL2DistanceSpan
			return cfg
		}
		cfg.DistanceMax = maxDistance * 1.01
	default:
		cfg.DistanceMax = cfg.DistanceMin + defaultL2DistanceSpan
	}

	if cfg.DistanceMax <= cfg.DistanceMin {
		cfg.DistanceMax = cfg.DistanceMin + defaultL2DistanceSpan
	}
	return cfg
}

func maxNeighborDistance(neighbors []*kvstorev1.Neighbor) float64 {
	maxDistance := 0.0
	for _, neighbor := range neighbors {
		distance := float64(neighbor.GetDistance())
		if distance > maxDistance {
			maxDistance = distance
		}
	}
	return maxDistance
}

func filterNeighborsByDistance(neighbors []*kvstorev1.Neighbor, minDistance float64, maxDistance float64) []*kvstorev1.Neighbor {
	if len(neighbors) == 0 {
		return nil
	}

	filtered := make([]*kvstorev1.Neighbor, 0, len(neighbors))
	for _, neighbor := range neighbors {
		distance := float64(neighbor.GetDistance())
		if distance < minDistance || distance > maxDistance {
			continue
		}
		filtered = append(filtered, neighbor)
	}
	return filtered
}

func computeBucketBoundaries(cfg bucketConfig, neighbors []*kvstorev1.Neighbor) ([]float64, error) {
	if cfg.Count <= 0 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.count must be > 0")
	}
	if cfg.Strategy != pb.BucketStrategy_EQUAL_WIDTH {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.strategy only supports EQUAL_WIDTH in phase B")
	}

	lower := cfg.DistanceMin
	upper := cfg.DistanceMax
	if upper <= lower {
		upper = lower + defaultL2DistanceSpan
	}
	if len(neighbors) == 0 && upper <= lower {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension distance range must be positive")
	}

	width := (upper - lower) / float64(cfg.Count)
	if width <= 0 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension distance range must be positive")
	}

	boundaries := make([]float64, cfg.Count+1)
	for i := 0; i <= cfg.Count; i++ {
		boundaries[i] = lower + (float64(i) * width)
	}
	boundaries[len(boundaries)-1] = upper
	return boundaries, nil
}

func buildVectorBuckets(boundaries []float64) []vectorDimensionBucket {
	if len(boundaries) < 2 {
		return nil
	}

	buckets := make([]vectorDimensionBucket, 0, len(boundaries)-1)
	for i := 0; i < len(boundaries)-1; i++ {
		buckets = append(buckets, vectorDimensionBucket{
			ID:         i,
			LowerBound: boundaries[i],
			UpperBound: boundaries[i+1],
		})
	}
	return buckets
}

func buildBucketInfos(buckets []vectorDimensionBucket, metric string) []*pb.BucketInfo {
	infos := make([]*pb.BucketInfo, 0, len(buckets))
	for index, bucket := range buckets {
		infos = append(infos, &pb.BucketInfo{
			BucketId:   int32(bucket.ID),
			LowerBound: float32(bucket.LowerBound),
			UpperBound: float32(bucket.UpperBound),
			Label:      formatBucketLabel(metric, bucket.LowerBound, bucket.UpperBound, index == len(buckets)-1),
		})
	}
	return infos
}

func formatBucketLabel(metric string, lower float64, upper float64, inclusiveUpper bool) string {
	prefix := ""
	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "cosine":
		prefix = "cos "
	case "l2":
		prefix = "L2 "
	}

	closing := ")"
	if inclusiveUpper {
		closing = "]"
	}
	return fmt.Sprintf("%s[%.2f, %.2f%s", prefix, lower, upper, closing)
}

func buildVectorAxisIDs(buckets []vectorDimensionBucket) map[int]int {
	ids := make(map[int]int, len(buckets))
	for index, bucket := range buckets {
		ids[bucket.ID] = index + 1
	}
	if len(ids) == 0 {
		ids[1] = defAxisPos
	}
	return ids
}

func assignBuckets(neighbors []*kvstorev1.Neighbor, boundaries []float64) ([]bucketedVectorResult, map[int][]int) {
	results := make([]bucketedVectorResult, 0, len(neighbors))
	objectIDsByBucket := make(map[int][]int)
	seen := make(map[int]struct{}, len(neighbors))

	for _, neighbor := range neighbors {
		objectID := int(neighbor.GetId())
		if _, ok := seen[objectID]; ok {
			continue
		}
		seen[objectID] = struct{}{}

		bucketID := locateBucket(float64(neighbor.GetDistance()), boundaries)
		if bucketID < 0 {
			continue
		}

		results = append(results, bucketedVectorResult{
			ObjectID: objectID,
			BucketID: bucketID,
			Distance: float64(neighbor.GetDistance()),
		})
		objectIDsByBucket[bucketID] = append(objectIDsByBucket[bucketID], objectID)
	}

	for bucketID := range objectIDsByBucket {
		sort.Ints(objectIDsByBucket[bucketID])
	}
	return results, objectIDsByBucket
}

func locateBucket(distance float64, boundaries []float64) int {
	if len(boundaries) < 2 {
		return -1
	}
	for index := 0; index < len(boundaries)-1; index++ {
		lower := boundaries[index]
		upper := boundaries[index+1]

		if index == len(boundaries)-2 {
			if distance >= lower && distance <= upper {
				return index
			}
			continue
		}
		if distance >= lower && distance < upper {
			return index
		}
	}
	return -1
}

func buildVectorAxisSQL(results []bucketedVectorResult) string {
	if len(results) == 0 {
		return "SELECT 0 AS object_id, 0 AS id WHERE false"
	}

	var b strings.Builder
	b.WriteString("SELECT V.object_id, V.id\nFROM (VALUES ")
	for index, result := range results {
		if index > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,%d)", result.ObjectID, result.BucketID)
	}
	b.WriteString(") AS V(object_id, id)")
	return b.String()
}

func cloneBucketInfos(infos []*pb.BucketInfo) []*pb.BucketInfo {
	if len(infos) == 0 {
		return nil
	}
	cloned := make([]*pb.BucketInfo, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		copyInfo := *info
		cloned = append(cloned, &copyInfo)
	}
	return cloned
}

func lookupBucketObjectIDs(idsByBucket map[int][]int, bucketID int32) []int {
	if len(idsByBucket) == 0 {
		return nil
	}
	ids := idsByBucket[int(bucketID)]
	if len(ids) == 0 {
		return []int{}
	}
	return append([]int(nil), ids...)
}

func parseCompatVectorDimension(raw string) (*pb.VectorSearchDimension, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var parsed compatVectorDimension
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorDimension JSON: %v", err)
	}

	maxResults := parsed.MaxResults
	if maxResults == 0 {
		maxResults = parsed.K
	}

	return &pb.VectorSearchDimension{
		ModelName: strings.TrimSpace(parsed.Model),
		Reference: &pb.VectorReference{
			Ref: &pb.VectorReference_ObjectId{ObjectId: parsed.ObjectID},
		},
		BucketCfg: &pb.BucketConfig{
			Strategy:     parseCompatBucketStrategy(parsed.BucketStrategy),
			Count:        parsed.BucketCount,
			DistMin:      parsed.DistMin,
			DistMax:      parsed.DistMax,
			CustomBreaks: nil,
		},
		MaxResults: maxResults,
		Axis:       parseCompatAxis(parsed.Axis),
	}, nil
}

func parseCompatVectorBucketID(raw string) (*int32, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	var bucketID int32
	if _, err := fmt.Sscanf(trimmed, "%d", &bucketID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vectorBucketId: %v", err)
	}
	return &bucketID, nil
}

func parseCompatAxis(raw string) pb.AxisType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "y", "y_axis", "y-axis":
		return pb.AxisType_Y_AXIS
	case "x", "x_axis", "x-axis":
		return pb.AxisType_X_AXIS
	case "z", "z_axis", "z-axis":
		return pb.AxisType_Z_AXIS
	default:
		return pb.AxisType_FILTER
	}
}

func parseCompatBucketStrategy(raw string) pb.BucketStrategy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "equal_width", "equal-width", "equalwidth":
		return pb.BucketStrategy_EQUAL_WIDTH
	case "equal_depth", "equal-depth", "equaldepth":
		return pb.BucketStrategy_EQUAL_DEPTH
	case "logarithmic", "log":
		return pb.BucketStrategy_LOGARITHMIC
	case "custom":
		return pb.BucketStrategy_CUSTOM
	default:
		return pb.BucketStrategy_CUSTOM
	}
}
