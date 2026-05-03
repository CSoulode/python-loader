package main

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

const (
	defaultCosineDistanceMax = 2.0
	defaultL2DistanceSpan    = 1.0
	logBucketEpsilon         = 1e-6
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
	Strategy     pb.BucketStrategy
	Count        int
	DistanceMin  float64
	DistanceMax  float64
	CustomBreaks []float64
}

func bucketConfigFromProto(cfg *pb.BucketConfig) bucketConfig {
	if cfg == nil {
		return bucketConfig{}
	}

	return bucketConfig{
		Strategy:     cfg.GetStrategy(),
		Count:        int(cfg.GetCount()),
		DistanceMin:  float64(cfg.GetDistMin()),
		DistanceMax:  float64(cfg.GetDistMax()),
		CustomBreaks: float32SliceToFloat64(cfg.GetCustomBreaks()),
	}
}

func float32SliceToFloat64(values []float32) []float64 {
	if len(values) == 0 {
		return nil
	}

	out := make([]float64, len(values))
	for index, value := range values {
		out[index] = float64(value)
	}
	return out
}

func applyMetricDefaults(cfg bucketConfig, metric string, neighbors []Neighbor) bucketConfig {
	if cfg.DistanceMax > 0 {
		return cfg
	}

	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "cosine":
		cfg.DistanceMax = defaultCosineDistanceMax
	case "l2":
		cfg.DistanceMax = deriveL2MaxDistance(cfg.DistanceMin, neighbors)
	default:
		cfg.DistanceMax = cfg.DistanceMin + defaultL2DistanceSpan
	}

	if cfg.DistanceMax <= cfg.DistanceMin {
		cfg.DistanceMax = cfg.DistanceMin + defaultL2DistanceSpan
	}
	return cfg
}

func deriveL2MaxDistance(minDistance float64, neighbors []Neighbor) float64 {
	maxDistance := maxNeighborDistance(neighbors)
	if maxDistance <= minDistance {
		return minDistance + defaultL2DistanceSpan
	}
	return maxDistance * 1.01
}

func maxNeighborDistance(neighbors []Neighbor) float64 {
	maxDistance := 0.0
	for _, neighbor := range neighbors {
		if neighbor.Distance > maxDistance {
			maxDistance = neighbor.Distance
		}
	}
	return maxDistance
}

func filterNeighborsByRange(neighbors []Neighbor, minDistance float64, maxDistance float64, limit int32) []Neighbor {
	candidates := neighbors
	if limit > 0 && len(candidates) > int(limit) {
		candidates = candidates[:limit]
	}

	filtered := make([]Neighbor, 0, len(candidates))
	for _, neighbor := range candidates {
		if neighbor.Distance < minDistance || neighbor.Distance > maxDistance {
			continue
		}
		filtered = append(filtered, neighbor)
	}
	return filtered
}

func computeBucketBoundaries(cfg bucketConfig, neighbors []Neighbor) ([]float64, error) {
	switch cfg.Strategy {
	case pb.BucketStrategy_EQUAL_WIDTH:
		return computeEqualWidthBoundaries(cfg.Count, cfg.DistanceMin, cfg.DistanceMax)
	case pb.BucketStrategy_EQUAL_DEPTH:
		return computeEqualDepthBoundaries(cfg, neighbors)
	case pb.BucketStrategy_LOGARITHMIC:
		return computeLogarithmicBoundaries(cfg.Count, cfg.DistanceMin, cfg.DistanceMax)
	case pb.BucketStrategy_CUSTOM:
		return computeCustomBoundaries(cfg.CustomBreaks)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported bucket strategy %s", cfg.Strategy.String())
	}
}

func computeEqualWidthBoundaries(count int, lower float64, upper float64) ([]float64, error) {
	if count <= 0 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.count must be > 0")
	}
	if upper <= lower {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension distance range must be positive")
	}

	width := (upper - lower) / float64(count)
	boundaries := make([]float64, count+1)
	for index := 0; index <= count; index++ {
		boundaries[index] = lower + (float64(index) * width)
	}
	boundaries[len(boundaries)-1] = upper
	return boundaries, nil
}

func computeEqualDepthBoundaries(cfg bucketConfig, neighbors []Neighbor) ([]float64, error) {
	if len(neighbors) == 0 {
		return computeEqualWidthBoundaries(cfg.Count, cfg.DistanceMin, cfg.DistanceMax)
	}

	distances := sortedNeighborDistances(neighbors)
	boundaries := make([]float64, 0, cfg.Count+1)
	boundaries = append(boundaries, cfg.DistanceMin)
	for step := 1; step < cfg.Count; step++ {
		index := quantileIndex(len(distances), cfg.Count, step)
		boundaries = append(boundaries, distances[index])
	}
	boundaries = append(boundaries, cfg.DistanceMax)
	return deduplicateSortedFloat64(boundaries), nil
}

func sortedNeighborDistances(neighbors []Neighbor) []float64 {
	distances := make([]float64, len(neighbors))
	for index, neighbor := range neighbors {
		distances[index] = neighbor.Distance
	}
	sort.Float64s(distances)
	return distances
}

func quantileIndex(total int, bucketCount int, step int) int {
	index := (step * total) / bucketCount
	if index >= total {
		return total - 1
	}
	return index
}

func deduplicateSortedFloat64(values []float64) []float64 {
	if len(values) == 0 {
		return nil
	}

	deduped := []float64{values[0]}
	for _, value := range values[1:] {
		if value != deduped[len(deduped)-1] {
			deduped = append(deduped, value)
		}
	}
	return deduped
}

func computeLogarithmicBoundaries(count int, lower float64, upper float64) ([]float64, error) {
	if count <= 0 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.count must be > 0")
	}
	if upper <= lower {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension distance range must be positive")
	}

	lowerEffective := math.Max(lower, logBucketEpsilon)
	logLower := math.Log(lowerEffective)
	logUpper := math.Log(upper)
	step := (logUpper - logLower) / float64(count)

	boundaries := make([]float64, count+1)
	boundaries[0] = lower
	for index := 1; index < count; index++ {
		boundaries[index] = math.Exp(logLower + (float64(index) * step))
	}
	boundaries[len(boundaries)-1] = upper
	return boundaries, nil
}

func computeCustomBoundaries(breaks []float64) ([]float64, error) {
	if len(breaks) < 2 {
		return nil, status.Error(codes.InvalidArgument, "vector_dimension.bucket_cfg.custom_breaks must contain at least 2 values")
	}
	return append([]float64(nil), breaks...), nil
}

func buildVectorBuckets(boundaries []float64) []vectorDimensionBucket {
	if len(boundaries) < 2 {
		return nil
	}

	buckets := make([]vectorDimensionBucket, 0, len(boundaries)-1)
	for index := 0; index < len(boundaries)-1; index++ {
		buckets = append(buckets, vectorDimensionBucket{
			ID:         index,
			LowerBound: boundaries[index],
			UpperBound: boundaries[index+1],
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

func assignBuckets(neighbors []Neighbor, boundaries []float64) ([]bucketedVectorResult, map[int][]int) {
	results := make([]bucketedVectorResult, 0, len(neighbors))
	objectIDsByBucket := make(map[int][]int)
	seen := make(map[int]struct{}, len(neighbors))

	for _, neighbor := range neighbors {
		objectID := int(neighbor.ObjectID)
		if _, ok := seen[objectID]; ok {
			continue
		}

		bucketID := locateBucket(neighbor.Distance, boundaries)
		if bucketID < 0 {
			continue
		}

		seen[objectID] = struct{}{}
		results = append(results, bucketedVectorResult{
			ObjectID: objectID,
			BucketID: bucketID,
			Distance: neighbor.Distance,
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

	last := len(boundaries) - 2
	for index := 0; index <= last; index++ {
		lower := boundaries[index]
		upper := boundaries[index+1]
		if index == last && distance >= lower && distance <= upper {
			return index
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

	var builder strings.Builder
	builder.WriteString("SELECT V.object_id, V.id\nFROM (VALUES ")
	for index, result := range results {
		if index > 0 {
			builder.WriteString(",")
		}
		fmt.Fprintf(&builder, "(%d,%d)", result.ObjectID, result.BucketID)
	}
	builder.WriteString(") AS V(object_id, id)")
	return builder.String()
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
		copyInfo, ok := proto.Clone(info).(*pb.BucketInfo)
		if ok {
			cloned = append(cloned, copyInfo)
		}
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
