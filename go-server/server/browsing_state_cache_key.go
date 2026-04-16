package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

const (
	bsCacheNamespaceGetBrowsingState                       = "grpc:GetBrowsingState"
	bsCacheNamespaceGetBrowsingState2                      = "grpc:GetBrowsingState2"
	bsCacheNamespaceDistinctBranchesIncrementalGrouping    = "grpc:GetBrowsingStateDistinctBranchesIncrementalGrouping"
	bsCacheNamespaceDistinctBranchesFull                   = "grpc:GetBrowsingStateDistinctBranchesFull"
	bsCacheNamespaceNonDistinctBranchesSingles             = "grpc:GetBrowsingStateNonDistinctBranchesSingles"
	bsCacheNamespaceNonDistinctBranchesDeduplicatedSingles = "grpc:GetBrowsingStateNonDistinctBranchesDeduplicatedSingles"
	bsCacheNamespaceNonDistinctBranchesFull                = "grpc:GetBrowsingStateNonDistinctBranchesFull"
	bsCacheNamespaceNonDistinctBranchesIncrementalGrouping = "grpc:GetBrowsingStateNonDistinctBranchesIncrementalGrouping"
	bsCacheNamespaceHTTPCompatCell                         = "http:GetMetaDataCubeCompatCell"
	bsBucketInfoStyleNone                                  = "none"
	bsBucketInfoStyleLegacy                                = "legacy"
	bsBucketInfoStyleAxis                                  = "axis"
)

func shouldCachePreparedBrowsingState(
	prepared *preparedBrowsingStateRequest,
) bool {
	if prepared == nil {
		return false
	}
	return !prepared.AllDefined && !prepared.TimelineDefined
}

func computeBSCacheKey(
	namespace string,
	prepared *preparedBrowsingStateRequest,
) (bsCacheStorageKey, error) {
	if prepared == nil {
		return bsCacheStorageKey{}, nil
	}

	parts := []string{
		"ns|" + strings.TrimSpace(namespace),
		"style|" + bucketInfoStyleForPrepared(prepared),
		"hybrid|" + prepared.ForcedStrategy.String(),
	}

	filterEntries, err := canonicalFilterEntries(prepared.Filters)
	if err != nil {
		return bsCacheStorageKey{}, err
	}
	for _, entry := range filterEntries {
		parts = append(parts, "f|"+entry)
	}

	for _, entry := range canonicalAxisEntries(
		prepared.AxisX,
		prepared.AxisY,
		prepared.AxisZ,
	) {
		parts = append(parts, "a|"+entry)
	}

	if prepared.VectorFilter != nil {
		entry, err := canonicalVectorFilterEntry(prepared.VectorFilter)
		if err != nil {
			return bsCacheStorageKey{}, err
		}
		parts = append(parts, "vf|"+entry)
	}

	dimEntries, err := canonicalVectorDimensionEntries(prepared.MergedVectorDims.Dims)
	if err != nil {
		return bsCacheStorageKey{}, err
	}
	for _, entry := range dimEntries {
		parts = append(parts, "vd|"+entry)
	}

	return bsCacheStorageKey{
		Namespace: namespace,
		Canonical: strings.Join(parts, "\n"),
	}, nil
}

func bucketInfoStyleForPrepared(
	prepared *preparedBrowsingStateRequest,
) string {
	if prepared == nil || len(prepared.MergedVectorDims.Dims) == 0 {
		return bsBucketInfoStyleNone
	}
	if prepared.MergedVectorDims.UseAxisBucketInfos {
		return bsBucketInfoStyleAxis
	}
	return bsBucketInfoStyleLegacy
}

func canonicalFilterEntries(filters []qg.ParsedFilter) ([]string, error) {
	entries := make([]string, 0, len(filters))
	for _, filter := range filters {
		entry, err := canonicalFilterHashEntry(filter)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	sort.Strings(entries)
	return entries, nil
}

func canonicalAxisEntries(axes ...qg.ParsedAxis) []string {
	entries := make([]string, 0, len(axes))
	for _, axis := range axes {
		if strings.TrimSpace(axis.Type) == "" {
			continue
		}
		entries = append(entries, canonicalAxisHashEntry(axis))
	}
	sort.Strings(entries)
	return entries
}

func canonicalVectorFilterEntry(cfg *pb.VectorFilterConfig) (string, error) {
	refHash, err := hashVectorReference(cfg.GetReference())
	if err != nil {
		return "", err
	}
	return strings.Join([]string{
		normalizeVectorModelName(cfg.GetModelName()),
		"ref=" + strconv.FormatUint(refHash, 10),
		"k=" + strconv.Itoa(int(cfg.GetK())),
		"max=" + formatFloat32ForCacheKey(cfg.GetMaxDistance()),
	}, "|"), nil
}

func canonicalVectorDimensionEntries(
	dims []*pb.VectorSearchDimension,
) ([]string, error) {
	entries := make([]string, 0, len(dims))
	for _, dim := range dims {
		if dim == nil {
			continue
		}
		entry, err := canonicalVectorDimensionEntry(dim)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	sort.Strings(entries)
	return entries, nil
}

func canonicalVectorDimensionEntry(
	dim *pb.VectorSearchDimension,
) (string, error) {
	refHash, err := hashVectorReference(dim.GetReference())
	if err != nil {
		return "", err
	}

	parts := []string{
		"axis=" + axisToken(dim.GetAxis()),
		"model=" + normalizeVectorModelName(dim.GetModelName()),
		"ref=" + strconv.FormatUint(refHash, 10),
		"bucket=" + canonicalBucketConfigEntry(dim.GetBucketCfg()),
		"max_results=" + strconv.Itoa(int(dim.GetMaxResults())),
		"range=" + canonicalDistanceRangeEntry(dim.GetDistanceRange()),
		"range_semantics=" + fmt.Sprintf("%d", dim.GetRangeSemantics()),
	}
	return strings.Join(parts, "|"), nil
}

func canonicalBucketConfigEntry(cfg *pb.BucketConfig) string {
	if cfg == nil {
		return "nil"
	}
	customBreaks := make([]string, 0, len(cfg.GetCustomBreaks()))
	for _, value := range cfg.GetCustomBreaks() {
		customBreaks = append(customBreaks, formatFloat32ForCacheKey(value))
	}
	return strings.Join([]string{
		fmt.Sprintf("%d", cfg.GetStrategy()),
		strconv.Itoa(int(cfg.GetCount())),
		formatFloat32ForCacheKey(cfg.GetDistMin()),
		formatFloat32ForCacheKey(cfg.GetDistMax()),
		"[" + strings.Join(customBreaks, ",") + "]",
	}, ":")
}

func canonicalDistanceRangeEntry(dr *pb.DistanceRange) string {
	if dr == nil {
		return "nil"
	}
	return formatFloat32ForCacheKey(dr.GetMinDistance()) +
		":" + formatFloat32ForCacheKey(dr.GetMaxDistance())
}

func formatFloat32ForCacheKey(value float32) string {
	return strconv.FormatUint(uint64(math.Float32bits(value)), 16)
}
