package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

func newHTTPBrowsingStateSnapshot(r *http.Request) *BrowsingStateSnapshot {
	if r == nil {
		return nil
	}
	query := r.URL.Query()
	dims, _ := parseCompatVectorDimensions(query)
	vectorSearchKey, vectorBucketKey, dimKeys := vectorSnapshotKeys(dims.Dims)
	strategy, _ := parseCompatHybridStrategy(compatHybridStrategyQueryValue(query))
	return &BrowsingStateSnapshot{
		FullKey:         string(computeHTTPBrowsingStateID(r)),
		ReuseKey:        computeHTTPBrowsingStateReuseKey(r),
		AxisKey:         canonicalQueryValues(query, "xAxis", "yAxis", "zAxis"),
		AxisDomainKey:   axisDomainKeyFromHTTP(r),
		FilterKey:       canonicalQueryValues(query, "filters", "vectorFilter"),
		FilterCount:     httpSnapshotFilterCount(query),
		FilterItems:     filterItemsFromHTTP(r),
		MetadataKey:     canonicalQueryValues(query, "xAxis", "yAxis", "zAxis", "filters", "vectorFilter"),
		VectorSearchKey: vectorSearchKey,
		VectorBucketKey: vectorBucketKey + "|" + canonicalQueryValues(query, "vectorBucketId", "vectorBucketIds"),
		StrategyKey:     canonicalCompatHybridStrategy(strategy),
		VectorDims:      dimKeys,
		Dependencies:    extractHTTPBrowsingStateDependencies(r),
	}
}

func newGRPCBrowsingStateSnapshot(req *pb.GetBrowsingStateRequest, targetID StateID, reuseKey string) *BrowsingStateSnapshot {
	if req == nil {
		return nil
	}
	merged, _ := mergeVectorDimensions(req)
	vectorSearchKey, vectorBucketKey, dimKeys := vectorSnapshotKeys(merged.Dims)
	return &BrowsingStateSnapshot{
		FullKey:         string(targetID),
		ReuseKey:        reuseKey,
		AxisKey:         hashProtoMessage("grpc-axis", axisSnapshotProto(req)),
		AxisDomainKey:   axisDomainKeyFromGRPC(req),
		FilterKey:       hashProtoMessage("grpc-filter", filterSnapshotProto(req)),
		FilterCount:     grpcSnapshotFilterCount(req),
		FilterItems:     filterItemsFromGRPC(req),
		MetadataKey:     hashProtoMessage("grpc-metadata", metadataSnapshotProto(req)),
		VectorSearchKey: vectorSearchKey,
		VectorBucketKey: vectorBucketKey + "|" + canonicalBucketIDs(merged.BucketIDs),
		StrategyKey:     canonicalHybridStrategy(req.GetHybridStrategy()),
		VectorDims:      dimKeys,
		Dependencies:    extractGRPCBrowsingStateDependencies(req),
	}
}

func inferTransitionDelta(parent *BrowsingStateSnapshot, target *BrowsingStateSnapshot, requested TransitionDelta) TransitionDelta {
	if parent == nil || target == nil {
		if requested.Kind == DeltaRebucketOnly {
			return requested
		}
		return TransitionDelta{Kind: DeltaRoot}
	}
	if parent.StrategyKey != target.StrategyKey {
		return TransitionDelta{Kind: DeltaChangeForcedStrategy, Payload: target.StrategyKey}
	}
	if requested.Kind == DeltaRebucketOnly {
		return requested
	}
	if parent.FilterKey != target.FilterKey {
		return inferFilterDelta(parent, target)
	}
	if parent.AxisKey != target.AxisKey {
		return TransitionDelta{Kind: DeltaChangeAxisNonVector, Payload: target.AxisKey}
	}
	if parent.VectorSearchKey != target.VectorSearchKey {
		return inferVectorDelta(parent, target)
	}
	if parent.VectorBucketKey != target.VectorBucketKey {
		return TransitionDelta{Kind: DeltaRebucketOnly, Payload: target.VectorBucketKey}
	}
	return TransitionDelta{Kind: DeltaRoot}
}

func inferFilterDelta(parent *BrowsingStateSnapshot, target *BrowsingStateSnapshot) TransitionDelta {
	if target.FilterCount <= parent.FilterCount {
		return TransitionDelta{Kind: DeltaRemoveFilter, Payload: target.FilterKey}
	}
	return TransitionDelta{Kind: DeltaAddFilter, Payload: target.FilterKey}
}

func inferVectorDelta(parent *BrowsingStateSnapshot, target *BrowsingStateSnapshot) TransitionDelta {
	if isStringMapSubset(target.VectorDims, parent.VectorDims) {
		return TransitionDelta{Kind: DeltaRemoveVectorDim, Payload: target.VectorSearchKey}
	}
	return TransitionDelta{Kind: DeltaAddVectorDim, Payload: target.VectorSearchKey}
}

func vectorSnapshotKeys(dims []*pb.VectorSearchDimension) (string, string, map[string]string) {
	if len(dims) == 0 {
		return "", "", nil
	}
	searchKeys := make([]string, 0, len(dims))
	bucketKeys := make([]string, 0, len(dims))
	dimKeys := make(map[string]string, len(dims))
	for _, dim := range dims {
		axisKey := axisToken(dim.GetAxis())
		searchKey := vectorSearchSnapshotKey(dim)
		searchKeys = append(searchKeys, axisKey+"="+searchKey)
		bucketKeys = append(bucketKeys, axisKey+"="+hashProtoMessage("vector-bucket", dim))
		dimKeys[axisKey] = searchKey
	}
	sort.Strings(searchKeys)
	sort.Strings(bucketKeys)
	return strings.Join(searchKeys, "|"), strings.Join(bucketKeys, "|"), dimKeys
}

func vectorSearchSnapshotKey(dim *pb.VectorSearchDimension) string {
	cloned := cloneVectorSearchConfig(dim)
	if cloned != nil {
		cloned.BucketCfg = nil
	}
	return hashProtoMessage("vector-search", cloned)
}

func hashProtoMessage(prefix string, message proto.Message) string {
	if message == nil {
		return ""
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return prefix + ":marshal_error"
	}
	return hashBytes(prefix, payload)
}

func hashBytes(prefix string, payload []byte) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(prefix))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(payload)
	return hex.EncodeToString(hasher.Sum(nil))
}

func axisSnapshotProto(req *pb.GetBrowsingStateRequest) proto.Message {
	cloned := proto.Clone(req).(*pb.GetBrowsingStateRequest)
	cloned.Filters = filterAxisFilters(req.GetFilters(), false)
	cloned.VectorFilter = nil
	clearVectorFields(cloned)
	return cloned
}

func filterSnapshotProto(req *pb.GetBrowsingStateRequest) proto.Message {
	cloned := proto.Clone(req).(*pb.GetBrowsingStateRequest)
	cloned.Filters = filterAxisFilters(req.GetFilters(), true)
	clearVectorFields(cloned)
	return cloned
}

func metadataSnapshotProto(req *pb.GetBrowsingStateRequest) proto.Message {
	cloned := proto.Clone(req).(*pb.GetBrowsingStateRequest)
	clearVectorFields(cloned)
	return cloned
}

func clearVectorFields(req *pb.GetBrowsingStateRequest) {
	req.VectorDimension = nil
	req.VectorDimensions = nil
	req.VectorBucketId = nil
	req.VectorBucketIds = nil
	req.RebucketOnly = false
	req.HybridStrategy = pb.HybridStrategy_AUTO
}

func filterAxisFilters(filters []*pb.AxisFilter, wantExplicit bool) []*pb.AxisFilter {
	out := make([]*pb.AxisFilter, 0, len(filters))
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		isExplicit := filter.GetAxisFilterType() == pb.AxisType_FILTER
		if isExplicit == wantExplicit {
			out = append(out, filter)
		}
	}
	return out
}

func grpcSnapshotFilterCount(req *pb.GetBrowsingStateRequest) int {
	count := 0
	for _, filter := range req.GetFilters() {
		if filter.GetAxisFilterType() == pb.AxisType_FILTER {
			count++
		}
	}
	if req.GetVectorFilter() != nil {
		count++
	}
	return count
}

func httpSnapshotFilterCount(query url.Values) int {
	count := 0
	var filters []json.RawMessage
	if json.Unmarshal([]byte(query.Get("filters")), &filters) == nil {
		count += len(filters)
	}
	if strings.TrimSpace(query.Get("vectorFilter")) != "" {
		count++
	}
	return count
}

func canonicalQueryValues(query url.Values, keys ...string) string {
	items := make([]string, 0, len(keys))
	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			items = append(items, key+"="+value)
		}
	}
	sort.Strings(items)
	return strings.Join(items, "\n")
}

func canonicalBucketIDs(bucketIDs map[string]int32) string {
	if len(bucketIDs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(bucketIDs))
	for key := range bucketIDs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]string, 0, len(keys))
	for _, key := range keys {
		items = append(items, key+"="+strconv.Itoa(int(bucketIDs[key])))
	}
	return strings.Join(items, "|")
}

func isStringMapSubset(left map[string]string, right map[string]string) bool {
	if len(left) >= len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
