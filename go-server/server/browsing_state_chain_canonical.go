package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	pb "m3.dataloader/dataloader"
)

const (
	browsingStateCanonicalPrefix      = "bs-canonical-v2"
	browsingStateReuseCanonicalPrefix = "bs-reuse-canonical-v2"
)

type canonicalBrowsingState struct {
	All             string          `json:"all,omitempty"`
	Timeline        string          `json:"timeline,omitempty"`
	Axes            []canonicalAxis `json:"axes,omitempty"`
	Filters         []canonicalItem `json:"filters,omitempty"`
	VectorFilter    string          `json:"vector_filter,omitempty"`
	VectorDims      []string        `json:"vector_dims,omitempty"`
	VectorBucketIDs string          `json:"vector_bucket_ids,omitempty"`
	RebucketOnly    bool            `json:"rebucket_only,omitempty"`
	HybridStrategy  string          `json:"hybrid_strategy,omitempty"`
}

type canonicalAxis struct {
	Axis string `json:"axis"`
	Type string `json:"type"`
	ID   int32  `json:"id"`
}

type canonicalItem struct {
	Type   string     `json:"type"`
	ID     int32      `json:"id,omitempty"`
	Ranges [][]string `json:"ranges,omitempty"`
}

func computeStateIDFromBytes(prefix string, payload []byte) StateID {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(prefix))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(payload)
	return StateID(hex.EncodeToString(hasher.Sum(nil)))
}

func computeGRPCBrowsingStateID(req *pb.GetBrowsingStateRequest) (StateID, error) {
	payload, err := canonicalGRPCBrowsingState(req, false)
	if err != nil {
		return "", err
	}
	return computeStateIDFromBytes(browsingStateCanonicalPrefix, payload), nil
}

func computeGRPCBrowsingStateReuseKey(req *pb.GetBrowsingStateRequest) (string, error) {
	payload, err := canonicalGRPCBrowsingState(req, true)
	if err != nil {
		return "", err
	}
	return string(computeStateIDFromBytes(browsingStateReuseCanonicalPrefix, payload)), nil
}

func computeHTTPBrowsingStateID(r *http.Request) StateID {
	id, _ := computeHTTPBrowsingStateIDStrict(r, false)
	return id
}

func computeHTTPBrowsingStateReuseKey(r *http.Request) string {
	id, _ := computeHTTPBrowsingStateIDStrict(r, true)
	return string(id)
}

func computeHTTPBrowsingStateIDStrict(r *http.Request, excludeBuckets bool) (StateID, error) {
	payload, err := canonicalHTTPBrowsingState(r, excludeBuckets)
	if err != nil {
		return "", err
	}
	prefix := browsingStateCanonicalPrefix
	if excludeBuckets {
		prefix = browsingStateReuseCanonicalPrefix
	}
	return computeStateIDFromBytes(prefix, payload), nil
}

func canonicalGRPCBrowsingState(req *pb.GetBrowsingStateRequest, excludeBuckets bool) ([]byte, error) {
	if req == nil {
		return []byte("{}"), nil
	}
	state := canonicalBrowsingState{
		All:          strings.TrimSpace(req.GetAll()),
		Timeline:     strings.TrimSpace(req.GetTimeline()),
		Axes:         canonicalAxesFromGRPC(req.GetFilters()),
		Filters:      canonicalFiltersFromGRPC(req.GetFilters()),
		VectorFilter: hashProtoMessage("vector-filter", req.GetVectorFilter()),
	}
	dims, err := canonicalVectorDimensions(req, excludeBuckets)
	if err != nil {
		return nil, err
	}
	state.VectorDims = dims
	state.VectorBucketIDs = canonicalBucketIDsForState(req.GetVectorBucketIds(), excludeBuckets)
	state.RebucketOnly = !excludeBuckets && req.GetRebucketOnly()
	state.HybridStrategy = canonicalHybridStrategy(req.GetHybridStrategy())
	return json.Marshal(state)
}

func canonicalHTTPBrowsingState(r *http.Request, excludeBuckets bool) ([]byte, error) {
	if r == nil {
		return []byte("{}"), nil
	}
	query := r.URL.Query()
	axisX, axisY, axisZ, filters, err := oldParseAxesAndFilters(httpCellRequest(r))
	if err != nil {
		return nil, err
	}
	vectorFilter, err := parseCompatVectorFilter(query.Get("vectorFilter"))
	if err != nil {
		return nil, err
	}
	merged, err := parseCompatVectorDimensions(query)
	if err != nil {
		return nil, err
	}
	canonicalFilters, err := canonicalFiltersFromHTTP(filters)
	if err != nil {
		return nil, err
	}
	state := canonicalBrowsingState{
		All:          strings.TrimSpace(query.Get("all")),
		Timeline:     strings.TrimSpace(query.Get("timeline")),
		Axes:         canonicalAxesFromHTTP(query, canonicalHTTPAxes{X: axisX, Y: axisY, Z: axisZ}),
		Filters:      canonicalFilters,
		VectorFilter: hashProtoMessage("vector-filter", vectorFilter),
	}
	dims, err := canonicalVectorDimensions(requestFromMergedVectorDimensions(merged), excludeBuckets)
	if err != nil {
		return nil, err
	}
	state.VectorDims = dims
	state.VectorBucketIDs = canonicalBucketIDsForState(merged.BucketIDs, excludeBuckets)
	state.RebucketOnly = !excludeBuckets && strings.EqualFold(strings.TrimSpace(query.Get("rebucketOnly")), "true")
	forcedStrategy, err := parseCompatHybridStrategy(compatHybridStrategyQueryValue(query))
	if err != nil {
		return nil, err
	}
	state.HybridStrategy = canonicalCompatHybridStrategy(forcedStrategy)
	return json.Marshal(state)
}

func requestFromMergedVectorDimensions(merged mergedVectorDimensions) *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		VectorDimensions: merged.Dims,
		VectorBucketIds:  cloneVectorBucketIDs(merged.BucketIDs),
	}
}

func httpCellRequest(r *http.Request) *pb.GetCellRequest {
	query := r.URL.Query()
	return &pb.GetCellRequest{
		XAxis:    query.Get("xAxis"),
		YAxis:    query.Get("yAxis"),
		ZAxis:    query.Get("zAxis"),
		Filters:  query.Get("filters"),
		All:      query.Get("all"),
		Timeline: query.Get("timeline"),
	}
}
