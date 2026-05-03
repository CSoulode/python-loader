package runner

import (
	"context"
	"fmt"
	"sort"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

const (
	grpcParentStateID   = "x-parent-browsing-state-id"
	grpcStateID         = "x-browsing-state-id"
	grpcCachePath       = "x-browsing-state-cache"
	grpcDeltaKind       = "x-browsing-delta-kind"
	grpcReusedFragments = "x-browsing-reused-fragments"
	grpcETag            = "etag"
	grpcIfNoneMatch     = "if-none-match"
	grpcAncestorETag    = "ancestor-if-none-match"
	grpcNotModified     = "bs-not-modified"
)

type phaseFBenchClient struct {
	nodes     map[string]phaseFBenchNode
	keyToID   map[string]string
	currentID string
}

type phaseFBenchNode struct {
	ID           string
	ParentID     string
	ETag         string
	Key          string
	Request      *pb.GetBrowsingStateRequest
	PayloadBytes int
}

type phaseFBenchResult struct {
	DeltaKind    string
	LookupPath   string
	Reusable     string
	L0Hit        bool
	L1PathHit    string
	ElapsedMS    float64
	NotModified  bool
	PayloadSaved int
}

func newPhaseFBenchClient() *phaseFBenchClient {
	return &phaseFBenchClient{nodes: map[string]phaseFBenchNode{}, keyToID: map[string]string{}}
}

func (c *phaseFBenchClient) Execute(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	item phaseFRequest,
) (phaseFBenchResult, error) {
	parentID, err := c.parentIDFor(item)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	md := c.metadataForParent(parentID, "", 4)
	benchID := benchRunID(dataset, Experiment10, item.key, 0)
	result, err := ExecuteBrowsingState2Request(ctx, session.DataLoader, benchID, item.req, md)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	observed, err := phaseFBenchResultFromResponse(result)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	if _, err := c.recordNode(item, parentID, result); err != nil {
		return phaseFBenchResult{}, err
	}
	observed.NotModified = firstObservationMetadata(result.Trailer, result.Header, grpcNotModified, "") == "true"
	return observed, nil
}

func (c *phaseFBenchClient) L0Hit(key string) (phaseFBenchResult, error) {
	id := c.keyToID[key]
	if id == "" {
		return phaseFBenchResult{}, fmt.Errorf("phase F L0 key %q was not recorded", key)
	}
	node := c.nodes[id]
	c.currentID = id
	return phaseFBenchResult{
		DeltaKind:    "Root",
		LookupPath:   "full_hit",
		Reusable:     "cellgrid",
		L0Hit:        true,
		L1PathHit:    "full_hit",
		PayloadSaved: node.PayloadBytes,
	}, nil
}

func (c *phaseFBenchClient) RevalidateAll(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	ancestors int,
	invalidatedKeys map[string]struct{},
) (phaseFInvalidationSummary, error) {
	summary := phaseFInvalidationSummary{Total: len(c.keyToID), AncestorCount: ancestors}
	for _, key := range c.sortedKeys() {
		id := c.keyToID[key]
		node := c.nodes[id]
		md := c.metadataForParent(node.ParentID, node.ETag, ancestors)
		benchID := benchRunID(dataset, Experiment11, key, ancestors)
		req := phaseFRequestsForNode(node)
		result, err := ExecuteBrowsingState2Request(ctx, session.DataLoader, benchID, req, md)
		if err != nil {
			return summary, err
		}
		if firstObservationMetadata(result.Trailer, result.Header, grpcNotModified, "") == "true" {
			summary.NotModified++
			summary.PayloadSaved += node.PayloadBytes
			if _, invalidated := invalidatedKeys[key]; invalidated {
				summary.MissedInvalidations++
			}
			continue
		}
		summary.Fresh++
	}
	return summary, nil
}

func (c *phaseFBenchClient) parentIDFor(item phaseFRequest) (string, error) {
	if item.parentKey == "" {
		return c.currentID, nil
	}
	id := c.keyToID[item.parentKey]
	if id == "" {
		return "", fmt.Errorf("phase F parent key %q was not recorded", item.parentKey)
	}
	return id, nil
}

func (c *phaseFBenchClient) recordNode(
	item phaseFRequest,
	parentID string,
	result *BrowsingState2Result,
) (phaseFBenchNode, error) {
	id := firstMetadata(result.Trailer, grpcStateID, firstMetadata(result.Header, grpcStateID, ""))
	etag := firstMetadata(result.Trailer, grpcETag, firstMetadata(result.Header, grpcETag, ""))
	if id == "" || etag == "" {
		return phaseFBenchNode{}, fmt.Errorf("phase F response missing state id or etag for %q", item.key)
	}
	node := phaseFBenchNode{
		ID: id, ParentID: parentID, ETag: etag, Key: item.key,
		Request: clonePhaseFRequest(item.req), PayloadBytes: result.PayloadBytes,
	}
	c.nodes[id] = node
	c.keyToID[item.key] = id
	c.currentID = id
	return node, nil
}

func (c *phaseFBenchClient) metadataForParent(parentID string, targetETag string, ancestors int) metadata.MD {
	items := []string{}
	if parentID != "" {
		items = append(items, grpcParentStateID, parentID)
	}
	if targetETag != "" {
		items = append(items, grpcIfNoneMatch, targetETag)
	}
	items = append(items, c.ancestorMetadata(parentID, ancestors)...)
	return metadata.Pairs(items...)
}

func (c *phaseFBenchClient) ancestorMetadata(startID string, limit int) []string {
	items := []string{}
	for id := startID; id != "" && len(items)/2 < limit; {
		node := c.nodes[id]
		if node.ETag != "" {
			items = append(items, grpcAncestorETag, node.ETag)
		}
		id = node.ParentID
	}
	return items
}

func (c *phaseFBenchClient) NodeCount() int {
	return len(c.nodes)
}

func (c *phaseFBenchClient) sortedKeys() []string {
	keys := make([]string, 0, len(c.keyToID))
	for key := range c.keyToID {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (c *phaseFBenchClient) keySet() map[string]struct{} {
	return stringSet(c.sortedKeys()...)
}

func stringSet(values ...string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func phaseFRequestsForNode(node phaseFBenchNode) *pb.GetBrowsingStateRequest {
	return clonePhaseFRequest(node.Request)
}

func firstMetadata(md metadata.MD, key string, fallback string) string {
	values := md.Get(key)
	if len(values) == 0 || values[0] == "" {
		return fallback
	}
	return values[0]
}

func phaseFBenchResultFromResponse(result *BrowsingState2Result) (phaseFBenchResult, error) {
	if result == nil {
		return phaseFBenchResult{}, fmt.Errorf("phase F response missing result")
	}
	elapsedMS := float64(result.Elapsed.Microseconds()) / 1000
	return phaseFBenchResultFromMetadata(result.Header, result.Trailer, elapsedMS)
}

func phaseFBenchResultFromMetadata(header metadata.MD, trailer metadata.MD, elapsedMS float64) (phaseFBenchResult, error) {
	cachePath, err := requiredObservationMetadata(trailer, header, grpcCachePath)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	deltaKind, err := requiredObservationMetadata(trailer, header, grpcDeltaKind)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	reused, err := requiredObservationMetadata(trailer, header, grpcReusedFragments)
	if err != nil {
		return phaseFBenchResult{}, err
	}
	return phaseFBenchResult{
		DeltaKind:  deltaKind,
		LookupPath: cachePath,
		Reusable:   reused,
		L1PathHit:  cachePath,
		ElapsedMS:  elapsedMS,
	}, nil
}

func requiredObservationMetadata(trailer metadata.MD, header metadata.MD, key string) (string, error) {
	value, ok := observationMetadata(trailer, header, key)
	if !ok {
		return "", fmt.Errorf("phase F response missing %s metadata", key)
	}
	return value, nil
}

func observationMetadata(trailer metadata.MD, header metadata.MD, key string) (string, bool) {
	if values := trailer.Get(key); len(values) > 0 {
		return values[0], true
	}
	if values := header.Get(key); len(values) > 0 {
		return values[0], true
	}
	return "", false
}

func firstObservationMetadata(trailer metadata.MD, header metadata.MD, key string, fallback string) string {
	if value, ok := observationMetadata(trailer, header, key); ok && value != "" {
		return value
	}
	return fallback
}

func clonePhaseFRequest(req *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	if req == nil {
		return nil
	}
	return proto.Clone(req).(*pb.GetBrowsingStateRequest)
}
