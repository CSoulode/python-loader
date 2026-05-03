package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

type browsingStateGRPCStream interface {
	Context() context.Context
	Send(*pb.BrowsingStateResponse) error
	SendHeader(metadata.MD) error
}

type browsingStateGRPCTrailer interface {
	SetTrailer(metadata.MD)
}

func (s *DataLoaderServer) prepareBrowsingStateChainGRPC(ctx context.Context, req *pb.GetBrowsingStateRequest) (*browsingStateChainLookup, error) {
	if shouldBypassBrowsingStateChainGRPC(req) {
		return nil, nil
	}
	targetID, err := computeGRPCBrowsingStateID(req)
	if err != nil {
		return nil, err
	}
	reuseKey, err := computeGRPCBrowsingStateReuseKey(req)
	if err != nil {
		return nil, err
	}
	prepared := browsingStateChainLookup{
		TargetID:         targetID,
		ParentID:         parentStateIDFromContext(ctx),
		ReuseKey:         reuseKey,
		Delta:            inferGRPCDelta(req),
		Snapshot:         newGRPCBrowsingStateSnapshot(req, targetID, reuseKey),
		TargetETags:      targetETagsFromContext(ctx),
		AncestorETags:    ancestorETagsFromContext(ctx),
		Dependencies:     extractGRPCBrowsingStateDependencies(req),
		NodeDependencies: extractGRPCBrowsingStateDependencies(req),
		Collector:        newBrowsingStateFragmentCollector(),
	}
	lookup := s.ensureBrowsingStateChain().Lookup(prepared)
	return &lookup, nil
}

func (s *DataLoaderServer) replayBrowsingStateChainGRPC(lookup *browsingStateChainLookup, stream browsingStateGRPCStream) (bool, error) {
	if lookup == nil {
		return false, nil
	}
	if lookup.Mode != chainLookupFullHit || lookup.Node == nil || !cellGridHasGRPCResponses(lookup.Node.CellGrid) {
		return false, nil
	}
	return s.replayBrowsingStateChainGRPCNode(lookup, stream)
}

func (s *DataLoaderServer) replayBrowsingStateChainGRPCNode(lookup *browsingStateChainLookup, stream browsingStateGRPCStream) (bool, error) {
	if err := sendBrowsingStateChainGRPCHeader(stream, lookup); err != nil {
		return true, err
	}
	if lookup.TargetETagMatch {
		s.ensureBrowsingStateChain().grpcNotModified.Add(1)
		sendBrowsingStateChainGRPCTrailer(stream, lookup, lookup.Node)
		return true, nil
	}
	for _, resp := range lookup.Node.CellGrid.Responses {
		if err := stream.Send(cloneBrowsingStateResponse(resp)); err != nil {
			return true, err
		}
	}
	sendBrowsingStateChainGRPCTrailer(stream, lookup, lookup.Node)
	logBrowsingStateChainEvent("replay", lookup, len(lookup.Node.CellGrid.Responses), time.Now())
	return true, nil
}

func (s *DataLoaderServer) replayInflightBrowsingStateGRPC(
	lookup *browsingStateChainLookup,
	wait func() (*StateNode, error),
	stream browsingStateGRPCStream,
) error {
	node, err := wait()
	if err != nil {
		return err
	}
	if node == nil || !cellGridHasGRPCResponses(node.CellGrid) {
		return status.Error(codes.Internal, "browsing state inflight completed without a published node")
	}
	lookup.Mode = chainLookupFullHit
	lookup.CachePath = string(chainLookupFullHit)
	markBrowsingStateCellGridReuse(lookup)
	lookup.Node = node
	_, err = s.replayBrowsingStateChainGRPCNode(lookup, stream)
	return err
}

func sendBrowsingStateChainGRPCHeader(stream browsingStateGRPCStream, lookup *browsingStateChainLookup) error {
	if stream == nil || lookup == nil || lookup.TargetID == "" {
		return nil
	}
	items := []string{
		browsingStateHeaderID, string(lookup.TargetID),
	}
	items = append(items, browsingStateObservationMetadataItems(lookup)...)
	if lookup.Node != nil && lookup.Node.ETag != "" {
		items = append(items, browsingStateGRPCETag, lookup.Node.ETag)
	}
	if lookup.TargetETagMatch {
		items = append(items, browsingStateGRPCNotModified, "true")
	}
	if lookup.AncestorETagMatch {
		items = append(items,
			browsingStateGRPCAncestorID, string(lookup.MatchedAncestorID),
			browsingStateGRPCAncestorMatch, lookup.MatchedAncestorTag,
		)
	}
	return stream.SendHeader(metadata.Pairs(items...))
}

func sendBrowsingStateChainGRPCTrailer(stream browsingStateGRPCStream, lookup *browsingStateChainLookup, node *StateNode) {
	trailer, ok := stream.(browsingStateGRPCTrailer)
	if !ok || lookup == nil {
		return
	}
	finalizeBrowsingStateObservation(lookup)
	items := []string{browsingStateHeaderID, string(lookup.TargetID)}
	items = append(items, browsingStateObservationMetadataItems(lookup)...)
	if node != nil && node.ETag != "" {
		items = append(items, browsingStateGRPCETag, node.ETag)
	}
	if lookup.TargetETagMatch {
		items = append(items, browsingStateGRPCNotModified, "true")
	}
	if lookup.AncestorETagMatch {
		items = append(items,
			browsingStateGRPCAncestorID, string(lookup.MatchedAncestorID),
			browsingStateGRPCAncestorMatch, lookup.MatchedAncestorTag,
		)
	}
	trailer.SetTrailer(metadata.Pairs(items...))
}

func browsingStateObservationMetadataItems(lookup *browsingStateChainLookup) []string {
	if lookup == nil {
		return nil
	}
	return []string{
		browsingStateHeaderCache, lookup.CachePath,
		browsingStateHeaderDelta, lookup.Delta.Kind.String(),
		browsingStateHeaderReusedFragments, strings.Join(lookup.Reusable, ","),
	}
}

func parentStateIDFromContext(ctx context.Context) StateID {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if id := firstMetadataValue(md, browsingStateParentHeaderID); id != "" {
		return id
	}
	return firstMetadataValue(md, "parent-state-id")
}

func firstMetadataValue(md metadata.MD, key string) StateID {
	for _, value := range md.Get(key) {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return StateID(trimmed)
		}
	}
	return ""
}

func targetETagsFromContext(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	return metadataValues(md, browsingStateGRPCIfNoneMatch)
}

func ancestorETagsFromContext(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	return metadataValues(md, browsingStateGRPCAncestorETag)
}

func metadataValues(md metadata.MD, key string) []string {
	values := make([]string, 0)
	for _, value := range md.Get(key) {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				values = append(values, trimmed)
			}
		}
	}
	return values
}

func inferGRPCDelta(req *pb.GetBrowsingStateRequest) TransitionDelta {
	if req != nil && req.GetRebucketOnly() {
		return TransitionDelta{Kind: DeltaRebucketOnly, Payload: "rebucket_only"}
	}
	return TransitionDelta{Kind: DeltaRoot}
}

func shouldBypassBrowsingStateChainGRPC(req *pb.GetBrowsingStateRequest) bool {
	return req == nil || strings.TrimSpace(req.GetAll()) != "" || strings.TrimSpace(req.GetTimeline()) != ""
}

func writeBrowsingStateChainHTTPJSON(w http.ResponseWriter, s *DataLoaderServer, lookup *browsingStateChainLookup, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "json encode failed", http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	writeBrowsingStateChainHTTPBody(w, s, lookup, body)
}

func writeBrowsingStateChainHTTPBody(w http.ResponseWriter, s *DataLoaderServer, lookup *browsingStateChainLookup, body []byte) {
	var node *StateNode
	if lookup != nil {
		finalizeBrowsingStateObservation(lookup)
		node = s.ensureBrowsingStateChain().PublishNode(*lookup, &bsCellGrid{HTTPBody: body, HasHTTPBody: true})
		if lookup.InflightKey != "" {
			s.ensureBrowsingStateChain().FinishInflight(lookup.InflightKey, node, nil)
		}
	}
	if lookup != nil {
		w.Header().Set("X-Browsing-State-Id", string(lookup.TargetID))
		w.Header().Set("X-Browsing-State-Cache", lookup.CachePath)
		w.Header().Set("X-Browsing-Cache-Path", lookup.CachePath)
		writeBrowsingStateHTTPObservationHeaders(w, lookup)
		if node != nil && node.ETag != "" {
			w.Header().Set("ETag", node.ETag)
		}
		if lookup.AncestorETagMatch {
			w.Header().Set(browsingStateHeaderAncestorID, string(lookup.MatchedAncestorID))
			w.Header().Set(browsingStateHeaderAncestorETag, lookup.MatchedAncestorTag)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func logBrowsingStateChainEvent(event string, lookup *browsingStateChainLookup, cellCount int, start time.Time) {
	if lookup == nil {
		return
	}
	log.Printf(
		"BENCH_METRIC {\"event\":\"browsing_state_chain_%s\",\"target_id\":%q,\"ancestor_id\":%q,\"delta_kind\":%q,\"lookup_path\":%q,\"reused_fragments\":%q,\"recompute_reason\":%q,\"cell_count\":%d,\"elapsed_ms\":%.3f}",
		event,
		string(lookup.TargetID),
		string(lookup.ParentID),
		lookup.Delta.Kind.String(),
		lookup.CachePath,
		strings.Join(lookup.Reusable, ","),
		lookup.FallbackReason,
		cellCount,
		durationMillis(start),
	)
}
