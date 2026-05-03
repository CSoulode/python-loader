package main

import (
	"errors"
	"net/http"
	"strings"
)

func (s *DataLoaderServer) prepareBrowsingStateChainHTTP(r *http.Request) (*browsingStateChainLookup, error) {
	if shouldBypassBrowsingStateChainHTTP(r) {
		return nil, nil
	}
	targetID, err := computeHTTPBrowsingStateIDStrict(r, false)
	if err != nil {
		return nil, err
	}
	reuseKey, err := computeHTTPBrowsingStateIDStrict(r, true)
	if err != nil {
		return nil, err
	}
	prepared := browsingStateChainLookup{
		TargetID:         targetID,
		ParentID:         parentStateIDFromHTTP(r),
		ReuseKey:         string(reuseKey),
		Delta:            inferHTTPDelta(r),
		Snapshot:         newHTTPBrowsingStateSnapshot(r),
		TargetETags:      parseHTTPETagList(r.Header.Values("If-None-Match")),
		AncestorETags:    parseHTTPETagList(r.Header.Values(browsingStateHeaderAncestorETags)),
		Dependencies:     extractHTTPBrowsingStateDependencies(r),
		NodeDependencies: extractHTTPBrowsingStateDependencies(r),
		Collector:        newBrowsingStateFragmentCollector(),
	}
	lookup := s.ensureBrowsingStateChain().Lookup(prepared)
	return &lookup, nil
}

func (s *DataLoaderServer) replayBrowsingStateChainHTTP(w http.ResponseWriter, lookup *browsingStateChainLookup) bool {
	if lookup == nil {
		return false
	}
	if lookup.Mode != chainLookupFullHit || lookup.Node == nil || !cellGridHasHTTPBody(lookup.Node.CellGrid) {
		return false
	}
	w.Header().Set("X-Browsing-State-Id", string(lookup.TargetID))
	w.Header().Set("X-Browsing-State-Cache", lookup.CachePath)
	w.Header().Set("X-Browsing-Cache-Path", lookup.CachePath)
	writeBrowsingStateHTTPObservationHeaders(w, lookup)
	writeBrowsingStateHTTPETagHeaders(w, lookup)
	if lookup.TargetETagMatch {
		s.ensureBrowsingStateChain().httpNotModified.Add(1)
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append([]byte(nil), lookup.Node.CellGrid.HTTPBody...))
	return true
}

func writeBrowsingStateHTTPETagHeaders(w http.ResponseWriter, lookup *browsingStateChainLookup) {
	if lookup == nil || lookup.Node == nil {
		return
	}
	if lookup.Node.ETag != "" {
		w.Header().Set("ETag", lookup.Node.ETag)
	}
	if lookup.AncestorETagMatch {
		w.Header().Set(browsingStateHeaderAncestorID, string(lookup.MatchedAncestorID))
		w.Header().Set(browsingStateHeaderAncestorETag, lookup.MatchedAncestorTag)
	}
}

func writeBrowsingStateHTTPObservationHeaders(w http.ResponseWriter, lookup *browsingStateChainLookup) {
	if lookup == nil {
		return
	}
	finalizeBrowsingStateObservation(lookup)
	w.Header().Set(browsingStateHeaderDelta, lookup.Delta.Kind.String())
	w.Header().Set(browsingStateHeaderReusedFragments, strings.Join(lookup.Reusable, ","))
}

func (s *DataLoaderServer) replayInflightBrowsingStateHTTP(
	w http.ResponseWriter,
	lookup *browsingStateChainLookup,
	wait func() (*StateNode, error),
) {
	node, err := wait()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if node == nil || node.CellGrid == nil {
		err := errors.New("browsing state inflight completed without a published node")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lookup.Mode = chainLookupFullHit
	lookup.CachePath = string(chainLookupFullHit)
	markBrowsingStateCellGridReuse(lookup)
	lookup.Node = node
	_ = s.replayBrowsingStateChainHTTP(w, lookup)
}

func parseHTTPETagList(values []string) []string {
	items := make([]string, 0)
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				items = append(items, trimmed)
			}
		}
	}
	return items
}

func shouldBypassBrowsingStateChainHTTP(r *http.Request) bool {
	if r == nil {
		return true
	}
	query := r.URL.Query()
	return strings.TrimSpace(query.Get("all")) != "" || strings.TrimSpace(query.Get("timeline")) != ""
}

func parentStateIDFromHTTP(r *http.Request) StateID {
	if r == nil {
		return ""
	}
	if value := strings.TrimSpace(r.Header.Get("X-Parent-Browsing-State-Id")); value != "" {
		return StateID(value)
	}
	if value := strings.TrimSpace(r.Header.Get("X-Browsing-Parent-State-Id")); value != "" {
		return StateID(value)
	}
	return StateID(strings.TrimSpace(r.URL.Query().Get("parentStateId")))
}

func inferHTTPDelta(r *http.Request) TransitionDelta {
	if r != nil && strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("rebucketOnly")), "true") {
		return TransitionDelta{Kind: DeltaRebucketOnly, Payload: "rebucket_only"}
	}
	return TransitionDelta{Kind: DeltaRoot}
}
