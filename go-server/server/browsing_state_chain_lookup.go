package main

import "fmt"

func (c *BrowsingStateChain) Lookup(prepared browsingStateChainLookup) browsingStateChainLookup {
	if c == nil || prepared.TargetID == "" {
		prepared.Mode = chainLookupColdMiss
		return prepared
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.purgeExpiredLocked(now)
	if node := c.nodes[prepared.TargetID]; node != nil && node.CellGrid != nil {
		c.touchLocked(node, now)
		c.fullHits.Add(1)
		prepared.Mode = chainLookupFullHit
		prepared.Node = node
		prepared.CachePath = string(chainLookupFullHit)
		prepared.Reusable = []string{"cellgrid"}
		prepared.TargetETagMatch = etagMatchesAny(node.ETag, prepared.TargetETags)
		return prepared
	}
	if parent := c.nodes[prepared.ParentID]; parent != nil {
		prepared.Delta = inferTransitionDelta(parent.Snapshot, prepared.Snapshot, prepared.Delta)
	}
	if c.canReuseAncestorLocked(prepared) {
		c.ancestorReuses.Add(1)
		prepared.Mode = chainLookupAncestorReuse
		prepared.Ancestor = c.nodes[prepared.ParentID]
		prepared.CachePath = string(chainLookupAncestorReuse)
		prepared.Fragments = c.safeReusableFragmentsLocked(prepared)
		c.markAncestorETagMatch(&prepared)
		return prepared
	}
	c.coldMisses.Add(1)
	prepared.Mode = chainLookupColdMiss
	prepared.CachePath = string(chainLookupColdMiss)
	prepared.FallbackReason = c.ancestorReuseBlockReasonLocked(prepared)
	return prepared
}

func (c *BrowsingStateChain) markAncestorETagMatch(prepared *browsingStateChainLookup) {
	if prepared == nil || prepared.Ancestor == nil {
		return
	}
	if !etagMatchesAny(prepared.Ancestor.ETag, prepared.AncestorETags) {
		return
	}
	prepared.AncestorETagMatch = true
	prepared.MatchedAncestorID = prepared.Ancestor.ID
	prepared.MatchedAncestorTag = prepared.Ancestor.ETag
	c.ancestorETagHits.Add(1)
}

func (c *BrowsingStateChain) StartInflight(key string) (leader bool, wait func() (*StateNode, error)) {
	if c == nil || key == "" {
		return true, func() (*StateNode, error) { return nil, nil }
	}
	c.mu.Lock()
	if active := c.inflight[key]; active != nil {
		c.inflightWaiters.Add(1)
		c.mu.Unlock()
		return false, func() (*StateNode, error) {
			<-active.done
			return active.node, active.err
		}
	}
	active := &browsingStateInflight{done: make(chan struct{})}
	c.inflight[key] = active
	c.inflightLeaders.Add(1)
	c.mu.Unlock()
	return true, func() (*StateNode, error) { return nil, nil }
}

func (c *BrowsingStateChain) FinishInflight(key string, node *StateNode, err error) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	active := c.inflight[key]
	if active != nil {
		active.node = node
		active.err = err
		delete(c.inflight, key)
		close(active.done)
	}
	c.mu.Unlock()
}

func (c *BrowsingStateChain) canReuseAncestorLocked(prepared browsingStateChainLookup) bool {
	if prepared.ParentID == "" || prepared.Delta.Kind == DeltaRemoveFilter {
		return false
	}
	parent := c.nodes[prepared.ParentID]
	if parent == nil || parent.CellGrid == nil {
		return false
	}
	switch prepared.Delta.Kind {
	case DeltaRebucketOnly:
		if parent.Snapshot != nil && prepared.Snapshot != nil {
			return parent.Snapshot.MetadataKey == prepared.Snapshot.MetadataKey &&
				parent.Snapshot.VectorSearchKey == prepared.Snapshot.VectorSearchKey &&
				parent.Snapshot.StrategyKey == prepared.Snapshot.StrategyKey
		}
		return parent.ReuseKey != "" && prepared.ReuseKey != "" && parent.ReuseKey == prepared.ReuseKey
	case DeltaAddFilter, DeltaAddVectorDim, DeltaRemoveVectorDim, DeltaChangeAxisNonVector:
		return fragmentsHaveReusable(c.safeReusableFragmentsLocked(prepared))
	default:
		return false
	}
}

func (c *BrowsingStateChain) ancestorReuseBlockReasonLocked(prepared browsingStateChainLookup) string {
	if prepared.ParentID == "" {
		return "no_parent_state"
	}
	parent := c.nodes[prepared.ParentID]
	if parent == nil {
		return "parent_not_found"
	}
	if parent.CellGrid == nil {
		return "parent_missing_cellgrid"
	}
	if prepared.Delta.Kind == DeltaRemoveFilter {
		return "remove_filter_expands_candidates"
	}
	if prepared.Delta.Kind == DeltaChangeForcedStrategy {
		return "forced_strategy_changes_vector_semantics"
	}
	if prepared.Delta.Kind == DeltaAddFilter && !canReuseAddFilterCandidates(parent.Snapshot, prepared.Snapshot) {
		return "add_filter_not_strict_narrowing"
	}
	if prepared.Delta.Kind == DeltaRebucketOnly && parent.Snapshot != nil && prepared.Snapshot != nil {
		return "rebucket_metadata_or_vector_changed"
	}
	if parent.Fragments == nil {
		return "parent_fragments_missing"
	}
	if prepared.Delta.Kind == DeltaChangeAxisNonVector && !canReuseAxisCandidates(parent.Snapshot, prepared.Snapshot) {
		return "axis_candidate_domain_changed"
	}
	return "delta_not_reusable"
}

func inflightKeyForLookup(lookup browsingStateChainLookup) string {
	return fmt.Sprintf("%s|%s|%s|%s", lookup.ParentID, lookup.TargetID, lookup.Delta.Kind, lookup.Delta.Payload)
}

func (c *BrowsingStateChain) safeReusableFragmentsLocked(prepared browsingStateChainLookup) *browsingStateFragments {
	parent := c.nodes[prepared.ParentID]
	if parent == nil || parent.Fragments == nil {
		return nil
	}
	switch prepared.Delta.Kind {
	case DeltaAddFilter:
		if !canReuseAddFilterCandidates(parent.Snapshot, prepared.Snapshot) {
			return nil
		}
		return cloneCandidateFragments(parent.Fragments)
	case DeltaChangeAxisNonVector:
		if canReuseAxisCandidates(parent.Snapshot, prepared.Snapshot) {
			return cloneBrowsingStateFragments(parent.Fragments)
		}
		return cloneVectorFragments(parent.Fragments)
	case DeltaAddVectorDim, DeltaRemoveVectorDim, DeltaRebucketOnly, DeltaChangeForcedStrategy:
		return cloneBrowsingStateFragments(parent.Fragments)
	default:
		return nil
	}
}
