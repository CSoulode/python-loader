package main

import (
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

func (c *BrowsingStateChain) PublishNode(prepared browsingStateChainLookup, grid *bsCellGrid) *StateNode {
	if c == nil || prepared.TargetID == "" || grid == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.purgeExpiredLocked(now)
	if existing := c.nodes[prepared.TargetID]; existing != nil {
		if mergeCellGrid(existing, grid) {
			c.recomputeETagSubtreeLocked(existing)
		}
		c.touchLocked(existing, now)
		c.publishCollisions.Add(1)
		return existing
	}
	parentID := lineageParentIDForLookup(prepared)
	parent := c.nodes[parentID]
	dependencyParent := dependencyParentForLookup(parent, prepared)
	node := &StateNode{
		ID:                prepared.TargetID,
		ParentID:          parentID,
		ReuseKey:          prepared.ReuseKey,
		Delta:             prepared.Delta,
		Snapshot:          cloneBrowsingStateSnapshot(prepared.Snapshot),
		CellGrid:          cloneCellGrid(grid),
		Fragments:         mergeLookupFragments(prepared),
		Dependencies:      mergeBrowsingStateDependencies(dependencyParent, prepared.Dependencies),
		NodeDependencies:  deriveNodeDependencies(dependencyParent, prepared.NodeDependencies),
		CreatedAt:         now,
		LastAccessAt:      now,
		SubtreeLastAccess: now,
	}
	node.ETag = computeMerkleETag(parent, node.Delta, node.CellGrid)
	c.nodes[node.ID] = node
	c.linkChildLocked(node)
	c.addLeafLocked(node)
	c.evictOverflowLocked()
	c.published.Add(1)
	return node
}

func lineageParentIDForLookup(prepared browsingStateChainLookup) StateID {
	if prepared.Delta.Kind == DeltaRemoveFilter {
		return ""
	}
	return prepared.ParentID
}

func dependencyParentForLookup(parent *StateNode, prepared browsingStateChainLookup) *StateNode {
	if prepared.Delta.Kind == DeltaRemoveFilter {
		return nil
	}
	return parent
}

func cloneBrowsingStateSnapshot(snapshot *BrowsingStateSnapshot) *BrowsingStateSnapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.Dependencies = cloneBrowsingStateDependencies(snapshot.Dependencies)
	cloned.FilterItems = append([]string(nil), snapshot.FilterItems...)
	if len(snapshot.VectorDims) > 0 {
		cloned.VectorDims = make(map[string]string, len(snapshot.VectorDims))
		for key, value := range snapshot.VectorDims {
			cloned.VectorDims[key] = value
		}
	}
	return &cloned
}

func (c *BrowsingStateChain) linkChildLocked(node *StateNode) {
	if node == nil || node.ParentID == "" {
		return
	}
	if c.children[node.ParentID] == nil {
		c.children[node.ParentID] = make(map[StateID]struct{})
	}
	c.children[node.ParentID][node.ID] = struct{}{}
	parent := c.nodes[node.ParentID]
	if parent == nil {
		return
	}
	parent.ChildCount++
	c.removeLeafLocked(parent)
}

func cloneCellGrid(grid *bsCellGrid) *bsCellGrid {
	if grid == nil {
		return nil
	}
	cloned := &bsCellGrid{
		HTTPBody:         append([]byte(nil), grid.HTTPBody...),
		HasHTTPBody:      cellGridHasHTTPBody(grid),
		HasGRPCResponses: cellGridHasGRPCResponses(grid),
	}
	if len(grid.Responses) > 0 {
		cloned.Responses = make([]*pb.BrowsingStateResponse, 0, len(grid.Responses))
		for _, resp := range grid.Responses {
			cloned.Responses = append(cloned.Responses, cloneBrowsingStateResponse(resp))
		}
	}
	return cloned
}

func mergeCellGrid(node *StateNode, grid *bsCellGrid) bool {
	if node == nil || grid == nil {
		return false
	}
	if node.CellGrid == nil {
		node.CellGrid = cloneCellGrid(grid)
		return true
	}
	changed := false
	if !cellGridHasHTTPBody(node.CellGrid) && cellGridHasHTTPBody(grid) {
		node.CellGrid.HTTPBody = append([]byte(nil), grid.HTTPBody...)
		node.CellGrid.HasHTTPBody = true
		changed = true
	}
	if !cellGridHasGRPCResponses(node.CellGrid) && cellGridHasGRPCResponses(grid) {
		node.CellGrid.Responses = cloneBrowsingStateResponses(grid.Responses)
		node.CellGrid.HasGRPCResponses = true
		changed = true
	}
	return changed
}

func (c *BrowsingStateChain) recomputeETagSubtreeLocked(node *StateNode) {
	if c == nil || node == nil {
		return
	}
	node.ETag = computeMerkleETag(c.nodes[node.ParentID], node.Delta, node.CellGrid)
	for childID := range c.children[node.ID] {
		c.recomputeETagSubtreeLocked(c.nodes[childID])
	}
}

func cloneBrowsingStateResponses(responses []*pb.BrowsingStateResponse) []*pb.BrowsingStateResponse {
	if responses == nil {
		return nil
	}
	cloned := make([]*pb.BrowsingStateResponse, 0, len(responses))
	for _, resp := range responses {
		cloned = append(cloned, cloneBrowsingStateResponse(resp))
	}
	return cloned
}

func cellGridHasHTTPBody(grid *bsCellGrid) bool {
	return grid != nil && (grid.HasHTTPBody || len(grid.HTTPBody) > 0)
}

func cellGridHasGRPCResponses(grid *bsCellGrid) bool {
	return grid != nil && (grid.HasGRPCResponses || grid.Responses != nil)
}

func cloneBrowsingStateResponse(resp *pb.BrowsingStateResponse) *pb.BrowsingStateResponse {
	if resp == nil {
		return nil
	}
	cloned := proto.Clone(resp)
	if typed, ok := cloned.(*pb.BrowsingStateResponse); ok {
		return typed
	}
	return nil
}
