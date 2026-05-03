package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	pb "m3.dataloader/dataloader"
)

func (c *BrowsingStateChain) InvalidateDependency(dep browsingStateDependency) int {
	if c == nil || dep.Kind == "" || dep.ID == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.firstIntroducersLocked(dep)
	removed := 0
	for _, id := range ids {
		removed += c.removeSubtreeCountLocked(id)
	}
	c.invalidationsDep.Add(int64(removed))
	return removed
}

func (c *BrowsingStateChain) InvalidateSubtree(id StateID) int {
	if c == nil || id == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := c.removeSubtreeCountLocked(id)
	c.invalidationsTree.Add(int64(removed))
	return removed
}

func (c *BrowsingStateChain) firstIntroducersLocked(dep browsingStateDependency) []StateID {
	ids := c.matchingDependencyNodesLocked(dep, true)
	if len(ids) == 0 {
		ids = c.matchingDependencyNodesLocked(dep, false)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids
}

func (c *BrowsingStateChain) matchingDependencyNodesLocked(dep browsingStateDependency, nodeLevel bool) []StateID {
	ids := make([]StateID, 0)
	for id, node := range c.nodes {
		deps := node.Dependencies
		if nodeLevel {
			deps = node.NodeDependencies
		}
		if dependencyListContains(deps, dep) {
			ids = append(ids, id)
		}
	}
	return ids
}

func extractGRPCBrowsingStateDependencies(req *pb.GetBrowsingStateRequest) []browsingStateDependency {
	deps := make([]browsingStateDependency, 0)
	if req == nil {
		return deps
	}
	for _, filter := range req.GetFilters() {
		deps = append(deps, dependencyFromAxisFilter(filter)...)
	}
	deps = append(deps, dependencyFromVectorFilter(req.GetVectorFilter())...)
	deps = append(deps, dependencyFromVectorDim(req.GetVectorDimension())...)
	for _, dim := range req.GetVectorDimensions() {
		deps = append(deps, dependencyFromVectorDim(dim)...)
	}
	return uniqueBrowsingStateDependencies(deps)
}

func extractHTTPBrowsingStateDependencies(r *http.Request) []browsingStateDependency {
	if r == nil {
		return nil
	}
	query := r.URL.Query()
	deps := extractHTTPAxesDependencies(query.Get("xAxis"), query.Get("yAxis"), query.Get("zAxis"))
	deps = append(deps, extractHTTPFilterDependencies(query.Get("filters"))...)
	deps = append(deps, extractHTTPVectorModelDependencies(
		query.Get("vectorDimension"),
		query.Get("vectorDimensions"),
		query.Get("vectorFilter"),
	)...)
	return uniqueBrowsingStateDependencies(deps)
}

func dependencyFromAxisFilter(filter *pb.AxisFilter) []browsingStateDependency {
	if filter == nil {
		return nil
	}
	value := strconv.Itoa(int(filter.GetValue()))
	switch filter.GetValueType() {
	case pb.FilterValueType_TAGSET:
		return []browsingStateDependency{{Kind: browsingStateDepTagset, ID: value}}
	case pb.FilterValueType_TAG:
		return []browsingStateDependency{{Kind: browsingStateDepTag, ID: value}}
	case pb.FilterValueType_NODE:
		return []browsingStateDependency{{Kind: browsingStateDepNode, ID: value}}
	default:
		return nil
	}
}

func dependencyFromVectorFilter(filter *pb.VectorFilterConfig) []browsingStateDependency {
	if filter == nil || strings.TrimSpace(filter.GetModelName()) == "" {
		return nil
	}
	return []browsingStateDependency{{Kind: browsingStateDepVectorModel, ID: filter.GetModelName()}}
}

func dependencyFromVectorDim(dim *pb.VectorSearchDimension) []browsingStateDependency {
	if dim == nil || strings.TrimSpace(dim.GetModelName()) == "" {
		return nil
	}
	return []browsingStateDependency{{Kind: browsingStateDepVectorModel, ID: dim.GetModelName()}}
}

func extractHTTPAxesDependencies(rawAxes ...string) []browsingStateDependency {
	deps := make([]browsingStateDependency, 0)
	for _, raw := range rawAxes {
		var axis struct {
			Type string `json:"type"`
			ID   int    `json:"id"`
		}
		if json.Unmarshal([]byte(raw), &axis) != nil || axis.ID == 0 {
			continue
		}
		deps = append(deps, dependencyFromHTTPType(axis.Type, axis.ID)...)
	}
	return deps
}

func extractHTTPFilterDependencies(raw string) []browsingStateDependency {
	var filters []struct {
		Type string `json:"type"`
		IDs  []int  `json:"ids"`
	}
	if json.Unmarshal([]byte(raw), &filters) != nil {
		return nil
	}
	deps := make([]browsingStateDependency, 0)
	for _, filter := range filters {
		for _, id := range filter.IDs {
			deps = append(deps, dependencyFromHTTPType(filter.Type, id)...)
		}
	}
	return deps
}

func extractHTTPVectorModelDependencies(rawSingle string, rawMany string, rawFilter string) []browsingStateDependency {
	models := make([]string, 0)
	var single struct {
		Model string `json:"model"`
	}
	if json.Unmarshal([]byte(rawSingle), &single) == nil && single.Model != "" {
		models = append(models, single.Model)
	}
	var many []struct {
		Model string `json:"model"`
	}
	if json.Unmarshal([]byte(rawMany), &many) == nil {
		for _, dim := range many {
			models = append(models, dim.Model)
		}
	}
	var filter compatVectorFilter
	if json.Unmarshal([]byte(rawFilter), &filter) == nil && filter.Model != "" {
		models = append(models, filter.Model)
	}
	deps := make([]browsingStateDependency, 0, len(models))
	for _, model := range models {
		deps = append(deps, browsingStateDependency{Kind: browsingStateDepVectorModel, ID: model})
	}
	return deps
}

func dependencyFromHTTPType(kind string, id int) []browsingStateDependency {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "tagset":
		return []browsingStateDependency{{Kind: browsingStateDepTagset, ID: fmt.Sprint(id)}}
	case "tag":
		return []browsingStateDependency{{Kind: browsingStateDepTag, ID: fmt.Sprint(id)}}
	case "node":
		return []browsingStateDependency{{Kind: browsingStateDepNode, ID: fmt.Sprint(id)}}
	default:
		return nil
	}
}

func uniqueBrowsingStateDependencies(deps []browsingStateDependency) []browsingStateDependency {
	seen := make(map[string]struct{}, len(deps))
	unique := make([]browsingStateDependency, 0, len(deps))
	for _, dep := range deps {
		normalized := normalizeBrowsingStateDependency(dep)
		if normalized.Kind == "" || normalized.ID == "" {
			continue
		}
		key := dependencyKey(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, normalized)
	}
	return unique
}

func mergeBrowsingStateDependencies(parent *StateNode, deps []browsingStateDependency) []browsingStateDependency {
	merged := cloneBrowsingStateDependencies(deps)
	if parent != nil {
		merged = append(merged, parent.Dependencies...)
	}
	return uniqueBrowsingStateDependencies(merged)
}

func deriveNodeDependencies(parent *StateNode, deps []browsingStateDependency) []browsingStateDependency {
	if parent == nil {
		return uniqueBrowsingStateDependencies(deps)
	}
	parentDeps := dependencySet(parent.Dependencies)
	nodeDeps := make([]browsingStateDependency, 0, len(deps))
	for _, dep := range uniqueBrowsingStateDependencies(deps) {
		if _, ok := parentDeps[dependencyKey(dep)]; !ok {
			nodeDeps = append(nodeDeps, dep)
		}
	}
	return nodeDeps
}

func cloneBrowsingStateDependencies(deps []browsingStateDependency) []browsingStateDependency {
	return append([]browsingStateDependency(nil), deps...)
}

func dependencyListContains(deps []browsingStateDependency, target browsingStateDependency) bool {
	targetKey := dependencyKey(normalizeBrowsingStateDependency(target))
	for _, dep := range deps {
		if dependencyKey(normalizeBrowsingStateDependency(dep)) == targetKey {
			return true
		}
	}
	return false
}

func dependencySet(deps []browsingStateDependency) map[string]struct{} {
	set := make(map[string]struct{}, len(deps))
	for _, dep := range uniqueBrowsingStateDependencies(deps) {
		set[dependencyKey(dep)] = struct{}{}
	}
	return set
}

func normalizeBrowsingStateDependency(dep browsingStateDependency) browsingStateDependency {
	return browsingStateDependency{
		Kind: strings.ToLower(strings.TrimSpace(dep.Kind)),
		ID:   strings.ToLower(strings.TrimSpace(dep.ID)),
	}
}

func dependencyKey(dep browsingStateDependency) string {
	return dep.Kind + ":" + dep.ID
}
