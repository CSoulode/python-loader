package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lib/pq"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

const (
	bsDependencyKindNode        = "node"
	bsDependencyKindTagset      = "tagset"
	bsDependencyKindVectorModel = "vector_model"
)

type bsCacheWrite struct {
	Key          bsCacheStorageKey
	Dependencies []bsCacheDependency
}

func (s *DataLoaderServer) newBrowsingStateCacheWrite(
	ctx context.Context,
	key bsCacheStorageKey,
	prepared *preparedBrowsingStateRequest,
) (*bsCacheWrite, error) {
	dependencies, err := s.extractBrowsingStateCacheDependencies(ctx, prepared)
	if err != nil {
		return nil, err
	}
	return &bsCacheWrite{
		Key:          key,
		Dependencies: dependencies,
	}, nil
}

func (s *DataLoaderServer) extractBrowsingStateCacheDependencies(
	ctx context.Context,
	prepared *preparedBrowsingStateRequest,
) ([]bsCacheDependency, error) {
	if prepared == nil {
		return nil, nil
	}

	dependencySet := make(map[bsCacheDependency]struct{})
	tagIDs := make(map[int]struct{})

	collectAxisDependency(dependencySet, tagIDs, prepared.AxisX)
	collectAxisDependency(dependencySet, tagIDs, prepared.AxisY)
	collectAxisDependency(dependencySet, tagIDs, prepared.AxisZ)
	for _, filter := range prepared.Filters {
		collectFilterDependency(dependencySet, tagIDs, filter)
	}
	collectVectorDependencies(dependencySet, prepared.VectorFilter, prepared.MergedVectorDims.Dims)

	tagsetDeps, err := s.lookupTagsetDependencies(ctx, tagIDs)
	if err != nil {
		return nil, err
	}
	for _, dep := range tagsetDeps {
		dependencySet[dep] = struct{}{}
	}

	dependencies := make([]bsCacheDependency, 0, len(dependencySet))
	for dep := range dependencySet {
		dependencies = append(dependencies, dep)
	}
	sort.Slice(dependencies, func(left, right int) bool {
		return dependencies[left].Kind < dependencies[right].Kind ||
			(dependencies[left].Kind == dependencies[right].Kind &&
				dependencies[left].Key < dependencies[right].Key)
	})
	return dependencies, nil
}

func collectAxisDependency(
	dependencies map[bsCacheDependency]struct{},
	tagIDs map[int]struct{},
	axis qg.ParsedAxis,
) {
	switch strings.TrimSpace(axis.Type) {
	case bsDependencyKindTagset:
		addBSDependency(dependencies, bsDependencyKindTagset, axis.Id)
	case bsDependencyKindNode:
		addBSDependency(dependencies, bsDependencyKindNode, axis.Id)
	case "tag":
		addTagID(tagIDs, axis.Id)
	}
}

func collectFilterDependency(
	dependencies map[bsCacheDependency]struct{},
	tagIDs map[int]struct{},
	filter qg.ParsedFilter,
) {
	switch strings.TrimSpace(filter.Type) {
	case bsDependencyKindTagset:
		for _, id := range filter.Ids {
			addBSDependency(dependencies, bsDependencyKindTagset, id)
		}
	case bsDependencyKindNode:
		for _, id := range filter.Ids {
			addBSDependency(dependencies, bsDependencyKindNode, id)
		}
	case "tag":
		for _, id := range filter.Ids {
			addTagID(tagIDs, id)
		}
	}
}

func collectVectorDependencies(
	dependencies map[bsCacheDependency]struct{},
	vectorFilter *pb.VectorFilterConfig,
	dims []*pb.VectorSearchDimension,
) {
	if vectorFilter != nil {
		addVectorModelDependency(dependencies, vectorFilter.GetModelName())
	}
	for _, dim := range dims {
		if dim == nil {
			continue
		}
		addVectorModelDependency(dependencies, dim.GetModelName())
	}
}

func addBSDependency(
	dependencies map[bsCacheDependency]struct{},
	kind string,
	id int,
) {
	if id <= 0 {
		return
	}
	dependencies[bsCacheDependency{
		Kind: kind,
		Key:  strconv.Itoa(id),
	}] = struct{}{}
}

func addTagID(tagIDs map[int]struct{}, id int) {
	if id > 0 {
		tagIDs[id] = struct{}{}
	}
}

func addVectorModelDependency(
	dependencies map[bsCacheDependency]struct{},
	modelName string,
) {
	normalized := normalizeVectorModelName(modelName)
	if normalized == "" {
		return
	}
	dependencies[bsCacheDependency{
		Kind: bsDependencyKindVectorModel,
		Key:  normalized,
	}] = struct{}{}
}

func (s *DataLoaderServer) lookupTagsetDependencies(
	ctx context.Context,
	tagIDs map[int]struct{},
) ([]bsCacheDependency, error) {
	if len(tagIDs) == 0 {
		return nil, nil
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("browsing-state cache dependency lookup requires a database")
	}

	ids := make([]int, 0, len(tagIDs))
	for id := range tagIDs {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT DISTINCT tagset_id FROM public.tags WHERE id = ANY($1)`,
		pq.Array(ids),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dependencies []bsCacheDependency
	for rows.Next() {
		var tagsetID int
		if err := rows.Scan(&tagsetID); err != nil {
			return nil, err
		}
		dependencies = append(dependencies, bsCacheDependency{
			Kind: bsDependencyKindTagset,
			Key:  strconv.Itoa(tagsetID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dependencies, nil
}

func buildBSCacheEntry(
	key bsCacheStorageKey,
	value bsCacheValue,
	write *bsCacheWrite,
) bsCacheEntrySnapshot {
	value = canonicalizeBSCacheValue(value)
	dependencies := []bsCacheDependency(nil)
	if write != nil {
		dependencies = write.Dependencies
	}
	return bsCacheEntrySnapshot{
		Key:          key,
		Value:        cloneBSCacheValue(value),
		ETag:         computeCanonicalBSETag(value),
		Dependencies: cloneBSCacheDependencies(dependencies),
	}
}

func (s *DataLoaderServer) writeBrowsingStateCacheEntry(
	key bsCacheStorageKey,
	value bsCacheValue,
	write *bsCacheWrite,
) bsCacheEntrySnapshot {
	entry := buildBSCacheEntry(key, value, write)
	s.ensureBrowsingStateCache().PutEntry(
		key,
		entry.Value,
		entry.ETag,
		entry.Dependencies,
	)
	return entry
}
