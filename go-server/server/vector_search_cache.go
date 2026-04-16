package main

import (
	"container/list"
	"math"
	"strings"
	"sync"
	"time"

	pb "m3.dataloader/dataloader"
)

const (
	defaultVectorCacheMaxEntries = 128
	defaultVectorCacheTTL        = 5 * time.Minute
)

type vectorCacheQuery struct {
	ModelName   string
	RefHash     uint64
	FilterHash  uint64
	Limit       int32
	IsRange     bool
	MinDistance float32
	MaxDistance float32
	Kind        SearchKind
}

type cacheKey struct {
	ModelName  string
	RefHash    uint64
	FilterHash uint64
	IsRange    bool
	MinBits    uint32
	MaxBits    uint32
	Kind       SearchKind
}

type cacheEntry struct {
	Key          cacheKey
	Query        vectorCacheQuery
	Result       searchResult
	CreatedAt    time.Time
	LastAccessAt time.Time
}

type VectorSearchCache struct {
	mu         sync.Mutex
	entries    map[cacheKey]*list.Element
	lru        *list.List
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
}

func newVectorSearchCache(maxEntries int, ttl time.Duration) *VectorSearchCache {
	if maxEntries <= 0 {
		maxEntries = defaultVectorCacheMaxEntries
	}
	if ttl <= 0 {
		ttl = defaultVectorCacheTTL
	}

	return &VectorSearchCache{
		entries:    make(map[cacheKey]*list.Element, maxEntries),
		lru:        list.New(),
		maxEntries: maxEntries,
		ttl:        ttl,
		now:        time.Now,
	}
}

func (s *DataLoaderServer) ensureVectorCache() *VectorSearchCache {
	if s.vectorCache == nil {
		s.vectorCache = newVectorSearchCache(defaultVectorCacheMaxEntries, defaultVectorCacheTTL)
	}
	return s.vectorCache
}

func (c *VectorSearchCache) TryGet(
	modelName string,
	refHash uint64,
	filterHash uint64,
	reqK int32,
) (searchResult, bool) {
	return c.tryGet(newKNNCacheQuery(modelName, refHash, filterHash, reqK, searchKindUnknown), searchKindUnknown)
}

func (c *VectorSearchCache) TryGetForConfig(
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
) (searchResult, bool) {
	return c.tryGet(newCacheQueryFromConfig(cfg, refHash, filterHash, searchKindUnknown), searchKindUnknown)
}

func (c *VectorSearchCache) TryGetForConfigKind(
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
	kind SearchKind,
) (searchResult, bool) {
	return c.tryGet(newCacheQueryFromConfig(cfg, refHash, filterHash, kind), kind)
}

func (c *VectorSearchCache) TryGetGlobalKNN(
	modelName string,
	refHash uint64,
	filterHash uint64,
	reqK int32,
) (searchResult, bool) {
	return c.tryGet(newKNNCacheQuery(modelName, refHash, filterHash, reqK, searchKindGlobalKNN), searchKindGlobalKNN)
}

func (c *VectorSearchCache) TryGetGlobalForConfig(
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
) (searchResult, bool) {
	kind := globalSearchKindForConfig(cfg)
	return c.tryGet(newCacheQueryFromConfig(cfg, refHash, filterHash, kind), kind)
}

func (c *VectorSearchCache) tryGet(query vectorCacheQuery, requiredKind SearchKind) (searchResult, bool) {
	if c == nil {
		return searchResult{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)

	if element, ok := c.entries[newCacheKey(query)]; ok {
		if result, hit := c.tryElement(element, query, requiredKind, now); hit {
			return result, true
		}
	}
	if !query.IsRange && query.Kind != searchKindUnknown {
		return searchResult{}, false
	}

	for element := c.lru.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*cacheEntry)
		if entry.Key == newCacheKey(query) {
			continue
		}
		if result, hit := c.tryElement(element, query, requiredKind, now); hit {
			return result, true
		}
	}
	return searchResult{}, false
}

func (c *VectorSearchCache) tryElement(
	element *list.Element,
	query vectorCacheQuery,
	requiredKind SearchKind,
	now time.Time,
) (searchResult, bool) {
	entry := element.Value.(*cacheEntry)
	if c.isExpired(entry, now) {
		c.removeElement(element)
		return searchResult{}, false
	}
	if requiredKind != searchKindUnknown && entry.Result.Kind != requiredKind {
		return searchResult{}, false
	}
	if !entryCanSatisfyQuery(entry, query) {
		return searchResult{}, false
	}

	entry.LastAccessAt = now
	c.lru.MoveToFront(element)
	return buildCachedResult(entry, query), true
}

func (c *VectorSearchCache) Put(
	modelName string,
	refHash uint64,
	filterHash uint64,
	result searchResult,
	reqK int32,
) {
	c.put(newKNNCacheQuery(modelName, refHash, filterHash, reqK, result.Kind), result)
}

func (c *VectorSearchCache) PutForConfig(
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
	result searchResult,
) {
	c.put(newCacheQueryFromConfig(cfg, refHash, filterHash, result.Kind), result)
}

func (c *VectorSearchCache) put(query vectorCacheQuery, result searchResult) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)

	key := newCacheKey(query)
	if element, ok := c.entries[key]; ok {
		entry := element.Value.(*cacheEntry)
		if shouldReplaceCacheEntry(entry, query, result) {
			entry.Query = query
			entry.Result = cloneSearchResult(result)
		}
		entry.LastAccessAt = now
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = now
		}
		c.lru.MoveToFront(element)
		return
	}

	entry := &cacheEntry{
		Key:          key,
		Query:        query,
		Result:       cloneSearchResult(result),
		CreatedAt:    now,
		LastAccessAt: now,
	}
	c.entries[key] = c.lru.PushFront(entry)
	c.evictOverflow()
}

func shouldReplaceCacheEntry(entry *cacheEntry, query vectorCacheQuery, result searchResult) bool {
	if entry == nil {
		return true
	}
	if query.Limit > entry.Query.Limit {
		return true
	}
	if query.Limit < entry.Query.Limit {
		return false
	}
	return len(result.RawNeighbors) >= len(entry.Result.RawNeighbors)
}

func (c *VectorSearchCache) purgeExpired(now time.Time) {
	for element := c.lru.Back(); element != nil; {
		prev := element.Prev()
		if c.isExpired(element.Value.(*cacheEntry), now) {
			c.removeElement(element)
		}
		element = prev
	}
}

func (c *VectorSearchCache) evictOverflow() {
	for len(c.entries) > c.maxEntries {
		c.removeElement(c.lru.Back())
	}
}

func (c *VectorSearchCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}

	entry := element.Value.(*cacheEntry)
	delete(c.entries, entry.Key)
	c.lru.Remove(element)
}

func (c *VectorSearchCache) isExpired(entry *cacheEntry, now time.Time) bool {
	if entry == nil {
		return true
	}
	return c.ttl > 0 && now.Sub(entry.CreatedAt) >= c.ttl
}

func newKNNCacheQuery(
	modelName string,
	refHash uint64,
	filterHash uint64,
	limit int32,
	kind SearchKind,
) vectorCacheQuery {
	return vectorCacheQuery{
		ModelName:  strings.TrimSpace(modelName),
		RefHash:    refHash,
		FilterHash: filterHash,
		Limit:      limit,
		Kind:       kind,
	}
}

func newCacheQueryFromConfig(
	cfg *pb.VectorSearchDimension,
	refHash uint64,
	filterHash uint64,
	kind SearchKind,
) vectorCacheQuery {
	if cfg == nil {
		return vectorCacheQuery{RefHash: refHash, FilterHash: filterHash, Kind: kind}
	}

	query := newKNNCacheQuery(
		cfg.GetModelName(),
		refHash,
		filterHash,
		effectiveVectorMaxResults(cfg),
		kind,
	)
	if !isRangeQuery(cfg) {
		return query
	}
	query.IsRange = true
	query.MinDistance = cfg.GetDistanceRange().GetMinDistance()
	query.MaxDistance = cfg.GetDistanceRange().GetMaxDistance()
	return query
}

func newCacheKey(query vectorCacheQuery) cacheKey {
	return cacheKey{
		ModelName:  strings.TrimSpace(query.ModelName),
		RefHash:    query.RefHash,
		FilterHash: query.FilterHash,
		IsRange:    query.IsRange,
		MinBits:    math.Float32bits(query.MinDistance),
		MaxBits:    math.Float32bits(query.MaxDistance),
		Kind:       query.Kind,
	}
}

func globalSearchKindForConfig(cfg *pb.VectorSearchDimension) SearchKind {
	if isRangeQuery(cfg) {
		return searchKindGlobalRange
	}
	return searchKindGlobalKNN
}

func entryCanSatisfyQuery(entry *cacheEntry, query vectorCacheQuery) bool {
	if entry == nil || entry.Query.IsRange != query.IsRange {
		return false
	}
	if entry.Query.ModelName != query.ModelName ||
		entry.Query.RefHash != query.RefHash ||
		entry.Query.FilterHash != query.FilterHash {
		return false
	}
	if query.Limit > 0 && entry.Query.Limit < query.Limit {
		return false
	}
	if !query.IsRange {
		return true
	}
	if !rangeContains(entry.Query, query) {
		return false
	}
	if sameRange(entry.Query, query) {
		return true
	}
	if isCompleteRangeEntry(entry) {
		return true
	}
	return isSafeBallSubset(entry.Query, query)
}

func buildCachedResult(entry *cacheEntry, query vectorCacheQuery) searchResult {
	if entry == nil {
		return searchResult{}
	}
	if !query.IsRange {
		return cloneSearchResult(limitSearchResult(entry.Result, query.Limit))
	}

	filtered := filterNeighborsByRange(
		entry.Result.RawNeighbors,
		float64(query.MinDistance),
		float64(query.MaxDistance),
		query.Limit,
	)
	return searchResult{
		RawNeighbors:   cloneNeighbors(filtered),
		DistanceMetric: entry.Result.DistanceMetric,
		Kind:           entry.Result.Kind,
	}
}

func rangeContains(superset vectorCacheQuery, subset vectorCacheQuery) bool {
	return superset.MinDistance <= subset.MinDistance && superset.MaxDistance >= subset.MaxDistance
}

func sameRange(left vectorCacheQuery, right vectorCacheQuery) bool {
	return left.MinDistance == right.MinDistance && left.MaxDistance == right.MaxDistance
}

func isCompleteRangeEntry(entry *cacheEntry) bool {
	if entry == nil || !entry.Query.IsRange || entry.Query.Limit <= 0 {
		return false
	}
	return len(entry.Result.RawNeighbors) < int(entry.Query.Limit)
}

func isSafeBallSubset(superset vectorCacheQuery, subset vectorCacheQuery) bool {
	return superset.MinDistance == 0 &&
		subset.MinDistance == 0 &&
		subset.MaxDistance <= superset.MaxDistance
}
