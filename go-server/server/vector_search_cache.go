package main

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

const (
	defaultVectorCacheMaxEntries = 64
	defaultVectorCacheTTL        = 5 * time.Minute
)

type cacheKey struct {
	ModelName  string
	RefHash    uint64
	FilterHash uint64
}

type cacheEntry struct {
	Key          cacheKey
	Result       searchResult
	K            int32
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
	return c.tryGet(modelName, refHash, filterHash, reqK, searchKindUnknown)
}

func (c *VectorSearchCache) TryGetGlobalKNN(
	modelName string,
	refHash uint64,
	filterHash uint64,
	reqK int32,
) (searchResult, bool) {
	return c.tryGet(modelName, refHash, filterHash, reqK, searchKindGlobalKNN)
}

func (c *VectorSearchCache) tryGet(
	modelName string,
	refHash uint64,
	filterHash uint64,
	reqK int32,
	requiredKind SearchKind,
) (searchResult, bool) {
	if c == nil {
		return searchResult{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	key := newCacheKey(modelName, refHash, filterHash)
	element, ok := c.entries[key]
	if !ok {
		return searchResult{}, false
	}

	entry := element.Value.(*cacheEntry)
	if c.isExpired(entry, now) || (reqK > 0 && entry.K < reqK) {
		if c.isExpired(entry, now) {
			c.removeElement(element)
		}
		return searchResult{}, false
	}
	if requiredKind != searchKindUnknown && entry.Result.Kind != requiredKind {
		return searchResult{}, false
	}

	entry.LastAccessAt = now
	c.lru.MoveToFront(element)
	return cloneSearchResult(limitSearchResult(entry.Result, reqK)), true
}

func (c *VectorSearchCache) Put(
	modelName string,
	refHash uint64,
	filterHash uint64,
	result searchResult,
	reqK int32,
) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)
	key := newCacheKey(modelName, refHash, filterHash)
	if element, ok := c.entries[key]; ok {
		c.updateEntry(element.Value.(*cacheEntry), result, reqK, now)
		c.lru.MoveToFront(element)
		return
	}

	entry := &cacheEntry{Key: key}
	c.updateEntry(entry, result, reqK, now)
	c.entries[key] = c.lru.PushFront(entry)
	c.evictOverflow()
}

func (c *VectorSearchCache) updateEntry(
	entry *cacheEntry,
	result searchResult,
	reqK int32,
	now time.Time,
) {
	entry.Result = cloneSearchResult(result)
	entry.K = reqK
	entry.LastAccessAt = now
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
}

func (c *VectorSearchCache) purgeExpired(now time.Time) {
	for element := c.lru.Back(); element != nil; {
		prev := element.Prev()
		entry := element.Value.(*cacheEntry)
		if c.isExpired(entry, now) {
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

func newCacheKey(modelName string, refHash uint64, filterHash uint64) cacheKey {
	return cacheKey{
		ModelName:  strings.TrimSpace(modelName),
		RefHash:    refHash,
		FilterHash: filterHash,
	}
}
