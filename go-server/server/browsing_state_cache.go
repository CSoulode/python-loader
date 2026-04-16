package main

import (
	"container/list"
	"sync"
	"time"

	pb "m3.dataloader/dataloader"
)

const (
	defaultBrowsingStateCacheMaxEntries = 256
	defaultBrowsingStateCacheTTL        = 5 * time.Minute
)

type bsCacheStorageKey struct {
	Namespace string
	Canonical string
}

type cachedCubeObject struct {
	ID           int32
	FileURI      string
	ThumbnailURI string
}

type cachedBrowsingStateCell struct {
	X           int32
	Y           int32
	Z           int32
	Count       int32
	CubeObjects []cachedCubeObject
}

type bsCacheValue struct {
	Cells           []cachedBrowsingStateCell
	BucketInfos     []*pb.BucketInfo
	AxisBucketInfos map[string][]*pb.BucketInfo
}

type bsCacheDependency struct {
	Kind string
	Key  string
}

type bsCacheEntry struct {
	Key          bsCacheStorageKey
	Value        bsCacheValue
	ETag         string
	Dependencies []bsCacheDependency
	CreatedAt    time.Time
	LastAccessAt time.Time
}

type bsCacheEntrySnapshot struct {
	Key          bsCacheStorageKey
	Value        bsCacheValue
	ETag         string
	Dependencies []bsCacheDependency
	CreatedAt    time.Time
	LastAccessAt time.Time
}

type bsCacheStats struct {
	Entries                 int   `json:"entries"`
	MaxEntries              int   `json:"max_entries"`
	TTLSeconds              int64 `json:"ttl_seconds"`
	ApproxMemoryBytes       int64 `json:"approx_memory_bytes"`
	Hits                    int64 `json:"hits"`
	Misses                  int64 `json:"misses"`
	Evictions               int64 `json:"evictions"`
	Expires                 int64 `json:"expires"`
	InvalidationsAll        int64 `json:"invalidations_all"`
	InvalidationsDependency int64 `json:"invalidations_dependency"`
	HTTPNotModified         int64 `json:"http_not_modified"`
	GRPCNotModified         int64 `json:"grpc_not_modified"`
}

type BrowsingStateCache struct {
	mu                      sync.Mutex
	entries                 map[bsCacheStorageKey]*list.Element
	lru                     *list.List
	maxEntries              int
	ttl                     time.Duration
	now                     func() time.Time
	hits                    int64
	misses                  int64
	evictions               int64
	expires                 int64
	invalidationsAll        int64
	invalidationsDependency int64
	httpNotModified         int64
	grpcNotModified         int64
}

func newBrowsingStateCache(maxEntries int, ttl time.Duration) *BrowsingStateCache {
	if maxEntries <= 0 {
		maxEntries = defaultBrowsingStateCacheMaxEntries
	}
	if ttl <= 0 {
		ttl = defaultBrowsingStateCacheTTL
	}

	return &BrowsingStateCache{
		entries:    make(map[bsCacheStorageKey]*list.Element, maxEntries),
		lru:        list.New(),
		maxEntries: maxEntries,
		ttl:        ttl,
		now:        time.Now,
	}
}

func (s *DataLoaderServer) ensureBrowsingStateCache() *BrowsingStateCache {
	if s.bsCache == nil {
		s.bsCache = newConfiguredBrowsingStateCache()
	}
	registerBrowsingStateCacheForExpvar(s.bsCache)
	return s.bsCache
}

func (c *BrowsingStateCache) TryGet(
	key bsCacheStorageKey,
) (bsCacheValue, bool) {
	entry, ok := c.TryGetEntry(key)
	return entry.Value, ok
}

func (c *BrowsingStateCache) TryGetEntry(
	key bsCacheStorageKey,
) (bsCacheEntrySnapshot, bool) {
	if c == nil {
		return bsCacheEntrySnapshot{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)

	element, ok := c.entries[key]
	if !ok {
		c.misses++
		return bsCacheEntrySnapshot{}, false
	}

	entry := element.Value.(*bsCacheEntry)
	if c.isExpired(entry, now) {
		c.removeElement(element)
		c.expires++
		c.misses++
		return bsCacheEntrySnapshot{}, false
	}

	entry.LastAccessAt = now
	c.lru.MoveToFront(element)
	c.hits++
	return cloneBSCacheEntry(entry), true
}

func (c *BrowsingStateCache) Put(
	key bsCacheStorageKey,
	value bsCacheValue,
) {
	canonical := canonicalizeBSCacheValue(value)
	c.PutEntry(key, canonical, computeCanonicalBSETag(canonical), nil)
}

func (c *BrowsingStateCache) PutEntry(
	key bsCacheStorageKey,
	value bsCacheValue,
	etag string,
	dependencies []bsCacheDependency,
) {
	if c == nil {
		return
	}
	value = canonicalizeBSCacheValue(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)

	if element, ok := c.entries[key]; ok {
		entry := element.Value.(*bsCacheEntry)
		entry.Value = cloneBSCacheValue(value)
		entry.ETag = etag
		entry.Dependencies = cloneBSCacheDependencies(dependencies)
		entry.LastAccessAt = now
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = now
		}
		c.lru.MoveToFront(element)
		return
	}

	entry := &bsCacheEntry{
		Key:          key,
		Value:        cloneBSCacheValue(value),
		ETag:         etag,
		Dependencies: cloneBSCacheDependencies(dependencies),
		CreatedAt:    now,
		LastAccessAt: now,
	}
	c.entries[key] = c.lru.PushFront(entry)
	c.evictOverflow()
}

func (c *BrowsingStateCache) InvalidateAll() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	invalidated := len(c.entries)
	c.entries = make(map[bsCacheStorageKey]*list.Element, c.maxEntries)
	c.lru.Init()
	c.invalidationsAll += int64(invalidated)
	return invalidated
}

func (c *BrowsingStateCache) InvalidateDependency(dep bsCacheDependency) int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	invalidated := 0
	for key, element := range c.entries {
		entry := element.Value.(*bsCacheEntry)
		if !entryHasDependency(entry, dep) {
			continue
		}
		delete(c.entries, key)
		c.lru.Remove(element)
		invalidated++
	}
	c.invalidationsDependency += int64(invalidated)
	return invalidated
}

func (c *BrowsingStateCache) RecordHTTPNotModified() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.httpNotModified++
}

func (c *BrowsingStateCache) RecordGRPCNotModified() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.grpcNotModified++
}

func (c *BrowsingStateCache) Stats() bsCacheStats {
	if c == nil {
		return bsCacheStats{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeExpired(c.now())
	return bsCacheStats{
		Entries:                 len(c.entries),
		MaxEntries:              c.maxEntries,
		TTLSeconds:              int64(c.ttl / time.Second),
		ApproxMemoryBytes:       c.approxMemoryBytesLocked(),
		Hits:                    c.hits,
		Misses:                  c.misses,
		Evictions:               c.evictions,
		Expires:                 c.expires,
		InvalidationsAll:        c.invalidationsAll,
		InvalidationsDependency: c.invalidationsDependency,
		HTTPNotModified:         c.httpNotModified,
		GRPCNotModified:         c.grpcNotModified,
	}
}

func (c *BrowsingStateCache) approxMemoryBytesLocked() int64 {
	var total int64
	for _, element := range c.entries {
		total += approximateBSCacheEntryBytes(element.Value.(*bsCacheEntry))
	}
	return total
}

func (c *BrowsingStateCache) purgeExpired(now time.Time) {
	for element := c.lru.Back(); element != nil; {
		prev := element.Prev()
		if c.isExpired(element.Value.(*bsCacheEntry), now) {
			c.removeElement(element)
			c.expires++
		}
		element = prev
	}
}

func (c *BrowsingStateCache) evictOverflow() {
	for len(c.entries) > c.maxEntries {
		c.removeElement(c.lru.Back())
		c.evictions++
	}
}

func (c *BrowsingStateCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}

	entry := element.Value.(*bsCacheEntry)
	delete(c.entries, entry.Key)
	c.lru.Remove(element)
}

func (c *BrowsingStateCache) isExpired(entry *bsCacheEntry, now time.Time) bool {
	if entry == nil {
		return true
	}
	return c.ttl > 0 && now.Sub(entry.CreatedAt) >= c.ttl
}

func cloneBSCacheEntry(entry *bsCacheEntry) bsCacheEntrySnapshot {
	if entry == nil {
		return bsCacheEntrySnapshot{}
	}
	return bsCacheEntrySnapshot{
		Key:          entry.Key,
		Value:        cloneBSCacheValue(entry.Value),
		ETag:         entry.ETag,
		Dependencies: cloneBSCacheDependencies(entry.Dependencies),
		CreatedAt:    entry.CreatedAt,
		LastAccessAt: entry.LastAccessAt,
	}
}

func cloneBSCacheDependencies(dependencies []bsCacheDependency) []bsCacheDependency {
	if len(dependencies) == 0 {
		return nil
	}
	cloned := make([]bsCacheDependency, len(dependencies))
	copy(cloned, dependencies)
	return cloned
}

func entryHasDependency(entry *bsCacheEntry, dep bsCacheDependency) bool {
	if entry == nil {
		return false
	}
	for _, candidate := range entry.Dependencies {
		if candidate == dep {
			return true
		}
	}
	return false
}
