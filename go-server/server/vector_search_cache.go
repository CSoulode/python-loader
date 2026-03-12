package main

import (
	"container/list"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
)

const (
	defaultVectorCacheMaxEntries = 64
	defaultVectorCacheTTL        = 5 * time.Minute
)

type searchResult struct {
	RawNeighbors   []Neighbor
	DistanceMetric string
}

type Neighbor struct {
	ObjectID int32
	Distance float64
}

type cacheKey struct {
	ModelName string
	RefHash   uint64
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

func (c *VectorSearchCache) TryGet(modelName string, refHash uint64, reqK int32) (searchResult, bool) {
	if c == nil {
		return searchResult{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	key := newCacheKey(modelName, refHash)
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

	entry.LastAccessAt = now
	c.lru.MoveToFront(element)
	return cloneSearchResult(limitSearchResult(entry.Result, reqK)), true
}

func (c *VectorSearchCache) Put(modelName string, refHash uint64, result searchResult, reqK int32) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.purgeExpired(now)
	key := newCacheKey(modelName, refHash)
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

func (c *VectorSearchCache) updateEntry(entry *cacheEntry, result searchResult, reqK int32, now time.Time) {
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

func newCacheKey(modelName string, refHash uint64) cacheKey {
	return cacheKey{
		ModelName: strings.TrimSpace(modelName),
		RefHash:   refHash,
	}
}

func hashVectorReference(ref *pb.VectorReference) (uint64, error) {
	if ref == nil {
		return 0, status.Error(codes.InvalidArgument, "vector reference is required")
	}

	hasher := fnv.New64a()
	switch value := ref.GetRef().(type) {
	case *pb.VectorReference_ObjectId:
		var data [5]byte
		data[0] = 'o'
		binary.LittleEndian.PutUint32(data[1:], uint32(value.ObjectId))
		_, _ = hasher.Write(data[:])
	case *pb.VectorReference_RawEmbedding:
		_, _ = hasher.Write([]byte{'r'})
		writeFloat32SliceHash(hasher, value.RawEmbedding.GetValues())
	default:
		return 0, status.Error(codes.InvalidArgument, "vector reference must set object_id or raw_embedding")
	}

	return hasher.Sum64(), nil
}

func writeFloat32SliceHash(hasher hash.Hash64, values []float32) {
	var data [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(data[:], math.Float32bits(value))
		_, _ = hasher.Write(data[:])
	}
}

func limitSearchResult(result searchResult, reqK int32) searchResult {
	if reqK <= 0 || len(result.RawNeighbors) <= int(reqK) {
		return result
	}

	limited := cloneNeighbors(result.RawNeighbors[:reqK])
	return searchResult{RawNeighbors: limited, DistanceMetric: result.DistanceMetric}
}

func cloneSearchResult(result searchResult) searchResult {
	return searchResult{
		RawNeighbors:   cloneNeighbors(result.RawNeighbors),
		DistanceMetric: result.DistanceMetric,
	}
}

func cloneNeighbors(neighbors []Neighbor) []Neighbor {
	if len(neighbors) == 0 {
		return nil
	}

	cloned := make([]Neighbor, len(neighbors))
	copy(cloned, neighbors)
	return cloned
}
