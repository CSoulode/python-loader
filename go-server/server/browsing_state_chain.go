package main

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type BrowsingStateChain struct {
	mu       sync.Mutex
	nodes    map[StateID]*StateNode
	children map[StateID]map[StateID]struct{}
	inflight map[string]*browsingStateInflight
	lru      *list.List
	maxNodes int
	ttl      time.Duration
	now      func() time.Time

	fullHits          atomic.Int64
	coldMisses        atomic.Int64
	ancestorReuses    atomic.Int64
	evictions         atomic.Int64
	expires           atomic.Int64
	inflightLeaders   atomic.Int64
	inflightWaiters   atomic.Int64
	fragmentReuses    atomic.Int64
	published         atomic.Int64
	publishCollisions atomic.Int64
	httpNotModified   atomic.Int64
	grpcNotModified   atomic.Int64
	ancestorETagHits  atomic.Int64
	invalidationsAll  atomic.Int64
	invalidationsDep  atomic.Int64
	invalidationsTree atomic.Int64
}

func newBrowsingStateChain(maxNodes int, ttl time.Duration) *BrowsingStateChain {
	if maxNodes <= 0 {
		maxNodes = defaultBrowsingStateChainMaxNodes
	}
	if ttl <= 0 {
		ttl = defaultBrowsingStateChainTTL
	}
	return &BrowsingStateChain{
		nodes:    make(map[StateID]*StateNode, maxNodes),
		children: make(map[StateID]map[StateID]struct{}),
		inflight: make(map[string]*browsingStateInflight),
		lru:      list.New(),
		maxNodes: maxNodes,
		ttl:      ttl,
		now:      time.Now,
	}
}

func (s *DataLoaderServer) ensureBrowsingStateChain() *BrowsingStateChain {
	if s.browsingChain == nil {
		s.browsingChain = newBrowsingStateChain(defaultBrowsingStateChainMaxNodes, defaultBrowsingStateChainTTL)
		setBrowsingStateChainForExpvar(s.browsingChain)
	}
	return s.browsingChain
}

func (c *BrowsingStateChain) NodeCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeExpiredLocked(c.now())
	return len(c.nodes)
}

func (c *BrowsingStateChain) InvalidateAll() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := len(c.nodes)
	c.nodes = make(map[StateID]*StateNode, c.maxNodes)
	c.children = make(map[StateID]map[StateID]struct{})
	c.lru.Init()
	c.invalidationsAll.Add(int64(count))
	return count
}

func (c *BrowsingStateChain) Snapshot() map[string]int64 {
	if c == nil {
		return map[string]int64{"nodes": 0}
	}
	return map[string]int64{
		"nodes":              int64(c.NodeCount()),
		"full_hits":          c.fullHits.Load(),
		"cold_misses":        c.coldMisses.Load(),
		"ancestor_reuses":    c.ancestorReuses.Load(),
		"evictions":          c.evictions.Load(),
		"expires":            c.expires.Load(),
		"inflight_leaders":   c.inflightLeaders.Load(),
		"inflight_waiters":   c.inflightWaiters.Load(),
		"fragment_reuses":    c.fragmentReuses.Load(),
		"published":          c.published.Load(),
		"publish_collisions": c.publishCollisions.Load(),
		"http_not_modified":  c.httpNotModified.Load(),
		"grpc_not_modified":  c.grpcNotModified.Load(),
		"ancestor_etag_hits": c.ancestorETagHits.Load(),
		"invalidations_all":  c.invalidationsAll.Load(),
		"invalidations_dep":  c.invalidationsDep.Load(),
		"invalidations_tree": c.invalidationsTree.Load(),
	}
}
