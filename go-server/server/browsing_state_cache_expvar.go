package main

import (
	"encoding/json"
	"expvar"
	"sync"
)

var browsingStateCacheVar = newBrowsingStateCacheVar()

func init() {
	expvar.Publish("browsing_state_cache", browsingStateCacheVar)
}

type browsingStateCacheExpvar struct {
	mu    sync.RWMutex
	cache *BrowsingStateCache
}

func newBrowsingStateCacheVar() *browsingStateCacheExpvar {
	return &browsingStateCacheExpvar{}
}

func registerBrowsingStateCacheForExpvar(cache *BrowsingStateCache) {
	browsingStateCacheVar.SetCache(cache)
}

func (v *browsingStateCacheExpvar) SetCache(cache *BrowsingStateCache) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache = cache
}

func (v *browsingStateCacheExpvar) String() string {
	v.mu.RLock()
	cache := v.cache
	v.mu.RUnlock()
	if cache == nil {
		return "{}"
	}

	encoded, err := json.Marshal(cache.Stats())
	if err != nil {
		return `{"error":"marshal browsing_state_cache stats"}`
	}
	return string(encoded)
}
