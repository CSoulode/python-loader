package main

import (
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

type vectorModelInfoCache struct {
	mu     sync.RWMutex
	models map[string]*kvstorev1.ModelInfo
}

func newVectorModelInfoCache() *vectorModelInfoCache {
	return &vectorModelInfoCache{models: make(map[string]*kvstorev1.ModelInfo)}
}

func (c *vectorModelInfoCache) get(name string) (*kvstorev1.ModelInfo, bool) {
	if c == nil {
		return nil, false
	}
	key := strings.TrimSpace(name)
	c.mu.RLock()
	model, ok := c.models[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneModelInfo(model), true
}

func (c *vectorModelInfoCache) putAll(models []*kvstorev1.ModelInfo) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, model := range models {
		name := strings.TrimSpace(model.GetName())
		if name == "" {
			continue
		}
		c.models[name] = cloneModelInfo(model)
	}
}

func cloneModelInfo(model *kvstorev1.ModelInfo) *kvstorev1.ModelInfo {
	if model == nil {
		return nil
	}
	cloned := proto.Clone(model)
	if typed, ok := cloned.(*kvstorev1.ModelInfo); ok {
		return typed
	}
	return nil
}

func (s *DataLoaderServer) ensureModelInfoCache() *vectorModelInfoCache {
	if s.modelInfoCache == nil {
		s.modelInfoCache = newVectorModelInfoCache()
	}
	return s.modelInfoCache
}
