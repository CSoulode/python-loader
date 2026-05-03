package main

import "expvar"

func init() {
	expvar.Publish("browsing_state_chain", expvar.Func(func() any {
		return globalBrowsingStateChainSnapshot()
	}))
}

var browsingStateChainForExpvar *BrowsingStateChain

func setBrowsingStateChainForExpvar(chain *BrowsingStateChain) {
	if browsingStateChainForExpvar == nil && chain != nil {
		browsingStateChainForExpvar = chain
	}
}

func globalBrowsingStateChainSnapshot() map[string]int64 {
	if browsingStateChainForExpvar == nil {
		return map[string]int64{"nodes": 0}
	}
	return browsingStateChainForExpvar.Snapshot()
}
