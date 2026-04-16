package main

import (
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
)

func BenchmarkPhaseF1CacheHitStatePath(b *testing.B) {
	req := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          11,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
	}
	prepared, err := prepareBrowsingStateRequest(req)
	if err != nil {
		b.Fatalf("prepare request: %v", err)
	}
	key, err := computeBSCacheKey(bsCacheNamespaceGetBrowsingState, prepared)
	if err != nil {
		b.Fatalf("compute key: %v", err)
	}

	server := &DataLoaderServer{bsCache: newBrowsingStateCache(4, time.Minute)}
	server.bsCache.Put(key, testBSCacheValue())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := newTestBrowsingStateStream()
		if err := server.GetBrowsingState(req, stream); err != nil {
			b.Fatalf("GetBrowsingState returned error: %v", err)
		}
		if len(stream.responses) != 1 {
			b.Fatalf("responses = %d, want 1", len(stream.responses))
		}
	}
}
