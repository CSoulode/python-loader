package main

import (
	"sync"

	pb "m3.dataloader/dataloader"
)

type bucketInfoAttacher struct {
	mu    sync.Mutex
	infos []*pb.BucketInfo
	sent  bool
}

func newBucketInfoAttacher(infos []*pb.BucketInfo) *bucketInfoAttacher {
	return &bucketInfoAttacher{infos: cloneBucketInfos(infos)}
}

func (a *bucketInfoAttacher) Attach(resp *pb.BrowsingStateResponse) *pb.BrowsingStateResponse {
	if a == nil || resp == nil || len(a.infos) == 0 {
		return resp
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sent {
		return resp
	}

	resp.BucketInfos = cloneBucketInfos(a.infos)
	a.sent = true
	return resp
}
