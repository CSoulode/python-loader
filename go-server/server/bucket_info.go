package main

import (
	"sync"

	pb "m3.dataloader/dataloader"
)

type bucketInfoAttacher struct {
	mu              sync.Mutex
	infos           []*pb.BucketInfo
	axisBucketInfos map[string][]*pb.BucketInfo
	sent            bool
}

func newBucketInfoAttacher(infos []*pb.BucketInfo) *bucketInfoAttacher {
	return &bucketInfoAttacher{infos: cloneBucketInfos(infos)}
}

func newBucketInfoAttacherMulti(axisBucketInfos map[string][]*pb.BucketInfo) *bucketInfoAttacher {
	return &bucketInfoAttacher{axisBucketInfos: cloneAxisBucketInfos(axisBucketInfos)}
}

func newBucketInfoAttacherForPlan(plan *browsingStateRequestPlan) *bucketInfoAttacher {
	if plan == nil {
		return nil
	}
	if plan.UseAxisBucketInfos {
		return newBucketInfoAttacherMulti(plan.AxisBucketInfos)
	}
	return newBucketInfoAttacher(plan.BucketInfos)
}

func (a *bucketInfoAttacher) Attach(resp *pb.BrowsingStateResponse) *pb.BrowsingStateResponse {
	if a == nil || resp == nil {
		return resp
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sent {
		return resp
	}

	if len(a.axisBucketInfos) > 0 {
		resp.AxisBucketInfos = make(map[string]*pb.BucketInfoList, len(a.axisBucketInfos))
		for axisKey, infos := range a.axisBucketInfos {
			resp.AxisBucketInfos[axisKey] = &pb.BucketInfoList{Items: cloneBucketInfos(infos)}
		}
		a.sent = true
		return resp
	}
	if len(a.infos) == 0 {
		return resp
	}

	resp.BucketInfos = cloneBucketInfos(a.infos)
	a.sent = true
	return resp
}

func cloneAxisBucketInfos(infos map[string][]*pb.BucketInfo) map[string][]*pb.BucketInfo {
	if len(infos) == 0 {
		return nil
	}

	cloned := make(map[string][]*pb.BucketInfo, len(infos))
	for axisKey, bucketInfos := range infos {
		cloned[axisKey] = cloneBucketInfos(bucketInfos)
	}
	return cloned
}
