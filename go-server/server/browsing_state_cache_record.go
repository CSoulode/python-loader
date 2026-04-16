package main

import (
	"context"
	"sort"

	pb "m3.dataloader/dataloader"
)

type browsingStateCacheRecorder struct {
	value     bsCacheValue
	cellIndex map[cacheRecorderCellKey]int
}

func newBrowsingStateCacheRecorder() *browsingStateCacheRecorder {
	return &browsingStateCacheRecorder{
		cellIndex: make(map[cacheRecorderCellKey]int),
	}
}

func (r *browsingStateCacheRecorder) Record(resp *pb.BrowsingStateResponse) {
	if r == nil || resp == nil {
		return
	}

	if len(r.value.BucketInfos) == 0 && len(resp.GetBucketInfos()) > 0 {
		r.value.BucketInfos = cloneBucketInfos(resp.GetBucketInfos())
	}
	if len(r.value.AxisBucketInfos) == 0 && len(resp.GetAxisBucketInfos()) > 0 {
		r.value.AxisBucketInfos = cloneResponseAxisBucketInfos(resp.GetAxisBucketInfos())
	}

	r.recordCell(cachedCellFromResponse(resp))
}

func (r *browsingStateCacheRecorder) Snapshot() bsCacheValue {
	if r == nil {
		return bsCacheValue{}
	}
	return canonicalizeBSCacheValue(r.value)
}

func (r *browsingStateCacheRecorder) recordCell(cell cachedBrowsingStateCell) {
	if r == nil {
		return
	}
	key := newCacheRecorderCellKey(cell)
	if index, ok := r.cellIndex[key]; ok {
		r.value.Cells[index] = cell
		return
	}
	r.cellIndex[key] = len(r.value.Cells)
	r.value.Cells = append(r.value.Cells, cell)
}

func cachedCellFromResponse(resp *pb.BrowsingStateResponse) cachedBrowsingStateCell {
	cell := cachedBrowsingStateCell{
		X:     resp.GetX(),
		Y:     resp.GetY(),
		Z:     resp.GetZ(),
		Count: resp.GetCount(),
	}
	for _, cubeObject := range resp.GetCubeObjects() {
		cell.CubeObjects = append(cell.CubeObjects, cachedCubeObject{
			ID:           cubeObject.GetId(),
			FileURI:      cubeObject.GetFileUri(),
			ThumbnailURI: cubeObject.GetThumbnailUri(),
		})
	}
	return cell
}

type cacheRecorderCellKey struct {
	X int32
	Y int32
	Z int32
}

func newCacheRecorderCellKey(cell cachedBrowsingStateCell) cacheRecorderCellKey {
	return cacheRecorderCellKey{
		X: cell.X,
		Y: cell.Y,
		Z: cell.Z,
	}
}

func cloneBSCacheValue(value bsCacheValue) bsCacheValue {
	cloned := bsCacheValue{
		BucketInfos:     cloneBucketInfos(value.BucketInfos),
		AxisBucketInfos: cloneAxisBucketInfos(value.AxisBucketInfos),
	}
	if len(value.Cells) == 0 {
		return cloned
	}

	cloned.Cells = make([]cachedBrowsingStateCell, len(value.Cells))
	for index, cell := range value.Cells {
		cloned.Cells[index] = cloneCachedBrowsingStateCell(cell)
	}
	return cloned
}

func cloneCachedBrowsingStateCell(
	cell cachedBrowsingStateCell,
) cachedBrowsingStateCell {
	cloned := cachedBrowsingStateCell{
		X:     cell.X,
		Y:     cell.Y,
		Z:     cell.Z,
		Count: cell.Count,
	}
	if len(cell.CubeObjects) == 0 {
		return cloned
	}

	cloned.CubeObjects = make([]cachedCubeObject, len(cell.CubeObjects))
	copy(cloned.CubeObjects, cell.CubeObjects)
	return cloned
}

func cloneResponseAxisBucketInfos(
	infos map[string]*pb.BucketInfoList,
) map[string][]*pb.BucketInfo {
	if len(infos) == 0 {
		return nil
	}

	cloned := make(map[string][]*pb.BucketInfo, len(infos))
	for axisKey, list := range infos {
		if list == nil {
			continue
		}
		cloned[axisKey] = cloneBucketInfos(list.GetItems())
	}
	return cloned
}

func cachedCellToResponse(cell cachedBrowsingStateCell) *pb.BrowsingStateResponse {
	resp := &pb.BrowsingStateResponse{
		X:     cell.X,
		Y:     cell.Y,
		Z:     cell.Z,
		Count: cell.Count,
	}
	if len(cell.CubeObjects) == 0 {
		return resp
	}

	resp.CubeObjects = make([]*pb.CubeObject, 0, len(cell.CubeObjects))
	for _, cubeObject := range cell.CubeObjects {
		resp.CubeObjects = append(resp.CubeObjects, &pb.CubeObject{
			Id:           cubeObject.ID,
			FileUri:      cubeObject.FileURI,
			ThumbnailUri: cubeObject.ThumbnailURI,
		})
	}
	return resp
}

func sendCachedBrowsingStateResponses(
	ctx context.Context,
	send func(*pb.BrowsingStateResponse) error,
	value bsCacheValue,
) error {
	for index, cell := range value.Cells {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp := cachedCellToResponse(cell)
		if index == 0 {
			if len(value.AxisBucketInfos) > 0 {
				resp.AxisBucketInfos = make(map[string]*pb.BucketInfoList, len(value.AxisBucketInfos))
				axisKeys := make([]string, 0, len(value.AxisBucketInfos))
				for axisKey := range value.AxisBucketInfos {
					axisKeys = append(axisKeys, axisKey)
				}
				sort.Strings(axisKeys)
				for _, axisKey := range axisKeys {
					infos := value.AxisBucketInfos[axisKey]
					resp.AxisBucketInfos[axisKey] = &pb.BucketInfoList{
						Items: cloneBucketInfos(infos),
					}
				}
			} else if len(value.BucketInfos) > 0 {
				resp.BucketInfos = cloneBucketInfos(value.BucketInfos)
			}
		}
		if err := send(resp); err != nil {
			return err
		}
	}
	return nil
}
