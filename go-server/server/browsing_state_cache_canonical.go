package main

import (
	"sort"

	pb "m3.dataloader/dataloader"
)

func canonicalizeBSCacheValue(value bsCacheValue) bsCacheValue {
	canonical := cloneBSCacheValue(value)
	sortCachedBrowsingStateCells(canonical.Cells)
	sortBucketInfosStable(canonical.BucketInfos)
	for axisKey, infos := range canonical.AxisBucketInfos {
		sortBucketInfosStable(infos)
		canonical.AxisBucketInfos[axisKey] = infos
	}
	return canonical
}

func sortCachedBrowsingStateCells(cells []cachedBrowsingStateCell) {
	for index := range cells {
		sortCachedCubeObjects(cells[index].CubeObjects)
	}
	sort.Slice(cells, func(i int, j int) bool {
		left := cells[i]
		right := cells[j]
		if left.X != right.X {
			return left.X < right.X
		}
		if left.Y != right.Y {
			return left.Y < right.Y
		}
		if left.Z != right.Z {
			return left.Z < right.Z
		}
		if left.Count != right.Count {
			return left.Count < right.Count
		}
		return compareCachedCubeObjects(left.CubeObjects, right.CubeObjects) < 0
	})
}

func sortCachedCubeObjects(objects []cachedCubeObject) {
	sort.Slice(objects, func(i int, j int) bool {
		left := objects[i]
		right := objects[j]
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		if left.FileURI != right.FileURI {
			return left.FileURI < right.FileURI
		}
		return left.ThumbnailURI < right.ThumbnailURI
	})
}

func compareCachedCubeObjects(left []cachedCubeObject, right []cachedCubeObject) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index].ID != right[index].ID {
			if left[index].ID < right[index].ID {
				return -1
			}
			return 1
		}
		if left[index].FileURI != right[index].FileURI {
			if left[index].FileURI < right[index].FileURI {
				return -1
			}
			return 1
		}
		if left[index].ThumbnailURI != right[index].ThumbnailURI {
			if left[index].ThumbnailURI < right[index].ThumbnailURI {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	default:
		return 0
	}
}

func sortBucketInfosStable(infos []*pb.BucketInfo) {
	sort.Slice(infos, func(i int, j int) bool {
		left := infos[i]
		right := infos[j]
		if left == nil || right == nil {
			return left == nil && right != nil
		}
		if left.GetBucketId() != right.GetBucketId() {
			return left.GetBucketId() < right.GetBucketId()
		}
		if left.GetLowerBound() != right.GetLowerBound() {
			return left.GetLowerBound() < right.GetLowerBound()
		}
		if left.GetUpperBound() != right.GetUpperBound() {
			return left.GetUpperBound() < right.GetUpperBound()
		}
		return left.GetLabel() < right.GetLabel()
	})
}
