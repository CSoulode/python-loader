package main

import pb "m3.dataloader/dataloader"

const (
	approxInt32Bytes     int64 = 4
	approxInt64Bytes     int64 = 8
	approxSliceOverhead  int64 = 24
	approxStringOverhead int64 = 16
)

func approximateBSCacheEntryBytes(entry *bsCacheEntry) int64 {
	if entry == nil {
		return 0
	}

	return approximateBSCacheKeyBytes(entry.Key) +
		approximateBSCacheValueBytes(entry.Value) +
		approximateStringBytes(entry.ETag) +
		approximateDependenciesBytes(entry.Dependencies) +
		(2 * approxInt64Bytes)
}

func approximateBSCacheKeyBytes(key bsCacheStorageKey) int64 {
	return approximateStringBytes(key.Namespace) + approximateStringBytes(key.Canonical)
}

func approximateBSCacheValueBytes(value bsCacheValue) int64 {
	total := approxSliceOverhead
	for _, cell := range value.Cells {
		total += approximateCachedCellBytes(cell)
	}

	total += approxSliceOverhead
	for _, info := range value.BucketInfos {
		total += approximateBucketInfoBytes(info)
	}

	total += approxSliceOverhead
	for axis, infos := range value.AxisBucketInfos {
		total += approximateStringBytes(axis) + approxSliceOverhead
		for _, info := range infos {
			total += approximateBucketInfoBytes(info)
		}
	}
	return total
}

func approximateCachedCellBytes(cell cachedBrowsingStateCell) int64 {
	total := 4 * approxInt32Bytes
	total += approxSliceOverhead
	for _, object := range cell.CubeObjects {
		total += approximateCubeObjectBytes(object)
	}
	return total
}

func approximateCubeObjectBytes(object cachedCubeObject) int64 {
	return approxInt32Bytes +
		approximateStringBytes(object.FileURI) +
		approximateStringBytes(object.ThumbnailURI)
}

func approximateBucketInfoBytes(info *pb.BucketInfo) int64 {
	if info == nil {
		return 0
	}

	return (3 * approxInt32Bytes) + approximateStringBytes(info.GetLabel())
}

func approximateDependenciesBytes(dependencies []bsCacheDependency) int64 {
	total := approxSliceOverhead
	for _, dependency := range dependencies {
		total += approximateStringBytes(dependency.Kind) +
			approximateStringBytes(dependency.Key)
	}
	return total
}

func approximateStringBytes(value string) int64 {
	return approxStringOverhead + int64(len(value))
}
