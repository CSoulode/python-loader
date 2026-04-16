package main

import pb "m3.dataloader/dataloader"

func (s *DataLoaderServer) lookupBrowsingStateCache(
	req *pb.GetBrowsingStateRequest,
	namespace string,
) (*preparedBrowsingStateRequest, bsCacheStorageKey, bsCacheEntrySnapshot, bool, error) {
	prepared, err := prepareBrowsingStateRequest(req)
	if err != nil {
		return nil, bsCacheStorageKey{}, bsCacheEntrySnapshot{}, false, err
	}
	if !shouldCachePreparedBrowsingState(prepared) {
		return prepared, bsCacheStorageKey{}, bsCacheEntrySnapshot{}, false, nil
	}

	key, err := computeBSCacheKey(namespace, prepared)
	if err != nil {
		return nil, bsCacheStorageKey{}, bsCacheEntrySnapshot{}, false, err
	}
	entry, ok := s.ensureBrowsingStateCache().TryGetEntry(key)
	return prepared, key, entry, ok, nil
}
