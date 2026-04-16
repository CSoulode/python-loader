package main

import (
	"context"

	pb "m3.dataloader/dataloader"
)

type recordingBrowsingStateStream struct {
	browsingStateResponseStream
	recorder     *browsingStateCacheRecorder
	passthrough  bool
	originalSend func(*pb.BrowsingStateResponse) error
}

func newRecordingBrowsingStateStream(
	stream browsingStateResponseStream,
	recorder *browsingStateCacheRecorder,
	passthrough bool,
) *recordingBrowsingStateStream {
	return &recordingBrowsingStateStream{
		browsingStateResponseStream: stream,
		recorder:                    recorder,
		passthrough:                 passthrough,
		originalSend:                stream.Send,
	}
}

func (s *recordingBrowsingStateStream) Send(resp *pb.BrowsingStateResponse) error {
	s.recorder.Record(resp)
	if s.passthrough {
		return s.originalSend(resp)
	}
	return nil
}

func (s *DataLoaderServer) finalizeBrowsingStateGRPCResponse(
	ctx context.Context,
	stream browsingStateResponseStream,
	cacheWrite *bsCacheWrite,
	recorder *browsingStateCacheRecorder,
	ifNoneMatch string,
	retErr error,
) error {
	if retErr != nil || cacheWrite == nil || recorder == nil {
		return retErr
	}

	entry := s.writeBrowsingStateCacheEntry(cacheWrite.Key, recorder.Snapshot(), cacheWrite)
	if matchIfNoneMatch(ifNoneMatch, entry.ETag) {
		s.ensureBrowsingStateCache().RecordGRPCNotModified()
		return sendBrowsingStateGRPCNotModified(stream, entry.ETag)
	}
	return sendBrowsingStateGRPCEntry(ctx, stream, entry)
}
