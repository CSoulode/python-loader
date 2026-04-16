package runner

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
)

const (
	grpcIfNoneMatchKey   = "if-none-match"
	grpcNotModifiedKey   = "bs-not-modified"
	grpcNotModifiedValue = "true"
)

type ExecuteRequestOptions struct {
	BenchID     string
	IfNoneMatch string
}

type ExecutedSSBRequest struct {
	Headers      metadata.MD
	Responses    []TimedResponse
	PayloadBytes int
	NotModified  bool
	EndToEnd     time.Duration
}

func ExecuteSSBRequestWithOptions(
	ctx context.Context,
	client pb.DataLoaderClient,
	req *pb.GetBrowsingStateRequest,
	options ExecuteRequestOptions,
) (*ExecutedSSBRequest, error) {
	reqCtx := metadata.NewOutgoingContext(ctx, buildExecuteRequestMetadata(options))
	stream, err := client.GetBrowsingStateNonDistinctBranchesIncrementalGrouping(reqCtx, req)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	headers, err := stream.Header()
	if err != nil {
		return nil, fmt.Errorf("stream header: %w", err)
	}

	responses := make([]TimedResponse, 0, 128)
	payloadBytes := 0
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				return &ExecutedSSBRequest{
					Headers:      headers,
					Responses:    responses,
					PayloadBytes: payloadBytes,
					NotModified:  isGRPCNotModified(headers),
					EndToEnd:     time.Since(start),
				}, nil
			}
			return nil, fmt.Errorf("recv stream: %w", recvErr)
		}

		now := time.Now()
		payloadBytes += proto.Size(resp)
		responses = append(responses, TimedResponse{
			Elapsed:   now.Sub(start),
			Timestamp: now,
			Response:  resp,
		})
	}
}

func buildExecuteRequestMetadata(options ExecuteRequestOptions) metadata.MD {
	pairs := make([]string, 0, 4)
	if options.BenchID != "" {
		pairs = append(pairs, "x-bench-id", options.BenchID)
	}
	if options.IfNoneMatch != "" {
		pairs = append(pairs, grpcIfNoneMatchKey, options.IfNoneMatch)
	}
	return metadata.Pairs(pairs...)
}

func isGRPCNotModified(headers metadata.MD) bool {
	values := headers.Get(grpcNotModifiedKey)
	return len(values) == 1 && values[0] == grpcNotModifiedValue
}
