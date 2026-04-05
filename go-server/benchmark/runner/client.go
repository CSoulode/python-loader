package runner

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

type TimedResponse struct {
	Elapsed   time.Duration
	Timestamp time.Time
	Response  *pb.BrowsingStateResponse
}

func DialDataLoader(ctx context.Context, addr string) (pb.DataLoaderClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return pb.NewDataLoaderClient(conn), conn, nil
}

func ExecuteSSBRequest(
	ctx context.Context,
	client pb.DataLoaderClient,
	benchID string,
	req *pb.GetBrowsingStateRequest,
) ([]TimedResponse, error) {
	md := metadata.Pairs("x-bench-id", benchID)
	reqCtx := metadata.NewOutgoingContext(ctx, md)
	stream, err := client.GetBrowsingStateNonDistinctBranchesIncrementalGrouping(reqCtx, req)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	out := make([]TimedResponse, 0, 128)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if isStreamEOF(err) {
				return out, nil
			}
			return nil, fmt.Errorf("recv stream: %w", err)
		}
		now := time.Now()
		out = append(out, TimedResponse{
			Elapsed:   now.Sub(start),
			Timestamp: now,
			Response:  resp,
		})
	}
}
