package runner

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	result, err := ExecuteSSBRequestWithOptions(ctx, client, req, ExecuteRequestOptions{
		BenchID: benchID,
	})
	if err != nil {
		return nil, err
	}
	return result.Responses, nil
}
