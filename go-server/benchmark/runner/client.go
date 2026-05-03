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

type BrowsingState2Result struct {
	Elapsed      time.Duration
	Responses    []*pb.BrowsingStateResponse
	Header       metadata.MD
	Trailer      metadata.MD
	PayloadBytes int
}

func ExecuteBrowsingState2Request(
	ctx context.Context,
	client pb.DataLoaderClient,
	benchID string,
	req *pb.GetBrowsingStateRequest,
	md metadata.MD,
) (*BrowsingState2Result, error) {
	outgoing := metadata.Join(metadata.Pairs("x-bench-id", benchID), md)
	reqCtx := metadata.NewOutgoingContext(ctx, outgoing)
	var header metadata.MD
	var trailer metadata.MD
	start := time.Now()
	stream, err := client.GetBrowsingState2(reqCtx, req, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, err
	}
	responses, payloadBytes, err := collectBrowsingState2Responses(stream)
	if err != nil {
		return nil, err
	}
	return &BrowsingState2Result{
		Elapsed:      time.Since(start),
		Responses:    responses,
		Header:       header,
		Trailer:      trailer,
		PayloadBytes: payloadBytes,
	}, nil
}

func collectBrowsingState2Responses(stream pb.DataLoader_GetBrowsingState2Client) ([]*pb.BrowsingStateResponse, int, error) {
	responses := make([]*pb.BrowsingStateResponse, 0, 64)
	payloadBytes := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			if isStreamEOF(err) {
				return responses, payloadBytes, nil
			}
			return nil, 0, fmt.Errorf("recv GetBrowsingState2: %w", err)
		}
		responses = append(responses, resp)
		payloadBytes += len(resp.String())
	}
}
