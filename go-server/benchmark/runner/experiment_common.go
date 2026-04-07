package runner

import (
	"context"
	"fmt"
	"time"

	pb "m3.dataloader/dataloader"
)

const (
	warmupRuns   = 1
	measuredRuns = 5
)

var (
	fullSelectivities        = []float64{0.01, 0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 1.0}
	filteredSelectivities    = []float64{0.01, 0.05, 0.10, 0.20, 0.30, 0.40, 0.50}
	mixedTargetSelectivities = []float64{0.05, 0.10, 0.30}
	defaultKValues           = []int32{100, 500, 1000, 5000}
)

const exp2RepeatCount = 2

type MeasuredStream struct {
	Summary StreamSummary
	Items   []TimedResponse
	Events  []BenchEvent
}

func executeMeasuredStream(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	caseID string,
	runIndex int,
	req *pb.GetBrowsingStateRequest,
) (*MeasuredStream, error) {
	benchID := benchRunID(dataset, experiment, caseID, runIndex)
	items, err := ExecuteSSBRequest(ctx, session.DataLoader, benchID, req)
	if err != nil {
		return nil, err
	}
	events, err := ReadBenchEvents(session.Runtime.ServerLogPath, benchID)
	if err != nil {
		return nil, err
	}
	return &MeasuredStream{
		Summary: SummarizeStream(items, events),
		Items:   items,
		Events:  events,
	}, nil
}

func streamMedian(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	caseID string,
	reqFactory func() *pb.GetBrowsingStateRequest,
) (StreamSummary, error) {
	ttfb := make([]float64, 0, measuredRuns)
	ttlb := make([]float64, 0, measuredRuns)
	vectorMS := make([]float64, 0, measuredRuns)
	sqlMS := make([]float64, 0, measuredRuns)
	var strategy string

	for runIndex := 0; runIndex < warmupRuns+measuredRuns; runIndex++ {
		result, err := executeMeasuredStream(ctx, session, dataset, experiment, caseID, runIndex, reqFactory())
		if err != nil {
			return StreamSummary{}, err
		}
		if runIndex < warmupRuns {
			continue
		}
		strategy = result.Summary.Strategy
		ttfb = append(ttfb, result.Summary.TTFB)
		ttlb = append(ttlb, result.Summary.TTLB)
		vectorMS = append(vectorMS, result.Summary.VectorSearchMS)
		sqlMS = append(sqlMS, result.Summary.SQLExecMS)
	}

	return StreamSummary{
		TTFB:           Median(ttfb),
		TTLB:           Median(ttlb),
		VectorSearchMS: Median(vectorMS),
		SQLExecMS:      Median(sqlMS),
		Strategy:       strategy,
	}, nil
}

func benchRunID(dataset DatasetID, experiment ExperimentID, caseID string, runIndex int) string {
	return fmt.Sprintf("%s-%s-%s-%d-%d", dataset, experiment, caseID, runIndex, time.Now().UnixNano())
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 2*time.Minute)
}
