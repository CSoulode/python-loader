package runner

import (
	"context"
	"fmt"
	"strings"
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

type measuredStreamConfig struct {
	Dataset       DatasetID
	Experiment    ExperimentID
	CaseID        string
	RunIndex      int
	Request       *pb.GetBrowsingStateRequest
	RequireEvents bool
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
	return executeMeasuredStreamWithConfig(ctx, session, measuredStreamConfig{
		Dataset:       dataset,
		Experiment:    experiment,
		CaseID:        caseID,
		RunIndex:      runIndex,
		Request:       req,
		RequireEvents: true,
	})
}

func executeMeasuredStreamAllowMissingEvents(
	ctx context.Context,
	session *ActiveSession,
	dataset DatasetID,
	experiment ExperimentID,
	caseID string,
	runIndex int,
	req *pb.GetBrowsingStateRequest,
) (*MeasuredStream, error) {
	return executeMeasuredStreamWithConfig(ctx, session, measuredStreamConfig{
		Dataset:       dataset,
		Experiment:    experiment,
		CaseID:        caseID,
		RunIndex:      runIndex,
		Request:       req,
		RequireEvents: false,
	})
}

func executeMeasuredStreamWithConfig(
	ctx context.Context,
	session *ActiveSession,
	config measuredStreamConfig,
) (*MeasuredStream, error) {
	benchID := benchRunID(config.Dataset, config.Experiment, config.CaseID, config.RunIndex)
	items, err := ExecuteSSBRequest(ctx, session.DataLoader, benchID, config.Request)
	if err != nil {
		return nil, err
	}
	events, err := readMeasuredStreamEvents(session.Runtime.ServerLogPath, benchID, len(items), config.RequireEvents)
	if err != nil {
		return nil, err
	}
	return &MeasuredStream{
		Summary: SummarizeStream(items, events),
		Items:   items,
		Events:  events,
	}, nil
}

func readMeasuredStreamEvents(path string, benchID string, itemCount int, required bool) ([]BenchEvent, error) {
	if !required {
		return ReadBenchEvents(path, benchID)
	}
	return ReadBenchEventsUntil(path, benchID, func(events []BenchEvent) bool {
		return benchEventsReady(events, itemCount)
	})
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
		if err := invalidateBrowsingStateCacheAll(ctx, session); err != nil {
			return StreamSummary{}, err
		}
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

func benchEventsReady(events []BenchEvent, expectedResponses int) bool {
	if len(events) == 0 {
		return false
	}
	if expectedResponses <= 0 {
		return true
	}
	return streamFlushCoverage(events) >= expectedResponses
}

func streamFlushCoverage(events []BenchEvent) int {
	total := 0
	for _, event := range events {
		if event.Event != "stream_flush" {
			continue
		}
		total += event.BatchSize
	}
	return total
}

func normalizeSelectedStrategy(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "post_filter", "range_post":
		return "post_filter"
	case "pre_filter", "range_pre":
		return "pre_filter"
	case "hybrid", "range_hybrid":
		return "hybrid"
	case "auto":
		return "auto"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func hybridStrategyLabel(strategy pb.HybridStrategy) string {
	switch strategy {
	case pb.HybridStrategy_POST_FILTER:
		return "post_filter"
	case pb.HybridStrategy_PRE_FILTER:
		return "pre_filter"
	case pb.HybridStrategy_HYBRID:
		return "hybrid"
	case pb.HybridStrategy_AUTO:
		return "auto"
	default:
		return strings.ToLower(strategy.String())
	}
}

func resolvedSelectedStrategy(selected string, forced pb.HybridStrategy) string {
	if normalized := normalizeSelectedStrategy(selected); normalized != "" {
		return normalized
	}
	if forced == pb.HybridStrategy_AUTO {
		return ""
	}
	return hybridStrategyLabel(forced)
}

func invalidateBrowsingStateCacheAll(ctx context.Context, session *ActiveSession) error {
	if session == nil || session.Runtime == nil {
		return nil
	}
	_, err := InvalidateBrowsingStateCache(ctx, session.Runtime.Ports.ServerHTTP, "all", "", "")
	return err
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 2*time.Minute)
}
