package runner

import (
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	pb "m3.dataloader/dataloader"
)

type StreamSummary struct {
	TTFB           float64
	TTLB           float64
	VectorSearchMS float64
	BucketingMS    float64
	SQLExecMS      float64
	Strategy       string
}

type CellState struct {
	Count int32
}

func BuildCellSeries(items []TimedResponse) ([]map[string]CellState, []time.Duration) {
	states := make([]map[string]CellState, 0, len(items))
	elapsed := make([]time.Duration, 0, len(items))
	current := make(map[string]CellState)
	for _, item := range items {
		key := cellKeyString(item.Response)
		current[key] = CellState{Count: item.Response.GetCount()}
		states = append(states, cloneCellStateMap(current))
		elapsed = append(elapsed, item.Elapsed)
	}
	return states, elapsed
}

func SummarizeStream(items []TimedResponse, events []BenchEvent) StreamSummary {
	summary := StreamSummary{}
	if len(items) > 0 {
		summary.TTFB = durationToMS(items[0].Elapsed)
		summary.TTLB = durationToMS(items[len(items)-1].Elapsed)
	}
	for _, event := range events {
		switch event.Event {
		case "strategy_selected":
			summary.Strategy = event.Strategy
		case "vector_search_done":
			summary.VectorSearchMS += event.VectorSearchMS
		case "bucketing_done":
			summary.BucketingMS += event.BucketingMS
		case "sql_exec_done":
			summary.SQLExecMS += event.SQLExecMS
		}
	}
	return summary
}

func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cloned := append([]float64(nil), values...)
	sort.Float64s(cloned)
	mid := len(cloned) / 2
	if len(cloned)%2 == 1 {
		return cloned[mid]
	}
	return (cloned[mid-1] + cloned[mid]) / 2
}

func durationToMS(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000.0
}

func cellKeyString(resp *pb.BrowsingStateResponse) string {
	return keyFromXYZ(resp.GetX(), resp.GetY(), resp.GetZ())
}

func keyFromXYZ(x int32, y int32, z int32) string {
	return fmtInt32(x) + "|" + fmtInt32(y) + "|" + fmtInt32(z)
}

func cloneCellStateMap(source map[string]CellState) map[string]CellState {
	cloned := make(map[string]CellState, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func isStreamEOF(err error) bool {
	return err == io.EOF
}

func fmtInt32(value int32) string {
	return strconv.FormatInt(int64(value), 10)
}

func almostZero(value float64) bool {
	return math.Abs(value) < 1e-12
}
