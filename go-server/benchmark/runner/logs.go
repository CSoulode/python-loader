package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

const benchmarkLogPrefix = "BENCH_METRIC "

const (
	benchEventRetryInterval = 25 * time.Millisecond
	benchEventRetryTimeout  = 10 * time.Second
	benchScannerBufferSize  = 1024 * 1024
)

type BenchEvent struct {
	BenchID                string  `json:"bench_id"`
	Event                  string  `json:"event"`
	Strategy               string  `json:"strategy"`
	ForcedStrategy         string  `json:"forced_strategy"`
	SearchKind             string  `json:"search_kind"`
	ModelName              string  `json:"model_name"`
	ANNIndex               string  `json:"ann_index"`
	DistanceMetric         string  `json:"distance_metric"`
	Path                   string  `json:"path"`
	VectorSearchMS         float64 `json:"vector_search_ms"`
	BucketingMS            float64 `json:"bucketing_ms"`
	SQLExecMS              float64 `json:"sql_exec_ms"`
	EstimatedFilteredCount int64   `json:"estimated_filtered_count"`
	TotalMediaCount        int64   `json:"total_media_count"`
	CandidateCount         int     `json:"candidate_count"`
	ResultCount            int     `json:"result_count"`
	InputCount             int     `json:"input_count"`
	BatchIdx               int     `json:"batch_idx"`
	BatchSize              int     `json:"batch_size"`
	RowsRead               int64   `json:"rows_read"`
	RequestedK             int32   `json:"requested_k"`
}

func ReadBenchEvents(path string, benchID string) ([]BenchEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	events := make([]BenchEvent, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), benchScannerBufferSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.Contains(line, benchmarkLogPrefix) {
			continue
		}
		offset := strings.Index(line, benchmarkLogPrefix)
		payload := strings.TrimSpace(line[offset+len(benchmarkLogPrefix):])
		var event BenchEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			continue
		}
		if benchID != "" && event.BenchID != benchID {
			continue
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func ReadBenchEventsUntil(path string, benchID string, ready func([]BenchEvent) bool) ([]BenchEvent, error) {
	deadline := time.Now().Add(benchEventRetryTimeout)
	for {
		events, err := ReadBenchEvents(path, benchID)
		if err != nil {
			return nil, err
		}
		if ready == nil || ready(events) {
			return events, nil
		}
		if time.Now().After(deadline) {
			return events, fmt.Errorf("bench events not ready after %s: bench_id=%s events=%d", benchEventRetryTimeout, benchID, len(events))
		}
		time.Sleep(benchEventRetryInterval)
	}
}
