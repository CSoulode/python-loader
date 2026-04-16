package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "m3.dataloader/dataloader"
)

func TestReadBenchEventsUntilWaitsForFlush(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "bench.log")
	if err := os.WriteFile(logPath, []byte(""), 0o644); err != nil {
		t.Fatalf("write initial log: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		defer file.Close()
		_, _ = file.WriteString("2026/04/15 00:00:00 BENCH_METRIC {\"bench_id\":\"bench-1\",\"event\":\"strategy_selected\",\"strategy\":\"hybrid\"}\n")
		time.Sleep(50 * time.Millisecond)
		_, _ = file.WriteString("2026/04/15 00:00:00 BENCH_METRIC {\"bench_id\":\"bench-1\",\"event\":\"stream_flush\",\"batch_idx\":1,\"batch_size\":10}\n")
		time.Sleep(50 * time.Millisecond)
		_, _ = file.WriteString("2026/04/15 00:00:00 BENCH_METRIC {\"bench_id\":\"bench-1\",\"event\":\"stream_flush\",\"batch_idx\":2,\"batch_size\":5}\n")
	}()

	start := time.Now()
	events, err := ReadBenchEventsUntil(logPath, "bench-1", func(events []BenchEvent) bool {
		return benchEventsReady(events, 15)
	})
	if err != nil {
		t.Fatalf("ReadBenchEventsUntil: %v", err)
	}
	if time.Since(start) < 140*time.Millisecond {
		t.Fatalf("ReadBenchEventsUntil returned before the final flush was appended")
	}
	if len(events) != 3 {
		t.Fatalf("event count = %d, want 3", len(events))
	}
	if events[0].Event != "strategy_selected" || events[1].Event != "stream_flush" || events[2].Event != "stream_flush" {
		t.Fatalf("unexpected events: %+v", events)
	}
	if got := streamFlushCoverage(events); got != 15 {
		t.Fatalf("streamFlushCoverage = %d, want 15", got)
	}
}

func TestBenchEventsReadyRequiresFullStreamCoverage(t *testing.T) {
	events := []BenchEvent{
		{Event: "strategy_selected"},
		{Event: "stream_flush", BatchSize: 10},
	}
	if benchEventsReady(events, 15) {
		t.Fatalf("benchEventsReady returned true before full flush coverage")
	}
	events = append(events, BenchEvent{Event: "stream_flush", BatchSize: 5})
	if !benchEventsReady(events, 15) {
		t.Fatalf("benchEventsReady returned false after full flush coverage")
	}
	if !benchEventsReady(events, 0) {
		t.Fatalf("benchEventsReady returned false for zero-response readiness")
	}
}

func TestReadMeasuredStreamEventsAllowsMissingOptionalEvents(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "bench.log")
	if err := os.WriteFile(logPath, []byte(""), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	events, err := readMeasuredStreamEvents(logPath, "bench-missing", 10, false)
	if err != nil {
		t.Fatalf("readMeasuredStreamEvents optional: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("optional event count = %d, want 0", len(events))
	}
}

func TestResolvedSelectedStrategyFallsBackToForcedValue(t *testing.T) {
	if got := resolvedSelectedStrategy("", pb.HybridStrategy_PRE_FILTER); got != "pre_filter" {
		t.Fatalf("pre-filter fallback = %q", got)
	}
	if got := resolvedSelectedStrategy("range_hybrid", pb.HybridStrategy_AUTO); got != "hybrid" {
		t.Fatalf("normalized strategy = %q", got)
	}
	if got := resolvedSelectedStrategy("", pb.HybridStrategy_AUTO); got != "" {
		t.Fatalf("auto blank fallback = %q, want empty", got)
	}
}
