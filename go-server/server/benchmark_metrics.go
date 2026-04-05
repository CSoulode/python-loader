package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
)

const benchmarkLogPrefix = "BENCH_METRIC "

func benchmarkIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("x-bench-id")
	if len(values) == 0 {
		values = md.Get("bench-id")
	}
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func logBenchmarkEvent(ctx context.Context, event string, fields map[string]any) {
	benchID := benchmarkIDFromContext(ctx)
	if benchID == "" || strings.TrimSpace(event) == "" {
		return
	}

	payload := map[string]any{
		"bench_id":     benchID,
		"event":        event,
		"ts_unix_nano": time.Now().UnixNano(),
	}
	for key, value := range fields {
		payload[key] = value
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("%s{\"bench_id\":%q,\"event\":\"marshal_error\",\"error\":%q}", benchmarkLogPrefix, benchID, err.Error())
		return
	}
	log.Printf("%s%s", benchmarkLogPrefix, data)
}

func durationMillis(start time.Time) float64 {
	if start.IsZero() {
		return 0
	}
	return float64(time.Since(start).Microseconds()) / 1000.0
}
