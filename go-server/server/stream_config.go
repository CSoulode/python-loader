package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

const defaultStreamBatchSize = 10240

var streamBatchSize = envIntOrDefault("STREAM_BATCH_SIZE", defaultStreamBatchSize)

func envIntOrDefault(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("environment variable %s must be an integer, got: %s", key, value)
	}
	return parsed
}
