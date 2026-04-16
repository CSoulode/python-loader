package main

import (
	"os"
	"strconv"
	"time"
)

const browsingStateCacheTTLMSEnv = "BROWSING_STATE_CACHE_TTL_MS"

func newConfiguredBrowsingStateCache() *BrowsingStateCache {
	return newBrowsingStateCache(
		defaultBrowsingStateCacheMaxEntries,
		browsingStateCacheTTLFromEnv(),
	)
}

func browsingStateCacheTTLFromEnv() time.Duration {
	value := os.Getenv(browsingStateCacheTTLMSEnv)
	if value == "" {
		return defaultBrowsingStateCacheTTL
	}

	ttlMS, err := strconv.Atoi(value)
	if err != nil || ttlMS <= 0 {
		return defaultBrowsingStateCacheTTL
	}
	return time.Duration(ttlMS) * time.Millisecond
}
