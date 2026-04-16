package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type BrowsingStateCacheStats struct {
	Entries           int   `json:"entries"`
	ApproxMemoryBytes int64 `json:"approx_memory_bytes"`
	Hits              int64 `json:"hits"`
	Misses            int64 `json:"misses"`
	GRPCNotModified   int64 `json:"grpc_not_modified"`
}

type browsingStateInvalidateResponse struct {
	Invalidated int `json:"invalidated"`
}

type debugVarsResponse struct {
	BrowsingStateCache BrowsingStateCacheStats `json:"browsing_state_cache"`
}

func ReadBrowsingStateCacheStats(
	ctx context.Context,
	httpPort int,
) (BrowsingStateCacheStats, error) {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/debug/vars", httpPort)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return BrowsingStateCacheStats{}, err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return BrowsingStateCacheStats{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return BrowsingStateCacheStats{}, fmt.Errorf("debug vars status = %d", response.StatusCode)
	}

	var payload debugVarsResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return BrowsingStateCacheStats{}, err
	}
	return payload.BrowsingStateCache, nil
}

func InvalidateBrowsingStateCache(
	ctx context.Context,
	httpPort int,
	scope string,
	kind string,
	key string,
) (int, error) {
	body, err := json.Marshal(map[string]string{
		"scope": scope,
		"kind":  kind,
		"key":   key,
	})
	if err != nil {
		return 0, err
	}

	endpoint := fmt.Sprintf(
		"http://127.0.0.1:%d/debug/cache/browsing-state/invalidate",
		httpPort,
	)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("invalidate status = %d", response.StatusCode)
	}

	var payload browsingStateInvalidateResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.Invalidated, nil
}
