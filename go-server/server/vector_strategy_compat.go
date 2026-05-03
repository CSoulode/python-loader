package main

import (
	"net/url"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func compatHybridStrategyQueryValue(query url.Values) string {
	if value := strings.TrimSpace(query.Get("hybridStrategy")); value != "" {
		return value
	}
	return query.Get("hybrid_strategy")
}

func parseCompatHybridStrategy(raw string) (HybridStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "auto":
		return Auto, nil
	case "post_filter", "post-filter", "postfilter":
		return PostFilter, nil
	case "pre_filter", "pre-filter", "prefilter":
		return PreFilter, nil
	case "hybrid":
		return Hybrid, nil
	default:
		return Auto, status.Error(codes.InvalidArgument, "invalid hybrid_strategy")
	}
}
