package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"google.golang.org/protobuf/proto"
)

func computeMerkleETag(parent *StateNode, delta TransitionDelta, grid *bsCellGrid) string {
	hasher := sha256.New()
	if parent != nil && parent.ETag != "" {
		_, _ = hasher.Write([]byte(parent.ETag))
	}
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(delta.Kind.String()))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(delta.Payload))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(hashCellGrid(grid)))
	return quoteETag(hex.EncodeToString(hasher.Sum(nil)))
}

func hashCellGrid(grid *bsCellGrid) string {
	hasher := sha256.New()
	if grid != nil && len(grid.HTTPBody) > 0 {
		_, _ = hasher.Write([]byte("http"))
		_, _ = hasher.Write(grid.HTTPBody)
	}
	if grid != nil && len(grid.Responses) > 0 {
		_, _ = hasher.Write([]byte("grpc"))
		for _, resp := range grid.Responses {
			payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(resp)
			_, _ = hasher.Write(payload)
		}
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func etagMatchesAny(etag string, candidates []string) bool {
	if normalizeETag(etag) == "" {
		return false
	}
	for _, candidate := range candidates {
		if normalizeETag(candidate) == normalizeETag(etag) {
			return true
		}
	}
	return false
}

func normalizeETag(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "W/")
	return strings.Trim(trimmed, "\"")
}

func quoteETag(value string) string {
	trimmed := normalizeETag(value)
	if trimmed == "" {
		return ""
	}
	return "\"" + trimmed + "\""
}
