package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"net/http"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "m3.dataloader/dataloader"
)

const (
	bsGRPCIfNoneMatchKey   = "if-none-match"
	bsGRPCETagKey          = "etag"
	bsGRPCNotModifiedKey   = "bs-not-modified"
	bsHTTPIfNoneMatchKey   = "If-None-Match"
	bsHTTPETagHeaderKey    = "ETag"
	bsWeakETagPrefix       = "W/"
	bsETagPrefix           = "bs-"
	bsNotModifiedTrueValue = "true"
)

type browsingStateResponseStream interface {
	grpc.ServerStream
	Send(*pb.BrowsingStateResponse) error
}

func computeBSETag(value bsCacheValue) string {
	canonical := canonicalizeBSCacheValue(value)
	return computeCanonicalBSETag(canonical)
}

func computeCanonicalBSETag(value bsCacheValue) string {
	hasher := sha256.New()
	writeBSCacheValueHash(hasher, value)
	return bsETagPrefix + hex.EncodeToString(hasher.Sum(nil))
}

func writeBSCacheValueHash(hasher hash.Hash, value bsCacheValue) {
	fmt.Fprintf(hasher, "cells:%d\n", len(value.Cells))
	for _, cell := range value.Cells {
		fmt.Fprintf(hasher, "cell:%d:%d:%d:%d:%d\n", cell.X, cell.Y, cell.Z, cell.Count, len(cell.CubeObjects))
		for _, cubeObject := range cell.CubeObjects {
			fmt.Fprintf(
				hasher,
				"obj:%d:%s:%s\n",
				cubeObject.ID,
				cubeObject.FileURI,
				cubeObject.ThumbnailURI,
			)
		}
	}

	fmt.Fprintf(hasher, "bucket_infos:%d\n", len(value.BucketInfos))
	for _, info := range value.BucketInfos {
		writeBucketInfoHash(hasher, info)
	}

	axisKeys := make([]string, 0, len(value.AxisBucketInfos))
	for axisKey := range value.AxisBucketInfos {
		axisKeys = append(axisKeys, axisKey)
	}
	sort.Strings(axisKeys)
	fmt.Fprintf(hasher, "axis_bucket_infos:%d\n", len(axisKeys))
	for _, axisKey := range axisKeys {
		fmt.Fprintf(hasher, "axis:%s:%d\n", axisKey, len(value.AxisBucketInfos[axisKey]))
		for _, info := range value.AxisBucketInfos[axisKey] {
			writeBucketInfoHash(hasher, info)
		}
	}
}

func writeBucketInfoHash(hasher hash.Hash, info *pb.BucketInfo) {
	if info == nil {
		fmt.Fprintln(hasher, "bucket:nil")
		return
	}
	fmt.Fprintf(
		hasher,
		"bucket:%d:%f:%f:%s\n",
		info.GetBucketId(),
		info.GetLowerBound(),
		info.GetUpperBound(),
		info.GetLabel(),
	)
}

func quoteBSETag(etag string) string {
	if strings.TrimSpace(etag) == "" {
		return ""
	}
	return `"` + etag + `"`
}

func matchIfNoneMatch(raw string, etag string) bool {
	target := canonicalizeETagToken(etag)
	if target == "" {
		return false
	}
	for _, token := range strings.Split(raw, ",") {
		candidate := canonicalizeETagToken(token)
		if candidate == "*" || candidate == target {
			return true
		}
	}
	return false
}

func canonicalizeETagToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, bsWeakETagPrefix) {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, bsWeakETagPrefix))
	}
	return strings.Trim(trimmed, `"`)
}

func getGRPCIfNoneMatch(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	return strings.Join(md.Get(bsGRPCIfNoneMatchKey), ",")
}

func writeBSETagHeader(w http.ResponseWriter, etag string) {
	if quoted := quoteBSETag(etag); quoted != "" {
		w.Header().Set(bsHTTPETagHeaderKey, quoted)
	}
}

func sendBrowsingStateGRPCEntry(
	ctx context.Context,
	stream browsingStateResponseStream,
	entry bsCacheEntrySnapshot,
) error {
	if err := stream.SendHeader(metadata.Pairs(bsGRPCETagKey, entry.ETag)); err != nil {
		return err
	}
	return sendCachedBrowsingStateResponses(ctx, stream.Send, entry.Value)
}

func sendBrowsingStateGRPCNotModified(
	stream browsingStateResponseStream,
	etag string,
) error {
	return stream.SendHeader(metadata.Pairs(
		bsGRPCETagKey,
		etag,
		bsGRPCNotModifiedKey,
		bsNotModifiedTrueValue,
	))
}
