package main

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

type mergedVectorDimensions struct {
	Dims               []*pb.VectorSearchDimension
	BucketIDs          map[string]int32
	UseAxisBucketInfos bool
}

func mergeVectorDimensions(req *pb.GetBrowsingStateRequest) (mergedVectorDimensions, error) {
	if req == nil {
		return mergedVectorDimensions{}, nil
	}

	oldDim := req.GetVectorDimension()
	newDims := req.GetVectorDimensions()
	if oldDim != nil && len(newDims) > 0 {
		return mergedVectorDimensions{}, status.Error(
			codes.InvalidArgument,
			"vector_dimension and vector_dimensions cannot be used together",
		)
	}
	if len(newDims) > 0 {
		if req.VectorBucketId != nil {
			return mergedVectorDimensions{}, status.Error(
				codes.InvalidArgument,
				"vector_bucket_id cannot be used with vector_dimensions; use vector_bucket_ids",
			)
		}
		return mergedVectorDimensions{
			Dims:               cloneVectorDimensions(newDims),
			BucketIDs:          cloneVectorBucketIDs(req.GetVectorBucketIds()),
			UseAxisBucketInfos: true,
		}, nil
	}
	if oldDim == nil {
		if len(req.GetVectorBucketIds()) > 0 {
			return mergedVectorDimensions{}, status.Error(
				codes.InvalidArgument,
				"vector_bucket_ids requires vector_dimensions",
			)
		}
		return mergedVectorDimensions{}, nil
	}
	if len(req.GetVectorBucketIds()) > 0 {
		return mergedVectorDimensions{}, status.Error(
			codes.InvalidArgument,
			"vector_bucket_ids cannot be used with vector_dimension; use vector_bucket_id",
		)
	}

	merged := mergedVectorDimensions{
		Dims: []*pb.VectorSearchDimension{oldDim},
	}
	if req.VectorBucketId == nil {
		return merged, nil
	}

	axisKey := axisToken(oldDim.GetAxis())
	if axisKey == "" {
		return mergedVectorDimensions{}, status.Error(
			codes.InvalidArgument,
			"vector_dimension.axis must be X_AXIS, Y_AXIS, or Z_AXIS",
		)
	}
	merged.BucketIDs = map[string]int32{axisKey: req.GetVectorBucketId()}
	return merged, nil
}

func cloneVectorDimensions(dims []*pb.VectorSearchDimension) []*pb.VectorSearchDimension {
	if len(dims) == 0 {
		return nil
	}

	cloned := make([]*pb.VectorSearchDimension, len(dims))
	copy(cloned, dims)
	return cloned
}

func cloneVectorBucketIDs(bucketIDs map[string]int32) map[string]int32 {
	if len(bucketIDs) == 0 {
		return nil
	}

	cloned := make(map[string]int32, len(bucketIDs))
	for axisKey, bucketID := range bucketIDs {
		cloned[axisKey] = bucketID
	}
	return cloned
}

func validateVectorFilterAgainstDimensions(
	vectorFilter *pb.VectorFilterConfig,
	dims []*pb.VectorSearchDimension,
) error {
	if vectorFilter == nil || len(dims) == 0 {
		return nil
	}
	if err := validateVectorFilterConfig(vectorFilter); err != nil {
		return err
	}

	filterHash, err := hashVectorReference(vectorFilter.GetReference())
	if err != nil {
		return err
	}
	filterModel := normalizeVectorModelName(vectorFilter.GetModelName())
	for _, dim := range dims {
		if dim == nil {
			continue
		}
		dimHash, err := hashVectorReference(dim.GetReference())
		if err != nil {
			return err
		}
		if filterModel == normalizeVectorModelName(dim.GetModelName()) && filterHash == dimHash {
			return status.Error(
				codes.InvalidArgument,
				"vector_filter duplicates a vector_dimension; merge into vector_dimensions",
			)
		}
	}
	return nil
}

func normalizeVectorModelName(name string) string {
	return strings.TrimSpace(name)
}

func validateVectorDimensionsRequestUsage(
	plan *browsingStateRequestPlan,
	merged mergedVectorDimensions,
	allDefined bool,
	timelineDefined bool,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) error {
	dims := merged.Dims
	bucketIDs := merged.BucketIDs

	if len(dims) == 0 {
		if len(bucketIDs) > 0 {
			return status.Error(codes.InvalidArgument, "vector bucket ids require vector dimensions")
		}
		if rebucketOnly {
			return status.Error(codes.InvalidArgument, "rebucket_only requires vector dimensions")
		}
		if forcedStrategy != Auto {
			return status.Error(codes.InvalidArgument, "hybrid_strategy requires vector dimensions")
		}
		return nil
	}
	if timelineDefined {
		return status.Error(codes.InvalidArgument, "vector dimensions are not supported with timeline requests")
	}
	if len(dims) > 3 {
		return status.Error(codes.InvalidArgument, "at most 3 vector dimensions are allowed")
	}
	if rebucketOnly && allDefined {
		return status.Error(codes.InvalidArgument, "rebucket_only is not supported with all requests")
	}

	seenAxes := make(map[string]struct{}, len(dims))
	for _, dim := range dims {
		if err := validateVectorDimensionConfig(dim); err != nil {
			return err
		}
		axisKey := axisToken(dim.GetAxis())
		if axisKey == "" {
			return status.Error(codes.InvalidArgument, "vector_dimension.axis must be X_AXIS, Y_AXIS, or Z_AXIS")
		}
		if _, exists := seenAxes[axisKey]; exists {
			return status.Errorf(codes.InvalidArgument, "duplicate vector dimension axis %q", axisKey)
		}
		if plan != nil && selectAxisByToken(plan, axisKey).Type != "" {
			return status.Errorf(codes.InvalidArgument, "vector_dimension.axis conflicts with an existing %s axis", axisKey)
		}
		seenAxes[axisKey] = struct{}{}
	}

	if allDefined {
		for _, dim := range dims {
			axisKey := axisToken(dim.GetAxis())
			bucketID, ok := bucketIDs[axisKey]
			if !ok {
				return status.Errorf(
					codes.InvalidArgument,
					"vector_bucket_ids[%q] must be set when all and vector dimensions are both provided",
					axisKey,
				)
			}
			if bucketID < 0 {
				return status.Errorf(
					codes.InvalidArgument,
					"vector_bucket_ids[%q] must be >= 0",
					axisKey,
				)
			}
		}
		return nil
	}
	if len(bucketIDs) > 0 {
		return status.Error(
			codes.InvalidArgument,
			"vector bucket ids are only valid when all and vector dimensions are both provided",
		)
	}
	return nil
}

func (s *DataLoaderServer) applyVectorDimensionsToPlan(
	ctx context.Context,
	plan *browsingStateRequestPlan,
	merged mergedVectorDimensions,
	allDefined bool,
	timelineDefined bool,
	rebucketOnly bool,
	forcedStrategy HybridStrategy,
) (*browsingStateRequestPlan, error) {
	if err := validateVectorDimensionsRequestUsage(
		plan,
		merged,
		allDefined,
		timelineDefined,
		rebucketOnly,
		forcedStrategy,
	); err != nil {
		return nil, err
	}
	if len(merged.Dims) == 0 {
		return plan, nil
	}

	results, err := s.resolveMultiVectorDimensions(
		ctx,
		merged.Dims,
		plan.Filters,
		metadataAxesFromPlan(plan),
		rebucketOnly,
		forcedStrategy,
	)
	if err != nil {
		return nil, err
	}

	if allDefined {
		for index, dim := range merged.Dims {
			axisKey := axisToken(dim.GetAxis())
			plan.Filters = appendVectorObjectIDFilter(
				plan.Filters,
				lookupBucketObjectIDs(results[index].ObjectIDsByBucket, merged.BucketIDs[axisKey]),
				true,
			)
		}
		if !containsAxisOrder(plan.AxisOrder, "filter") {
			plan.AxisOrder = append(plan.AxisOrder, "filter")
		}
		plan.BucketInfos = nil
		plan.AxisBucketInfos = nil
		plan.UseAxisBucketInfos = false
		return plan, nil
	}

	if plan.AxisSubqueries == nil {
		plan.AxisSubqueries = make(map[string]string, len(merged.Dims))
	}
	for index, dim := range merged.Dims {
		axisKey := axisToken(dim.GetAxis())
		setAxisByToken(plan, axisKey, results[index].ParsedAxis)
		plan.AxisOrder = ensureAxisOrder(plan.AxisOrder, axisKey)
		plan.AxisSubqueries[axisKey] = results[index].AxisSQL
	}

	if merged.UseAxisBucketInfos {
		plan.BucketInfos = nil
		plan.AxisBucketInfos = buildAxisBucketInfos(merged.Dims, results)
		plan.UseAxisBucketInfos = true
		return plan, nil
	}

	plan.BucketInfos = cloneBucketInfos(results[0].BucketInfos)
	plan.AxisBucketInfos = nil
	plan.UseAxisBucketInfos = false
	return plan, nil
}

func buildAxisBucketInfos(
	dims []*pb.VectorSearchDimension,
	results []*vectorDimensionResult,
) map[string][]*pb.BucketInfo {
	if len(dims) == 0 || len(results) == 0 {
		return nil
	}

	axisBucketInfos := make(map[string][]*pb.BucketInfo, len(dims))
	for index, dim := range dims {
		if dim == nil || index >= len(results) || results[index] == nil {
			continue
		}
		axisBucketInfos[axisToken(dim.GetAxis())] = cloneBucketInfos(results[index].BucketInfos)
	}
	return axisBucketInfos
}

func metadataAxesFromAxes(axes ...qg.ParsedAxis) []qg.ParsedAxis {
	filtered := make([]qg.ParsedAxis, 0, len(axes))
	for _, axis := range axes {
		if strings.TrimSpace(axis.Type) == "" {
			continue
		}
		filtered = append(filtered, axis)
	}
	return filtered
}
