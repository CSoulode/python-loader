package main

import (
	"context"
	"strings"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

type preparedBrowsingStateRequest struct {
	AxisOrder        []string
	AxisX            qg.ParsedAxis
	AxisY            qg.ParsedAxis
	AxisZ            qg.ParsedAxis
	Filters          []qg.ParsedFilter
	VectorFilter     *pb.VectorFilterConfig
	MergedVectorDims mergedVectorDimensions
	ForcedStrategy   HybridStrategy
	AllDefined       bool
	TimelineDefined  bool
	RebucketOnly     bool
}

func prepareBrowsingStateRequest(
	req *pb.GetBrowsingStateRequest,
) (*preparedBrowsingStateRequest, error) {
	if err := rejectAxisLevelVectorFilters(req.GetFilters()); err != nil {
		return nil, err
	}

	forcedStrategy, err := strategyFromProto(req.GetHybridStrategy())
	if err != nil {
		return nil, err
	}

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if err != nil {
		return nil, err
	}
	merged, err := mergeVectorDimensions(req)
	if err != nil {
		return nil, err
	}
	if err := validateVectorFilterAgainstDimensions(req.GetVectorFilter(), merged.Dims); err != nil {
		return nil, err
	}
	if err := validateVectorFilterConfig(req.GetVectorFilter()); err != nil {
		return nil, err
	}

	prepared := &preparedBrowsingStateRequest{
		AxisOrder:        axisOrder,
		AxisX:            axisX,
		AxisY:            axisY,
		AxisZ:            axisZ,
		Filters:          filters,
		VectorFilter:     req.GetVectorFilter(),
		MergedVectorDims: merged,
		ForcedStrategy:   forcedStrategy,
		AllDefined:       strings.TrimSpace(req.GetAll()) != "",
		TimelineDefined:  strings.TrimSpace(req.GetTimeline()) != "",
		RebucketOnly:     req.GetRebucketOnly(),
	}

	if err := validatePreparedBrowsingStateRequest(prepared); err != nil {
		return nil, err
	}
	return prepared, nil
}

func validatePreparedBrowsingStateRequest(
	prepared *preparedBrowsingStateRequest,
) error {
	if prepared == nil {
		return nil
	}

	plan := &browsingStateRequestPlan{
		AxisOrder: prepared.AxisOrder,
		AxisX:     prepared.AxisX,
		AxisY:     prepared.AxisY,
		AxisZ:     prepared.AxisZ,
		Filters:   prepared.Filters,
	}
	return validateVectorDimensionsRequestUsage(
		plan,
		prepared.MergedVectorDims,
		prepared.AllDefined,
		prepared.TimelineDefined,
		prepared.RebucketOnly,
		prepared.ForcedStrategy,
	)
}

func (s *DataLoaderServer) materializePreparedBrowsingStateRequest(
	ctx context.Context,
	prepared *preparedBrowsingStateRequest,
) (*browsingStateRequestPlan, error) {
	if prepared == nil {
		return nil, nil
	}

	axisOrder := append([]string(nil), prepared.AxisOrder...)
	filters := append([]qg.ParsedFilter(nil), prepared.Filters...)
	vectorIDs, active, err := s.resolvePreparedVectorFilter(
		ctx,
		prepared.VectorFilter,
		prepared.TimelineDefined,
	)
	if err != nil {
		return nil, err
	}

	filters = appendVectorObjectIDFilter(filters, vectorIDs, active)
	if active && !containsAxisOrder(axisOrder, "filter") {
		axisOrder = append(axisOrder, "filter")
	}

	plan := &browsingStateRequestPlan{
		AxisOrder: axisOrder,
		AxisX:     prepared.AxisX,
		AxisY:     prepared.AxisY,
		AxisZ:     prepared.AxisZ,
		Filters:   filters,
	}
	return s.applyVectorDimensionsToPlan(
		ctx,
		plan,
		prepared.MergedVectorDims,
		prepared.AllDefined,
		prepared.TimelineDefined,
		prepared.RebucketOnly,
		prepared.ForcedStrategy,
	)
}
