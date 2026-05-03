package runner

import (
	"context"
	"fmt"

	pb "m3.dataloader/dataloader"
)

const (
	phaseFBaseSelectivity = 0.10
	phaseFMaxResults      = 500
)

type phaseFVariant struct {
	ID             string
	Complexity     string
	VectorDimCount int
	Request        *pb.GetBrowsingStateRequest
}

type phaseFVectorDimensionSpec struct {
	Model     BenchmarkModel
	ObjectID  int32
	Axis      pb.AxisType
	BucketCfg *pb.BucketConfig
}

func (r *BenchRunner) buildPhaseFVariants(ctx context.Context, session *ActiveSession) ([]phaseFVariant, error) {
	primary, err := r.catalog.Default()
	if err != nil {
		return nil, err
	}
	secondary, err := r.catalog.Model("hsv")
	if err != nil {
		return nil, err
	}
	variants := make([]phaseFVariant, 0, len(DefaultRepresentativeStates())*3)
	for index, state := range DefaultRepresentativeStates() {
		query, err := BuildBenchmarkQuery(ctx, session.DB, state, primary, phaseFBaseSelectivity, state.Name, index)
		if err != nil {
			return nil, err
		}
		current, err := buildPhaseFVariantsForQuery(query, secondary)
		if err != nil {
			return nil, err
		}
		variants = append(variants, current...)
	}
	return variants, nil
}

func buildPhaseFVariantsForQuery(query *BenchmarkQuery, secondary BenchmarkModel) ([]phaseFVariant, error) {
	variants := make([]phaseFVariant, 0, 3)
	for _, dimCount := range []int{0, 1, 2} {
		variant, err := buildPhaseFVariant(query, secondary, dimCount)
		if err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}
	return variants, nil
}

func buildPhaseFVariant(query *BenchmarkQuery, secondary BenchmarkModel, dimCount int) (phaseFVariant, error) {
	request, err := buildPhaseFRequest(query, secondary, dimCount)
	if err != nil {
		return phaseFVariant{}, err
	}
	return phaseFVariant{
		ID:             fmt.Sprintf("%s-%dd", query.Complexity, dimCount),
		Complexity:     query.Complexity,
		VectorDimCount: dimCount,
		Request:        request,
	}, nil
}

func buildPhaseFRequest(query *BenchmarkQuery, secondary BenchmarkModel, dimCount int) (*pb.GetBrowsingStateRequest, error) {
	baseFilters := cloneAxisFilters(query.MetadataFilters)
	switch dimCount {
	case 0:
		return &pb.GetBrowsingStateRequest{Filters: baseFilters, HybridStrategy: pb.HybridStrategy_AUTO}, nil
	case 1:
		return buildPhaseFOneDimRequest(query, baseFilters), nil
	case 2:
		return buildPhaseFTwoDimRequest(query, secondary, baseFilters), nil
	default:
		return nil, fmt.Errorf("unsupported vector dim count %d", dimCount)
	}
}

func buildPhaseFOneDimRequest(query *BenchmarkQuery, filters []*pb.AxisFilter) *pb.GetBrowsingStateRequest {
	dim := buildPhaseFVectorDimension(phaseFVectorDimensionSpec{
		Model: query.Model, ObjectID: query.ReferenceObjectID,
		Axis: pb.AxisType_X_AXIS, BucketCfg: DefaultBucketConfig(),
	})
	return &pb.GetBrowsingStateRequest{
		Filters:         convertAxesToFilters(filters, pb.AxisType_X_AXIS),
		VectorDimension: dim,
		HybridStrategy:  pb.HybridStrategy_AUTO,
	}
}

func buildPhaseFTwoDimRequest(query *BenchmarkQuery, secondary BenchmarkModel, filters []*pb.AxisFilter) *pb.GetBrowsingStateRequest {
	dims := []*pb.VectorSearchDimension{
		buildPhaseFVectorDimension(phaseFVectorDimensionSpec{
			Model: query.Model, ObjectID: query.ReferenceObjectID,
			Axis: pb.AxisType_X_AXIS, BucketCfg: DefaultBucketConfig(),
		}),
		buildPhaseFVectorDimension(phaseFVectorDimensionSpec{
			Model: secondary, ObjectID: query.ReferenceObjectID,
			Axis: pb.AxisType_Y_AXIS, BucketCfg: DefaultBucketConfig(),
		}),
	}
	return &pb.GetBrowsingStateRequest{
		Filters:          convertAxesToFilters(filters, pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS),
		VectorDimensions: dims,
		HybridStrategy:   pb.HybridStrategy_AUTO,
	}
}

func buildPhaseFVectorDimension(spec phaseFVectorDimensionSpec) *pb.VectorSearchDimension {
	return newVectorDimension(spec.Model, spec.ObjectID, KNNPlan(phaseFMaxResults), spec.BucketCfg, spec.Axis)
}

func convertAxesToFilters(filters []*pb.AxisFilter, axes ...pb.AxisType) []*pb.AxisFilter {
	out := cloneAxisFilters(filters)
	for _, filter := range out {
		if shouldConvertAxis(filter, axes) {
			filter.AxisFilterType = pb.AxisType_FILTER
		}
	}
	return out
}

func shouldConvertAxis(filter *pb.AxisFilter, axes []pb.AxisType) bool {
	if filter == nil {
		return false
	}
	for _, axis := range axes {
		if filter.GetAxisFilterType() == axis {
			return true
		}
	}
	return false
}
