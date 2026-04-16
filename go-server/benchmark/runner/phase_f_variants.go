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
	CacheState     phaseFL0State
}

func (r *BenchRunner) buildPhaseFVariants(
	ctx context.Context,
	session *ActiveSession,
) ([]phaseFVariant, error) {
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
		query, err := BuildBenchmarkQuery(
			ctx,
			session.DB,
			state,
			primary,
			phaseFBaseSelectivity,
			fmt.Sprintf("phasef%02d", index+1),
			index,
		)
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

func buildPhaseFVariantsForQuery(
	query *BenchmarkQuery,
	secondary BenchmarkModel,
) ([]phaseFVariant, error) {
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

func buildPhaseFVariant(
	query *BenchmarkQuery,
	secondary BenchmarkModel,
	dimCount int,
) (phaseFVariant, error) {
	request, state, err := buildPhaseFRequest(query, secondary, dimCount)
	if err != nil {
		return phaseFVariant{}, err
	}
	return phaseFVariant{
		ID:             fmt.Sprintf("%s-%dd", query.Complexity, dimCount),
		Complexity:     query.Complexity,
		VectorDimCount: dimCount,
		Request:        request,
		CacheState:     state,
	}, nil
}

func buildPhaseFRequest(
	query *BenchmarkQuery,
	secondary BenchmarkModel,
	dimCount int,
) (*pb.GetBrowsingStateRequest, phaseFL0State, error) {
	baseFilters := cloneAxisFilters(query.MetadataFilters)
	switch dimCount {
	case 0:
		request := &pb.GetBrowsingStateRequest{
			Filters:        baseFilters,
			HybridStrategy: pb.HybridStrategy_AUTO,
		}
		return request, buildPhaseFL0State(baseFilters, nil), nil
	case 1:
		filters := convertAxesToFilters(baseFilters, pb.AxisType_X_AXIS)
		vectorDimension := buildPhaseFVectorDimension(
			query.Model,
			query.ReferenceObjectID,
			pb.AxisType_X_AXIS,
			DefaultBucketConfig(),
		)
		request := &pb.GetBrowsingStateRequest{
			Filters:         filters,
			VectorDimension: vectorDimension,
			HybridStrategy:  pb.HybridStrategy_AUTO,
		}
		return request, buildPhaseFL0State(filters, []*pb.VectorSearchDimension{vectorDimension}), nil
	case 2:
		filters := convertAxesToFilters(baseFilters, pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS)
		vectorDimensions := []*pb.VectorSearchDimension{
			buildPhaseFVectorDimension(
				query.Model,
				query.ReferenceObjectID,
				pb.AxisType_X_AXIS,
				DefaultBucketConfig(),
			),
			buildPhaseFVectorDimension(
				secondary,
				query.ReferenceObjectID,
				pb.AxisType_Y_AXIS,
				DefaultBucketConfig(),
			),
		}
		request := &pb.GetBrowsingStateRequest{
			Filters:          filters,
			VectorDimensions: vectorDimensions,
			HybridStrategy:   pb.HybridStrategy_AUTO,
		}
		return request, buildPhaseFL0State(filters, vectorDimensions), nil
	default:
		return nil, phaseFL0State{}, fmt.Errorf("unsupported vector dim count %d", dimCount)
	}
}

func buildPhaseFVectorDimension(
	model BenchmarkModel,
	objectID int32,
	axis pb.AxisType,
	bucketCfg *pb.BucketConfig,
) *pb.VectorSearchDimension {
	return newVectorDimension(
		model,
		objectID,
		KNNPlan(phaseFMaxResults),
		bucketCfg,
		axis,
	)
}

func convertAxesToFilters(
	filters []*pb.AxisFilter,
	axes ...pb.AxisType,
) []*pb.AxisFilter {
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

func buildPhaseFL0State(
	filters []*pb.AxisFilter,
	vectorDimensions []*pb.VectorSearchDimension,
) phaseFL0State {
	state := phaseFL0State{
		Filters:          make([]phaseFL0Filter, 0, len(filters)),
		VectorDimensions: make([]phaseFL0VectorDimension, 0, len(vectorDimensions)),
	}
	for _, filter := range filters {
		appendPhaseFL0Filter(&state, filter)
	}
	for _, vectorDimension := range vectorDimensions {
		state.VectorDimensions = append(
			state.VectorDimensions,
			buildPhaseFL0VectorDimension(vectorDimension),
		)
	}
	return state
}

func appendPhaseFL0Filter(state *phaseFL0State, filter *pb.AxisFilter) {
	if state == nil || filter == nil {
		return
	}

	switch filter.GetAxisFilterType() {
	case pb.AxisType_X_AXIS:
		state.XAxis = &phaseFL0Axis{
			Type: phaseFFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		}
	case pb.AxisType_Y_AXIS:
		state.YAxis = &phaseFL0Axis{
			Type: phaseFFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		}
	case pb.AxisType_FILTER:
		state.Filters = append(state.Filters, phaseFL0Filter{
			Type:    phaseFFilterType(filter.GetValueType()),
			ID:      filter.GetValue(),
			GroupID: filter.GetValue(),
		})
	}
}

func buildPhaseFL0VectorDimension(
	vectorDimension *pb.VectorSearchDimension,
) phaseFL0VectorDimension {
	if vectorDimension == nil {
		return phaseFL0VectorDimension{}
	}

	bucketCfg := vectorDimension.GetBucketCfg()
	return phaseFL0VectorDimension{
		Axis:           phaseFAxisToken(vectorDimension.GetAxis()),
		Model:          vectorDimension.GetModelName(),
		ObjectID:       vectorDimension.GetReference().GetObjectId(),
		BucketCount:    bucketCfg.GetCount(),
		BucketStrategy: phaseFBucketStrategy(bucketCfg.GetStrategy()),
		MaxResults:     vectorDimension.GetMaxResults(),
		DistMin:        optionalFloat32(bucketCfg.GetDistMin()),
		DistMax:        optionalFloat32(bucketCfg.GetDistMax()),
		CustomBreaks:   append([]float32(nil), bucketCfg.GetCustomBreaks()...),
		QueryMode:      "knn",
	}
}

func phaseFFilterType(valueType pb.FilterValueType) string {
	switch valueType {
	case pb.FilterValueType_TAGSET:
		return "tagset"
	case pb.FilterValueType_NODE:
		return "node"
	default:
		return "tag"
	}
}

func phaseFAxisToken(axis pb.AxisType) string {
	switch axis {
	case pb.AxisType_X_AXIS:
		return "x"
	case pb.AxisType_Y_AXIS:
		return "y"
	default:
		return "z"
	}
}

func phaseFBucketStrategy(strategy pb.BucketStrategy) string {
	switch strategy {
	case pb.BucketStrategy_EQUAL_WIDTH:
		return "equal_width"
	case pb.BucketStrategy_LOGARITHMIC:
		return "logarithmic"
	case pb.BucketStrategy_CUSTOM:
		return "custom"
	default:
		return "equal_depth"
	}
}

func optionalFloat32(value float32) *float32 {
	return &value
}
