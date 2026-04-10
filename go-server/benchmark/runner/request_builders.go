package runner

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

type BenchmarkQuery struct {
	ID                   string
	Complexity           string
	FilterTypes          string
	TargetSelectivity    float64
	ActualSelectivity    float64
	ActualCandidateCount int64
	Model                BenchmarkModel
	VectorAxis           pb.AxisType
	MetadataFilters      []*pb.AxisFilter
	ParsedAxes           []qg.ParsedAxis
	ParsedFilters        []qg.ParsedFilter
	ReferenceObjectID    int32
}

func BuildBenchmarkQuery(
	ctx context.Context,
	db *sql.DB,
	state RepresentativeState,
	model BenchmarkModel,
	targetSelectivity float64,
	queryID string,
	referenceOffset int,
) (*BenchmarkQuery, error) {
	filters, err := ResolveRepresentativeRequest(ctx, db, state)
	if err != nil {
		return nil, err
	}

	baseAxes, baseFilters, err := ParseMetadataFilters(filters)
	if err != nil {
		return nil, err
	}
	if targetSelectivity > 0 && targetSelectivity < 1 {
		dynamic, err := PickTagFilterNearSelectivity(ctx, db, model, baseFilters, baseAxes, targetSelectivity)
		if err != nil {
			return nil, err
		}
		filters = append(filters, &pb.AxisFilter{
			AxisFilterType: pb.AxisType_FILTER,
			Value:          dynamic.TagID,
			ValueType:      pb.FilterValueType_TAG,
		})
	}

	parsedAxes, parsedFilters, err := ParseMetadataFilters(filters)
	if err != nil {
		return nil, err
	}
	candidateCount, actualSelectivity, err := MeasureQueryActuals(ctx, db, model, parsedFilters, parsedAxes)
	if err != nil {
		return nil, err
	}
	vectorAxis, err := reserveVectorAxis(filters)
	if err != nil {
		return nil, err
	}
	referenceQuery := &BenchmarkQuery{
		Model:         model,
		ParsedAxes:    parsedAxes,
		ParsedFilters: parsedFilters,
	}
	referenceObjectID, err := SelectReferenceObjectID(
		ctx,
		db,
		referenceQuery,
		referenceOffset,
		candidateCount,
	)
	if err != nil {
		return nil, err
	}

	return &BenchmarkQuery{
		ID:                   queryID,
		Complexity:           state.Complexity,
		FilterTypes:          filterTypesLabel(parsedFilters),
		TargetSelectivity:    targetSelectivity,
		ActualSelectivity:    actualSelectivity,
		ActualCandidateCount: candidateCount,
		Model:                model,
		VectorAxis:           vectorAxis,
		MetadataFilters:      filters,
		ParsedAxes:           parsedAxes,
		ParsedFilters:        parsedFilters,
		ReferenceObjectID:    referenceObjectID,
	}, nil
}

func ParseMetadataFilters(filters []*pb.AxisFilter) ([]qg.ParsedAxis, []qg.ParsedFilter, error) {
	axes := make([]qg.ParsedAxis, 0, 3)
	parsedFilters := make([]qg.ParsedFilter, 0, len(filters))
	for _, filter := range filters {
		kind, err := filterKind(filter.GetValueType())
		if err != nil {
			return nil, nil, err
		}
		switch filter.GetAxisFilterType() {
		case pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS, pb.AxisType_Z_AXIS:
			axes = append(axes, qg.ParsedAxis{Type: kind, Id: int(filter.GetValue())})
		case pb.AxisType_FILTER:
			parsedFilters = append(parsedFilters, qg.ParsedFilter{
				Type: kind,
				Ids:  []int{int(filter.GetValue())},
			})
		default:
			return nil, nil, fmt.Errorf("unsupported axis filter type %s", filter.GetAxisFilterType())
		}
	}
	return axes, parsedFilters, nil
}

func (q *BenchmarkQuery) Request(
	k int32,
	strategy pb.HybridStrategy,
	rebucketOnly bool,
	bucketCfg *pb.BucketConfig,
) *pb.GetBrowsingStateRequest {
	return q.RequestForPlan(KNNPlan(k), strategy, rebucketOnly, bucketCfg)
}

func (q *BenchmarkQuery) RequestForPlan(
	plan VectorQueryPlan,
	strategy pb.HybridStrategy,
	rebucketOnly bool,
	bucketCfg *pb.BucketConfig,
) *pb.GetBrowsingStateRequest {
	return &pb.GetBrowsingStateRequest{
		Filters:         cloneAxisFilters(q.MetadataFilters),
		VectorDimension: newVectorDimension(q.Model, q.ReferenceObjectID, plan, bucketCfg, q.VectorAxis),
		HybridStrategy:  strategy,
		RebucketOnly:    rebucketOnly,
	}
}

func CountCandidates(ctx context.Context, db *sql.DB, query *BenchmarkQuery) (int64, error) {
	sqlStr, err := candidateSQL(query.ParsedFilters, query.ParsedAxes)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+sqlStr+") q").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func LoadCandidateIDs(ctx context.Context, db *sql.DB, query *BenchmarkQuery) ([]int32, error) {
	sqlStr, err := candidateSQL(query.ParsedFilters, query.ParsedAxes)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]int32, 0, 1024)
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func candidateSQL(filters []qg.ParsedFilter, axes []qg.ParsedAxis) (string, error) {
	sqlStr, err := qg.GenerateCandidateObjectIDs(filters, axes)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(sqlStr) != "" {
		return sqlStr, nil
	}
	return `SELECT id AS object_id FROM public.medias ORDER BY id ASC`, nil
}

func newVectorDimension(
	model BenchmarkModel,
	objectID int32,
	plan VectorQueryPlan,
	bucketCfg *pb.BucketConfig,
	axis pb.AxisType,
) *pb.VectorSearchDimension {
	cfg := &pb.VectorSearchDimension{
		ModelName:  model.Name,
		Reference:  &pb.VectorReference{Ref: &pb.VectorReference_ObjectId{ObjectId: objectID}},
		BucketCfg:  cloneBucketConfig(bucketCfg),
		MaxResults: plan.MaxResults,
		Axis:       axis,
	}
	if plan.DistanceRange != nil {
		cfg.DistanceRange = &pb.DistanceRange{
			MinDistance: plan.DistanceRange.GetMinDistance(),
			MaxDistance: plan.DistanceRange.GetMaxDistance(),
		}
		cfg.RangeSemantics = pb.RangeSemantics_DISTANCE
	}
	return cfg
}

func reserveVectorAxis(filters []*pb.AxisFilter) (pb.AxisType, error) {
	used := map[pb.AxisType]bool{
		pb.AxisType_X_AXIS: false,
		pb.AxisType_Y_AXIS: false,
		pb.AxisType_Z_AXIS: false,
	}
	for _, filter := range filters {
		switch filter.GetAxisFilterType() {
		case pb.AxisType_X_AXIS, pb.AxisType_Y_AXIS, pb.AxisType_Z_AXIS:
			used[filter.GetAxisFilterType()] = true
		}
	}
	for _, axis := range []pb.AxisType{pb.AxisType_Z_AXIS, pb.AxisType_Y_AXIS, pb.AxisType_X_AXIS} {
		if !used[axis] {
			return axis, nil
		}
	}
	return pb.AxisType_FILTER, fmt.Errorf("no free axis available for vector_dimension")
}

func DefaultBucketConfig() *pb.BucketConfig {
	return &pb.BucketConfig{
		Strategy: pb.BucketStrategy_EQUAL_DEPTH,
		Count:    10,
	}
}

func RebucketVariants() []*pb.BucketConfig {
	return []*pb.BucketConfig{
		{Strategy: pb.BucketStrategy_EQUAL_WIDTH, Count: 5},
		{Strategy: pb.BucketStrategy_EQUAL_WIDTH, Count: 10},
		{Strategy: pb.BucketStrategy_EQUAL_DEPTH, Count: 5},
		{Strategy: pb.BucketStrategy_EQUAL_DEPTH, Count: 10},
		{Strategy: pb.BucketStrategy_EQUAL_DEPTH, Count: 20},
		{Strategy: pb.BucketStrategy_LOGARITHMIC, Count: 5},
		{Strategy: pb.BucketStrategy_LOGARITHMIC, Count: 10},
		{Strategy: pb.BucketStrategy_LOGARITHMIC, Count: 20},
		{Strategy: pb.BucketStrategy_CUSTOM, CustomBreaks: []float32{0, 0.2, 0.4, 0.6, 0.8, 1.0}},
		{Strategy: pb.BucketStrategy_CUSTOM, CustomBreaks: []float32{0, 0.1, 0.25, 0.5, 0.75, 1.0}},
	}
}

func cloneAxisFilters(filters []*pb.AxisFilter) []*pb.AxisFilter {
	out := make([]*pb.AxisFilter, 0, len(filters))
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		out = append(out, &pb.AxisFilter{
			AxisFilterType: filter.GetAxisFilterType(),
			Value:          filter.GetValue(),
			ValueType:      filter.GetValueType(),
		})
	}
	return out
}

func cloneBucketConfig(cfg *pb.BucketConfig) *pb.BucketConfig {
	if cfg == nil {
		return nil
	}
	return &pb.BucketConfig{
		Strategy:     cfg.GetStrategy(),
		Count:        cfg.GetCount(),
		DistMin:      cfg.GetDistMin(),
		DistMax:      cfg.GetDistMax(),
		CustomBreaks: append([]float32(nil), cfg.GetCustomBreaks()...),
	}
}

func filterKind(valueType pb.FilterValueType) (string, error) {
	switch valueType {
	case pb.FilterValueType_TAG:
		return "tag", nil
	case pb.FilterValueType_TAGSET:
		return "tagset", nil
	case pb.FilterValueType_NODE:
		return "node", nil
	default:
		return "", fmt.Errorf("unsupported filter value type %s", valueType)
	}
}

func filterTypesLabel(filters []qg.ParsedFilter) string {
	if len(filters) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(filters))
	for _, filter := range filters {
		parts = append(parts, filter.Type)
	}
	return strings.Join(parts, "+")
}

func FormatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}
