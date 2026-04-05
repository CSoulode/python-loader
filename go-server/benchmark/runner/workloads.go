package runner

import (
	"context"
	"database/sql"
	"fmt"

	pb "m3.dataloader/dataloader"
)

type RepresentativeState struct {
	Name        string
	Complexity  string
	Axes        []namedAxis
	BaseFilters []namedTagFilter
}

func DefaultRepresentativeStates() []RepresentativeState {
	return []RepresentativeState{
		{
			Name:       "lw",
			Complexity: "LW",
			Axes: []namedAxis{
				{Axis: "x", Kind: "node", Candidates: []string{"Mammals"}},
				{Axis: "y", Kind: "tagset", Candidates: []string{"Day of week", "DayOfWeek"}},
			},
			BaseFilters: []namedTagFilter{
				{Kind: "tag", TagsetCandidates: []string{"Year"}, ValueCandidates: []string{"2020"}},
			},
		},
		{
			Name:       "gh",
			Complexity: "GH",
			Axes: []namedAxis{
				{Axis: "x", Kind: "tagset", Candidates: []string{"Distance"}},
				{Axis: "y", Kind: "tagset", Candidates: []string{"Day of month", "DayOfMonth", "Day within month"}},
			},
			BaseFilters: []namedTagFilter{
				{Kind: "node", ValueCandidates: []string{"Clothing"}},
			},
		},
		{
			Name:       "rh",
			Complexity: "RH",
			Axes: []namedAxis{
				{Axis: "x", Kind: "node", Candidates: []string{"Year Month", "YearMonth"}},
				{Axis: "y", Kind: "tagset", Candidates: []string{"Day of week", "DayOfWeek"}},
			},
			BaseFilters: []namedTagFilter{
				{Kind: "node", ValueCandidates: []string{"Sleep Level", "SleepLevel"}},
			},
		},
		{
			Name:       "hw",
			Complexity: "HW",
			Axes: []namedAxis{
				{Axis: "x", Kind: "tagset", Candidates: []string{"Year Month", "YearMonth"}},
				{Axis: "y", Kind: "tagset", Candidates: []string{"Minute"}},
			},
			BaseFilters: []namedTagFilter{
				{Kind: "node", ValueCandidates: []string{"Day of week", "DayOfWeek"}},
			},
		},
	}
}

func ResolveRepresentativeRequest(
	ctx context.Context,
	db *sql.DB,
	state RepresentativeState,
) ([]*pb.AxisFilter, error) {
	filters := make([]*pb.AxisFilter, 0, len(state.Axes)+len(state.BaseFilters))
	for _, axis := range state.Axes {
		value, err := resolveNamedAxis(ctx, db, axis)
		if err != nil {
			return nil, fmt.Errorf("%s axis %s: %w", state.Name, axis.Axis, err)
		}
		filters = append(filters, &pb.AxisFilter{
			AxisFilterType: axisTypeFromLabel(axis.Axis),
			Value:          value,
			ValueType:      valueTypeFromKind(axis.Kind),
		})
	}
	for _, filter := range state.BaseFilters {
		value, err := resolveNamedTagFilter(ctx, db, filter)
		if err != nil {
			return nil, fmt.Errorf("%s filter: %w", state.Name, err)
		}
		filters = append(filters, &pb.AxisFilter{
			AxisFilterType: pb.AxisType_FILTER,
			Value:          value,
			ValueType:      valueTypeFromKind(filter.Kind),
		})
	}
	return filters, nil
}

func axisTypeFromLabel(value string) pb.AxisType {
	switch value {
	case "x":
		return pb.AxisType_X_AXIS
	case "y":
		return pb.AxisType_Y_AXIS
	default:
		return pb.AxisType_Z_AXIS
	}
}

func valueTypeFromKind(kind string) pb.FilterValueType {
	switch kind {
	case "tagset":
		return pb.FilterValueType_TAGSET
	case "node":
		return pb.FilterValueType_NODE
	default:
		return pb.FilterValueType_TAG
	}
}
