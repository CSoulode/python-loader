package main

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func canonicalAxesFromGRPC(filters []*pb.AxisFilter) []canonicalAxis {
	axes := make([]canonicalAxis, 0, 3)
	for _, filter := range filters {
		axis := canonicalAxisName(filter.GetAxisFilterType())
		if axis == "" {
			continue
		}
		axes = append(axes, canonicalAxis{
			Axis: axis,
			Type: canonicalFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		})
	}
	sortCanonicalAxes(axes)
	return axes
}

type canonicalHTTPAxes struct {
	X qg.ParsedAxis
	Y qg.ParsedAxis
	Z qg.ParsedAxis
}

func canonicalAxesFromHTTP(query map[string][]string, values canonicalHTTPAxes) []canonicalAxis {
	axes := make([]canonicalAxis, 0, 3)
	if _, ok := query["xAxis"]; ok {
		axes = append(axes, canonicalAxis{Axis: "x", Type: values.X.Type, ID: int32(values.X.Id)})
	}
	if _, ok := query["yAxis"]; ok {
		axes = append(axes, canonicalAxis{Axis: "y", Type: values.Y.Type, ID: int32(values.Y.Id)})
	}
	if _, ok := query["zAxis"]; ok {
		axes = append(axes, canonicalAxis{Axis: "z", Type: values.Z.Type, ID: int32(values.Z.Id)})
	}
	sortCanonicalAxes(axes)
	return axes
}

func sortCanonicalAxes(axes []canonicalAxis) {
	sort.Slice(axes, func(i, j int) bool {
		if axes[i].Axis != axes[j].Axis {
			return axes[i].Axis < axes[j].Axis
		}
		if axes[i].Type != axes[j].Type {
			return axes[i].Type < axes[j].Type
		}
		return axes[i].ID < axes[j].ID
	})
}

func canonicalFiltersFromGRPC(filters []*pb.AxisFilter) []canonicalItem {
	items := make([]canonicalItem, 0, len(filters))
	for _, filter := range filters {
		if filter.GetAxisFilterType() != pb.AxisType_FILTER {
			continue
		}
		items = append(items, canonicalItem{
			Type: canonicalFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		})
	}
	sortCanonicalItems(items)
	return items
}

func canonicalFiltersFromHTTP(filters []qg.ParsedFilter) ([]canonicalItem, error) {
	items := make([]canonicalItem, 0, len(filters))
	for _, filter := range filters {
		filterType := strings.ToLower(strings.TrimSpace(filter.Type))
		if len(filter.Ranges) > 0 || isRangeFilterType(filterType) {
			rangeItems, err := canonicalRangeItemsFromHTTP(filterType, filter)
			if err != nil {
				return nil, err
			}
			items = append(items, rangeItems...)
			continue
		}
		for _, id := range filter.Ids {
			items = append(items, canonicalItem{Type: filterType, ID: int32(id)})
		}
	}
	sortCanonicalItems(items)
	return items, nil
}

func canonicalRangeItemsFromHTTP(filterType string, filter qg.ParsedFilter) ([]canonicalItem, error) {
	if len(filter.Ids) != len(filter.Ranges) {
		return nil, fmt.Errorf("range filter %s has %d ids but %d ranges", filterType, len(filter.Ids), len(filter.Ranges))
	}
	items := make([]canonicalItem, 0, len(filter.Ids))
	for index, id := range filter.Ids {
		if len(filter.Ranges[index]) != 2 {
			return nil, fmt.Errorf("range filter %s entry %d must have 2 bounds", filterType, index)
		}
		items = append(items, canonicalItem{
			Type:   filterType,
			ID:     int32(id),
			Ranges: [][]string{{filter.Ranges[index][0], filter.Ranges[index][1]}},
		})
	}
	return items, nil
}

func sortCanonicalItems(items []canonicalItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Type != items[j].Type {
			return items[i].Type < items[j].Type
		}
		if items[i].ID != items[j].ID {
			return items[i].ID < items[j].ID
		}
		leftMin, leftMax := canonicalItemRangeBounds(items[i])
		rightMin, rightMax := canonicalItemRangeBounds(items[j])
		if leftMin != rightMin {
			return leftMin < rightMin
		}
		return leftMax < rightMax
	})
}

func canonicalItemRangeBounds(item canonicalItem) (string, string) {
	if len(item.Ranges) == 0 || len(item.Ranges[0]) < 2 {
		return "", ""
	}
	return item.Ranges[0][0], item.Ranges[0][1]
}

func canonicalVectorDimensions(req *pb.GetBrowsingStateRequest, excludeBuckets bool) ([]string, error) {
	merged, err := mergeVectorDimensions(req)
	if err != nil {
		return nil, err
	}
	items := make([]string, 0, len(merged.Dims))
	for _, dim := range merged.Dims {
		items = append(items, hashProtoMessage("vector-dim", canonicalVectorDimension(dim, excludeBuckets)))
	}
	sort.Strings(items)
	return items, nil
}

func canonicalVectorDimension(dim *pb.VectorSearchDimension, excludeBuckets bool) *pb.VectorSearchDimension {
	if dim == nil {
		return nil
	}
	cloned := proto.Clone(dim).(*pb.VectorSearchDimension)
	if excludeBuckets {
		cloned.BucketCfg = nil
	}
	return cloned
}

func canonicalBucketIDsForState(bucketIDs map[string]int32, excludeBuckets bool) string {
	if excludeBuckets {
		return ""
	}
	return canonicalBucketIDs(bucketIDs)
}

func canonicalAxisName(axis pb.AxisType) string {
	switch axis {
	case pb.AxisType_X_AXIS:
		return "x"
	case pb.AxisType_Y_AXIS:
		return "y"
	case pb.AxisType_Z_AXIS:
		return "z"
	default:
		return ""
	}
}

func canonicalFilterType(value pb.FilterValueType) string {
	return strings.ToLower(value.String())
}

func canonicalHybridStrategy(strategy pb.HybridStrategy) string {
	compatStrategy, err := strategyFromProto(strategy)
	if err != nil || compatStrategy == Auto {
		return ""
	}
	return compatStrategy.String()
}

func canonicalCompatHybridStrategy(strategy HybridStrategy) string {
	if strategy == Auto {
		return ""
	}
	return strategy.String()
}
