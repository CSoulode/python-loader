package main

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func axisDomainKeyFromGRPC(req *pb.GetBrowsingStateRequest) string {
	if req == nil {
		return ""
	}
	items := make([]string, 0, 3)
	for _, filter := range req.GetFilters() {
		if canonicalAxisName(filter.GetAxisFilterType()) == "" {
			continue
		}
		items = append(items, filterItemKey(canonicalItem{
			Type: canonicalFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		}))
	}
	return sortedKey(items)
}

func axisDomainKeyFromHTTP(r *http.Request) string {
	axisX, axisY, axisZ, _, err := oldParseAxesAndFilters(httpCellRequest(r))
	if err != nil {
		return ""
	}
	return sortedKey(axisDomainItems(axisX, axisY, axisZ))
}

func axisDomainItems(axes ...qg.ParsedAxis) []string {
	items := make([]string, 0, len(axes))
	for _, axis := range axes {
		if strings.TrimSpace(axis.Type) == "" || axis.Id < 0 {
			continue
		}
		items = append(items, strings.ToLower(strings.TrimSpace(axis.Type))+"="+strconv.Itoa(axis.Id))
	}
	return items
}

func filterItemsFromGRPC(req *pb.GetBrowsingStateRequest) []string {
	if req == nil {
		return nil
	}
	items := make([]string, 0, len(req.GetFilters())+1)
	for _, filter := range req.GetFilters() {
		if filter.GetAxisFilterType() != pb.AxisType_FILTER {
			continue
		}
		items = append(items, filterItemKey(canonicalItem{
			Type: canonicalFilterType(filter.GetValueType()),
			ID:   filter.GetValue(),
		}))
	}
	return appendVectorFilterItem(items, req.GetVectorFilter())
}

func filterItemsFromHTTP(r *http.Request) []string {
	if r == nil {
		return nil
	}
	_, _, _, filters, err := oldParseAxesAndFilters(httpCellRequest(r))
	if err != nil {
		return nil
	}
	items, err := filterItemsFromParsedFilters(filters)
	if err != nil {
		return nil
	}
	vectorFilter, err := parseCompatVectorFilter(r.URL.Query().Get("vectorFilter"))
	if err != nil {
		return nil
	}
	return appendVectorFilterItem(items, vectorFilter)
}

func filterItemsFromParsedFilters(filters []qg.ParsedFilter) ([]string, error) {
	canonicalFilters, err := canonicalFiltersFromHTTP(filters)
	if err != nil {
		return nil, err
	}
	items := make([]string, 0, len(filters))
	for _, item := range canonicalFilters {
		items = append(items, filterItemKey(item))
	}
	return items, nil
}

func appendVectorFilterItem(items []string, filter *pb.VectorFilterConfig) []string {
	if filter != nil {
		items = append(items, "vector_filter="+hashProtoMessage("vector-filter", filter))
	}
	return sortedStrings(items)
}

func filterItemKey(item canonicalItem) string {
	var builder strings.Builder
	builder.WriteString(strings.ToLower(strings.TrimSpace(item.Type)))
	builder.WriteString("=")
	builder.WriteString(strconv.Itoa(int(item.ID)))
	for _, bounds := range item.Ranges {
		builder.WriteString("[")
		builder.WriteString(strings.Join(bounds, ","))
		builder.WriteString("]")
	}
	return builder.String()
}

func sortedKey(items []string) string {
	return strings.Join(sortedStrings(items), "|")
}

func sortedStrings(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}
