package runner

import (
	"strings"
	"testing"
)

func TestExactQueriesMaterializeDistance(t *testing.T) {
	model := BenchmarkModel{Table: "public.siglip2_tags", DistanceMetric: "cosine"}
	queries := map[string]string{
		"knn":            exactKNNQuery(model),
		"filtered_knn":   exactFilteredKNNQuery(model),
		"range":          exactRangeQuery(model),
		"filtered_range": exactFilteredRangeQuery(model),
	}

	for name, query := range queries {
		if !strings.Contains(query, "WITH exact AS MATERIALIZED") {
			t.Fatalf("%s query does not materialize exact distances", name)
		}
		if !strings.Contains(query, "FROM exact") {
			t.Fatalf("%s query does not read from exact CTE", name)
		}
		if !strings.Contains(query, "ORDER BY distance, object_id ASC") {
			t.Fatalf("%s query does not order by the materialized distance", name)
		}
	}
}
