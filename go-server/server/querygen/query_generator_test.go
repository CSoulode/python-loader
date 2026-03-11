package querygen

import (
	"strings"
	"testing"
)

func TestGenerateSQLQueryForStateWithEmptyObjectIDFilter(t *testing.T) {
	sqlStr := GenerateSQLQueryForState(
		[]string{"filter"},
		"", -1,
		"", -1,
		"", -1,
		[]ParsedFilter{{Type: "objectid", Ids: nil}},
	)

	if !strings.Contains(sqlStr, "WHERE 1 = 0") {
		t.Fatalf("GenerateSQLQueryForState = %s", sqlStr)
	}
}

func TestGenerateSQLQueryForCellWithObjectIDFilterUsesArray(t *testing.T) {
	sqlStr := GenerateSQLQueryForCell(
		"", -1,
		"", -1,
		"", -1,
		[]ParsedFilter{{Type: "objectid", Ids: []int{7, 3}}},
	)

	if !strings.Contains(sqlStr, "unnest(ARRAY[7,3]::integer[]) AS object_id") {
		t.Fatalf("GenerateSQLQueryForCell = %s", sqlStr)
	}
}

func TestGenerateUngroupedSQLForStateWithLargeObjectIDFilterUsesValues(t *testing.T) {
	ids := make([]int, 1001)
	for i := range ids {
		ids[i] = i + 1
	}

	sqlStr := GenerateUngroupedSQLForState(
		[]string{"filter"},
		"", -1,
		"", -1,
		"", -1,
		[]ParsedFilter{{Type: "objectid", Ids: ids}},
	)

	if !strings.Contains(sqlStr, "FROM (VALUES") {
		t.Fatalf("GenerateUngroupedSQLForState = %s", sqlStr)
	}
}
