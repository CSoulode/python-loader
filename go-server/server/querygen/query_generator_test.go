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

func TestGenerateSQLQueryForStateWithAxisSubquery(t *testing.T) {
	sqlStr := GenerateSQLQueryForState(
		[]string{"x"},
		"vector", -1,
		"", -1,
		"", -1,
		nil,
		StateQueryOpts{
			AxisSubqueries: map[string]string{
				"x": "SELECT V.object_id, V.id\nFROM (VALUES (7,0),(8,1)) AS V(object_id, id)",
			},
		},
	)

	if !strings.Contains(sqlStr, "VALUES (7,0),(8,1)") {
		t.Fatalf("GenerateSQLQueryForState = %s", sqlStr)
	}
}

func TestGenerateUngroupedSQLForStateWithAxisSubquery(t *testing.T) {
	sqlStr := GenerateUngroupedSQLForState(
		[]string{"y"},
		"", -1,
		"vector", -1,
		"", -1,
		nil,
		UngroupedOpts{
			AxisSubqueries: map[string]string{
				"y": "SELECT V.object_id, V.id\nFROM (VALUES (7,0),(8,1)) AS V(object_id, id)",
			},
		},
	)

	if !strings.Contains(sqlStr, "VALUES (7,0),(8,1)") {
		t.Fatalf("GenerateUngroupedSQLForState = %s", sqlStr)
	}
}
