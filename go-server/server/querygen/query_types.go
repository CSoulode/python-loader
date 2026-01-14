package querygen

import (
	"fmt"
)

type ParsedAxis struct {
	Type string      `json:"type"`
	Id   int         `json:"id"`
	Ids  map[int]int // populated by InitializeIds
}

// String implements fmt.Stringer for easy debugging.
func (p *ParsedAxis) String() string {
	return fmt.Sprintf("Type = %s\nId = %d\nIds = %v", p.Type, p.Id, p.Ids)
}

// ParsedFilter mirrors C# ParsedFilter
type ParsedFilter struct {
	Type   string     `json:"type"`
	Ids    []int      `json:"ids"`
	Ranges [][]string `json:"ranges"`
}

type joinBranch struct{ sql, ax string }

// UngroupedOpts controls optional behaviors for GenerateUngroupedSQLForState.
type UngroupedOpts struct {
	// BranchDistinct: when true, each branch SELECT uses DISTINCT (slower TTFB, fewer dup rows).
	// Default: false (faster first rows; dedupe in Go/C#).
	BranchDistinct bool

	// RestrictIDs: optional object_id whitelist (for chunked exec).
	// When non-empty, a CTE WITH f(object_id) AS (VALUES ...) is used and all branches add
	// AND <alias>.object_id IN (SELECT object_id FROM f).
	RestrictIDs []int

	// UseLateralMediaJoin: when true, uses LATERAL JOIN for media the branch.
	UseLateralMediaJoin bool
}

type InitializeIdsPlan struct {
	Kind     string // "tagset", "node", "fallback"
	MainSQL  string // main query to list ids
	MainArgs []any
	PreSQL   string // optional prelim query (for node.hierarchy_id)
	PreArgs  []any
}
