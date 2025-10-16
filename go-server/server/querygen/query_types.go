package querygen

import (
	"context"
	"database/sql"
	"fmt"
)

type ParsedAxis struct {
	Type string      `json:"type"`
	Id   int         `json:"id"`
	Ids  map[int]int // populated by InitializeIds
}

// InitializeIds populates p.Ids with a stable position index per axis member.
//   - tagset: list all tags in the set, compute display name from the appropriate subtype table,
//     drop the tag whose display name == tagset.name, order by display name.
//   - node:   list immediate child nodes (get_level_from_parent_node), order by the node tag's alphanumerical name.
//   - else:   fallback 1→1.
func (p *ParsedAxis) InitializeIds(ctx context.Context, db *sql.DB) error {
	idList := make(map[int]int)
	counter := 1

	switch p.Type {
	case "tagset":
		// Resolve tag display name across all subtype tables; exclude the tag that shares the tagset's own name.
		// NOTE: Exactly one of (a, ts, tm, d, n) will be non-null for a given tag id.
		rows, err := db.QueryContext(ctx, `
			SELECT t.id,
			       COALESCE(
			         a.name,                                   -- alphanumerical_tags.name (text)
			         to_char(ts.name, 'YYYY-MM-DD HH24:MI:SS'),-- timestamp_tags.name
			         to_char(tm.name, 'HH24:MI'),              -- time_tags.name
			         to_char(d.name,  'YYYY-MM-DD'),           -- date_tags.name
			         n.name::text                              -- numerical_tags.name
			       ) AS disp_name
			FROM tagsets s
			JOIN tags t ON t.tagset_id = s.id
			LEFT JOIN alphanumerical_tags a ON a.id = t.id
			LEFT JOIN timestamp_tags      ts ON ts.id = t.id
			LEFT JOIN time_tags           tm ON tm.id = t.id
			LEFT JOIN date_tags            d ON  d.id = t.id
			LEFT JOIN numerical_tags       n ON  n.id = t.id
			WHERE s.id = $1
			  AND COALESCE(
			         a.name,
			         to_char(ts.name, 'YYYY-MM-DD HH24:MI:SS'),
			         to_char(tm.name, 'HH24:MI'),
			         to_char(d.name,  'YYYY-MM-DD'),
			         n.name::text
			      ) <> s.name
			ORDER BY disp_name
		`, p.Id)
		if err != nil {
			return fmt.Errorf("initializeIds(tagset): %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tagID int
			var _disp string
			if err := rows.Scan(&tagID, &_disp); err != nil {
				return fmt.Errorf("initializeIds(tagset) scan: %w", err)
			}
			idList[tagID] = counter
			counter++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("initializeIds(tagset) rows: %w", err)
		}

	case "node":
		// Keep your existing approach: list immediate children ordered by the node tag’s alphanumerical label.
		var hierarchyID int
		if err := db.QueryRowContext(ctx, `SELECT hierarchy_id FROM nodes WHERE id = $1`, p.Id).
			Scan(&hierarchyID); err != nil {
			return fmt.Errorf("initializeIds(node) fetch hierarchy_id: %w", err)
		}

		rows, err := db.QueryContext(ctx, `
			SELECT n.id
			FROM nodes n
			JOIN alphanumerical_tags a ON n.tag_id = a.id
			WHERE n.id IN (
				SELECT id FROM get_level_from_parent_node($1, $2)
			)
			ORDER BY a.name
		`, p.Id, hierarchyID)
		if err != nil {
			return fmt.Errorf("initializeIds(node) query child nodes: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var nodeID int
			if err := rows.Scan(&nodeID); err != nil {
				return fmt.Errorf("initializeIds(node) scan: %w", err)
			}
			idList[nodeID] = counter
			counter++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("initializeIds(node) rows: %w", err)
		}

	default:
		// Fallback: stable singleton
		idList[1] = 1
	}

	// If nothing was found (empty tagset or node), still provide a stable default to avoid zero-length maps.
	if len(idList) == 0 {
		idList[1] = 1
	}

	p.Ids = idList
	return nil
}

// String implements fmt.Stringer for easy debugging.
func (p *ParsedAxis) String() string {
	return fmt.Sprintf("Type = %s\nId = %d\nIds = %v", p.Type, p.Id, p.Ids)
}

// ParsedFilter mirrors your C# ParsedFilter (you’ll need to fill in fields)
type ParsedFilter struct {
	Type   string     `json:"type"`
	Ids    []int      `json:"ids"`
	Ranges [][]string `json:"ranges"`
}
