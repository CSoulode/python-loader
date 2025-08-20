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

// InitializeIds populates p.Ids by querying the database.
// For a "tagset", it loads all tags in that tagset (excluding the tagset’s own name) ordered by name.
// For a "node", it finds immediate child nodes (via the get_level_from_parent_node function) ordered by their tag name.
// Otherwise it defaults to mapping 1→1.
func (p *ParsedAxis) InitializeIds(ctx context.Context, db *sql.DB) error {
	idList := make(map[int]int)
	counter := 1

	switch p.Type {
	case "tagset":
		// Load tags in the tagset, excluding the tag with the same name
		rows, err := db.QueryContext(ctx, `
			SELECT t.id
			FROM tagsets ts
			JOIN tags t ON t.tagset_id = ts.id
			WHERE ts.id = $1
			  AND t.name <> ts.name
			ORDER BY t.name
		`, p.Id)
		if err != nil {
			return fmt.Errorf("query tags: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tagID int
			if err := rows.Scan(&tagID); err != nil {
				return fmt.Errorf("scan tag id: %w", err)
			}
			idList[tagID] = counter
			counter++
		}

	case "node":
		// Find this node's hierarchy
		var hierarchyID int
		if err := db.QueryRowContext(ctx,
			`SELECT hierarchy_id FROM nodes WHERE id = $1`, p.Id,
		).Scan(&hierarchyID); err != nil {
			return fmt.Errorf("fetch hierarchy_id: %w", err)
		}

		// Load child nodes one level down, ordered by their tag name
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
			return fmt.Errorf("query child nodes: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var nodeID int
			if err := rows.Scan(&nodeID); err != nil {
				return fmt.Errorf("scan node id: %w", err)
			}
			idList[nodeID] = counter
			counter++
		}

	default:
		// Fallback: map 1→1
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
