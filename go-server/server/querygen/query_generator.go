package querygen

import (
	"fmt"
	"strings"
)

// prettyJoinChain renders: FROM (<b0>) R1
//
//	JOIN (<b1>) R2 ON R2.object_id = R1.object_id
//	...
//
// and returns the formatted string plus a map of axis->alias used ("x","y","z" -> "Rk").
func prettyJoinChain(branches []joinBranch) (string, map[string]string) {
	var b strings.Builder
	axisAlias := make(map[string]string)

	b.WriteString("\nFROM\n  (")
	b.WriteString(strings.TrimSpace(branches[0].sql))
	b.WriteString(") R1\n")

	if branches[0].ax != "" {
		axisAlias[branches[0].ax] = "R1"
	}

	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		b.WriteString("  JOIN (\n    ")
		b.WriteString(strings.ReplaceAll(strings.TrimSpace(branches[i].sql), "\n", "\n    "))
		b.WriteString("\n  ) ")
		b.WriteString(alias)
		b.WriteString(" ON ")
		b.WriteString(alias)
		b.WriteString(".object_id = R1.object_id\n")
		if branches[i].ax != "" {
			axisAlias[branches[i].ax] = alias
		}
	}
	return b.String(), axisAlias
}

// prettyValuesCTE renders WITH f(object_id) AS (VALUES (...),(...))
func prettyValuesCTE(ids []int) string {
	if len(ids) == 0 {
		return ""
	}
	var vb strings.Builder
	for i, id := range ids {
		if i > 0 {
			vb.WriteString(", ")
		}
		vb.WriteString(fmt.Sprintf("(%d)", id))
	}
	return "WITH f(object_id) AS (VALUES " + vb.String() + ")\n"
}

// helper to add optional IN (SELECT object_id FROM f) filter to a branch alias
func addRestrict(alias string) string {
	return " AND " + alias + ".object_id IN (SELECT object_id FROM f)"
}

// BuildInitializeIdsPlan builds the SQL needed to initialize p.Ids,
// but does NOT hit the database.
func (p *ParsedAxis) BuildInitializeIdsPlan() (*InitializeIdsPlan, error) {
	switch p.Type {
	case "tagset":
		// Get all tags in this tagset, compute display name from subtype tables,
		// drop the tag whose display name == tagset.name, order by display name.
		return &InitializeIdsPlan{
			Kind: "tagset",
			MainSQL: `
                SELECT t.id,
                       COALESCE(
                         a.name,
                         to_char(ts.name, 'YYYY-MM-DD HH24:MI:SS'),
                         to_char(tm.name, 'HH24:MI'),
                         to_char(d.name,  'YYYY-MM-DD'),
                         n.name::text
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
            `,
			MainArgs: []any{p.Id},
		}, nil

	case "node":
		// PreSQL returns hierarchy_id for this node.
		// MainSQL uses ($1 parent_node_id, $2 hierarchy_id).
		return &InitializeIdsPlan{
			Kind: "node",
			PreSQL: `
                SELECT hierarchy_id
                FROM nodes
                WHERE id = $1
            `,
			PreArgs: []any{p.Id},
			MainSQL: `
                SELECT n.id
                FROM nodes n
                JOIN alphanumerical_tags a ON n.tag_id = a.id
                WHERE n.id IN (
                    SELECT id
                    FROM get_level_from_parent_node($1, $2)
                )
                ORDER BY a.name
            `,
			// place-holders: $1 will be parent node id, $2 will be hierarchy_id we learn from PreSQL
			MainArgs: nil, // we'll fill this at execution time because we don't know hierarchy_id yet
		}, nil

	default:
		// Fallback: singleton. No SQL required.
		return &InitializeIdsPlan{
			Kind: "fallback",
		}, nil
	}
}

// GenerateUngroupedSQLForState builds a streaming-friendly join (no GROUP BY/ORDER BY).
// It intersects axis/filters by object_id and returns rows:
//
//	x_id, y_id, z_id, object_id, file_uri, thumbnail_uri
//
// Usage:
//
//	// default behavior (BranchDistinct=false):
//	sql := GenerateUngroupedSQLForState(xT,xID,yT,yID,zT,zID,filters)
//
//	// enable DISTINCT in branches:
//	sql := GenerateUngroupedSQLForState(xT,xID,yT,yID,zT,zID,filters, qg.UngroupedOpts{BranchDistinct:true})
//
//	// restrict to a chunk of object_ids (e.g., incremental3):
//	sql := GenerateUngroupedSQLForState(xT,xID,yT,yID,zT,zID,filters, qg.UngroupedOpts{RestrictIDs: ids})
func GenerateUngroupedSQLForState(
	filterOrder []string,
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filters []ParsedFilter,
	opts ...UngroupedOpts,
) string {
	// ---- options ----
	var o UngroupedOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	branchDistinct := o.BranchDistinct
	restrictIDs := o.RestrictIDs
	useLateralMediaJoin := o.UseLateralMediaJoin

	type branch struct {
		sql string
		ax  string
	}

	// SELECT prefix per branch
	sel := func(cols ...string) string {
		if branchDistinct {
			return "SELECT DISTINCT " + strings.Join(cols, ", ")
		}
		return "SELECT " + strings.Join(cols, ", ")
	}

	// Optional CTE for restricting by object_id (used by chunked Incremental2) TODO: Remove if Incremental2 is removed.
	cte := prettyValuesCTE(restrictIDs)
	inF := ""
	if len(restrictIDs) > 0 {
		inF = " AND <alias>.object_id IN (SELECT object_id FROM f)"
	}

	branches := make([]branch, 0, 3+len(filters))

	addAxis := func(axisType string, vertexID int, ax string) {
		if axisType == "" {
			return
		}
		switch axisType {
		case "node":
			sql := sel("N.object_id", "N.node_id AS id") + "\nFROM nodes_taggings N\nWHERE N.parentnode_id = " + fmt.Sprint(vertexID)
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "N", 1)
			}
			branches = append(branches, branch{sql: sql, ax: ax})
		case "tagset":
			sql := sel("T.object_id", "T.tag_id AS id") + "\nFROM tagsets_taggings T\nWHERE T.tagset_id = " + fmt.Sprint(vertexID)
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "T", 1)
			}
			branches = append(branches, branch{sql: sql, ax: ax})
		}
	}

	addFilters := func() {
		// Filters: only object_id
		for _, f := range filters {
			switch f.Type {
			case "node":
				if len(f.Ids) == 1 {
					sql := "SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id = " + fmt.Sprint(f.Ids[0])
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "N", 1)
					}
					branches = append(branches, branch{sql: sql})
				} else if len(f.Ids) > 1 {
					sql := "SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id IN " + generateIdList(f)
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "N", 1)
					}
					branches = append(branches, branch{sql: sql})
				}
			case "tagset":
				if len(f.Ids) == 1 {
					sql := "SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = " + fmt.Sprint(f.Ids[0])
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "T", 1)
					}
					branches = append(branches, branch{sql: sql})
				} else if len(f.Ids) > 1 {
					sql := "SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id IN " + generateIdList(f)
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "T", 1)
					}
					branches = append(branches, branch{sql: sql})
				}
			case "tag":
				if len(f.Ids) == 1 {
					sql := "SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id = " + fmt.Sprint(f.Ids[0])
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "R", 1)
					}
					branches = append(branches, branch{sql: sql})
				} else if len(f.Ids) > 1 {
					sql := "SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id IN " + generateIdList(f)
					if inF != "" {
						sql += strings.Replace(inF, "<alias>", "R", 1)
					}
					branches = append(branches, branch{sql: sql})
				}
			case "numrange":
				sql := "SELECT R.object_id\nFROM numerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE " + generateRangeList(f, "")
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			case "alpharange":
				sql := "SELECT R.object_id\nFROM alphanumerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE " + generateRangeList(f, "'")
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			case "daterange":
				sql := "SELECT R.object_id\nFROM date_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE " + generateRangeList(f, "'")
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			case "timerange":
				sql := "SELECT R.object_id\nFROM time_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE " + generateRangeList(f, "'")
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			case "timestamprange":
				sql := "SELECT R.object_id\nFROM timestamp_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE " + generateRangeList(f, "'")
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			}
		}
	}

	for _, axisFilterType := range filterOrder {
		switch axisFilterType {
		case "x":
			addAxis(xType, xVertexID, "x")
		case "y":
			addAxis(yType, yVertexID, "y")
		case "z":
			addAxis(zType, zVertexID, "z")
		case "filter":
			addFilters()
		}
	}

	// If no axes/filters: stream all medias (neutral axes)
	if len(branches) == 0 {
		return cte + "SELECT 1 AS x_id, 1 AS y_id, 1 AS z_id, O.id AS object_id, O.file_uri, O.thumbnail_uri\nFROM medias O;\n"
	}

	// FROM / JOIN chain (formatted)
	tmp := make([]joinBranch, len(branches))
	for i := range branches {
		tmp[i] = joinBranch{sql: branches[i].sql, ax: branches[i].ax}
	}
	mid, axisAlias := prettyJoinChain(tmp)

	// SELECT list
	xSel, ySel, zSel := "1 AS x_id", "1 AS y_id", "1 AS z_id"
	if a, ok := axisAlias["x"]; ok {
		xSel = a + ".id AS x_id"
	}
	if a, ok := axisAlias["y"]; ok {
		ySel = a + ".id AS y_id"
	}
	if a, ok := axisAlias["z"]; ok {
		zSel = a + ".id AS z_id"
	}

	var sql strings.Builder
	if cte != "" {
		sql.WriteString(cte)
	}
	sql.WriteString("SELECT\n  ")
	sql.WriteString(xSel + ",\n  " + ySel + ",\n  " + zSel + ",\n  R1.object_id,\n  O.file_uri,\n  O.thumbnail_uri")
	sql.WriteString(mid)

	if useLateralMediaJoin {
		sql.WriteString("  JOIN LATERAL (\n SELECT m.file_uri, m.thumbnail_uri\n FROM medias m\n WHERE m.id = R1.object_id\n) O ON true;\n")
	} else {
		sql.WriteString("  JOIN medias O ON O.id = R1.object_id;\n")
	}

	return sql.String()
}

// -------- PREVIEW (Phase 1) for STATE --------
func GeneratePreviewSQLForStateIncremental2(
	axisOrder []string,
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
	limit int,
) string {
	base := GenerateSQLQueryForState(
		axisOrder,
		xType, xVertexID,
		yType, yVertexID,
		zType, zVertexID,
		filtersList,
	)
	if limit > 0 {
		if strings.HasSuffix(base, ";") {
			base = base[:len(base)-1]
		}
		return fmt.Sprintf("%s LIMIT %d;", base, limit)
	}
	return base
}

// -------- ID collectors for STATE semantics --------

// Axis ID SQL for *state* semantics:
// - node axis -> subtree: parentnode_id = <axisId>, yielding (object_id, node_id)
// - tagset axis -> tagset_id = <axisId>, yielding (object_id, tag_id)
// We only need object_id sets for intersection; use the minimal form.
func BuildAxisObjectIDSQLForState(axisType string, vertexID int) (string, error) {
	switch axisType {
	case "":
		return "", nil
	case "node":
		return fmt.Sprintf(
			"SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.parentnode_id = %d",
			vertexID,
		), nil
	case "tagset":
		return fmt.Sprintf(
			"SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d",
			vertexID,
		), nil
	default:
		return "", fmt.Errorf("unsupported axis type for state: %s", axisType)
	}
}

// Existing BuildFilterIDSQL works (filters intersect on object_id).
// (We reuse your earlier BuildFilterIDSQL; if you didn’t keep it, copy from previous message.)

// -------- STATE grouping restricted by an ID set (Phase 2) --------

// Build a state-style grouped query but restricted to a filtered ID set.
// We inject a CTE `WITH f(object_id) AS (VALUES (...))` and add
// `AND R*.object_id IN (SELECT object_id FROM f)` in each branch,
// so the grouping only scans over the filtered domain.
//
//	x, y, z, id (representative), fileURI, thumbnailURI, count
func BuildStateSQLRestrictedByIDs(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
	ids []int,
) (string, error) {
	if len(ids) == 0 {
		return "SELECT 1 AS x,1 AS y,1 AS z, NULL AS id, NULL AS fileURI, NULL AS thumbnailURI, 0 AS count WHERE 1=0;\n", nil
	}

	cte := prettyValuesCTE(ids)

	type br struct {
		sql   string
		isDim bool
		ax    string
	}
	branches := make([]br, 0, 3+len(filtersList))

	addAxis := func(axisType string, vertexID int, ax string) error {
		if axisType == "" {
			return nil
		}
		switch axisType {
		case "node":
			branches = append(branches, br{
				sql:   fmt.Sprintf("SELECT N.object_id, N.node_id AS id\nFROM nodes_taggings N\nWHERE N.parentnode_id = %d%s", vertexID, addRestrict("N")),
				isDim: true, ax: ax,
			})
		case "tagset":
			branches = append(branches, br{
				sql:   fmt.Sprintf("SELECT T.object_id, T.tag_id AS id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d%s", vertexID, addRestrict("T")),
				isDim: true, ax: ax,
			})
		default:
			return fmt.Errorf("unsupported %s axis type for state: %s", ax, axisType)
		}
		return nil
	}

	if err := addAxis(xType, xVertexID, "x"); err != nil {
		return "", err
	}
	if err := addAxis(yType, yVertexID, "y"); err != nil {
		return "", err
	}
	if err := addAxis(zType, zVertexID, "z"); err != nil {
		return "", err
	}

	for _, flt := range filtersList {
		switch flt.Type {
		case "node":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id = %d%s", flt.Ids[0], addRestrict("N")),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id IN %s%s", generateIdList(flt), addRestrict("N")),
				})
			}
		case "tagset":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d%s", flt.Ids[0], addRestrict("T")),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id IN %s%s", generateIdList(flt), addRestrict("T")),
				})
			}
		case "tag":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id = %d%s", flt.Ids[0], addRestrict("R")),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id IN %s%s", generateIdList(flt), addRestrict("R")),
				})
			}
		case "numrange", "alpharange", "daterange", "timerange", "timestamprange":
			var tableName, quote string
			switch flt.Type {
			case "numrange":
				tableName, quote = "numerical_tags", ""
			case "alpharange":
				tableName, quote = "alphanumerical_tags", "'"
			case "daterange":
				tableName, quote = "date_tags", "'"
			case "timerange":
				tableName, quote = "time_tags", "'"
			case "timestamprange":
				tableName, quote = "timestamp_tags", "'"
			}
			cond := generateRangeList(flt, quote)
			branches = append(branches, br{
				sql: fmt.Sprintf("SELECT R.object_id\nFROM %s T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s%s", tableName, cond, addRestrict("R")),
			})
		default:
			return "", fmt.Errorf("unsupported filter type: %s", flt.Type)
		}
	}

	if len(branches) == 0 {
		return cte + "SELECT 1 AS x,1 AS y,1 AS z, NULL AS id, NULL AS fileURI, NULL AS thumbnailURI, 0 AS count WHERE 1=0;\n", nil
	}

	// shape for prettyJoinChain
	tmp := make([]joinBranch, len(branches))
	for i := range branches {
		tmp[i] = joinBranch{sql: branches[i].sql, ax: branches[i].ax}
	}
	mid, _ := prettyJoinChain(tmp)

	// SELECT fields provider
	xSel, ySel, zSel := "1 AS idx", "1 AS idy", "1 AS idz"

	for i, b := range branches {
		if !b.isDim {
			continue
		}
		alias := fmt.Sprintf("R%d", i+1)
		switch b.ax {
		case "x":
			xSel = alias + ".id AS idx"
		case "y":
			ySel = alias + ".id AS idy"
		case "z":
			zSel = alias + ".id AS idz"
		}
	}

	var front, end strings.Builder
	front.WriteString(cte)
	front.WriteString("SELECT\n")
	front.WriteString("  X.idx AS x,\n  X.idy AS y,\n  X.idz AS z,\n")
	front.WriteString("  X.object_id AS id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n")
	front.WriteString("  X.cnt AS count\nFROM (\n  SELECT\n    ")
	front.WriteString(xSel + ",\n    " + ySel + ",\n    " + zSel + ",\n")
	front.WriteString("    MAX(R1.object_id) AS object_id,\n")
	front.WriteString("    COUNT(DISTINCT R1.object_id) AS cnt")
	end.WriteString("\n  GROUP BY idx, idy, idz\n) X\nJOIN medias O ON X.object_id = O.id;\n")

	return front.String() + mid + end.String(), nil
}

// SQL to fetch object_id sets for a single additional filter
func BuildFilterIDSQL(filter ParsedFilter) (string, error) {
	switch filter.Type {
	case "node":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf("SELECT N.object_id FROM nodes_taggings N WHERE N.node_id = %d", filter.Ids[0]), nil
		}
		return fmt.Sprintf("SELECT N.object_id FROM nodes_taggings N WHERE N.node_id IN %s", generateIdList(filter)), nil

	case "tagset":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf("SELECT T.object_id FROM tagsets_taggings T WHERE T.tagset_id = %d", filter.Ids[0]), nil
		}
		return fmt.Sprintf("SELECT T.object_id FROM tagsets_taggings T WHERE T.tagset_id IN %s", generateIdList(filter)), nil

	case "tag":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf("SELECT R.object_id FROM taggings R WHERE R.tag_id = %d", filter.Ids[0]), nil
		}
		return fmt.Sprintf("SELECT R.object_id FROM taggings R WHERE R.tag_id IN %s", generateIdList(filter)), nil

	case "numrange", "alpharange", "daterange", "timerange", "timestamprange":
		var tableName, quote string
		switch filter.Type {
		case "numrange":
			tableName, quote = "numerical_tags", ""
		case "alpharange":
			tableName, quote = "alphanumerical_tags", "'"
		case "daterange":
			tableName, quote = "date_tags", "'"
		case "timerange":
			tableName, quote = "time_tags", "'"
		case "timestamprange":
			tableName, quote = "timestamp_tags", "'"
		}
		cond := generateRangeList(filter, quote)
		return fmt.Sprintf(
			"SELECT R.object_id FROM %s T JOIN taggings R ON T.id = R.tag_id WHERE %s",
			tableName, cond,
		), nil

	default:
		return "", fmt.Errorf("unsupported filter type: %s", filter.Type)
	}
}

func GenerateSQLQueryForState(
	filterOrder []string,
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	numberOfAdditionalFilters := len(filtersList)

	// No axes/filters => simple base query
	if xType == "" && yType == "" && zType == "" && numberOfAdditionalFilters == 0 {
		return "" +
			"SELECT\n" +
			"  X.idx AS x,\n  X.idy AS y,\n  X.idz AS z,\n" +
			"  X.object_id AS id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n" +
			"  X.cnt AS count\n" +
			"FROM (\n" +
			"  SELECT 1 AS idx, 1 AS idy, 1 AS idz,\n" +
			"         MAX(R1.id) AS object_id,\n" +
			"         COUNT(*) AS cnt\n" +
			"  FROM medias R1\n" +
			"  GROUP BY idx, idy, idz\n" +
			") X\nJOIN medias O ON X.object_id = O.id;\n"
	}

	type branch struct {
		sql   string
		isDim bool
		ax    string
	}
	branches := make([]branch, 0, 3+numberOfAdditionalFilters)

	// Axis branches
	addAxis := func(axisType string, vertexID int, ax string) {
		if axisType == "" {
			return
		}
		switch axisType {
		case "node":
			branches = append(branches, branch{
				sql:   fmt.Sprintf("SELECT N.object_id, N.node_id AS id\nFROM nodes_taggings N\nWHERE N.parentnode_id = %d", vertexID),
				isDim: true, ax: ax,
			})
		case "tagset":
			branches = append(branches, branch{
				sql:   fmt.Sprintf("SELECT T.object_id, T.tag_id AS id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d", vertexID),
				isDim: true, ax: ax,
			})
		}
	}

	addFilters := func() {
		// Filter branches (object_id only)
		for _, f := range filtersList {
			switch f.Type {
			case "node":
				if len(f.Ids) == 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id = %d", f.Ids[0])})
				} else if len(f.Ids) > 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id IN %s", generateIdList(f))})
				}
			case "tagset":
				if len(f.Ids) == 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d", f.Ids[0])})
				} else if len(f.Ids) > 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id IN %s", generateIdList(f))})
				}
			case "tag":
				if len(f.Ids) == 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id = %d", f.Ids[0])})
				} else if len(f.Ids) > 1 {
					branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id IN %s", generateIdList(f))})
				}
			case "numrange":
				branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM numerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, ""))})
			case "alpharange":
				branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM alphanumerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'"))})
			case "daterange":
				branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM date_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'"))})
			case "timerange":
				branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM time_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'"))})
			case "timestamprange":
				branches = append(branches, branch{sql: fmt.Sprintf("SELECT R.object_id\nFROM timestamp_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'"))})
			}
		}
	}

	for _, axisFilterType := range filterOrder {
		switch axisFilterType {
		case "x":
			addAxis(xType, xVertexID, "x")
		case "y":
			addAxis(yType, yVertexID, "y")
		case "z":
			addAxis(zType, zVertexID, "z")
		case "filter":
			addFilters()
		}
	}

	if len(branches) == 0 {
		return "" +
			"SELECT\n" +
			"  X.idx AS x,\n  X.idy AS y,\n  X.idz AS z,\n" +
			"  X.object_id AS id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n" +
			"  X.cnt AS count\n" +
			"FROM (\n" +
			"  SELECT 1 AS idx, 1 AS idy, 1 AS idz,\n" +
			"         MAX(R1.id) AS object_id,\n" +
			"         COUNT(*) AS cnt\n" +
			"  FROM medias R1\n" +
			"  GROUP BY idx, idy, idz\n" +
			") X\nJOIN medias O ON X.object_id = O.id;\n"
	}

	// shape for prettyJoinChain
	tmp := make([]joinBranch, len(branches))
	for i := range branches {
		tmp[i] = joinBranch{sql: branches[i].sql, ax: branches[i].ax}
	}
	mid, _ := prettyJoinChain(tmp)

	// axis select providers
	xSel, ySel, zSel := "1 AS idx", "1 AS idy", "1 AS idz"
	for i, b := range branches {
		if !b.isDim {
			continue
		}
		alias := fmt.Sprintf("R%d", i+1)
		switch b.ax {
		case "x":
			xSel = alias + ".id AS idx"
		case "y":
			ySel = alias + ".id AS idy"
		case "z":
			zSel = alias + ".id AS idz"
		}
	}

	var front, end strings.Builder
	front.WriteString("SELECT\n")
	front.WriteString("  X.idx AS x,\n  X.idy AS y,\n  X.idz AS z,\n")
	front.WriteString("  X.object_id AS id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n")
	front.WriteString("  X.cnt AS count\nFROM (\n")
	front.WriteString("  SELECT\n    " + xSel + ",\n    " + ySel + ",\n    " + zSel + ",\n")
	front.WriteString("    MAX(R1.object_id) AS object_id,\n")
	front.WriteString("    COUNT(DISTINCT R1.object_id) AS cnt")

	end.WriteString("\n  GROUP BY idx, idy, idz\n) X\nJOIN medias O ON X.object_id = O.id;\n")

	return front.String() + mid + end.String()
}

// GenerateSQLQueryForCell builds the SQL for the “cell” endpoint.
// It returns a DISTINCT list of medias (id, file_uri, thumbnail_uri) with a timestamp tag “T”
func GenerateSQLQueryForCell(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	branches := make([]string, 0, 3+len(filtersList))

	axis := func(axisType string, vertexID int) {
		switch axisType {
		case "node":
			branches = append(branches, fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id = %d", vertexID))
		case "tag":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id = %d", vertexID))
		case "tagset":
			branches = append(branches, fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d", vertexID))
		}
	}
	if xType != "" {
		axis(xType, xVertexID)
	}
	if yType != "" {
		axis(yType, yVertexID)
	}
	if zType != "" {
		axis(zType, zVertexID)
	}

	for _, f := range filtersList {
		switch f.Type {
		case "node":
			if len(f.Ids) == 1 {
				branches = append(branches, fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches, fmt.Sprintf("SELECT N.object_id\nFROM nodes_taggings N\nWHERE N.node_id IN %s", generateIdList(f)))
			}
		case "tagset":
			if len(f.Ids) == 1 {
				branches = append(branches, fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches, fmt.Sprintf("SELECT T.object_id\nFROM tagsets_taggings T\nWHERE T.tagset_id IN %s", generateIdList(f)))
			}
		case "tag":
			if len(f.Ids) == 1 {
				branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM taggings R\nWHERE R.tag_id IN %s", generateIdList(f)))
			}
		case "numrange":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM numerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "")))
		case "alpharange":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM alphanumerical_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'")))
		case "daterange":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM date_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'")))
		case "timerange":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM time_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'")))
		case "timestamprange":
			branches = append(branches, fmt.Sprintf("SELECT R.object_id\nFROM timestamp_tags T\nJOIN taggings R ON T.id = R.tag_id\nWHERE %s", generateRangeList(f, "'")))
		}
	}

	if len(branches) == 0 {
		return "SELECT O.id AS Id, O.file_uri AS fileURI, O.thumbnail_uri AS thumbnailURI\nFROM medias O;\n"
	}

	// join chain for object_id intersection
	tmp := make([]joinBranch, len(branches))
	for i := range branches {
		// each branch is just a SQL fragment; no axis label for 'cell' query
		tmp[i] = joinBranch{sql: branches[i], ax: ""}
	}

	mid, _ := prettyJoinChain(tmp)
	mid = strings.Replace(mid, "\nFROM\n", "\nFROM (\n", 1) // open wrapper
	mid = mid[:len(mid)-1] + ")\n"                          // close wrapper after chain

	var front, end strings.Builder
	front.WriteString("SELECT DISTINCT\n")
	front.WriteString("  O.id AS Id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n  TS.name AS T\nFROM (\n  SELECT R1.object_id")
	end.WriteString("\n) X\nJOIN medias O      ON X.object_id = O.id\n" +
		"JOIN taggings R2    ON O.id = R2.object_id\n" +
		"JOIN timestamp_tags TS ON R2.tag_id = TS.id\n" +
		"JOIN tagsets S      ON TS.tagset_id = S.id\n" +
		"WHERE S.name = 'Timestamp UTC'\n" +
		"ORDER BY TS.name;\n")

	return front.String() + mid + end.String()
}

// GenerateSQLQueryForTimeline builds the SQL for the “timeline” endpoint.
func GenerateSQLQueryForTimeline(filtersList []ParsedFilter) string {
	if len(filtersList) != 1 {
		return "SELECT O.id AS Id, O.file_uri AS fileURI, O.thumbnail_uri AS thumbnailURI\nFROM medias O;\n"
	}
	id := filtersList[0].Ids[0]

	var sb strings.Builder
	sb.WriteString("SELECT\n  O.id AS Id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI,\n  TS1.name AS T\n")
	sb.WriteString("FROM medias O\n")
	sb.WriteString("JOIN taggings R1      ON O.id = R1.object_id\n")
	sb.WriteString("JOIN timestamp_tags TS1 ON R1.tag_id = TS1.id\n")
	sb.WriteString("JOIN tagsets S         ON TS1.tagset_id = S.id\n")
	sb.WriteString("JOIN timestamp_tags TS2 ON TS1.tagset_id = TS2.tagset_id\n")
	sb.WriteString("  AND TS1.name BETWEEN TS2.name - INTERVAL '30 minutes'\n")
	sb.WriteString("                      AND TS2.name + INTERVAL '30 minutes'\n")
	sb.WriteString("JOIN taggings R2       ON TS2.id = R2.tag_id\n")
	sb.WriteString("WHERE S.name = 'Timestamp UTC'\n")
	sb.WriteString("  AND R2.object_id = ")
	sb.WriteString(fmt.Sprint(id))
	sb.WriteString("\nORDER BY TS1.name;\n")
	return sb.String()
}

// Helpers
func generateAxisQueryForState(axisType string, vertexID, filterNum int) string {
	switch axisType {
	case "node":
		return fmt.Sprintf(
			" select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %d) R%d ",
			vertexID, filterNum,
		)
	case "tagset":
		return fmt.Sprintf(
			" select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %d) R%d ",
			vertexID, filterNum,
		)
	default:
		return ""
	}
}

func generateAxisQueryForCell(axisType string, vertexID, filterNum int) string {
	switch axisType {
	case "node":
		return fmt.Sprintf(
			" select N.object_id from nodes_taggings N where N.node_id = %d) R%d ",
			vertexID, filterNum,
		)
	case "tag":
		return fmt.Sprintf(
			" select R.object_id from taggings R where R.tag_id = %d) R%d ",
			vertexID, filterNum,
		)
	default:
		return ""
	}
}

func generateFilterQueryForState(filter ParsedFilter, filterNum int) string {
	switch filter.Type {
	case "node":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select N.object_id from nodes_taggings N where N.node_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select N.object_id from nodes_taggings N where N.node_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "tagset":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select T.object_id from tagsets_taggings T where T.tagset_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select T.object_id from tagsets_taggings T where T.tagset_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "tag":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select R.object_id from taggings R where R.tag_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select R.object_id from taggings R where R.tag_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "numrange":
		return fmt.Sprintf(
			" select R.object_id from numerical_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, ""), filterNum,
		)
	case "alpharange":
		return fmt.Sprintf(
			" select R.object_id from alphanumerical_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "daterange":
		return fmt.Sprintf(
			" select R.object_id from date_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "timerange":
		return fmt.Sprintf(
			" select R.object_id from time_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "timestamprange":
		return fmt.Sprintf(
			" select R.object_id from timestamp_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	default:
		return ""
	}
}

// generateFilterQueryForCell is identical to generateFilterQueryForState
func generateFilterQueryForCell(filter ParsedFilter, filterNum int) string {
	return generateFilterQueryForState(filter, filterNum)
}

func generateIdList(filter ParsedFilter) string {
	var b strings.Builder
	b.WriteString("(")
	for i, id := range filter.Ids {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%d", id))
	}
	b.WriteString(")")
	return b.String()
}

func generateRangeList(filter ParsedFilter, quote string) string {
	var b strings.Builder
	b.WriteString("(")
	for i := range filter.Ids {
		if i > 0 {
			b.WriteString(") or (")
		}
		min := filter.Ranges[i][0]
		max := filter.Ranges[i][1]
		b.WriteString(fmt.Sprintf(
			"T.tagset_id = %d and T.name between %s%s%s and %s%s%s",
			filter.Ids[i], quote, min, quote, quote, max, quote,
		))
	}
	b.WriteString(")")
	return b.String()
}
