package querygen

import (
	"fmt"
	"strings"
)

// GenerateUngroupedSQLForState builds a streaming-friendly join (no GROUP BY/ORDER BY).
// It intersects axis/filters by object_id and returns rows:
//
//	x_id, y_id, z_id, object_id, file_uri, thumbnail_uri
//
// Use notes:
//   - For getCellIncremental4: set branchDistinct=false (faster first-row), no restrictIDs; client aggregates.
//   - For getCellIncremental3: either use the same SQL and aggregate on the server as rows arrive,
//     or pass restrictIDs in chunks (from an earlier ID-intersection phase) to bound the join cost.
func GenerateUngroupedSQLForState(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filters []ParsedFilter,
) string {
	// ---- tunables (currently unused or defaulted) TODO: turn into function params ----
	branchDistinct := false // <- set true if you want DISTINCT inside branches
	var restrictIDs []int   // <- leave nil for full space; put chunk IDs for Incremental3

	type branch struct {
		sql string
		ax  string
	}

	// Helper for SELECT prefix per branch
	sel := func(cols ...string) string {
		if branchDistinct {
			return "select distinct " + strings.Join(cols, ", ")
		}
		return "select " + strings.Join(cols, ", ")
	}

	// Optional CTE for restricting by object_id (was used by chunked Incremental3)
	var cte string
	var inF string
	if len(restrictIDs) > 0 {
		var vb strings.Builder
		for i, id := range restrictIDs {
			if i > 0 {
				vb.WriteString(",")
			}
			vb.WriteString(fmt.Sprintf("(%d)", id))
		}
		cte = "WITH f(object_id) AS (VALUES " + vb.String() + ") "
		inF = " AND <alias>.object_id IN (SELECT object_id FROM f)"
	}

	branches := make([]branch, 0, 3+len(filters))

	addAxis := func(axisType string, vertexID int, ax string) {
		if axisType == "" {
			return
		}
		switch axisType {
		case "node":
			sql := sel("N.object_id", "N.node_id as id") +
				fmt.Sprintf(" from nodes_taggings N where N.parentnode_id = %d", vertexID)
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "N", 1)
			}
			branches = append(branches, branch{sql: sql, ax: ax})
		case "tagset":
			sql := sel("T.object_id", "T.tag_id as id") +
				fmt.Sprintf(" from tagsets_taggings T where T.tagset_id = %d", vertexID)
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "T", 1)
			}
			branches = append(branches, branch{sql: sql, ax: ax})
		}
	}

	addAxis(xType, xVertexID, "x")
	addAxis(yType, yVertexID, "y")
	addAxis(zType, zVertexID, "z")

	// Filters: only object_id
	for _, f := range filters {
		switch f.Type {
		case "node":
			if len(f.Ids) == 1 {
				sql := sel("N.object_id") + fmt.Sprintf(" from nodes_taggings N where N.node_id = %d", f.Ids[0])
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "N", 1)
				}
				branches = append(branches, branch{sql: sql})
			} else if len(f.Ids) > 1 {
				sql := sel("N.object_id") + fmt.Sprintf(" from nodes_taggings N where N.node_id in %s", generateIdList(f))
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "N", 1)
				}
				branches = append(branches, branch{sql: sql})
			}
		case "tagset":
			if len(f.Ids) == 1 {
				sql := sel("T.object_id") + fmt.Sprintf(" from tagsets_taggings T where T.tagset_id = %d", f.Ids[0])
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "T", 1)
				}
				branches = append(branches, branch{sql: sql})
			} else if len(f.Ids) > 1 {
				sql := sel("T.object_id") + fmt.Sprintf(" from tagsets_taggings T where T.tagset_id in %s", generateIdList(f))
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "T", 1)
				}
				branches = append(branches, branch{sql: sql})
			}
		case "tag":
			if len(f.Ids) == 1 {
				sql := sel("R.object_id") + fmt.Sprintf(" from taggings R where R.tag_id = %d", f.Ids[0])
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			} else if len(f.Ids) > 1 {
				sql := sel("R.object_id") + fmt.Sprintf(" from taggings R where R.tag_id in %s", generateIdList(f))
				if inF != "" {
					sql += strings.Replace(inF, "<alias>", "R", 1)
				}
				branches = append(branches, branch{sql: sql})
			}
		case "numrange":
			sql := sel("R.object_id") + " from numerical_tags T join taggings R on T.id = R.tag_id where " + generateRangeList(f, "")
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "R", 1)
			}
			branches = append(branches, branch{sql: sql})
		case "alpharange":
			sql := sel("R.object_id") + " from alphanumerical_tags T join taggings R on T.id = R.tag_id where " + generateRangeList(f, "'")
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "R", 1)
			}
			branches = append(branches, branch{sql: sql})
		case "daterange":
			sql := sel("R.object_id") + " from date_tags T join taggings R on T.id = R.tag_id where " + generateRangeList(f, "'")
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "R", 1)
			}
			branches = append(branches, branch{sql: sql})
		case "timerange":
			sql := sel("R.object_id") + " from time_tags T join taggings R on T.id = R.tag_id where " + generateRangeList(f, "'")
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "R", 1)
			}
			branches = append(branches, branch{sql: sql})
		case "timestamprange":
			sql := sel("R.object_id") + " from timestamp_tags T join taggings R on T.id = R.tag_id where " + generateRangeList(f, "'")
			if inF != "" {
				sql += strings.Replace(inF, "<alias>", "R", 1)
			}
			branches = append(branches, branch{sql: sql})
		}
	}

	// If no axes/filters: stream all medias (neutral axes)
	if len(branches) == 0 {
		return cte + "select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;"
	}

	// Assemble FROM ( ... ) R1 JOIN ( ... ) Rk ON Rk.object_id = R1.object_id
	var from strings.Builder
	from.WriteString(" from ")
	from.WriteString("(" + branches[0].sql + ") R1")

	axisAlias := map[string]string{} // "x"|"y"|"z" -> "Rk"
	if branches[0].ax != "" {
		axisAlias[branches[0].ax] = "R1"
	}

	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		from.WriteString(" join (")
		from.WriteString(branches[i].sql)
		from.WriteString(fmt.Sprintf(") %s on %s.object_id = R1.object_id", alias, alias))
		if branches[i].ax != "" {
			axisAlias[branches[i].ax] = alias
		}
	}

	// SELECT list: axis ids (or 1), object_id, media URIs
	xSel := "1 as x_id"
	ySel := "1 as y_id"
	zSel := "1 as z_id"
	if a, ok := axisAlias["x"]; ok {
		xSel = a + ".id as x_id"
	}
	if a, ok := axisAlias["y"]; ok {
		ySel = a + ".id as y_id"
	}
	if a, ok := axisAlias["z"]; ok {
		zSel = a + ".id as z_id"
	}

	var sql strings.Builder
	sql.WriteString(cte) // optional WITH f(...)
	sql.WriteString("select ")
	sql.WriteString(xSel + ", " + ySel + ", " + zSel + ", ")
	sql.WriteString("R1.object_id, O.file_uri, O.thumbnail_uri")
	sql.WriteString(from.String())
	sql.WriteString(" join medias O on O.id = R1.object_id")
	sql.WriteString(";")

	return sql.String()
}

// -------- PREVIEW (Phase 1) for STATE --------
func GeneratePreviewSQLForStateIncremental2(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
	limit int,
) string {
	base := GenerateSQLQueryForState(
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
		// subtree under parent
		return fmt.Sprintf(
			"SELECT N.object_id FROM nodes_taggings N WHERE N.parentnode_id = %d",
			vertexID,
		), nil
	case "tagset":
		return fmt.Sprintf(
			"SELECT T.object_id FROM tagsets_taggings T WHERE T.tagset_id = %d",
			vertexID,
		), nil
	default:
		// For state we only support node/tagset axes (like your generator).
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
// Output columns must match your state endpoint:
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
		return "select 1 as x,1 as y,1 as z, null as id, null as fileURI, null as thumbnailURI, 0 as count where 1=0;", nil
	}

	// Build VALUES list for CTE: WITH f(object_id) AS (VALUES (..),(..),...)
	var vb strings.Builder
	for i, id := range ids {
		if i > 0 {
			vb.WriteString(",")
		}
		vb.WriteString(fmt.Sprintf("(%d)", id))
	}
	values := vb.String()

	// Build branches: each branch is a subquery WITHOUT alias, later aliased as R1..Rn
	type br struct {
		sql   string // SELECT ... (object_id[, id])
		isDim bool   // true if provides ".id" for an axis
		ax    string // "x","y","z" for axis branches
	}
	branches := make([]br, 0, 3+len(filtersList))

	addAxis := func(axisType string, vertexID int, ax string) error {
		if axisType == "" {
			return nil
		}
		switch axisType {
		case "node":
			branches = append(branches, br{
				sql:   fmt.Sprintf("SELECT N.object_id, N.node_id AS id FROM nodes_taggings N WHERE N.parentnode_id = %d AND N.object_id IN (SELECT object_id FROM f)", vertexID),
				isDim: true, ax: ax,
			})
		case "tagset":
			branches = append(branches, br{
				sql:   fmt.Sprintf("SELECT T.object_id, T.tag_id AS id FROM tagsets_taggings T WHERE T.tagset_id = %d AND T.object_id IN (SELECT object_id FROM f)", vertexID),
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

	// Filters: only object_id
	for _, flt := range filtersList {
		switch flt.Type {
		case "node":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT N.object_id FROM nodes_taggings N WHERE N.node_id = %d AND N.object_id IN (SELECT object_id FROM f)", flt.Ids[0]),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT N.object_id FROM nodes_taggings N WHERE N.node_id IN %s AND N.object_id IN (SELECT object_id FROM f)", generateIdList(flt)),
				})
			}
		case "tagset":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT T.object_id FROM tagsets_taggings T WHERE T.tagset_id = %d AND T.object_id IN (SELECT object_id FROM f)", flt.Ids[0]),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT T.object_id FROM tagsets_taggings T WHERE T.tagset_id IN %s AND T.object_id IN (SELECT object_id FROM f)", generateIdList(flt)),
				})
			}
		case "tag":
			if len(flt.Ids) == 1 {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT R.object_id FROM taggings R WHERE R.tag_id = %d AND R.object_id IN (SELECT object_id FROM f)", flt.Ids[0]),
				})
			} else {
				branches = append(branches, br{
					sql: fmt.Sprintf("SELECT R.object_id FROM taggings R WHERE R.tag_id IN %s AND R.object_id IN (SELECT object_id FROM f)", generateIdList(flt)),
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
				sql: fmt.Sprintf(
					"SELECT R.object_id FROM %s T JOIN taggings R ON T.id = R.tag_id WHERE %s AND R.object_id IN (SELECT object_id FROM f)",
					tableName, cond),
			})
		default:
			return "", fmt.Errorf("unsupported filter type: %s", flt.Type)
		}
	}

	if len(branches) == 0 {
		// Shouldn’t happen (ids were non-empty), but keep a safe fallback:
		return "" +
			"WITH f(object_id) AS (VALUES " + values + ") " +
			"SELECT 1 AS x,1 AS y,1 AS z, NULL AS id, NULL AS fileURI, NULL AS thumbnailURI, 0 AS count WHERE 1=0;", nil
	}

	// Assemble FROM chain: (subquery) R1 [JOIN (subquery) Rk ON Rk.object_id = R1.object_id]...
	var mid strings.Builder
	mid.WriteString(" FROM ")
	mid.WriteString("(" + branches[0].sql + ") R1")
	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		mid.WriteString(" JOIN (")
		mid.WriteString(branches[i].sql)
		mid.WriteString(fmt.Sprintf(") %s ON %s.object_id = R1.object_id", alias, alias))
	}

	// Map axis selects
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

	// Final SQL
	var front, end strings.Builder
	front.WriteString(
		"WITH f(object_id) AS (VALUES " + values + ") " +
			"SELECT X.idx AS x, X.idy AS y, X.idz AS z, X.object_id AS id, " +
			"O.file_uri AS fileURI, O.thumbnail_uri AS thumbnailURI, X.cnt AS count " +
			"FROM (SELECT ",
	)
	front.WriteString(xSel + ", " + ySel + ", " + zSel + ", ")
	front.WriteString("MAX(R1.object_id) AS object_id, COUNT(DISTINCT R1.object_id) AS cnt")

	end.WriteString(" GROUP BY idx, idy, idz) X JOIN medias O ON X.object_id = O.id;")

	return front.String() + mid.String() + end.String(), nil
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
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	numberOfAdditionalFilters := len(filtersList)

	// If there are no axes and no filters, return the simple base query
	if xType == "" && yType == "" && zType == "" && numberOfAdditionalFilters == 0 {
		return "" +
			"select X.idx as x, X.idy as y, X.idz as z, X.object_id as id, " +
			"O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, X.cnt as count " +
			"from (select 1 as idx, 1 as idy, 1 as idz, max(R1.id) as object_id, count(*) as cnt " +
			"from medias R1 group by idx, idy, idz) X " +
			"join medias O on X.object_id = O.id;"
	}

	type branch struct {
		sql   string // subquery without alias
		isDim bool   // axis branch provides ".id"
		ax    string // "x","y","z" if isDim
	}
	branches := make([]branch, 0, 3+numberOfAdditionalFilters)

	// --- Axis branches (STATE semantics: (object_id, id)) ---
	addAxis := func(axisType string, vertexID int, ax string) {
		if axisType == "" {
			return
		}
		switch axisType {
		case "node":
			branches = append(branches, branch{
				sql:   fmt.Sprintf("select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %d", vertexID),
				isDim: true, ax: ax,
			})
		case "tagset":
			branches = append(branches, branch{
				sql:   fmt.Sprintf("select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %d", vertexID),
				isDim: true, ax: ax,
			})
		}
	}
	addAxis(xType, xVertexID, "x")
	addAxis(yType, yVertexID, "y")
	addAxis(zType, zVertexID, "z")

	// --- Filter branches (STATE: only object_id) ---
	for _, f := range filtersList {
		switch f.Type {
		case "node":
			if len(f.Ids) == 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id = %d", f.Ids[0])})
			} else if len(f.Ids) > 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id in %s", generateIdList(f))})
			}
		case "tagset":
			if len(f.Ids) == 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id = %d", f.Ids[0])})
			} else if len(f.Ids) > 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id in %s", generateIdList(f))})
			}
		case "tag":
			if len(f.Ids) == 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from taggings R where R.tag_id = %d", f.Ids[0])})
			} else if len(f.Ids) > 1 {
				branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from taggings R where R.tag_id in %s", generateIdList(f))})
			}
		case "numrange":
			branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from numerical_tags T join taggings R on T.id = R.tag_id where %s", generateRangeList(f, ""))})
		case "alpharange":
			branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from alphanumerical_tags T join taggings R on T.id = R.tag_id where %s", generateRangeList(f, "'"))})
		case "daterange":
			branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from date_tags T join taggings R on T.id = R.tag_id where %s", generateRangeList(f, "'"))})
		case "timerange":
			branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from time_tags T join taggings R on T.id = R.tag_id where %s", generateRangeList(f, "'"))})
		case "timestamprange":
			branches = append(branches, branch{sql: fmt.Sprintf("select R.object_id from timestamp_tags T join taggings R on T.id = R.tag_id where %s", generateRangeList(f, "'"))})
		}
	}

	// Fallback (shouldn’t happen due to earlier guard)
	if len(branches) == 0 {
		return "" +
			"select X.idx as x, X.idy as y, X.idz as z, X.object_id as id, " +
			"O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, X.cnt as count " +
			"from (select 1 as idx, 1 as idy, 1 as idz, max(R1.id) as object_id, count(*) as cnt " +
			"from medias R1 group by idx, idy, idz) X " +
			"join medias O on X.object_id = O.id;"
	}

	// --- Build FROM chain WITHOUT wrapping in extra parentheses ---
	var mid strings.Builder
	mid.WriteString(" from ")
	// R1
	mid.WriteString("(" + branches[0].sql + ") R1")
	// R2..Rn
	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		mid.WriteString(" join (")
		mid.WriteString(branches[i].sql)
		mid.WriteString(fmt.Sprintf(") %s on %s.object_id = R1.object_id", alias, alias))
	}
	// NOTE: no trailing ")"

	// --- Map which alias provides idx/idy/idz ---
	xSel, ySel, zSel := "1 as idx", "1 as idy", "1 as idz"
	for i, b := range branches {
		if !b.isDim {
			continue
		}
		alias := fmt.Sprintf("R%d", i+1)
		switch b.ax {
		case "x":
			xSel = alias + ".id as idx"
		case "y":
			ySel = alias + ".id as idy"
		case "z":
			zSel = alias + ".id as idz"
		}
	}

	// --- Assemble final SQL ---
	var front, end strings.Builder
	front.WriteString(
		"select X.idx as x, X.idy as y, X.idz as z, X.object_id as id, " +
			"O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, X.cnt as count from (select ",
	)
	front.WriteString(xSel + ", " + ySel + ", " + zSel + ", ")
	front.WriteString("max(R1.object_id) as object_id, count(distinct R1.object_id) as cnt")

	end.WriteString(" group by idx, idy, idz) X join medias O on X.object_id = O.id;")

	return front.String() + mid.String() + end.String()
}

// GenerateSQLQueryForCell builds the SQL for the “cell” endpoint.
// It returns a DISTINCT list of medias (id, file_uri, thumbnail_uri) with a timestamp tag “T”
// (as in your original cell query), optionally constrained by axes and filters.
func GenerateSQLQueryForCell(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	// Collect subqueries that each produce a single column: object_id
	branches := make([]string, 0, 3+len(filtersList))

	// Axis -> object_id branches (cell semantics)
	if xType != "" {
		switch xType {
		case "node":
			// cell semantics for node axis = exact node_id
			branches = append(branches,
				fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id = %d", xVertexID))
		case "tag":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from taggings R where R.tag_id = %d", xVertexID))
		case "tagset":
			branches = append(branches,
				fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id = %d", xVertexID))
		}
	}
	if yType != "" {
		switch yType {
		case "node":
			branches = append(branches,
				fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id = %d", yVertexID))
		case "tag":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from taggings R where R.tag_id = %d", yVertexID))
		case "tagset":
			branches = append(branches,
				fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id = %d", yVertexID))
		}
	}
	if zType != "" {
		switch zType {
		case "node":
			branches = append(branches,
				fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id = %d", zVertexID))
		case "tag":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from taggings R where R.tag_id = %d", zVertexID))
		case "tagset":
			branches = append(branches,
				fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id = %d", zVertexID))
		}
	}

	// Filters -> object_id branches
	for _, f := range filtersList {
		switch f.Type {
		case "node":
			if len(f.Ids) == 1 {
				branches = append(branches,
					fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches,
					fmt.Sprintf("select N.object_id from nodes_taggings N where N.node_id in %s", generateIdList(f)))
			}
		case "tagset":
			if len(f.Ids) == 1 {
				branches = append(branches,
					fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches,
					fmt.Sprintf("select T.object_id from tagsets_taggings T where T.tagset_id in %s", generateIdList(f)))
			}
		case "tag":
			if len(f.Ids) == 1 {
				branches = append(branches,
					fmt.Sprintf("select R.object_id from taggings R where R.tag_id = %d", f.Ids[0]))
			} else if len(f.Ids) > 1 {
				branches = append(branches,
					fmt.Sprintf("select R.object_id from taggings R where R.tag_id in %s", generateIdList(f)))
			}
		case "numrange":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from numerical_tags T join taggings R on T.id = R.tag_id where %s",
					generateRangeList(f, "")))
		case "alpharange":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from alphanumerical_tags T join taggings R on T.id = R.tag_id where %s",
					generateRangeList(f, "'")))
		case "daterange":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from date_tags T join taggings R on T.id = R.tag_id where %s",
					generateRangeList(f, "'")))
		case "timerange":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from time_tags T join taggings R on T.id = R.tag_id where %s",
					generateRangeList(f, "'")))
		case "timestamprange":
			branches = append(branches,
				fmt.Sprintf("select R.object_id from timestamp_tags T join taggings R on T.id = R.tag_id where %s",
					generateRangeList(f, "'")))
		}
	}

	// If no branches at all: simple list
	if len(branches) == 0 {
		return "select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI from medias O;"
	}

	// Build the intersecting FROM (...) R1 JOIN (...) R2 ON R2.object_id = R1.object_id ...
	var mid strings.Builder
	mid.WriteString(" from (")
	// R1
	mid.WriteString("(" + branches[0] + ") R1")
	// R2..Rn
	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		mid.WriteString(" join (")
		mid.WriteString(branches[i])
		mid.WriteString(fmt.Sprintf(") %s on %s.object_id = R1.object_id", alias, alias))
	}
	mid.WriteString(")")

	// Full query (same shape as your original “cell” endpoint)
	var front, end strings.Builder
	front.WriteString(
		"select distinct O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, TS.name as T from (select R1.object_id ",
	)
	end.WriteString(
		") X join medias O on X.object_id = O.id " +
			"join taggings R2 on O.id = R2.object_id " +
			"join timestamp_tags TS on R2.tag_id = TS.id " +
			"join tagsets S on TS.tagset_id = S.id " +
			"where S.name = 'Timestamp UTC' order by TS.name;",
	)

	return front.String() + mid.String() + end.String()
}

// GenerateSQLQueryForTimeline builds the SQL for the “timeline” endpoint.
func GenerateSQLQueryForTimeline(filtersList []ParsedFilter) string {
	totalNumberOfFilters := len(filtersList)

	// If not exactly one filter, return simple media list
	if totalNumberOfFilters != 1 {
		return "select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI from medias O;"
	}

	var front, middle, end strings.Builder
	front.WriteString(
		"select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, TS1.name as T ",
	)
	middle.WriteString(
		"from medias O join taggings R1 on O.id = R1.object_id " +
			"join timestamp_tags TS1 on R1.tag_id = TS1.id " +
			"join tagsets S on TS1.tagset_id = S.id " +
			"join timestamp_tags TS2 on TS1.tagset_id = TS2.tagset_id " +
			"and TS1.name between TS2.name - interval '30 minutes' " +
			"and TS2.name + interval '30 minutes' " +
			"join taggings R2 on TS2.id = R2.tag_id where S.name = 'Timestamp UTC' and R2.object_id = ",
	)
	end.WriteString(" order by TS1.name;")

	// Exactly one filter
	return front.String() + middle.String() + fmt.Sprintf("%d", filtersList[0].Ids[0]) + end.String()
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
