package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

// -----------------------------------------------------------------------------
// MetaDataCube-Client_2024 compatibility layer (REST)
//
// The Angular client expects the C# server's REST shapes and casing conventions.
// These handlers provide compatible endpoints on top of the Go server.
// -----------------------------------------------------------------------------

type compatTagsetListItem struct {
	Id        int64  `json:"id"`
	Name      string `json:"name"`
	TagTypeId int64  `json:"tagTypeId"`
}

type compatTagsetTag struct {
	Id       int64  `json:"id"`
	Name     string `json:"name"`
	TagsetId int64  `json:"tagsetId"`
}

type compatTagsetHierarchy struct {
	Id         int64  `json:"id"`
	Name       string `json:"name"`
	TagsetId   int64  `json:"tagsetId"`
	RootNodeId int64  `json:"rootNodeId"`
}

type compatTagsetDetail struct {
	Id          int64                   `json:"id"`
	Name        string                  `json:"name"`
	Tags        []compatTagsetTag       `json:"tags"`
	Hierarchies []compatTagsetHierarchy `json:"hierarchies"`
}

type compatNodeChild struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type compatHierarchyTag struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type compatHierarchyNode struct {
	Id       int64                  `json:"id"`
	Tag      compatHierarchyTag     `json:"tag"`
	Children []*compatHierarchyNode `json:"children,omitempty"`
}

type compatHierarchyResponse struct {
	Id         int64                  `json:"id"`
	Name       string                 `json:"name"`
	TagsetId   int64                  `json:"tagsetId"`
	RootNodeId int64                  `json:"rootNodeId"`
	Nodes      []*compatHierarchyNode `json:"nodes"`
}

type compatCubeObject struct {
	Id           int32  `json:"id"`
	FileURI      string `json:"fileURI"`
	ThumbnailURI string `json:"thumbnailURI"`

	// Also expose the proto-style casing (helps other consumers).
	FileUri      string `json:"fileUri,omitempty"`
	ThumbnailUri string `json:"thumbnailUri,omitempty"`
}

type compatBrowsingStateResponse struct {
	X           int32              `json:"x"`
	Y           int32              `json:"y"`
	Z           int32              `json:"z"`
	Count       int32              `json:"count"`
	CubeObjects []compatCubeObject `json:"cubeObjects"`
}

type compatBucketInfo struct {
	BucketID   int32   `json:"bucketId"`
	LowerBound float32 `json:"lowerBound"`
	UpperBound float32 `json:"upperBound"`
	Label      string  `json:"label"`
}

type compatBrowsingStateEnvelope struct {
	BucketInfos []compatBucketInfo            `json:"bucketInfos"`
	Cells       []compatBrowsingStateResponse `json:"cells"`
}

type compatCubeObjectTag struct {
	TagsetName string `json:"tagsetName"`
	Name       string `json:"name"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parsePathInt64(r *http.Request, key string) (int64, error) {
	v := strings.TrimSpace(r.PathValue(key))
	if v == "" {
		return 0, fmt.Errorf("missing path value %q", key)
	}
	return strconv.ParseInt(v, 10, 64)
}

func tagDisplayNameSQL(exprPrefix string) string {
	// exprPrefix should include the trailing dot, e.g. "t." or "n." when referencing a tag id column.
	// We assume subtype tables are joined on the tag id.
	return fmt.Sprintf(
		`COALESCE(
			ant.name,
			to_char(tst.name, 'YYYY-MM-DD HH24:MI:SS'),
			to_char(tt.name,  'HH24:MI'),
			to_char(dt.name,  'YYYY-MM-DD'),
			nt.name::text
		)`,
	) + " /* " + exprPrefix + "id */"
}

// GetMetaDataCubeCompatTagsetsHandler serves GET /api/tagset as a plain JSON array.
func GetMetaDataCubeCompatTagsetsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var (
			args []any
			sb   strings.Builder
		)
		sb.WriteString("SELECT id, name, tagtype_id FROM public.tagsets")

		if v := strings.TrimSpace(r.URL.Query().Get("tagTypeId")); v != "" {
			if id, err := strconv.ParseInt(v, 10, 64); err == nil && id > 0 {
				sb.WriteString(" WHERE tagtype_id = $1")
				args = append(args, id)
			}
		}
		sb.WriteString(" ORDER BY name;")

		rows, err := db.QueryContext(r.Context(), sb.String(), args...)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		var out []compatTagsetListItem
		for rows.Next() {
			var item compatTagsetListItem
			if err := rows.Scan(&item.Id, &item.Name, &item.TagTypeId); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		writeJSON(w, http.StatusOK, out)
	}
}

// GetMetaDataCubeCompatTagsetDetailHandler serves GET /api/tagset/{id}.
func GetMetaDataCubeCompatTagsetDetailHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := parsePathInt64(r, "id")
		if err != nil || id <= 0 {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		var name string
		err = db.QueryRowContext(r.Context(), "SELECT name FROM public.tagsets WHERE id = $1", id).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "tagset not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}

		// Tags (match server axis ordering + formatting)
		tagSQL := `
			SELECT
				t.id,
				COALESCE(
					ant.name,
					to_char(tst.name, 'YYYY-MM-DD HH24:MI:SS'),
					to_char(tt.name,  'HH24:MI'),
					to_char(dt.name,  'YYYY-MM-DD'),
					nt.name::text
				) AS disp_name
			FROM public.tagsets s
			JOIN public.tags t ON t.tagset_id = s.id
			LEFT JOIN public.alphanumerical_tags ant ON ant.id = t.id
			LEFT JOIN public.timestamp_tags      tst ON tst.id = t.id
			LEFT JOIN public.time_tags           tt  ON tt.id = t.id
			LEFT JOIN public.date_tags           dt  ON dt.id = t.id
			LEFT JOIN public.numerical_tags      nt  ON nt.id = t.id
			WHERE s.id = $1
			  AND COALESCE(
					ant.name,
					to_char(tst.name, 'YYYY-MM-DD HH24:MI:SS'),
					to_char(tt.name,  'HH24:MI'),
					to_char(dt.name,  'YYYY-MM-DD'),
					nt.name::text
			  ) <> s.name
			ORDER BY disp_name;
		`
		tagRows, err := db.QueryContext(r.Context(), tagSQL, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer tagRows.Close()

		var tags []compatTagsetTag
		for tagRows.Next() {
			var t compatTagsetTag
			t.TagsetId = id
			if err := tagRows.Scan(&t.Id, &t.Name); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			tags = append(tags, t)
		}
		if err := tagRows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		// Hierarchies
		hRows, err := db.QueryContext(r.Context(), "SELECT id, name, tagset_id, rootnode_id FROM public.hierarchies WHERE tagset_id = $1 ORDER BY name;", id)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer hRows.Close()

		var hierarchies []compatTagsetHierarchy
		for hRows.Next() {
			var h compatTagsetHierarchy
			var root sql.NullInt64
			if err := hRows.Scan(&h.Id, &h.Name, &h.TagsetId, &root); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			if root.Valid {
				h.RootNodeId = root.Int64
			} else {
				// Best-effort fallback: use the hierarchy's parent-less node as root if present.
				var fallback sql.NullInt64
				_ = db.QueryRowContext(r.Context(), "SELECT id FROM public.nodes WHERE hierarchy_id = $1 AND parentnode_id IS NULL LIMIT 1;", h.Id).Scan(&fallback)
				if fallback.Valid {
					h.RootNodeId = fallback.Int64
				} else {
					h.RootNodeId = -1
				}
			}
			hierarchies = append(hierarchies, h)
		}
		if err := hRows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		out := compatTagsetDetail{
			Id:          id,
			Name:        name,
			Tags:        tags,
			Hierarchies: hierarchies,
		}
		writeJSON(w, http.StatusOK, out)
	}
}

// GetMetaDataCubeCompatHierarchyHandler serves GET /api/hierarchy/{id}.
// It returns nodes[0] as the full tree root (C# server shape).
func GetMetaDataCubeCompatHierarchyHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := parsePathInt64(r, "id")
		if err != nil || id <= 0 {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		var (
			name     string
			tagsetId int64
			root     sql.NullInt64
		)
		err = db.QueryRowContext(r.Context(), "SELECT name, tagset_id, rootnode_id FROM public.hierarchies WHERE id = $1", id).Scan(&name, &tagsetId, &root)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "hierarchy not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}

		nodeSQL := `
			SELECT
				n.id,
				n.tag_id,
				n.parentnode_id,
				COALESCE(
					ant.name,
					to_char(tst.name, 'YYYY-MM-DD HH24:MI:SS'),
					to_char(tt.name,  'HH24:MI'),
					to_char(dt.name,  'YYYY-MM-DD'),
					nt.name::text
				) AS tag_name
			FROM public.nodes n
			LEFT JOIN public.alphanumerical_tags ant ON ant.id = n.tag_id
			LEFT JOIN public.timestamp_tags      tst ON tst.id = n.tag_id
			LEFT JOIN public.time_tags           tt  ON tt.id = n.tag_id
			LEFT JOIN public.date_tags           dt  ON dt.id = n.tag_id
			LEFT JOIN public.numerical_tags      nt  ON nt.id = n.tag_id
			WHERE n.hierarchy_id = $1;
		`
		rows, err := db.QueryContext(r.Context(), nodeSQL, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		type row struct {
			nodeId   int64
			tagId    int64
			parentId sql.NullInt64
			tagName  string
		}

		nodesById := make(map[int64]*compatHierarchyNode)
		parentById := make(map[int64]sql.NullInt64)

		for rows.Next() {
			var rrow row
			if err := rows.Scan(&rrow.nodeId, &rrow.tagId, &rrow.parentId, &rrow.tagName); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			nodesById[rrow.nodeId] = &compatHierarchyNode{
				Id: rrow.nodeId,
				Tag: compatHierarchyTag{
					Id:   rrow.tagId,
					Name: rrow.tagName,
				},
			}
			parentById[rrow.nodeId] = rrow.parentId
		}
		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		// Attach children.
		var roots []*compatHierarchyNode
		for nodeID, node := range nodesById {
			parent := parentById[nodeID]
			if parent.Valid {
				if p, ok := nodesById[parent.Int64]; ok {
					p.Children = append(p.Children, node)
					continue
				}
			}
			roots = append(roots, node)
		}

		// Sort children recursively for stable UI.
		var sortTree func(n *compatHierarchyNode)
		sortTree = func(n *compatHierarchyNode) {
			if len(n.Children) == 0 {
				return
			}
			sort.Slice(n.Children, func(i, j int) bool {
				return n.Children[i].Tag.Name < n.Children[j].Tag.Name
			})
			for _, c := range n.Children {
				sortTree(c)
			}
		}
		for _, rnode := range roots {
			sortTree(rnode)
		}
		sort.Slice(roots, func(i, j int) bool { return roots[i].Tag.Name < roots[j].Tag.Name })

		var rootID int64 = -1
		if root.Valid {
			rootID = root.Int64
		} else if len(roots) == 1 {
			rootID = roots[0].Id
		}

		// Prefer the explicit root node when present (client uses nodes[0]).
		if rootID > 0 {
			if rn, ok := nodesById[rootID]; ok {
				roots = []*compatHierarchyNode{rn}
			}
		}

		out := compatHierarchyResponse{
			Id:         id,
			Name:       name,
			TagsetId:   tagsetId,
			RootNodeId: rootID,
			Nodes:      roots,
		}
		writeJSON(w, http.StatusOK, out)
	}
}

// GetMetaDataCubeCompatNodeChildrenHandler serves GET /api/node/{id}/Children (and /children).
// It returns the immediate children nodes ordered exactly as the browsing state coordinate mapping.
func GetMetaDataCubeCompatNodeChildrenHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		parentID, err := parsePathInt64(r, "id")
		if err != nil || parentID <= 0 {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		var hierarchyID int64
		err = db.QueryRowContext(r.Context(), "SELECT hierarchy_id FROM public.nodes WHERE id = $1", parentID).Scan(&hierarchyID)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}

		rows, err := db.QueryContext(r.Context(), `
			SELECT n.id, a.name
			FROM public.nodes n
			JOIN public.alphanumerical_tags a ON n.tag_id = a.id
			WHERE n.id IN (
				SELECT id
				FROM public.get_level_from_parent_node($1, $2)
			)
			ORDER BY a.name;
		`, parentID, hierarchyID)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		var out []compatNodeChild
		for rows.Next() {
			var c compatNodeChild
			if err := rows.Scan(&c.Id, &c.Name); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			out = append(out, c)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		writeJSON(w, http.StatusOK, out)
	}
}

// GetMetaDataCubeCompatCubeObjectTagsHandler serves GET /api/cubeobject/{id}/tags.
func GetMetaDataCubeCompatCubeObjectTagsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := parsePathInt64(r, "id")
		if err != nil || id <= 0 {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		rows, err := db.QueryContext(r.Context(), `
			SELECT
				ts.name AS tagset_name,
				COALESCE(
					ant.name,
					to_char(tst.name, 'YYYY-MM-DD HH24:MI:SS'),
					to_char(tt.name,  'HH24:MI'),
					to_char(dt.name,  'YYYY-MM-DD'),
					nt.name::text
				) AS tag_name
			FROM public.taggings t
			JOIN public.tags tg ON t.tag_id = tg.id
			JOIN public.tagsets ts ON tg.tagset_id = ts.id
			LEFT JOIN public.alphanumerical_tags ant ON t.tag_id = ant.id
			LEFT JOIN public.timestamp_tags      tst ON t.tag_id = tst.id
			LEFT JOIN public.time_tags           tt  ON t.tag_id = tt.id
			LEFT JOIN public.date_tags           dt  ON t.tag_id = dt.id
			LEFT JOIN public.numerical_tags      nt  ON t.tag_id = nt.id
			WHERE t.object_id = $1
			ORDER BY ts.name ASC, tag_name ASC;
		`, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		var out []compatCubeObjectTag
		for rows.Next() {
			var item compatCubeObjectTag
			if err := rows.Scan(&item.TagsetName, &item.Name); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}

		writeJSON(w, http.StatusOK, out)
	}
}

func objectIDBranchSQL(axisType string, ids []int) (string, error) {
	if len(ids) == 0 {
		if axisType == "objectid" {
			return "SELECT O.id AS object_id FROM public.medias O WHERE 1 = 0", nil
		}
		return "", nil
	}
	if axisType == "objectid" {
		if len(ids) <= 1000 {
			var b strings.Builder
			b.WriteString("SELECT unnest(ARRAY[")
			for i, id := range ids {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString(fmt.Sprint(id))
			}
			b.WriteString("]::integer[]) AS object_id")
			return b.String(), nil
		}

		var b strings.Builder
		b.WriteString("SELECT V.object_id FROM (VALUES ")
		for i, id := range ids {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf("(%d)", id))
		}
		b.WriteString(") AS V(object_id)")
		return b.String(), nil
	}
	if len(ids) == 1 {
		id := ids[0]
		switch axisType {
		case "node":
			return fmt.Sprintf("SELECT DISTINCT N.object_id FROM public.nodes_taggings N WHERE N.node_id = %d", id), nil
		case "tagset":
			return fmt.Sprintf("SELECT DISTINCT T.object_id FROM public.tagsets_taggings T WHERE T.tagset_id = %d", id), nil
		case "tag":
			return fmt.Sprintf("SELECT DISTINCT R.object_id FROM public.taggings R WHERE R.tag_id = %d", id), nil
		default:
			return "", fmt.Errorf("unsupported filter type %q", axisType)
		}
	}

	// IN-list
	var b strings.Builder
	b.WriteString("(")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprint(id))
	}
	b.WriteString(")")
	inList := b.String()

	switch axisType {
	case "node":
		return fmt.Sprintf("SELECT DISTINCT N.object_id FROM public.nodes_taggings N WHERE N.node_id IN %s", inList), nil
	case "tagset":
		return fmt.Sprintf("SELECT DISTINCT T.object_id FROM public.tagsets_taggings T WHERE T.tagset_id IN %s", inList), nil
	case "tag":
		return fmt.Sprintf("SELECT DISTINCT R.object_id FROM public.taggings R WHERE R.tag_id IN %s", inList), nil
	default:
		return "", fmt.Errorf("unsupported filter type %q", axisType)
	}
}

func rangeBranchSQL(filter qg.ParsedFilter) (string, error) {
	if len(filter.Ids) == 0 {
		return "", nil
	}
	if len(filter.Ranges) < len(filter.Ids) {
		return "", fmt.Errorf("invalid range filter: ranges length < ids length")
	}

	var (
		table string
		quote string
	)
	switch filter.Type {
	case "numrange":
		table = "public.numerical_tags"
		quote = ""
	case "alpharange":
		table = "public.alphanumerical_tags"
		quote = "'"
	case "daterange":
		table = "public.date_tags"
		quote = "'"
	case "timerange":
		table = "public.time_tags"
		quote = "'"
	case "timestamprange":
		table = "public.timestamp_tags"
		quote = "'"
	default:
		return "", fmt.Errorf("unsupported range type %q", filter.Type)
	}

	// Mirrors querygen.generateRangeList (kept here to avoid depending on unexported functions).
	var where strings.Builder
	where.WriteString("(")
	for i := range filter.Ids {
		if i > 0 {
			where.WriteString(") or (")
		}
		if len(filter.Ranges[i]) < 2 {
			return "", fmt.Errorf("invalid range filter: range[%d] must have 2 values", i)
		}
		min := filter.Ranges[i][0]
		max := filter.Ranges[i][1]
		where.WriteString(fmt.Sprintf(
			"T.tagset_id = %d and T.name between %s%s%s and %s%s%s",
			filter.Ids[i], quote, min, quote, quote, max, quote,
		))
	}
	where.WriteString(")")

	return fmt.Sprintf("SELECT DISTINCT R.object_id FROM %s T JOIN public.taggings R ON T.id = R.tag_id WHERE %s", table, where.String()), nil
}

func buildAllMediasSQL(axisX, axisY, axisZ qg.ParsedAxis, filters []qg.ParsedFilter) (string, error) {
	var branches []string

	addAxis := func(a qg.ParsedAxis) error {
		if a.Type == "" || a.Id <= 0 {
			return nil
		}
		branch, err := objectIDBranchSQL(a.Type, []int{a.Id})
		if err != nil {
			return err
		}
		if branch != "" {
			branches = append(branches, branch)
		}
		return nil
	}
	if err := addAxis(axisX); err != nil {
		return "", err
	}
	if err := addAxis(axisY); err != nil {
		return "", err
	}
	if err := addAxis(axisZ); err != nil {
		return "", err
	}

	for _, f := range filters {
		switch f.Type {
		case "node", "tagset", "tag", "objectid":
			branch, err := objectIDBranchSQL(f.Type, f.Ids)
			if err != nil {
				return "", err
			}
			if branch != "" {
				branches = append(branches, branch)
			}
		case "numrange", "alpharange", "daterange", "timerange", "timestamprange":
			branch, err := rangeBranchSQL(f)
			if err != nil {
				return "", err
			}
			if branch != "" {
				branches = append(branches, branch)
			}
		case "":
			// ignore
		default:
			return "", fmt.Errorf("unsupported filter type %q", f.Type)
		}
	}

	if len(branches) == 0 {
		return "SELECT O.id AS id, O.file_uri AS fileURI, O.thumbnail_uri AS thumbnailURI FROM public.medias O ORDER BY O.id;", nil
	}

	indent := func(s string) string {
		s = strings.TrimSpace(s)
		return "  " + strings.ReplaceAll(s, "\n", "\n  ")
	}

	var sb strings.Builder
	sb.WriteString("SELECT DISTINCT\n  O.id AS id,\n  O.file_uri AS fileURI,\n  O.thumbnail_uri AS thumbnailURI\n")

	// join chain on object_id
	sb.WriteString("FROM (\n")
	sb.WriteString(indent(branches[0]))
	sb.WriteString("\n) R1\n")
	for i := 1; i < len(branches); i++ {
		alias := fmt.Sprintf("R%d", i+1)
		sb.WriteString("JOIN (\n")
		sb.WriteString(indent(branches[i]))
		sb.WriteString("\n) ")
		sb.WriteString(alias)
		sb.WriteString(" ON ")
		sb.WriteString(alias)
		sb.WriteString(".object_id = R1.object_id\n")
	}
	sb.WriteString("JOIN public.medias O ON R1.object_id = O.id\nORDER BY O.id;")

	return sb.String(), nil
}

func queryAllMedias(ctx context.Context, db *sql.DB, axisX, axisY, axisZ qg.ParsedAxis, filters []qg.ParsedFilter) ([]compatCubeObject, error) {
	sqlStr, err := buildAllMediasSQL(axisX, axisY, axisZ, filters)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []compatCubeObject
	for rows.Next() {
		var (
			id    int32
			file  string
			thumb sql.NullString
		)
		if err := rows.Scan(&id, &file, &thumb); err != nil {
			return nil, err
		}

		t := ""
		if thumb.Valid {
			t = thumb.String
		}
		out = append(out, compatCubeObject{
			Id:           id,
			FileURI:      file,
			ThumbnailURI: t,
			FileUri:      file,
			ThumbnailUri: t,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// GetMetaDataCubeCompatCellHandler serves GET /api/cell and /api/cell/.
// - If `all` is set (client uses `all=[]`): returns a flat array of media objects: [{id,fileURI,thumbnailURI},...]
// - Otherwise: returns browsing state cells: [{x,y,z,count,cubeObjects:[{...}]}...]
func GetMetaDataCubeCompatCellHandler(db *sql.DB, vectorFilters *vectorFilterResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		req := &pb.GetCellRequest{
			XAxis:    r.URL.Query().Get("xAxis"),
			YAxis:    r.URL.Query().Get("yAxis"),
			ZAxis:    r.URL.Query().Get("zAxis"),
			Filters:  r.URL.Query().Get("filters"),
			All:      r.URL.Query().Get("all"),
			Timeline: r.URL.Query().Get("timeline"),
		}

		axisX, axisY, axisZ, filters, err := oldParseAxesAndFilters(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid parameters: %v", err), http.StatusBadRequest)
			return
		}

		vectorFilterCfg, err := parseCompatVectorFilter(r.URL.Query().Get("vectorFilter"))
		if err != nil {
			http.Error(w, vectorFilterHTTPMessage(err), mapVectorFilterHTTPStatus(err))
			return
		}
		vectorDimensionCfg, err := parseCompatVectorDimension(r.URL.Query().Get("vectorDimension"))
		if err != nil {
			http.Error(w, vectorFilterHTTPMessage(err), mapVectorFilterHTTPStatus(err))
			return
		}
		vectorBucketID, err := parseCompatVectorBucketID(r.URL.Query().Get("vectorBucketId"))
		if err != nil {
			http.Error(w, vectorFilterHTTPMessage(err), mapVectorFilterHTTPStatus(err))
			return
		}
		if vectorFilterCfg != nil && vectorDimensionCfg != nil {
			http.Error(w, "vectorFilter and vectorDimension cannot be used together", http.StatusBadRequest)
			return
		}
		if vectorFilterCfg != nil {
			vectorIDs, err := vectorFilters.ResolveObjectIDs(r.Context(), vectorFilterCfg)
			if err != nil {
				http.Error(w, vectorFilterHTTPMessage(err), mapVectorFilterHTTPStatus(err))
				return
			}
			filters = appendVectorObjectIDFilter(filters, vectorIDs, true)
		}

		server := &DataLoaderServer{db: db, vectorFilters: vectorFilters}
		plan := &browsingStateRequestPlan{
			AxisOrder: buildCompatAxisOrder(axisX, axisY, axisZ, filters),
			AxisX:     axisX,
			AxisY:     axisY,
			AxisZ:     axisZ,
			Filters:   filters,
		}
		plan, err = server.applyVectorDimensionToPlan(
			r.Context(),
			plan,
			vectorDimensionCfg,
			vectorBucketID,
			strings.TrimSpace(req.All) != "",
			strings.TrimSpace(req.Timeline) != "",
		)
		if err != nil {
			http.Error(w, vectorFilterHTTPMessage(err), mapVectorFilterHTTPStatus(err))
			return
		}
		axisX = plan.AxisX
		axisY = plan.AxisY
		axisZ = plan.AxisZ
		filters = plan.Filters

		// `all` mode: return flat list of medias (do not depend on Timestamp UTC tagset).
		if strings.TrimSpace(req.All) != "" {
			objects, err := queryAllMedias(r.Context(), db, axisX, axisY, axisZ, filters)
			if err != nil {
				http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
				return
			}
			writeJSON(w, http.StatusOK, objects)
			return
		}

		if err := initXYZAxes(r.Context(), db, &axisX, &axisY, &axisZ, "CompatCell(initAxes).exec"); err != nil {
			http.Error(w, fmt.Sprintf("axis init failed: %v", err), http.StatusBadGateway)
			return
		}

		sqlStr := qg.GenerateSQLQueryForState(
			plan.AxisOrder,
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
			qg.StateQueryOpts{AxisSubqueries: plan.AxisSubqueries},
		)

		tx, err := db.BeginTx(r.Context(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			http.Error(w, fmt.Sprintf("db tx begin failed: %v", err), http.StatusBadGateway)
			return
		}
		defer func() { _ = tx.Rollback() }()

		if disableHashJoins {
			if _, err := tx.ExecContext(r.Context(), "SET LOCAL enable_hashjoin = off"); err != nil {
				http.Error(w, fmt.Sprintf("db set enable_hashjoin=off failed: %v", err), http.StatusBadGateway)
				return
			}
		}

		rows, err := tx.QueryContext(r.Context(), sqlStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("db query failed: %v", err), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		var out []compatBrowsingStateResponse
		for rows.Next() {
			var (
				xID, yID, zID int
				objID         int32
				fileURI       string
				thumb         sql.NullString
				count         int32
			)
			if err := rows.Scan(&xID, &yID, &zID, &objID, &fileURI, &thumb, &count); err != nil {
				http.Error(w, fmt.Sprintf("db scan failed: %v", err), http.StatusBadGateway)
				return
			}

			t := ""
			if thumb.Valid {
				t = thumb.String
			}

			posX := axisX.Ids[xID]
			posY := axisY.Ids[yID]
			posZ := axisZ.Ids[zID]

			out = append(out, compatBrowsingStateResponse{
				X:     int32(posX),
				Y:     int32(posY),
				Z:     int32(posZ),
				Count: count,
				CubeObjects: []compatCubeObject{{
					Id:           objID,
					FileURI:      fileURI,
					ThumbnailURI: t,
					FileUri:      fileURI,
					ThumbnailUri: t,
				}},
			})
		}
		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("db rows failed: %v", err), http.StatusBadGateway)
			return
		}
		if err := tx.Commit(); err != nil {
			http.Error(w, fmt.Sprintf("db tx commit failed: %v", err), http.StatusBadGateway)
			return
		}

		if len(plan.BucketInfos) > 0 {
			writeJSON(w, http.StatusOK, compatBrowsingStateEnvelope{
				BucketInfos: convertCompatBucketInfos(plan.BucketInfos),
				Cells:       out,
			})
			return
		}

		writeJSON(w, http.StatusOK, out)
	}
}

func buildCompatAxisOrder(axisX qg.ParsedAxis, axisY qg.ParsedAxis, axisZ qg.ParsedAxis, filters []qg.ParsedFilter) []string {
	axisOrder := make([]string, 0, 4)
	if axisX.Type != "" && axisX.Id != -1 {
		axisOrder = append(axisOrder, "x")
	}
	if axisY.Type != "" && axisY.Id != -1 {
		axisOrder = append(axisOrder, "y")
	}
	if axisZ.Type != "" && axisZ.Id != -1 {
		axisOrder = append(axisOrder, "z")
	}
	if len(filters) > 0 {
		axisOrder = append(axisOrder, "filter")
	}
	return axisOrder
}

func convertCompatBucketInfos(infos []*pb.BucketInfo) []compatBucketInfo {
	out := make([]compatBucketInfo, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		out = append(out, compatBucketInfo{
			BucketID:   info.GetBucketId(),
			LowerBound: info.GetLowerBound(),
			UpperBound: info.GetUpperBound(),
			Label:      info.GetLabel(),
		})
	}
	return out
}
