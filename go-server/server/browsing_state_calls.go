package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
	"m3.dataloader/utilities"
)

// Configurables for streaming responsiveness. TODO: make adjustable via ENV file or server config.
const (
	statePreviewLimit = 64                     // Phase-1 quick preview rows
	idChunkSize       = 10                     // IDs per final-select chunk (Incremental 2)
	streamBatchSize   = 10240                  // CellResponse messages per flush
	batchSize         = 10240                  // send once we have this many rows
	flushInterval     = 50 * time.Millisecond  // or at least this often
	maxFirstFlush     = 150 * time.Millisecond // ensure first batch <~ 150ms
)

// ---- SQL tracing (file-backed) ---------------------------------------------

// Compile-time override (handy during dev)
const forceSQLTrace = false

var (
	sqlTraceInit        sync.Once
	sqlTraceOn          bool
	sqlTraceLogger      *log.Logger
	disableHashJoins    = utilities.MustGetEnv("UNGROUPED_QUERY_DISABLE_HASH_JOIN") == "1"
	useLateralMediaJoin = utilities.MustGetEnv("UNGROUPED_QUERY_USE_LATERAL_MEDIA_JOIN") == "1"
)

// sqlTraceEnabled checks env/const once and builds the logger.
func sqlTraceEnabled() bool {
	sqlTraceInit.Do(func() {
		// 1) Decide if tracing is enabled
		if forceSQLTrace {
			sqlTraceOn = true
		} else {
			v := strings.ToLower(strings.TrimSpace(os.Getenv("SQL_TRACE")))
			sqlTraceOn = (v == "1" || v == "true" || v == "yes")
		}
		if !sqlTraceOn {
			return
		}

		// 2) Determine log path
		//   - SQL_TRACE_PATH can be a directory or a file path.
		//   - If directory or empty, we create file: sql-trace-YYYYMMDD-HHMMSS.log
		path := strings.TrimSpace(os.Getenv("SQL_TRACE_PATH"))

		var logFilePath string
		if path == "" || strings.HasSuffix(path, string(os.PathSeparator)) || isDir(path) {
			dir := path
			if dir == "" {
				dir = "logs"
			}
			_ = os.MkdirAll(dir, 0o755)
			logFilePath = filepath.Join(dir, "sql-trace-"+time.Now().Format("20060102-150405")+".log")
		} else {
			// a concrete file path
			_ = os.MkdirAll(filepath.Dir(path), 0o755)
			logFilePath = path
		}

		f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			// Fallback to stdout if file open fails
			sqlTraceLogger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
			sqlTraceLogger.Printf("[sql-trace] failed to open file '%s': %v (logging to stdout)", logFilePath, err)
			return
		}

		// 3) Also mirror to stdout if SQL_TRACE_STDOUT=1/true/yes
		stdoutToo := false
		if v := strings.ToLower(strings.TrimSpace(os.Getenv("SQL_TRACE_STDOUT"))); v == "1" || v == "true" || v == "yes" {
			stdoutToo = true
		}

		var w io.Writer = f
		if stdoutToo {
			w = io.MultiWriter(f, os.Stdout)
		}
		sqlTraceLogger = log.New(w, "", log.LstdFlags|log.Lmicroseconds)
		sqlTraceLogger.Printf("[sql-trace] started, writing to %s (stdout=%v)", logFilePath, stdoutToo)
	})
	return sqlTraceOn
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// traceSQL writes a single labeled SQL statement to the trace log (if enabled).
func traceSQL(label, sql string) {
	if !sqlTraceEnabled() || sqlTraceLogger == nil {
		return
	}
	// One line to keep it grep-friendly
	sqlTraceLogger.Printf("[SQL][%s] %s", label, sql)
}

var dollarParamRe = regexp.MustCompile(`\$(\d+)`)

// formatSQLForLog renders a PostgreSQL query with $1, $2... replaced by
// safely-quoted literal representations for LOGGING ONLY.
func formatSQLForLog(query string, args []any, maxLiteralLen int) string {
	quote := func(v any) string {
		switch x := v.(type) {
		case nil:
			return "NULL"
		case time.Time:
			// Format consistently
			return pq.QuoteLiteral(x.UTC().Format(time.RFC3339Nano))
		case []byte:
			// Render as bytea hex literal: E'\\x....', but shorten if huge
			if maxLiteralLen > 0 && len(x) > maxLiteralLen {
				return fmt.Sprintf("E'\\\\x%s…'/*%dB*/", hex.EncodeToString(x[:maxLiteralLen/2]), len(x))
			}
			return fmt.Sprintf("E'\\\\x%s'", hex.EncodeToString(x))
		case fmt.Stringer:
			s := x.String()
			if maxLiteralLen > 0 && len(s) > maxLiteralLen {
				s = s[:maxLiteralLen] + "…"
			}
			return pq.QuoteLiteral(s)
		case string:
			s := x
			if maxLiteralLen > 0 && len(s) > maxLiteralLen {
				s = s[:maxLiteralLen] + "…"
			}
			return pq.QuoteLiteral(s)
		case bool:
			if x {
				return "TRUE"
			}
			return "FALSE"
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return fmt.Sprint(x) // numbers don’t get quotes
		default:
			// Arrays/slices: render as Postgres array literal when possible
			// Fallback: use fmt + quote as string
			switch vv := any(x).(type) {
			case []string:
				var b bytes.Buffer
				b.WriteByte('\'')
				b.WriteByte('{')
				for i, el := range vv {
					if i > 0 {
						b.WriteByte(',')
					}
					// escape quotes and backslashes as per array input rules
					escaped := strings.ReplaceAll(strings.ReplaceAll(el, `\`, `\\`), `"`, `\"`)
					b.WriteByte('"')
					b.WriteString(escaped)
					b.WriteByte('"')
				}
				b.WriteByte('}')
				b.WriteByte('\'')
				return b.String()
			case []int, []int64, []float64:
				// let fmt print the slice; wrap as comment to avoid invalid SQL
				return pq.QuoteLiteral(fmt.Sprint(vv))
			default:
				return pq.QuoteLiteral(fmt.Sprint(v))
			}
		}
	}

	out := dollarParamRe.ReplaceAllStringFunc(query, func(m string) string {
		sub := dollarParamRe.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		// 1-based index
		var idx int
		fmt.Sscanf(sub[1], "%d", &idx)
		if idx <= 0 || idx > len(args) {
			// Leave placeholder if out of range
			return m
		}
		return quote(args[idx-1])
	})

	return out
}

// parseAxesAndFiltersFromRequest parses x/y/z axis JSON blobs and the filters JSON array.
// - Missing axis => defaults to {Type:"", Id:-1, Ids:{1:1}}
// - Missing/empty filters => returns nil slice
func parseAxesAndFiltersFromRequest(req *pb.GetBrowsingStateRequest) ([]string, qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	defAxis := func() qg.ParsedAxis { return qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}} }

	axisX, axisY, axisZ := defAxis(), defAxis(), defAxis()
	var filters []qg.ParsedFilter
	var axisOrder []string

	var foundX, foundY, foundZ, foundFilter bool

	for _, af := range req.Filters {
		switch af.AxisFilterType {
		case pb.AxisType_X_AXIS:
			if !foundX {
				axisOrder = append(axisOrder, "x")
				foundX = true
			}

		case pb.AxisType_Y_AXIS:
			if !foundY {
				axisOrder = append(axisOrder, "y")
				foundY = true
			}
		case pb.AxisType_Z_AXIS:
			if !foundZ {
				axisOrder = append(axisOrder, "z")
				foundZ = true
			}
		case pb.AxisType_FILTER:
			if !foundFilter {
				axisOrder = append(axisOrder, "filter")
				foundFilter = true
			}
		}
	}

	checkAxisType := func(axisType pb.AxisType) func(*pb.AxisFilter) bool {
		return func(f *pb.AxisFilter) bool {
			return f.AxisFilterType == axisType
		}
	}

	// X axis
	if axisFilter, found := utilities.TryFind(req.Filters, checkAxisType(pb.AxisType_X_AXIS)); found {
		axisX.Type = strings.ToLower(axisFilter.ValueType.String())
		axisX.Id = int(axisFilter.Value)
		axisX.Ids = map[int]int{1: 1}
	}

	// Y axis
	if axisFilter, found := utilities.TryFind(req.Filters, checkAxisType(pb.AxisType_Y_AXIS)); found {
		axisY.Type = strings.ToLower(axisFilter.ValueType.String())
		axisY.Id = int(axisFilter.Value)
		axisY.Ids = map[int]int{1: 1}
	}

	// Z axis
	if axisFilter, found := utilities.TryFind(req.Filters, checkAxisType(pb.AxisType_Z_AXIS)); found {
		axisZ.Type = strings.ToLower(axisFilter.ValueType.String())
		axisZ.Id = int(axisFilter.Value)
		axisZ.Ids = map[int]int{1: 1}
	}

	filterMap := make(map[pb.FilterValueType]*qg.ParsedFilter)

	for _, af := range req.Filters {
		if af.AxisFilterType != pb.AxisType_FILTER {
			continue
		}

		pf, ok := filterMap[af.ValueType]
		if !ok {
			pf = &qg.ParsedFilter{
				Type: strings.ToLower(af.ValueType.String()),
				Ids:  []int{},
				// Ranges: left as nil unless you use them for range-based filters
			}
			filterMap[af.ValueType] = pf
		}

		// Each AxisFilter has one Value; aggregate them into Ids
		pf.Ids = append(pf.Ids, int(af.Value))
	}

	// Flatten map into the filters slice
	filters = make([]qg.ParsedFilter, 0, len(filterMap))
	for _, pf := range filterMap {
		filters = append(filters, *pf)
	}
	return axisOrder, axisX, axisY, axisZ, filters, nil
}

// parseAxesAndFiltersFromRequest parses x/y/z axis JSON blobs and the filters JSON array.
// - Missing axis => defaults to {Type:"", Id:-1, Ids:{1:1}}
// - Missing/empty filters => returns nil slice
func oldParseAxesAndFiltersFromRequest(req *pb.GetCellRequest) (qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	defAxis := func() qg.ParsedAxis { return qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}} }

	axisX, axisY, axisZ := defAxis(), defAxis(), defAxis()
	var filters []qg.ParsedFilter

	// Axes
	if req.XAxis != "" {
		if err := json.Unmarshal([]byte(req.XAxis), &axisX); err != nil {
			return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("invalid xAxis JSON: %w", err)
		}
		if axisX.Ids == nil {
			axisX.Ids = map[int]int{1: 1}
		}
	}
	if req.YAxis != "" {
		if err := json.Unmarshal([]byte(req.YAxis), &axisY); err != nil {
			return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("invalid yAxis JSON: %w", err)
		}
		if axisY.Ids == nil {
			axisY.Ids = map[int]int{1: 1}
		}
	}
	if req.ZAxis != "" {
		if err := json.Unmarshal([]byte(req.ZAxis), &axisZ); err != nil {
			return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("invalid zAxis JSON: %w", err)
		}
		if axisZ.Ids == nil {
			axisZ.Ids = map[int]int{1: 1}
		}
	}

	// Filters (JSON array)
	if req.Filters != "" {
		if err := json.Unmarshal([]byte(req.Filters), &filters); err != nil {
			return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("invalid filters JSON: %w", err)
		}
		// (Optional) light normalization to avoid nil references later
		for i := range filters {
			if filters[i].Ids == nil {
				filters[i].Ids = []int{}
			}
			if filters[i].Ranges == nil {
				filters[i].Ranges = [][]string{}
			}
		}
	}

	return axisX, axisY, axisZ, filters, nil
}

// parseInitAxesAndFilters parses axes+filters and initializes the axis Ids maps via DB.
func parseAxesAndFilters(req *pb.GetBrowsingStateRequest) ([]string, qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFiltersFromRequest(req)
	if err != nil {
		return nil, qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	return axisOrder, axisX, axisY, axisZ, filters, nil
}

// parseInitAxesAndFilters parses axes+filters and initializes the axis Ids maps via DB.
func oldParseAxesAndFilters(req *pb.GetCellRequest) (qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	axisX, axisY, axisZ, filters, err := oldParseAxesAndFiltersFromRequest(req)
	if err != nil {
		return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	return axisX, axisY, axisZ, filters, nil
}

// initXYZAxes initializes the Ids maps for X/Y/Z axes by querying the DB.
func initXYZAxes(ctx context.Context, db *sql.DB, axisX, axisY, axisZ *qg.ParsedAxis, logContext string) error {
	var (
		planX *qg.InitializeIdsPlan
		planY *qg.InitializeIdsPlan
		planZ *qg.InitializeIdsPlan
		err   error
	)

	planX, err = axisX.BuildInitializeIdsPlan()
	if err != nil {
		return fmt.Errorf("init xAxis (build): %w", err)
	}

	planY, err = axisY.BuildInitializeIdsPlan()
	if err != nil {
		return fmt.Errorf("init yAxis (build): %w", err)
	}

	planZ, err = axisZ.BuildInitializeIdsPlan()
	if err != nil {
		return fmt.Errorf("init zAxis (build): %w", err)
	}

	if err := ExecuteInitializeIdsPlan(ctx, db, planX, axisX, logContext+".xAxis"); err != nil {
		return fmt.Errorf("init xAxis (exec): %w", err)
	}
	if err := ExecuteInitializeIdsPlan(ctx, db, planY, axisY, logContext+".yAxis"); err != nil {
		return fmt.Errorf("init yAxis (exec): %w", err)
	}
	if err := ExecuteInitializeIdsPlan(ctx, db, planZ, axisZ, logContext+".zAxis"); err != nil {
		return fmt.Errorf("init zAxis (exec): %w", err)
	}

	return nil
}

// ExecuteInitializeIdsPlan actually runs the queries in the plan,
// assembles the stable {axisMemberId -> positionIndex} map,
// guarantees non-empty, and writes p.Ids.
func ExecuteInitializeIdsPlan(ctx context.Context, db *sql.DB, plan *qg.InitializeIdsPlan, p *qg.ParsedAxis, logContext string) error {
	idList := make(map[int]int)
	counter := 1
	switch plan.Kind {
	case "tagset":
		traceSQL(logContext, formatSQLForLog(plan.MainSQL, plan.MainArgs, 128))
		rows, err := db.QueryContext(ctx, plan.MainSQL, plan.MainArgs...)
		if err != nil {
			return fmt.Errorf("initializeIds(tagset) query: %w", err)
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
		// 1) run PreSQL to get hierarchy_id
		var hierarchyID int
		traceSQL(logContext, formatSQLForLog(plan.PreSQL, plan.PreArgs, 128))
		if err := db.QueryRowContext(ctx, plan.PreSQL, plan.PreArgs...).Scan(&hierarchyID); err != nil {
			return fmt.Errorf("initializeIds(node) fetch hierarchy_id: %w", err)
		}

		// 2) now run MainSQL using parent node id (p.Id) and hierarchyID
		traceSQL(logContext, formatSQLForLog(plan.MainSQL, []any{p.Id, hierarchyID}, 128))
		rows, err := db.QueryContext(ctx, plan.MainSQL, p.Id, hierarchyID)
		if err != nil {
			return fmt.Errorf("initializeIds(node) child query: %w", err)
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

	case "fallback":
		idList[1] = 1

	default:
		return fmt.Errorf("unknown plan kind %q", plan.Kind)
	}

	// safety: never leave it empty
	if len(idList) == 0 {
		idList[1] = 1
	}

	p.Ids = idList
	return nil
}

// GetBrowsingStateDistinctBranchesChunks:
// - Uses the ungrouped (no GROUP BY) SQL with DISTINCT branches like Incremental5.
// - Aggregates per-cell in Go with DISTINCT(object_id) and MAX(object_id) as representative.
// - Streams authoritative updates periodically while scanning, like Incremental3.
func (s *DataLoaderServer) GetBrowsingStateDistinctBranchesChunks(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateDistinctBranchesChunksServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions (needed to map ids -> positions) ----------
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "DistinctBranchesChunks(initAxes).exec"); err != nil {
		return err
	}

	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""

	if allDefined {
		sqlstr := qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
		traceSQL("DistinctBranchesChunks(all).exec", formatSQLForLog("\n"+sqlstr, nil, 128))

		rows, err := s.db.QueryContext(ctx, sqlstr)
		if err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesChunks all: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("getBrowsingStateDistinctBranchesChunks all scan: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesChunks all rows: %w", err)
		}
		if len(cubeObjects) > 0 {
			if err := stream.Send(&pb.BrowsingStateResponse{CubeObjects: cubeObjects}); err != nil {
				return err
			}
		}
		return nil
	}

	if timelineDefined {
		sqlStr := qg.GenerateSQLQueryForTimeline(filters)
		traceSQL("DistinctBranchesChunks(timeline).exec", formatSQLForLog("\n"+sqlStr, nil, 128))

		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesChunks timeline: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("getBrowsingStateDistinctBranchesChunks timeline scan: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesChunks timeline rows: %w", err)
		}
		if len(cubeObjects) > 0 {
			if err := stream.Send(&pb.BrowsingStateResponse{CubeObjects: cubeObjects}); err != nil {
				return err
			}
		}
		return nil
	}

	// ---------- State semantics: ungrouped SQL with DISTINCT branches ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{BranchDistinct: true},
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("DistinctBranchesChunks.ungrouped-distinct", formatSQLForLog("\n"+sqlStr, nil, 128))

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("getBrowsingStateDistinctBranchesChunks ungrouped: %w", err)
	}
	defer rows.Close()

	// Row model from ungrouped SQL -- TODO: reuse
	type rowT struct {
		XID      int
		YID      int
		ZID      int
		ObjectID int32
		FileURI  sql.NullString
		ThumbURI sql.NullString
	}

	// Per-cell aggregator (DISTINCT object_id + representative = MAX(object_id))
	type cellKey struct{ x, y, z int32 }
	type cellAgg struct {
		count    int32
		repID    int32
		fileURI  string
		thumbURI string
		seen     map[int32]struct{}
	}

	cells := make(map[cellKey]*cellAgg, 2048)
	dirty := make(map[cellKey]struct{}, 512)

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flush := func(force bool) error {
		if !force && len(dirty) == 0 {
			return nil
		}
		sent := 0
		for k := range dirty {
			agg := cells[k]
			resp := &pb.BrowsingStateResponse{
				X:     k.x,
				Y:     k.y,
				Z:     k.z,
				Count: agg.count,
				CubeObjects: []*pb.CubeObject{{
					Id:           agg.repID,
					FileUri:      agg.fileURI,
					ThumbnailUri: agg.thumbURI,
				}},
			}
			// Early abort on client cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			delete(dirty, k)
			sent++
			if !force && sent >= streamBatchSize {
				break
			}
		}
		return nil
	}

scanLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTicker.C:
			if err := flush(false); err != nil {
				return err
			}
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowT
		if err := rows.Scan(&r.XID, &r.YID, &r.ZID, &r.ObjectID, &r.FileURI, &r.ThumbURI); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesChunks scan: %w", err)
		}

		// Map DB IDs -> cube positions (default 1 when axis empty)
		px := axisX.Ids[r.XID]
		if px == 0 {
			px = 1
		}
		py := axisY.Ids[r.YID]
		if py == 0 {
			py = 1
		}
		pz := axisZ.Ids[r.ZID]
		if pz == 0 {
			pz = 1
		}
		key := cellKey{int32(px), int32(py), int32(pz)}

		agg := cells[key]
		if agg == nil {
			agg = &cellAgg{seen: make(map[int32]struct{}, 16)}
			cells[key] = agg
		}

		// DISTINCT object per cell (as in Incr5)
		// Note: DISTINCT already applied per-branch in SQL, but retain guard in case of
		// cross-branch duplication reaching same (x,y,z) after position mapping.
		if _, ok := agg.seen[r.ObjectID]; !ok {
			agg.seen[r.ObjectID] = struct{}{}
			agg.count++
		}

		// Representative = MAX(object_id) (as in Incr5)
		if r.ObjectID > agg.repID {
			agg.repID = r.ObjectID
			if r.FileURI.Valid {
				agg.fileURI = r.FileURI.String
			}
			if r.ThumbURI.Valid {
				agg.thumbURI = r.ThumbURI.String
			}
		}

		dirty[key] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("getBrowsingStateDistinctBranchesChunks rows: %w", err)
	}

	// Final flush to emit any remaining dirty cells
	return flush(true)
}

// GetBrowsingStateDistinctBranchesFull: Same logical result as baseline getCell (STATE path),
// but executes an ungrouped join (no GROUP BY) with DISTINCT in branches,
// and performs grouping entirely in memory in Go. It sends responses only
// after grouping completes (authoritative).
func (s *DataLoaderServer) GetBrowsingStateDistinctBranchesFull(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateDistinctBranchesFullServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ, "DistinctBranchesFull(initAxes).exec"); err != nil {
		return err
	}

	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""

	if allDefined {
		sqlstr := qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)

		traceSQL("DistinctBranchesFull(all).exec", formatSQLForLog("\n"+sqlstr, nil, 128))
		rows, err := s.db.QueryContext(ctx, sqlstr)
		if err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull all: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("getBrowsingStateDistinctBranchesFull all scan: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull all rows: %w", err)
		}
		if len(cubeObjects) > 0 {
			if err := stream.Send(&pb.BrowsingStateResponse{CubeObjects: cubeObjects}); err != nil {
				return err
			}
		}
		return nil
	}

	if timelineDefined {
		sqlStr := qg.GenerateSQLQueryForTimeline(filters)
		traceSQL("DistinctBranchesFull(timeline).exec", formatSQLForLog("\n"+sqlStr, nil, 128))

		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull timeline: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("getBrowsingStateDistinctBranchesFull timeline scan: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull timeline rows: %w", err)
		}
		if len(cubeObjects) > 0 {
			if err := stream.Send(&pb.BrowsingStateResponse{CubeObjects: cubeObjects}); err != nil {
				return err
			}
		}
		return nil
	}

	// ---------- State semantics via in-memory grouping ----------
	// Build the ungrouped SQL (NO GROUP BY), but DISTINCT per-branch enabled.
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{BranchDistinct: true}, 
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("DistinctBranchesFull(ungrouped, distinct-branches).exec", formatSQLForLog("\n"+sqlStr, nil, 128))

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("getBrowsingStateDistinctBranchesFull ungrouped: %w", err)
	}
	defer rows.Close()

	// Row model returned by ungrouped SQL
	type rowT struct {
		XID      int
		YID      int
		ZID      int
		ObjectID int32
		FileURI  string
		ThumbURI string
	}

	// Per-cell aggregator with DISTINCT(object_id) semantics + MAX(object_id) rep
	type cellAgg struct {
		count    int32
		repID    int32
		fileURI  string
		thumbURI string
		seen     map[int32]struct{}
	}

	type cellKey struct{ x, y, z int32 }

	cells := make(map[cellKey]*cellAgg, 2048)

	// Scan all rows (no streaming yet — emit only final results)
	for rows.Next() {
		var r rowT
		if err := rows.Scan(&r.XID, &r.YID, &r.ZID, &r.ObjectID, &r.FileURI, &r.ThumbURI); err != nil {
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull scan: %w", err)
		}

		// Map DB IDs -> cube positions (default 1 when axis empty)
		px := axisX.Ids[r.XID]
		if px == 0 {
			px = 1
		}
		py := axisY.Ids[r.YID]
		if py == 0 {
			py = 1
		}
		pz := axisZ.Ids[r.ZID]
		if pz == 0 {
			pz = 1
		}
		key := cellKey{int32(px), int32(py), int32(pz)}

		agg := cells[key]
		if agg == nil {
			agg = &cellAgg{seen: make(map[int32]struct{}, 8)}
			cells[key] = agg
		}

		// DISTINCT object count per cell
		if _, ok := agg.seen[r.ObjectID]; !ok {
			agg.seen[r.ObjectID] = struct{}{}
			agg.count++
		}
		// Representative = MAX(object_id)
		if r.ObjectID > agg.repID {
			agg.repID = r.ObjectID
			agg.fileURI = r.FileURI
			agg.thumbURI = r.ThumbURI
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("getBrowsingStateDistinctBranchesFull rows: %w", err)
	}

	for k := range cells {
		a := cells[k]
		resp := &pb.BrowsingStateResponse{
			X:     k.x,
			Y:     k.y,
			Z:     k.z,
			Count: a.count,
			CubeObjects: []*pb.CubeObject{{
				Id:           a.repID,
				FileUri:      a.fileURI,
				ThumbnailUri: a.thumbURI,
			}},
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

// GetBrowsingStateNonDistinctBranchesSingles: DB join only, no grouping; stream each tuple with Count=1.
// Client is responsible for grouping/aggregation.
func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesSingles(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesSinglesServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesSingles(initAxes).exec"); err != nil {
		return err
	}

	// ---------- Ungrouped SQL ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}

	traceSQL("NonDistinctBranchesSingles.ungrouped", formatSQLForLog("\n"+sqlStr, nil, 128))

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles state query: %w", err)
	}
	defer rows.Close()

	type rowT struct {
		XID      int
		YID      int
		ZID      int
		ObjectID int32
		FileURI  sql.NullString
		ThumbURI sql.NullString
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	pending := make([]*pb.BrowsingStateResponse, 0, batchSize)

	flush := func(force bool) error {
		if !force && len(pending) < batchSize {
			return nil
		}
		for _, r := range pending {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := stream.Send(r); err != nil {
				return err
			}
		}
		pending = pending[:0]
		return nil
	}

sendLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTicker.C:
			if err := flush(false); err != nil {
				return err
			}
		default:
		}

		if !rows.Next() {
			break sendLoop
		}

		var r rowT
		if err := rows.Scan(&r.XID, &r.YID, &r.ZID, &r.ObjectID, &r.FileURI, &r.ThumbURI); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles scan: %w", err)
		}

		px := axisX.Ids[r.XID]
		if px == 0 {
			px = 1
		}
		py := axisY.Ids[r.YID]
		if py == 0 {
			py = 1
		}
		pz := axisZ.Ids[r.ZID]
		if pz == 0 {
			pz = 1
		}

		resp := &pb.BrowsingStateResponse{
			X:     int32(px),
			Y:     int32(py),
			Z:     int32(pz),
			Count: 1, // each tuple contributes 1; client aggregates and deduplicates
			CubeObjects: []*pb.CubeObject{{
				Id:           r.ObjectID,
				FileUri:      ternaryStr(r.FileURI.Valid, r.FileURI.String, ""),
				ThumbnailUri: ternaryStr(r.ThumbURI.Valid, r.ThumbURI.String, ""),
			}},
		}
		pending = append(pending, resp)
		if err := flush(false); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles rows iteration: %w", err)
	}
	return flush(true)
}

// GetBrowsingStateNonDistinctBranchesDeduplicatedSingles:
// - Runs the same ungrouped (no GROUP BY, no DISTINCT) SQL as NonDistinctBranchesSingles.
// - Streams one response per *unique* (cell, object_id) tuple with Count=1.
// - Dedup semantics per cell mirror NonDistinctBranchesChunks (maintains a per-cell seen set).
func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesDeduplicatedSingles(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesDeduplicatedSinglesServer,
) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesDedupSingles(initAxes).exec"); err != nil {
		return err
	}

	// ---------- Ungrouped SQL (no DISTINCT, no GROUP BY) ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("NonDistinctBranchesDedupSingles.ungrouped", formatSQLForLog("\n"+sqlStr, nil, 128))

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles state query: %w", err)
	}
	defer rows.Close()

	type rowT struct {
		XID      int
		YID      int
		ZID      int
		ObjectID int32
		FileURI  sql.NullString
		ThumbURI sql.NullString
	}
	type cellKey struct{ x, y, z int32 }

	// Per-cell DISTINCT guard: object_id set
	seen := make(map[cellKey]map[int32]struct{}, 2048)

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	pending := make([]*pb.BrowsingStateResponse, 0, batchSize)

	flush := func(force bool) error {
		if !force && len(pending) < batchSize {
			return nil
		}
		for _, r := range pending {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := stream.Send(r); err != nil {
				return err
			}
		}
		pending = pending[:0]
		return nil
	}

sendLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTicker.C:
			if err := flush(false); err != nil {
				return err
			}
		default:
		}

		if !rows.Next() {
			break sendLoop
		}

		var r rowT
		if err := rows.Scan(&r.XID, &r.YID, &r.ZID, &r.ObjectID, &r.FileURI, &r.ThumbURI); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles scan: %w", err)
		}

		// Map DB IDs -> axis positions (default 1 when axis empty)
		px := axisX.Ids[r.XID]
		if px == 0 {
			px = 1
		}
		py := axisY.Ids[r.YID]
		if py == 0 {
			py = 1
		}
		pz := axisZ.Ids[r.ZID]
		if pz == 0 {
			pz = 1
		}
		key := cellKey{int32(px), int32(py), int32(pz)}

		// Deduplicate like *Chunks*: ensure each object_id is emitted once per cell
		sset := seen[key]
		if sset == nil {
			sset = make(map[int32]struct{}, 16)
			seen[key] = sset
		}
		if _, dup := sset[r.ObjectID]; dup {
			// skip duplicates for this cell
			continue
		}
		sset[r.ObjectID] = struct{}{}

		// Emit a single-row response (like Singles), Count=1
		resp := &pb.BrowsingStateResponse{
			X:     key.x,
			Y:     key.y,
			Z:     key.z,
			Count: 1,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.ObjectID,
				FileUri:      ternaryStr(r.FileURI.Valid, r.FileURI.String, ""),
				ThumbnailUri: ternaryStr(r.ThumbURI.Valid, r.ThumbURI.String, ""),
			}},
		}
		pending = append(pending, resp)

		if err := flush(false); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles rows iteration: %w", err)
	}
	return flush(true)
}

func ternaryStr(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// GetBrowsingStateNonDistinctBranchesChunks: DB does one intersecting join (no GROUP BY, no DISTINCT).
// Go aggregates per (x,y,z) online and streams authoritative updates (stream.Send happens on a dedicated goroutine).
// TODO: eliminate magic numbers and make configurable
func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesChunks(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesChunksServer,
) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesChunks(initAxes).exec"); err != nil {
		return err
	}

	qgOpts := qg.UngroupedOpts{
		BranchDistinct:      false,
		UseLateralMediaJoin: useLateralMediaJoin,
	}

	// ---------- Ungrouped SQL (no DISTINCT, no GROUP BY) ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters, qgOpts,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}

	traceSQL("NonDistinctBranchesChunks.ungrouped", formatSQLForLog("\n"+sqlStr, nil, 128))

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks set enable_hashjoin=off: %w", err)
		}
	}
	// If you also want merge joins off, do it separately:
	// if _, err := tx.ExecContext(ctx, "SET enable_mergejoin = off"); err != nil { ... }

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks state query: %w", err)
	}
	defer rows.Close()

	type rowT struct {
		XID      int
		YID      int
		ZID      int
		ObjectID int32
		FileURI  sql.NullString
		ThumbURI sql.NullString
	}
	type cellKey struct{ x, y, z int32 }
	type cellAgg struct {
		count    int32
		repID    int32
		fileURI  string
		thumbURI string
		seen     map[int32]struct{} // DISTINCT object_id per cell
		rev      uint64             // increments whenever cell state changes, added to ensure no messages are dropped
	}

	// Shared state between scanner (writer) and sender (reader)
	cells := make(map[cellKey]*cellAgg, 65536)

	// "queued" prevents enqueueing the same key many times before it is sent.
	queued := make(map[cellKey]bool, 65536)

	var mu sync.RWMutex // protects both cells and queued

	// Dirty key queue to sender. Bounded buffer provides backpressure.
	dirtyCh := make(chan cellKey, 65536)

	// Sender reports its first error here (or nil on clean finish).
	sendErrCh := make(chan error, 1)

	// ---------- Sender goroutine: ONLY place that calls stream.Send ----------
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		// pending is a set of keys that should be flushed.
		pending := make(map[cellKey]struct{}, 65536)

		flush := func(force bool) error {
			if !force && len(pending) == 0 {
				return nil
			}

			// Choose keys to send this round (bounded by streamBatchSize unless forced).
			keys := make([]cellKey, 0, len(pending))
			for k := range pending {
				keys = append(keys, k)
				if !force && len(keys) >= streamBatchSize {
					break
				}
			}

			// Snapshot cell values under RLock so we don't hold locks during network I/O.
			type snap struct {
				k        cellKey
				count    int32
				repID    int32
				fileURI  string
				thumbURI string
				rev      uint64
			}
			snaps := make([]snap, 0, len(keys))

			mu.RLock()
			for _, k := range keys {
				agg := cells[k]
				if agg == nil {
					continue
				}
				snaps = append(snaps, snap{
					k:        k,
					count:    agg.count,
					repID:    agg.repID,
					fileURI:  agg.fileURI,
					thumbURI: agg.thumbURI,
					rev:      agg.rev,
				})
			}
			mu.RUnlock()

			// Send outside locks.
			for _, s := range snaps {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				resp := &pb.BrowsingStateResponse{
					X:     s.k.x,
					Y:     s.k.y,
					Z:     s.k.z,
					Count: s.count,
					CubeObjects: []*pb.CubeObject{{
						Id:           s.repID,
						FileUri:      s.fileURI,
						ThumbnailUri: s.thumbURI,
					}},
				}
				if err := stream.Send(resp); err != nil {
					return err
				}

				// Mark as no longer pending and allow scanner to enqueue again if it changes later.
				delete(pending, s.k)
				mu.Lock()
				cur := cells[s.k]
				if cur != nil && cur.rev > s.rev {
					// It changed after we snapshotted (or while we were sending).
					// Keep it queued and re-send by putting back into pending.
					pending[s.k] = struct{}{}
					// queued stays true (do NOT set to false)
				} else {
					// No changes since snapshot: allow scanner to enqueue again later
					queued[s.k] = false
				}
				mu.Unlock()
			}

			return nil
		}

		for {
			select {
			case <-ctx.Done():
				sendErrCh <- ctx.Err()
				return

			case k, ok := <-dirtyCh:
				if !ok {
					// Scanner finished: flush remaining pending keys.
					sendErrCh <- flush(true)
					return
				}
				pending[k] = struct{}{}

				// If we have enough pending, flush immediately (not just on ticks).
				if len(pending) >= streamBatchSize {
					if err := flush(false); err != nil {
						sendErrCh <- err
						return
					}
				}

			case <-ticker.C:
				if err := flush(false); err != nil {
					sendErrCh <- err
					return
				}
			}
		}
	}()

	// ---------- Scanner loop: reads rows, updates aggregates, enqueues dirty keys ----------
scanLoop:
	for {
		select {
		case <-ctx.Done():
			// Stop early; sender will also exit via ctx.
			close(dirtyCh)
			_ = <-sendErrCh
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowT
		if err := rows.Scan(&r.XID, &r.YID, &r.ZID, &r.ObjectID, &r.FileURI, &r.ThumbURI); err != nil {
			close(dirtyCh)
			_ = <-sendErrCh
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks scan: %w", err)
		}

		px := axisX.Ids[r.XID]
		if px == 0 {
			px = 1
		}
		py := axisY.Ids[r.YID]
		if py == 0 {
			py = 1
		}
		pz := axisZ.Ids[r.ZID]
		if pz == 0 {
			pz = 1
		}

		key := cellKey{int32(px), int32(py), int32(pz)}

		// Update aggregate under write lock
		mu.Lock()
		agg := cells[key]
		if agg == nil {
			agg = &cellAgg{seen: make(map[int32]struct{}, 16)}
			cells[key] = agg
		}

		changed := false

		// DISTINCT object per cell
		if _, ok := agg.seen[r.ObjectID]; !ok {
			agg.seen[r.ObjectID] = struct{}{}
			agg.count++
			changed = true
		}

		// Representative policy: max object_id
		if r.ObjectID > agg.repID {
			agg.repID = r.ObjectID
			if r.FileURI.Valid {
				agg.fileURI = r.FileURI.String
			}
			if r.ThumbURI.Valid {
				agg.thumbURI = r.ThumbURI.String
			}
			changed = true
		}

		if changed {
			agg.rev++ // added to ensure result accuracy after result streaming was moved to its own go-routine
		}

		// Enqueue key only once until it has been flushed.
		shouldEnqueue := !queued[key]
		if shouldEnqueue {
			queued[key] = true
		}
		mu.Unlock()

		if shouldEnqueue {
			select {
			case dirtyCh <- key:
			case <-ctx.Done():
				close(dirtyCh)
				_ = <-sendErrCh
				return ctx.Err()
			}
		}
	}

	if err := rows.Err(); err != nil {
		close(dirtyCh)
		_ = <-sendErrCh
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks rows: %w", err)
	}

	// Signal sender we're done scanning and wait for it to flush & finish.
	close(dirtyCh)
	if err := <-sendErrCh; err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesChunks commit: %w", err)
	}
	return nil
}

func (s *DataLoaderServer) GetBrowsingStateByIdChunks(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateByIdChunksServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ, "ByIdChunks(initAxes).exec"); err != nil {
		return err
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()
	firstFlushDeadline := time.NewTimer(maxFirstFlush)
	defer firstFlushDeadline.Stop()

	// batch buffer of BrowsingStateResponse rows that will be wrapped into a CellChunk
	pendingRows := make([]*pb.BrowsingStateResponse, 0, streamBatchSize)
	pendingAuth := false // false during preview, true during phase 2

	flush := func(force bool) error {
		if !force && len(pendingRows) < streamBatchSize {
			return nil
		}
		if len(pendingRows) == 0 {
			return nil
		}
		if err := flushCellsChunk(ctx, stream, pendingRows, pendingAuth); err != nil {
			return err
		}
		pendingRows = pendingRows[:0]
		return nil
	}
	queue := func(r *pb.BrowsingStateResponse) error {
		pendingRows = append(pendingRows, r)
		return flush(false)
	}

	// Row shape from SQL
	type rowCell struct {
		X            int
		Y            int
		Z            int
		Id           int32
		FileUri      string
		ThumbnailUri string
		Count        int32
	}

	// ======================================================
	// Phase 1: quick STATE preview (authoritative=false)
	// ======================================================
	pendingAuth = false
	previewSQL := qg.GeneratePreviewSQLForStateIncremental2(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		statePreviewLimit,
	)
	{
		traceSQL("ByIdChunks.preview", formatSQLForLog(previewSQL, nil, 128))
		rows, err := s.db.QueryContext(ctx, previewSQL)
		if err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks preview state query: %w", err)
		}
		defer rows.Close()

	ScanPrev:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-flushTicker.C:
				if err := flush(false); err != nil {
					return err
				}
			case <-firstFlushDeadline.C:
				if err := flush(true); err != nil {
					return err
				}
			default:
			}
			if !rows.Next() {
				break ScanPrev
			}
			var r rowCell
			if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri, &r.Count); err != nil {
				return fmt.Errorf("GetBrowsingStateByIdChunks preview state scan: %w", err)
			}
			// map axis IDs -> positions
			posX := axisX.Ids[r.X]
			posY := axisY.Ids[r.Y]
			posZ := axisZ.Ids[r.Z]
			resp := &pb.BrowsingStateResponse{
				X:     int32(posX),
				Y:     int32(posY),
				Z:     int32(posZ),
				Count: r.Count,
				CubeObjects: []*pb.CubeObject{{
					Id:           r.Id,
					FileUri:      r.FileUri,
					ThumbnailUri: r.ThumbnailUri,
				}},
			}
			if err := queue(resp); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks preview state rows: %w", err)
		}

		// force a first paint (one chunk with authoritative=false)
		if err := flush(true); err != nil {
			return err
		}
	}

	// ======================================================
	// Phase 2: build filtered object_id set (state semantics)
	// ======================================================
	var currentIDs map[int]struct{}
	intersectWith := func(newIDs map[int]struct{}) {
		if currentIDs == nil {
			currentIDs = newIDs
			return
		}
		for id := range currentIDs {
			if _, ok := newIDs[id]; !ok {
				delete(currentIDs, id)
			}
		}
	}
	fetchIDSet := func(sqlStr string) (map[int]struct{}, error) {
		if sqlStr == "" {
			return nil, nil
		}

		traceSQL("ByIdChunks.idset", formatSQLForLog("\n"+sqlStr, nil, 128))
		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make(map[int]struct{})
		for rows.Next() {
			var objID int
			if err := rows.Scan(&objID); err != nil {
				return nil, err
			}
			out[objID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}

	if sqlX, err := qg.BuildAxisObjectIDSQLForState(axisX.Type, axisX.Id); err != nil {
		return fmt.Errorf("GetBrowsingStateByIdChunks axis X ids: %w", err)
	} else if sqlX != "" {
		ids, err := fetchIDSet(sqlX)
		if err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks axis X fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			// nothing beyond preview
			return nil
		}
	}
	if sqlY, err := qg.BuildAxisObjectIDSQLForState(axisY.Type, axisY.Id); err != nil {
		return fmt.Errorf("GetBrowsingStateByIdChunks axis Y ids: %w", err)
	} else if sqlY != "" {
		ids, err := fetchIDSet(sqlY)
		if err != nil {
			return fmt.Errorf("axis Y fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			return nil
		}
	}
	if sqlZ, err := qg.BuildAxisObjectIDSQLForState(axisZ.Type, axisZ.Id); err != nil {
		return fmt.Errorf("GetBrowsingStateByIdChunks axis Z ids: %w", err)
	} else if sqlZ != "" {
		ids, err := fetchIDSet(sqlZ)
		if err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks axis Z fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			return nil
		}
	}
	for _, f := range filters {
		sqlF, err := qg.BuildFilterIDSQL(f)
		if err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks filter build: %w", err)
		}
		ids, err := fetchIDSet(sqlF)
		if err != nil {
			return fmt.Errorf("GetBrowsingStateByIdChunks filter fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			return nil
		}
	}

	// If nothing left beyond preview, done.
	if len(currentIDs) == 0 {
		return nil
	}

	// Switch to authoritative chunks
	pendingAuth = true

	// Chunk IDs and run state-style grouping restricted to those IDs
	idChunk := make([]int, 0, idChunkSize)

	flushStateChunk := func(ids []int) error {
		sqlStr, err := qg.BuildStateSQLRestrictedByIDs(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
			ids,
		)
		if err != nil {
			return err
		}

		traceSQL("ByIdChunks.stateChunk", formatSQLForLog("\n"+sqlStr, nil, 128))
		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var r rowCell
			if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri, &r.Count); err != nil {
				return fmt.Errorf("GetBrowsingStateByIdChunks state chunk scan: %w", err)
			}
			resp := &pb.BrowsingStateResponse{
				X:     int32(axisX.Ids[r.X]),
				Y:     int32(axisY.Ids[r.Y]),
				Z:     int32(axisZ.Ids[r.Z]),
				Count: r.Count,
				CubeObjects: []*pb.CubeObject{{
					Id:           r.Id,
					FileUri:      r.FileUri,
					ThumbnailUri: r.ThumbnailUri,
				}},
			}
			if err := queue(resp); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-flushTicker.C:
				if err := flush(false); err != nil {
					return err
				}
			default:
			}
		}
		return rows.Err()
	}

	idsThisChunk := 0
	for id := range currentIDs {
		idChunk = append(idChunk, id)
		idsThisChunk++
		if idsThisChunk >= idChunkSize {
			if err := flushStateChunk(idChunk); err != nil {
				return err
			}
			idChunk = idChunk[:0]
			idsThisChunk = 0
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	if idsThisChunk > 0 {
		if err := flushStateChunk(idChunk); err != nil {
			return err
		}
	}
	return flush(true)
}

// Updated helper: sends a single CellChunk with the given rows/flag.
func flushCellsChunk(ctx context.Context, stream pb.DataLoader_GetBrowsingStateByIdChunksServer, rows []*pb.BrowsingStateResponse, authoritative bool) error {
	if len(rows) == 0 {
		return nil
	}
	chunk := &pb.BrowsingStateChunk{
		Authoritative: authoritative,
		Cells:         rows,
	}
	// Early abort on client cancellation.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return stream.Send(chunk)
}

// Helper to flush a slice of responses on the stream.
func flushCells(ctx context.Context, stream pb.DataLoader_GetBrowsingStateIncrementallyServer, out []*pb.BrowsingStateResponse) error {
	if len(out) == 0 {
		return nil
	}
	for _, r := range out {
		// Early abort on client cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return nil
}

// Simple online-aggregation / progressive streaming version.
// Streams partial results quickly in random/load-optimized order.
func (s *DataLoaderServer) GetBrowsingStateIncrementally(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateIncrementallyServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""

	// If X/Y/Z need ID-position maps.
	if !allDefined && !timelineDefined {
		// ---------- Axis positions ----------
		if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ, "GetBrowsingStateIncrementally(initAxes).exec"); err != nil {
			return err
		}
	}

	// -------- Build SQL string --------
	var sqlStr string
	switch {
	case allDefined:
		sqlStr = qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	case timelineDefined:
		sqlStr = qg.GenerateSQLQueryForTimeline(filters)
	default:
		sqlStr = qg.GenerateSQLQueryForState(
			axisOrder,
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}

	// -------- Execute query with context --------
	traceSQL("GetBrowsingStateIncrementally.exec", formatSQLForLog("\n"+sqlStr, nil, 128))
	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateIncrementally query error: %w", err)
	}
	defer rows.Close()

	firstFlushDeadline := time.NewTimer(maxFirstFlush)
	defer firstFlushDeadline.Stop()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	// Two shapes of response depending on query type:
	if allDefined || timelineDefined {
		// Stream batches of CubeObjects (no XYZ, no per-cell count).
		type rowObj struct {
			Id           int32
			FileUri      string
			ThumbnailUri string
		}

		var pending []*pb.BrowsingStateResponse
		pendingSize := 0
		sendNow := func(force bool) error {
			if force || pendingSize >= batchSize {
				if err := flushCells(ctx, stream, pending); err != nil {
					return fmt.Errorf("GetBrowsingStateIncrementallysend batch (all/timeline): %w", err)
				}
				pending = pending[:0]
				pendingSize = 0
			}
			return nil
		}

	ScanLoop:
		for {
			// Non-blocking time-based flush for responsiveness.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-flushTicker.C:
				if err := sendNow(false); err != nil {
					return err
				}
			case <-firstFlushDeadline.C:
				// ensure user sees something quickly
				if err := sendNow(true); err != nil {
					return err
				}
			default:
			}

			if !rows.Next() {
				break ScanLoop
			}
			var r rowObj
			if err := rows.Scan(&r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
				return fmt.Errorf("GetBrowsingStateIncrementally scan row (all/timeline): %w", err)
			}

			// pack this object into a minimal BrowsingStateResponse
			resp := &pb.BrowsingStateResponse{
				CubeObjects: []*pb.CubeObject{{
					Id:           r.Id,
					FileUri:      r.FileUri,
					ThumbnailUri: r.ThumbnailUri,
				}},
			}
			pending = append(pending, resp)
			pendingSize++

			if pendingSize >= batchSize {
				if err := sendNow(true); err != nil {
					return err
				}
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("GetBrowsingStateIncrementallyrows iteration (all/timeline): %w", err)
		}
		// final flush
		if err := flushCells(ctx, stream, pending); err != nil {
			return err
		}
		return nil
	}

	// -------- State query: stream per-cell rows progressively --------
	// local struct mirrors your SELECT for state:
	type rowCell struct {
		X            int
		Y            int
		Z            int
		Id           int32
		FileUri      string
		ThumbnailUri string
		Count        int32
	}

	var batch []*pb.BrowsingStateResponse
	sendBatch := func(force bool) error {
		if !force && len(batch) < batchSize {
			return nil
		}
		if err := flushCells(ctx, stream, batch); err != nil {
			return fmt.Errorf("GetBrowsingStateIncrementallysend batch (state): %w", err)
		}
		batch = batch[:0]
		return nil
	}

ScanState:
	for {
		// periodic flush to avoid "silent" gaps
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTicker.C:
			if err := sendBatch(false); err != nil {
				return err
			}
		case <-firstFlushDeadline.C:
			// guarantee first paint fast
			if err := sendBatch(true); err != nil {
				return err
			}
		default:
		}

		if !rows.Next() {
			break ScanState
		}
		var r rowCell
		if err := rows.Scan(
			&r.X, &r.Y, &r.Z,
			&r.Id, &r.FileUri, &r.ThumbnailUri,
			&r.Count,
		); err != nil {
			return fmt.Errorf("GetBrowsingStateIncrementallyscan state row: %w", err)
		}

		// Map source IDs -> axis positions (like in GetCell)
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		resp := &pb.BrowsingStateResponse{
			X:     int32(posX),
			Y:     int32(posY),
			Z:     int32(posZ),
			Count: r.Count,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}
		batch = append(batch, resp)

		if len(batch) >= batchSize {
			if err := sendBatch(true); err != nil {
				return err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("GetBrowsingStateIncrementally rows iteration (state): %w", err)
	}
	// final flush
	if err := flushCells(ctx, stream, batch); err != nil {
		return err
	}

	return nil
}

func (s *DataLoaderServer) GetBrowsingState(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateServer) error {
	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)

	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// Flags for “all” and “timeline”
	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""
	sqlStr := ""
	// 2) Shortcut: “all” → PublicCubeObjects
	if allDefined {
		sqlStr = qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}

	if timelineDefined {
		sqlStr = qg.GenerateSQLQueryForTimeline(filters)
	}

	if sqlStr != "" {
		traceSQL("GetBrowsingState.exec(all/timeline)", formatSQLForLog("\n"+sqlStr, nil, 128))
		rows, err := s.db.QueryContext(stream.Context(), sqlStr)
		if err != nil {
			return fmt.Errorf("GetBrowsingState failed to execute query: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("GetBrowsingState failed to scan row: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}

		if len(cubeObjects) > 0 {
			resp := &pb.BrowsingStateResponse{
				CubeObjects: cubeObjects,
			}
			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("GetBrowsingState failed to send CellResponse: %w", err)
			}
		}

		return nil
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ, "GetBrowsingState(initAxes).exec"); err != nil {
		return err
	}

	sqlStr = qg.GenerateSQLQueryForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	traceSQL("GetBrowsingState.exec(state)", formatSQLForLog("\n"+sqlStr, nil, 128))
	rows, err := s.db.QueryContext(stream.Context(), sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingState query error: %w", err)
	}
	defer rows.Close()

	// local struct to hold each DB row
	type rowCell struct {
		X            int
		Y            int
		Z            int
		Id           int32
		FileUri      string
		ThumbnailUri string
		Count        int32
	}

	for rows.Next() {
		var r rowCell
		if err := rows.Scan(
			&r.X, &r.Y, &r.Z,
			&r.Id, &r.FileUri, &r.ThumbnailUri,
			&r.Count,
		); err != nil {
			return fmt.Errorf("GetBrowsingState scan state row: %w", err)
		}

		// map the axis‐IDs through the position maps
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		// build and send one BrowsingStateResponse per row
		resp := &pb.BrowsingStateResponse{
			X:     int32(posX),
			Y:     int32(posY),
			Z:     int32(posZ),
			Count: r.Count,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("GetBrowsingState send state response: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("GetBrowsingState rows iteration: %w", err)
	}

	return nil
}

func (s *DataLoaderServer) GetBrowsingState2(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingStateServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// -----------------------------------------------------------------------------
	// Sender goroutine plumbing:
	// - ONLY the sender goroutine calls stream.Send (gRPC streams are not safe for concurrent Send).
	// - The DB scan goroutine pushes fully-built responses onto a bounded channel.
	// - Bounded channel provides backpressure (prevents unbounded RAM growth).
	// -----------------------------------------------------------------------------
	const sendQueueSize = 5120 // tune: larger = more buffering, smaller = more backpressure
	sendCh := make(chan *pb.BrowsingStateResponse, sendQueueSize)
	sendErrCh := make(chan error, 1)

	// Sender goroutine (single writer to gRPC stream)
	go func() {
		for resp := range sendCh {
			select {
			case <-ctx.Done():
				sendErrCh <- ctx.Err()
				return
			default:
			}
			if err := stream.Send(resp); err != nil {
				sendErrCh <- err
				return
			}
		}
		// Normal completion
		sendErrCh <- nil
	}()

	// helper to enqueue responses safely (handles cancellation + sender failure)
	enqueue := func(resp *pb.BrowsingStateResponse) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sendErrCh:
			// sender already failed/finished early
			if err == nil {
				return fmt.Errorf("sender exited unexpectedly")
			}
			return err
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case sendCh <- resp:
			return nil
		case err := <-sendErrCh:
			// sender failed while we were trying to enqueue
			if err == nil {
				return fmt.Errorf("sender exited unexpectedly")
			}
			return err
		}
	}

	// ensure we always close sendCh and wait for sender before returning
	finish := func(retErr error) error {
		// stop sender
		close(sendCh)
		// wait for sender to drain / exit
		sendErr := <-sendErrCh

		// Prefer the "real" error if one exists.
		if retErr != nil {
			return retErr
		}
		if sendErr != nil {
			return sendErr
		}
		return nil
	}

	// Flags for “all” and “timeline”
	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""
	sqlStr := ""
	// 2) Shortcut: “all” → PublicCubeObjects
	if allDefined {
		sqlStr = qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}
	if timelineDefined {
		sqlStr = qg.GenerateSQLQueryForTimeline(filters)
	}

	// -------------------- all/timeline path --------------------
	if sqlStr != "" {
		traceSQL("GetBrowsingState.exec(all/timeline)", formatSQLForLog("\n"+sqlStr, nil, 128))
		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return finish(fmt.Errorf("GetBrowsingState failed to execute query: %w", err))
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return finish(fmt.Errorf("GetBrowsingState failed to scan row: %w", err))
			}
			cubeObjects = append(cubeObjects, c)
		}
		if err := rows.Err(); err != nil {
			return finish(fmt.Errorf("GetBrowsingState rows iteration (all/timeline): %w", err))
		}

		if len(cubeObjects) > 0 {
			resp := &pb.BrowsingStateResponse{CubeObjects: cubeObjects}
			if err := enqueue(resp); err != nil {
				return finish(fmt.Errorf("GetBrowsingState failed to enqueue CellResponse: %w", err))
			}
		}

		return finish(nil)
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "GetBrowsingState(initAxes).exec"); err != nil {
		return finish(err)
	}

	sqlStr = qg.GenerateSQLQueryForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	traceSQL("GetBrowsingState.exec(state)", formatSQLForLog("\n"+sqlStr, nil, 128))

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingState2 begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingState2 set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return finish(fmt.Errorf("GetBrowsingState query error: %w", err))
	}
	defer rows.Close()

	// local struct to hold each DB row
	type rowCell struct {
		X            int
		Y            int
		Z            int
		Id           int32
		FileUri      string
		ThumbnailUri string
		Count        int32
	}
	isFirst := true
	for rows.Next() {
		if isFirst {
			isFirst = false
		}
		var r rowCell
		if err := rows.Scan(
			&r.X, &r.Y, &r.Z,
			&r.Id, &r.FileUri, &r.ThumbnailUri,
			&r.Count,
		); err != nil {
			return finish(fmt.Errorf("GetBrowsingState scan state row: %w", err))
		}

		// map the axis‐IDs through the position maps
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		// build one response per row (same behavior as before)
		resp := &pb.BrowsingStateResponse{
			X:     int32(posX),
			Y:     int32(posY),
			Z:     int32(posZ),
			Count: r.Count,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}

		// enqueue instead of stream.Send (non-blocking until queue fills)
		if err := enqueue(resp); err != nil {
			return finish(fmt.Errorf("GetBrowsingState enqueue state response: %w", err))
		}
	}

	if err := rows.Err(); err != nil {
		return finish(fmt.Errorf("GetBrowsingState rows iteration: %w", err))
	}

	return finish(nil)
}
