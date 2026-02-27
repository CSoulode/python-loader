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
	"sync/atomic"
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
	defaultCellMapCap = 65536
	defaultDirtyCap   = 65536
	dirtyChanCap      = 65536
	defAxisPos        = 1
	unsetAxisId       = -1
	sqlTraceMaxLtr    = 128
)

type rowCell struct {
	X            int
	Y            int
	Z            int
	Id           int32
	FileUri      string
	ThumbnailUri string
	Count        int32
}

// ---- SQL tracing (file-backed) ---------------------------------------------

// Compile-time override (handy during dev)
const forceSQLTrace = false

var (
	sqlTraceInit        sync.Once
	sqlTraceOn          bool
	sqlTraceLogger      *log.Logger
	disableHashJoins    = utilities.MustGetEnv("UNGROUPED_QUERY_DISABLE_HASH_JOIN") == "1"
	forceJoinOrders     = utilities.MustGetEnv("UNGROUPED_QUERY_FORCE_JOIN_ORDERS") == "1"
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
		traceSQL(logContext, formatSQLForLog(plan.MainSQL, plan.MainArgs, sqlTraceMaxLtr))
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
		traceSQL(logContext, formatSQLForLog(plan.PreSQL, plan.PreArgs, sqlTraceMaxLtr))
		if err := db.QueryRowContext(ctx, plan.PreSQL, plan.PreArgs...).Scan(&hierarchyID); err != nil {
			return fmt.Errorf("initializeIds(node) fetch hierarchy_id: %w", err)
		}

		// 2) now run MainSQL using parent node id (p.Id) and hierarchyID
		traceSQL(logContext, formatSQLForLog(plan.MainSQL, []any{p.Id, hierarchyID}, sqlTraceMaxLtr))
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
		idList[1] = defAxisPos

	default:
		return fmt.Errorf("unknown plan kind %q", plan.Kind)
	}

	// safety: never leave it empty
	if len(idList) == 0 {
		idList[1] = defAxisPos
	}

	p.Ids = idList
	return nil
}

// GetBrowsingStateDistinctBranchesChunks:
// - Uses the ungrouped (no GROUP BY) SQL with DISTINCT branches.
// - Aggregates per-cell in Go with DISTINCT(object_id) and MAX(object_id) as representative.
// - Streams authoritative updates periodically while scanning (stream.Send happens in a dedicated goroutine).
// - Uses transaction scope and optional SET LOCAL enable_hashjoin=off.
func (s *DataLoaderServer) GetBrowsingStateDistinctBranchesIncrementalGrouping(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateDistinctBranchesIncrementalGroupingServer,
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

	// ---------- Axis positions (needed to map ids -> positions) ----------
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "DistinctBranchesIncrementalGrouping(initAxes).exec"); err != nil {
		return err
	}

	// ---------- State semantics: ungrouped SQL with DISTINCT branches ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{
			BranchDistinct:      true,
			UseLateralMediaJoin: useLateralMediaJoin,
		},
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("DistinctBranchesIncrementalGrouping.ungrouped-distinct", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("DistinctBranchesIncrementalGrouping")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping ungrouped: %w", err)
	}
	defer rows.Close()

	// Keep large buffers (match your previous hot-path behavior unless you tune later)
	dirtyCh := make(chan cellKey, 65536)

	agg := NewCellAggregator(CellAggregatorOpts{
		InitialCap: 65536,
		SeenCap:    16,
	})

	// Dedicated sender goroutine: owns stream.Send + periodic flush semantics.
	// Metrics:
	// - consumes dirty keys => queue depth decreases
	// - sends responses => first send + sent counter
	sendErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- RunKeyFlusherWithMetrics(
			ctx,
			stream,
			dirtyCh,
			agg,
			flushInterval,
			streamBatchSize,
			65536, // pendingCap
			func(sn snap) *pb.BrowsingStateResponse {
				return &pb.BrowsingStateResponse{
					X:     sn.k.x,
					Y:     sn.k.y,
					Z:     sn.k.z,
					Count: sn.count,
					CubeObjects: []*pb.CubeObject{{
						Id:           sn.repID,
						FileUri:      sn.fileURI,
						ThumbnailUri: sn.thumbURI,
					}},
				}
			},
			m,
		)
	}()

scanLoop:
	for {
		select {
		case <-ctx.Done():
			close(dirtyCh)
			_ = <-sendErrCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			close(dirtyCh)
			_ = <-sendErrCh
			m.LogSummary("scan_error=true")
			return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping scan: %w", err)
		}

		// successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		// Map DB IDs -> cube positions (default 1 when axis empty)
		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}
		key := cellKey{int32(px), int32(py), int32(pz)}

		_, shouldEnqueue := agg.ApplyRow(key, r.Id, r.FileUri, r.ThumbnailUri, 16)

		if shouldEnqueue {
			// We only count produced items when the enqueue actually succeeds.
			// Track dirty-key queue depth as the backpressure signal for this mode.
			m.IncQueue(+1)

			select {
			case dirtyCh <- key:
				atomic.AddInt64(&m.ItemsProduced, 1)
			case <-ctx.Done():
				// undo queue increment because enqueue didn't happen
				m.IncQueue(-1)
				close(dirtyCh)
				_ = <-sendErrCh
				m.LogSummary("ctx_cancelled=true")
				return ctx.Err()
			}
		}
	}

	if err := rows.Err(); err != nil {
		close(dirtyCh)
		_ = <-sendErrCh
		m.LogSummary("rows_err=true")
		return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping rows: %w", err)
	}

	// Finish sending remaining pending updates and wait for sender to exit.
	close(dirtyCh)
	if err := <-sendErrCh; err != nil {
		m.LogSummary("send_err=true")
		return err
	}

	if err := tx.Commit(); err != nil {
		m.LogSummary("commit_err=true")
		return fmt.Errorf("GetBrowsingStateDistinctBranchesIncrementalGrouping commit: %w", err)
	}

	// final metrics
	m.LogSummary("")
	return nil
}

func (s *DataLoaderServer) GetBrowsingStateDistinctBranchesFull(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateDistinctBranchesFullServer,
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
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "DistinctBranchesFull(initAxes).exec"); err != nil {
		return err
	}

	// ---------- State semantics via in-memory grouping ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{
			BranchDistinct:      true,
			UseLateralMediaJoin: useLateralMediaJoin,
		},
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("DistinctBranchesFull(ungrouped, distinct-branches).exec", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("DistinctBranchesFull")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateDistinctBranchesFull begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateDistinctBranchesFull set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("getBrowsingStateDistinctBranchesFull ungrouped: %w", err)
	}
	defer rows.Close()

	// ---------- Grouping goroutine ----------
	type tuple struct {
		key      cellKey
		objectID int32
		fileURI  string
		thumbURI string
	}

	tupleCh := make(chan tuple, 4096)
	resultCh := make(chan map[cellKey]*cellAgg, 1)
	groupErrCh := make(chan error, 1)

	go func() {
		cells := make(map[cellKey]*cellAgg, defaultCellMapCap)

		for t := range tupleCh {
			// dequeue -> decrease depth
			m.IncQueue(-1)

			agg := cells[t.key]
			if agg == nil {
				agg = &cellAgg{seen: make(map[int32]struct{}, 8)}
				cells[t.key] = agg
			}

			if _, ok := agg.seen[t.objectID]; !ok {
				agg.seen[t.objectID] = struct{}{}
				agg.count++
			}
			if t.objectID > agg.repID {
				agg.repID = t.objectID
				agg.fileURI = t.fileURI
				agg.thumbURI = t.thumbURI
			}
		}

		resultCh <- cells
		groupErrCh <- nil
	}()

	// ---------- Scan all rows (no streaming yet — emit only final results) ----------
scanLoop:
	for {
		select {
		case <-ctx.Done():
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			return fmt.Errorf("getBrowsingStateDistinctBranchesFull scan: %w", err)
		}

		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}

		t := tuple{
			key:      cellKey{int32(px), int32(py), int32(pz)},
			objectID: r.Id,
			fileURI:  r.FileUri,
			thumbURI: r.ThumbnailUri,
		}

		m.IncQueue(+1)
		select {
		case tupleCh <- t:
			atomic.AddInt64(&m.ItemsProduced, 1)
		case <-ctx.Done():
			m.IncQueue(-1) // undo the +1 since it never entered the queue
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			return ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		close(tupleCh)
		_ = <-groupErrCh
		_ = <-resultCh
		return fmt.Errorf("getBrowsingStateDistinctBranchesFull rows: %w", err)
	}

	// Finish grouping
	close(tupleCh)
	if err := <-groupErrCh; err != nil {
		_ = <-resultCh
		return err
	}
	cells := <-resultCh
	m.MarkGroupDone()

	// ---------- Send final authoritative results ----------
	for k, a := range cells {
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

		m.MarkFirstSend()

		if err := stream.Send(resp); err != nil {
			return err
		}
		atomic.AddInt64(&m.ItemsSent, 1)
	}

	// Commit (matches your other tx-scoped handlers)
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("GetBrowsingStateDistinctBranchesFull commit: %w", err)
	}

	total := time.Since(m.Start)

	// ---------- One-line summary (not in hot loops) ----------
	// Use your own logger/tracer if you prefer.
	log.Printf(
		"DistinctBranchesFull metrics: rowsRead=%d uniqueCells=%d maxQueueDepth=%d firstRow=%s groupingDone=%s firstSend=%s total=%s",
		atomic.LoadInt64(&m.RowsRead),
		len(cells),
		atomic.LoadInt64(&m.MaxQueueDepth),
		time.Duration(atomic.LoadInt64(&m.FirstRowNS)),
		time.Duration(atomic.LoadInt64(&m.DoneGroupNS)),
		time.Duration(atomic.LoadInt64(&m.FirstSendNS)),
		total,
	)

	return nil
}

func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesSingles(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesSinglesServer,
) error {
	ctx := stream.Context()

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesSingles(initAxes).exec"); err != nil {
		return err
	}

	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{
			BranchDistinct:      false,
			UseLateralMediaJoin: useLateralMediaJoin,
		},
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}

	traceSQL("NonDistinctBranchesSingles.ungrouped", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("NonDistinctBranchesSingles")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles state query: %w", err)
	}
	defer rows.Close()

	// ---------- Dedicated sender goroutine ----------
	respCh := make(chan *pb.BrowsingStateResponse, 4096)
	sendErrCh := make(chan error, 1)

	go func() {
		sendErrCh <- RunQueueSenderWithMetrics(ctx, stream, respCh, m)
	}()

scanLoop:
	for {
		select {
		case <-ctx.Done():
			close(respCh)
			_ = <-sendErrCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			close(respCh)
			_ = <-sendErrCh
			m.LogSummary("scan_error=true")
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles scan: %w", err)
		}

		// Successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}

		resp := &pb.BrowsingStateResponse{
			X:     int32(px),
			Y:     int32(py),
			Z:     int32(pz),
			Count: 1,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}

		// Count "produced" only if enqueue succeeds. Track queue depth as backpressure evidence.
		m.IncQueue(+1)
		select {
		case respCh <- resp:
			atomic.AddInt64(&m.ItemsProduced, 1)
		case <-ctx.Done():
			// Undo queue increment since it never entered the queue.
			m.IncQueue(-1)
			close(respCh)
			_ = <-sendErrCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		close(respCh)
		_ = <-sendErrCh
		m.LogSummary("rows_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles rows iteration: %w", err)
	}

	close(respCh)
	if err := <-sendErrCh; err != nil {
		m.LogSummary("send_err=true")
		return err
	}

	if err := tx.Commit(); err != nil {
		m.LogSummary("commit_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesSingles commit: %w", err)
	}

	m.LogSummary("")
	return nil
}

func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesDeduplicatedSingles(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesDeduplicatedSinglesServer,
) error {
	ctx := stream.Context()

	// Toggle for experiments:
	// - false: dedup in scan loop (likely faster, less queue traffic)
	// - true:  dedup in sender goroutine (interesting for comparison / backpressure evidence)
	dedupInSender := false

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesDedupSingles(initAxes).exec"); err != nil {
		return err
	}

	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.UngroupedOpts{
			BranchDistinct:      false,
			UseLateralMediaJoin: useLateralMediaJoin,
		},
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("NonDistinctBranchesDedupSingles.ungrouped", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("NonDistinctBranchesDeduplicatedSingles")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles state query: %w", err)
	}
	defer rows.Close()

	// ---------- Dedicated sender goroutine ----------
	sendErrCh := make(chan error, 1)

	// For “dedup in scan” we enqueue responses.
	var respCh chan *pb.BrowsingStateResponse
	// For “dedup in sender” we enqueue tuples.
	var itemCh chan DedupTuple

	if dedupInSender {
		itemCh = make(chan DedupTuple, 4096)
		go func() {
			sendErrCh <- RunQueueSenderDedupWithMetrics(ctx, stream, itemCh, 16, 65536, m)
		}()
	} else {
		respCh = make(chan *pb.BrowsingStateResponse, 4096)
		go func() {
			sendErrCh <- RunQueueSenderWithMetrics(ctx, stream, respCh, m)
		}()
	}

	// If dedup is in scan:
	var seen map[cellKey]map[int32]struct{}
	if !dedupInSender {
		seen = make(map[cellKey]map[int32]struct{}, 65536)
	}

scanLoop:
	for {
		select {
		case <-ctx.Done():
			if dedupInSender {
				close(itemCh)
			} else {
				close(respCh)
			}
			_ = <-sendErrCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			if dedupInSender {
				close(itemCh)
			} else {
				close(respCh)
			}
			_ = <-sendErrCh
			m.LogSummary("scan_error=true")
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles scan: %w", err)
		}

		// Successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}
		key := cellKey{int32(px), int32(py), int32(pz)}

		if dedupInSender {
			// Enqueue everything; sender deduplicates.
			it := DedupTuple{
				Key:      key,
				ObjectID: r.Id,
				FileURI:  r.FileUri,
				ThumbURI: r.ThumbnailUri,
			}

			// Count "produced" only if enqueue succeeds; track queue depth for backpressure.
			m.IncQueue(+1)
			select {
			case itemCh <- it:
				atomic.AddInt64(&m.ItemsProduced, 1)
			case <-ctx.Done():
				m.IncQueue(-1)
				close(itemCh)
				_ = <-sendErrCh
				m.LogSummary("ctx_cancelled=true")
				return ctx.Err()
			}
			continue
		}

		// Deduplicate in scan loop (likely faster)
		sset := seen[key]
		if sset == nil {
			sset = make(map[int32]struct{}, 16)
			seen[key] = sset
		}
		if _, dup := sset[r.Id]; dup {
			atomic.AddInt64(&m.DupsSkipped, 1)
			continue
		}
		sset[r.Id] = struct{}{}

		resp := &pb.BrowsingStateResponse{
			X:     key.x,
			Y:     key.y,
			Z:     key.z,
			Count: 1,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}

		// Count "produced" only if enqueue succeeds; track queue depth for backpressure.
		m.IncQueue(+1)
		select {
		case respCh <- resp:
			atomic.AddInt64(&m.ItemsProduced, 1)
		case <-ctx.Done():
			m.IncQueue(-1)
			close(respCh)
			_ = <-sendErrCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		if dedupInSender {
			close(itemCh)
		} else {
			close(respCh)
		}
		_ = <-sendErrCh
		m.LogSummary("rows_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles rows iteration: %w", err)
	}

	// Signal sender completion, wait, then commit
	if dedupInSender {
		close(itemCh)
	} else {
		close(respCh)
	}

	if err := <-sendErrCh; err != nil {
		m.LogSummary("send_err=true")
		return err
	}

	if err := tx.Commit(); err != nil {
		m.LogSummary("commit_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesDeduplicatedSingles commit: %w", err)
	}

	m.LogSummary("")
	return nil
}

// GetBrowsingStateNonDistinctBranchesFull:
// - Runs an ungrouped (no GROUP BY, no DISTINCT) join on the DB (non-distinct branches).
// - Performs per-cell DISTINCT(object_id) counting + representative MAX(object_id) in a grouping goroutine.
// - Sends authoritative final results ONLY after grouping completes (no incremental streaming).
// - Uses tx scope and optional SET LOCAL enable_hashjoin=off.
// - Includes StreamMetrics instrumentation (rowsRead, produced tuples, max queue depth, firstRow, groupDone, firstSend, sent).
func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesFull(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesFullServer,
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
	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesFull(initAxes).exec"); err != nil {
		return err
	}

	// ---------- Ungrouped SQL (NO DISTINCT, NO GROUP BY) ----------
	qgOpts := qg.UngroupedOpts{
		BranchDistinct:      false,
		UseLateralMediaJoin: useLateralMediaJoin,
	}
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qgOpts,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}
	traceSQL("NonDistinctBranchesFull.ungrouped", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("NonDistinctBranchesFull")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull set enable_hashjoin=off: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull state query: %w", err)
	}
	defer rows.Close()

	// ---------- Grouping goroutine ----------
	type tuple struct {
		key      cellKey
		objectID int32
		fileURI  string
		thumbURI string
	}

	// Same buffering strategy as your other full/grouped variants
	tupleCh := make(chan tuple, 4096)
	resultCh := make(chan map[cellKey]*cellAgg, 1)
	groupErrCh := make(chan error, 1)

	go func() {
		cells := make(map[cellKey]*cellAgg, defaultCellMapCap)

		for t := range tupleCh {
			// consumed from queue
			m.IncQueue(-1)

			agg := cells[t.key]
			if agg == nil {
				agg = &cellAgg{seen: make(map[int32]struct{}, 16)}
				cells[t.key] = agg
			}

			// DISTINCT object_id per cell
			if _, ok := agg.seen[t.objectID]; !ok {
				agg.seen[t.objectID] = struct{}{}
				agg.count++
			}

			// Representative = MAX(object_id)
			if t.objectID > agg.repID {
				agg.repID = t.objectID
				agg.fileURI = t.fileURI
				agg.thumbURI = t.thumbURI
			}
		}

		resultCh <- cells
		groupErrCh <- nil
	}()

	// ---------- Scan rows, push tuples (no grouping in scan loop) ----------
scanLoop:
	for {
		select {
		case <-ctx.Done():
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			m.LogSummary("scan_error=true")
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull scan: %w", err)
		}

		// Successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}

		t := tuple{
			key:      cellKey{int32(px), int32(py), int32(pz)},
			objectID: r.Id,
			fileURI:  r.FileUri,
			thumbURI: r.ThumbnailUri,
		}

		// Track queue depth; count produced only if enqueue succeeds.
		m.IncQueue(+1)
		select {
		case tupleCh <- t:
			atomic.AddInt64(&m.ItemsProduced, 1)
		case <-ctx.Done():
			m.IncQueue(-1)
			close(tupleCh)
			_ = <-groupErrCh
			_ = <-resultCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		close(tupleCh)
		_ = <-groupErrCh
		_ = <-resultCh
		m.LogSummary("rows_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull rows: %w", err)
	}

	// Finish grouping
	close(tupleCh)
	if err := <-groupErrCh; err != nil {
		_ = <-resultCh
		m.LogSummary("group_err=true")
		return err
	}
	cells := <-resultCh
	m.MarkGroupDone()

	// ---------- Send final authoritative results ----------
	for k, a := range cells {
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
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		m.MarkFirstSend()
		if err := stream.Send(resp); err != nil {
			m.LogSummary("send_err=true")
			return err
		}
		atomic.AddInt64(&m.ItemsSent, 1)
	}

	if err := tx.Commit(); err != nil {
		m.LogSummary("commit_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesFull commit: %w", err)
	}

	// Include uniqueCells in the summary as extra context
	m.LogSummary(fmt.Sprintf("uniqueCells=%d", len(cells)))
	return nil
}

// GetBrowsingStateNonDistinctBranches: DB does one intersecting join (no GROUP BY, no DISTINCT).
// Go aggregates per (x,y,z) online and streams authoritative updates.
// stream.Send happens only inside KeyFlusher (dedicated goroutine).
func (s *DataLoaderServer) GetBrowsingStateNonDistinctBranchesIncrementalGrouping(
	req *pb.GetBrowsingStateRequest,
	stream pb.DataLoader_GetBrowsingStateNonDistinctBranchesIncrementalGroupingServer,
) error {
	ctx := stream.Context()

	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	if err := initXYZAxes(ctx, s.db, &axisX, &axisY, &axisZ, "NonDistinctBranchesIncrementalGrouping(initAxes).exec"); err != nil {
		return err
	}

	qgOpts := qg.UngroupedOpts{
		BranchDistinct:      false,
		UseLateralMediaJoin: useLateralMediaJoin,
	}

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

	traceSQL("NonDistinctBranchesIncrementalGrouping.ungrouped", formatSQLForLog("\n"+sqlStr, nil, 128))

	// ---------- Instrumentation Init ----------
	m := NewStreamMetrics("NonDistinctBranchesIncrementalGrouping")

	// ---------- TX scope (ReadOnly + optional SET LOCAL) ----------
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping set enable_hashjoin=off: %w", err)
		}
	}

	if forceJoinOrders {
		if _, err := tx.ExecContext(ctx, "SET LOCAL join_collapse_limit = 1"); err != nil {
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping set join_collapse_limit=1: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping state query: %w", err)
	}
	defer rows.Close()

	// Keep old large buffers for perf baseline
	dirtyCh := make(chan cellKey, 65536)

	agg := NewCellAggregator(CellAggregatorOpts{
		InitialCap: 65536,
		SeenCap:    16,
	})

	// Key flusher goroutine: owns stream.Send
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunKeyFlusherWithMetrics(
			ctx,
			stream,
			dirtyCh,
			agg,
			flushInterval,
			streamBatchSize,
			65536,
			func(sn snap) *pb.BrowsingStateResponse {
				return &pb.BrowsingStateResponse{
					X:     sn.k.x,
					Y:     sn.k.y,
					Z:     sn.k.z,
					Count: sn.count,
					CubeObjects: []*pb.CubeObject{{
						Id:           sn.repID,
						FileUri:      sn.fileURI,
						ThumbnailUri: sn.thumbURI,
					}},
				}
			},
			m,
		)
	}()

scanLoop:
	for {
		select {
		case <-ctx.Done():
			close(dirtyCh)
			_ = <-errCh
			m.LogSummary("ctx_cancelled=true")
			return ctx.Err()
		default:
		}

		if !rows.Next() {
			break scanLoop
		}

		var r rowCell
		if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri); err != nil {
			close(dirtyCh)
			_ = <-errCh
			m.LogSummary("scan_error=true")
			return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping scan: %w", err)
		}

		// Successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

		px := axisX.Ids[r.X]
		if px == 0 {
			px = defAxisPos
		}
		py := axisY.Ids[r.Y]
		if py == 0 {
			py = defAxisPos
		}
		pz := axisZ.Ids[r.Z]
		if pz == 0 {
			pz = defAxisPos
		}

		key := cellKey{int32(px), int32(py), int32(pz)}
		_, shouldEnqueue := agg.ApplyRow(key, r.Id, r.FileUri, r.ThumbnailUri, 16)

		if shouldEnqueue {
			// Track dirty-key queue depth as backpressure evidence.
			// Count produced only if enqueue succeeds.
			m.IncQueue(+1)
			select {
			case dirtyCh <- key:
				atomic.AddInt64(&m.ItemsProduced, 1)
			case <-ctx.Done():
				m.IncQueue(-1)
				close(dirtyCh)
				_ = <-errCh
				m.LogSummary("ctx_cancelled=true")
				return ctx.Err()
			}
		}
	}

	if err := rows.Err(); err != nil {
		close(dirtyCh)
		_ = <-errCh
		m.LogSummary("rows_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping rows: %w", err)
	}

	close(dirtyCh)
	if err := <-errCh; err != nil {
		m.LogSummary("send_err=true")
		return err
	}

	if err := tx.Commit(); err != nil {
		m.LogSummary("commit_err=true")
		return fmt.Errorf("GetBrowsingStateNonDistinctBranchesIncrementalGrouping commit: %w", err)
	}

	m.LogSummary("")
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
		traceSQL("GetBrowsingState.exec(all/timeline)", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))
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
	traceSQL("GetBrowsingState.exec(state)", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))
	rows, err := s.db.QueryContext(stream.Context(), sqlStr)
	if err != nil {
		return fmt.Errorf("GetBrowsingState query error: %w", err)
	}
	defer rows.Close()

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

func (s *DataLoaderServer) GetBrowsingState2(req *pb.GetBrowsingStateRequest, stream pb.DataLoader_GetBrowsingState2Server) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisOrder, axisX, axisY, axisZ, filters, err := parseAxesAndFilters(req)
	if axisOrder == nil {
		return fmt.Errorf("invalid axis filter order")
	}
	if err != nil {
		return err
	}

	// ---------- Instrumentation Init ----------
	// Captures everything from here (sender plumbing, tx begin, query, scan, send).
	m := NewStreamMetrics("GetBrowsingState2")

	// -----------------------------------------------------------------------------
	// Sender goroutine plumbing:
	// - ONLY the sender goroutine calls stream.Send (gRPC streams are not safe for concurrent Send).
	// - The DB scan goroutine pushes fully-built responses onto a bounded channel.
	// - Bounded channel provides backpressure (prevents unbounded RAM growth).
	// -----------------------------------------------------------------------------
	sendCh := make(chan *pb.BrowsingStateResponse, defaultCellMapCap)
	sendErrCh := make(chan error, 1)

	// Sender goroutine (single writer to gRPC stream)
	go func() {
		for resp := range sendCh {
			// consumed from queue
			m.IncQueue(-1)

			select {
			case <-ctx.Done():
				sendErrCh <- ctx.Err()
				return
			default:
			}

			m.MarkFirstSend()
			if err := stream.Send(resp); err != nil {
				sendErrCh <- err
				return
			}
			atomic.AddInt64(&m.ItemsSent, 1)
		}
		// Normal completion
		sendErrCh <- nil
	}()

	// helper to enqueue responses safely (handles cancellation + sender failure)
	enqueue := func(resp *pb.BrowsingStateResponse) error {
		// Fast path: check cancellation/sender failure without blocking.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sendErrCh:
			if err == nil {
				return fmt.Errorf("sender exited unexpectedly")
			}
			return err
		default:
		}

		// Track backpressure: only count produced if enqueue succeeds.
		m.IncQueue(+1)
		select {
		case <-ctx.Done():
			m.IncQueue(-1) // undo (+1) because it wasn't enqueued
			return ctx.Err()
		case sendCh <- resp:
			atomic.AddInt64(&m.ItemsProduced, 1)
			return nil
		case err := <-sendErrCh:
			m.IncQueue(-1) // undo (+1) because it wasn't enqueued
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
			m.LogSummary("retErr=true")
			return retErr
		}
		if sendErr != nil {
			m.LogSummary("sendErr=true")
			return sendErr
		}
		m.LogSummary("")
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
		traceSQL("GetBrowsingState.exec(all/timeline)", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))
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
			// Successfully scanned a row
			atomic.AddInt64(&m.RowsRead, 1)
			m.MarkFirstRow()

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

	traceSQL("GetBrowsingState.exec(state)", formatSQLForLog("\n"+sqlStr, nil, sqlTraceMaxLtr))

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

	for rows.Next() {
		var r rowCell
		if err := rows.Scan(
			&r.X, &r.Y, &r.Z,
			&r.Id, &r.FileUri, &r.ThumbnailUri,
			&r.Count,
		); err != nil {
			return finish(fmt.Errorf("GetBrowsingState scan state row: %w", err))
		}

		// Successfully scanned a row
		atomic.AddInt64(&m.RowsRead, 1)
		m.MarkFirstRow()

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

		// enqueue instead of stream.Send (bounded queue provides backpressure)
		if err := enqueue(resp); err != nil {
			return finish(fmt.Errorf("GetBrowsingState enqueue state response: %w", err))
		}
	}

	if err := rows.Err(); err != nil {
		return finish(fmt.Errorf("GetBrowsingState rows iteration: %w", err))
	}

	// Note: original code didn't commit tx; keeping behavior identical.
	// If you want to commit, do it here and wrap with finish(...).
	return finish(nil)
}
