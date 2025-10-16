package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

// Configurables for streaming responsiveness. TODO: make adjustable via ENV file or server config.
const (
	statePreviewLimit = 64                     // Phase-1 quick preview rows
	idChunkSize       = 10                     // IDs per final-select chunk (Incremental 2)
	streamBatchSize   = 64                     // CellResponse messages per flush
	batchSize         = 64                     // send once we have this many rows
	flushInterval     = 5 * time.Millisecond   // or at least this often
	maxFirstFlush     = 150 * time.Millisecond // ensure first batch <~ 150ms
)

// ---- SQL tracing (file-backed) ---------------------------------------------

// Compile-time override (handy during dev)
const forceSQLTrace = false

var (
	sqlTraceInit   sync.Once
	sqlTraceOn     bool
	sqlTraceLogger *log.Logger
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

// parseAxesAndFiltersFromRequest parses x/y/z axis JSON blobs and the filters JSON array.
// - Missing axis => defaults to {Type:"", Id:-1, Ids:{1:1}}
// - Missing/empty filters => returns nil slice
func parseAxesAndFiltersFromRequest(req *pb.GetCellRequest) (qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
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
// Use this in handlers that need x/y/z to be mapped to cube positions (id -> index).
func parseInitAxesAndFilters(ctx context.Context, db *sql.DB, req *pb.GetCellRequest) (qg.ParsedAxis, qg.ParsedAxis, qg.ParsedAxis, []qg.ParsedFilter, error) {
	axisX, axisY, axisZ, filters, err := parseAxesAndFiltersFromRequest(req)
	if err != nil {
		return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, err
	}

	if err := axisX.InitializeIds(ctx, db); err != nil {
		return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("init xAxis: %w", err)
	}
	if err := axisY.InitializeIds(ctx, db); err != nil {
		return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("init yAxis: %w", err)
	}
	if err := axisZ.InitializeIds(ctx, db); err != nil {
		return qg.ParsedAxis{}, qg.ParsedAxis{}, qg.ParsedAxis{}, nil, fmt.Errorf("init zAxis: %w", err)
	}

	return axisX, axisY, axisZ, filters, nil
}

// initXYZAxes initializes the Ids maps for X/Y/Z axes by querying the DB.
// It mirrors your inline code but wraps the three calls with clear error tags.
func initXYZAxes(ctx context.Context, db *sql.DB, axisX, axisY, axisZ *qg.ParsedAxis) error {
	if err := axisX.InitializeIds(ctx, db); err != nil {
		return fmt.Errorf("init xAxis: %w", err)
	}
	if err := axisY.InitializeIds(ctx, db); err != nil {
		return fmt.Errorf("init yAxis: %w", err)
	}
	if err := axisZ.InitializeIds(ctx, db); err != nil {
		return fmt.Errorf("init zAxis: %w", err)
	}
	return nil
}

// GetCellIncremental4: DB join only, no grouping; stream each tuple with Count=1.
// Client is responsible for grouping/aggregation.
func (s *DataLoaderServer) GetCellIncremental4(req *pb.GetCellRequest, stream pb.DataLoader_GetCellIncremental4Server) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisX, axisY, axisZ, filters, err := parseInitAxesAndFilters(stream.Context(), s.db, req)
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ); err != nil {
		return err
	}

	// ---------- Ungrouped SQL ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}

	traceSQL("Incremental4.ungrouped", sqlStr)

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("ungrouped state query: %w", err)
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

	pending := make([]*pb.CellResponse, 0, batchSize)

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
			return fmt.Errorf("ungrouped scan: %w", err)
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

		resp := &pb.CellResponse{
			X:     int32(px),
			Y:     int32(py),
			Z:     int32(pz),
			Count: 1, // each tuple contributes 1; client aggregates
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
		return fmt.Errorf("rows iteration: %w", err)
	}
	return flush(true)
}

func ternaryStr(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// GetCellIncremental3: DB does one intersecting join (no GROUP BY, no DISTINCT).
// Go aggregates per (x,y,z) online and streams authoritative updates.
func (s *DataLoaderServer) GetCellIncremental3(req *pb.GetCellRequest, stream pb.DataLoader_GetCellIncremental3Server) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisX, axisY, axisZ, filters, err := parseInitAxesAndFilters(stream.Context(), s.db, req)
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ); err != nil {
		return err
	}
	// ---------- Ungrouped SQL (no DISTINCT, no GROUP BY) ----------
	sqlStr := qg.GenerateUngroupedSQLForState(
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	if sqlStr == "" {
		sqlStr = `select 1 as x_id, 1 as y_id, 1 as z_id, O.id as object_id, O.file_uri, O.thumbnail_uri from medias O;`
	}

	traceSQL("Incremental3.ungrouped", sqlStr)

	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("ungrouped state query: %w", err)
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
	}

	cells := make(map[cellKey]*cellAgg, 2048)
	dirty := make(map[cellKey]struct{}, 512)

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flush := func(force bool) error {
		if !force && len(dirty) == 0 {
			return nil
		}
		i := 0
		for k := range dirty {
			agg := cells[k]
			resp := &pb.CellResponse{
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			delete(dirty, k)
			i++
			if !force && i >= streamBatchSize {
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
			return fmt.Errorf("ungrouped scan: %w", err)
		}

		// Map IDs -> axis positions
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

		// DISTINCT object per cell (compensates for removed DISTINCT in SQL branches)
		if _, ok := agg.seen[r.ObjectID]; !ok {
			agg.seen[r.ObjectID] = struct{}{}
			agg.count++
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
		}

		dirty[key] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("ungrouped rows: %w", err)
	}
	return flush(true)
}

func (s *DataLoaderServer) GetCellIncremental2(req *pb.GetCellRequest, stream pb.DataLoader_GetCellIncremental2Server) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisX, axisY, axisZ, filters, err := parseInitAxesAndFilters(stream.Context(), s.db, req)
	if err != nil {
		return err
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ); err != nil {
		return err
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()
	firstFlushDeadline := time.NewTimer(maxFirstFlush)
	defer firstFlushDeadline.Stop()

	// batch buffer of CellResponse rows that will be wrapped into a CellChunk
	pendingRows := make([]*pb.CellResponse, 0, streamBatchSize)
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
	queue := func(r *pb.CellResponse) error {
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
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		statePreviewLimit,
	)
	{
		traceSQL("Incremental2.preview", previewSQL)
		rows, err := s.db.QueryContext(ctx, previewSQL)
		if err != nil {
			return fmt.Errorf("preview state query: %w", err)
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
				return fmt.Errorf("preview state scan: %w", err)
			}
			// map axis IDs -> positions
			posX := axisX.Ids[r.X]
			posY := axisY.Ids[r.Y]
			posZ := axisZ.Ids[r.Z]
			resp := &pb.CellResponse{
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
			return fmt.Errorf("preview state rows: %w", err)
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

		traceSQL("Incremental2.idset", sqlStr)
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
		return fmt.Errorf("axis X ids: %w", err)
	} else if sqlX != "" {
		ids, err := fetchIDSet(sqlX)
		if err != nil {
			return fmt.Errorf("axis X fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			// nothing beyond preview
			return nil
		}
	}
	if sqlY, err := qg.BuildAxisObjectIDSQLForState(axisY.Type, axisY.Id); err != nil {
		return fmt.Errorf("axis Y ids: %w", err)
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
		return fmt.Errorf("axis Z ids: %w", err)
	} else if sqlZ != "" {
		ids, err := fetchIDSet(sqlZ)
		if err != nil {
			return fmt.Errorf("axis Z fetch: %w", err)
		}
		intersectWith(ids)
		if currentIDs == nil || len(currentIDs) == 0 {
			return nil
		}
	}
	for _, f := range filters {
		sqlF, err := qg.BuildFilterIDSQL(f)
		if err != nil {
			return fmt.Errorf("filter build: %w", err)
		}
		ids, err := fetchIDSet(sqlF)
		if err != nil {
			return fmt.Errorf("filter fetch: %w", err)
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

		traceSQL("Incremental2.stateChunk", sqlStr)
		rows, err := s.db.QueryContext(ctx, sqlStr)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var r rowCell
			if err := rows.Scan(&r.X, &r.Y, &r.Z, &r.Id, &r.FileUri, &r.ThumbnailUri, &r.Count); err != nil {
				return fmt.Errorf("state chunk scan: %w", err)
			}
			resp := &pb.CellResponse{
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
func flushCellsChunk(ctx context.Context, stream pb.DataLoader_GetCellIncremental2Server, rows []*pb.CellResponse, authoritative bool) error {
	if len(rows) == 0 {
		return nil
	}
	chunk := &pb.CellChunk{
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
func flushCells(ctx context.Context, stream pb.DataLoader_GetCellIncrementalServer, out []*pb.CellResponse) error {
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
func (s *DataLoaderServer) GetCellIncremental(req *pb.GetCellRequest, stream pb.DataLoader_GetCellIncrementalServer) error {
	ctx := stream.Context()

	// ---------- Parse request params ----------
	axisX, axisY, axisZ, filters, err := parseInitAxesAndFilters(stream.Context(), s.db, req)
	if err != nil {
		return err
	}

	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""

	// If X/Y/Z need ID-position maps, init them *before* streaming state rows.
	if !allDefined && !timelineDefined {
		// ---------- Axis positions ----------
		if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ); err != nil {
			return err
		}
	}

	// -------- Build SQL string (reuse your generators) --------
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
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}

	// -------- Execute query with context (streaming rows.Next) --------
	traceSQL("Incremental1.exec", sqlStr)
	rows, err := s.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
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

		var pending []*pb.CellResponse
		pendingSize := 0
		sendNow := func(force bool) error {
			if force || pendingSize >= batchSize {
				if err := flushCells(ctx, stream, pending); err != nil {
					return fmt.Errorf("send batch (all/timeline): %w", err)
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
				return fmt.Errorf("scan row (all/timeline): %w", err)
			}

			// pack this object into a minimal CellResponse
			resp := &pb.CellResponse{
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
			return fmt.Errorf("rows iteration (all/timeline): %w", err)
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

	var batch []*pb.CellResponse
	sendBatch := func(force bool) error {
		if !force && len(batch) < batchSize {
			return nil
		}
		if err := flushCells(ctx, stream, batch); err != nil {
			return fmt.Errorf("send batch (state): %w", err)
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
			return fmt.Errorf("scan state row: %w", err)
		}

		// Map source IDs -> axis positions (like in GetCell)
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		resp := &pb.CellResponse{
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
		return fmt.Errorf("rows iteration (state): %w", err)
	}
	// final flush
	if err := flushCells(ctx, stream, batch); err != nil {
		return err
	}

	return nil
}

func (s *DataLoaderServer) GetCell(req *pb.GetCellRequest, stream pb.DataLoader_GetCellServer) error {
	// Axes come in as JSON blobs
	// ---------- Parse request params ----------
	axisX, axisY, axisZ, filters, err := parseInitAxesAndFilters(stream.Context(), s.db, req)
	if err != nil {
		return err
	}

	// Flags for “all” and “timeline”
	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""
	sqlstr := ""
	// 2) Shortcut: “all” → PublicCubeObjects
	if allDefined {
		sqlstr = qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}

	if timelineDefined {
		sqlstr = qg.GenerateSQLQueryForTimeline(filters)
	}

	if sqlstr != "" {
		traceSQL("GetCell.exec(all/timeline)", sqlstr)
		rows, err := s.db.QueryContext(stream.Context(), sqlstr)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}

		if len(cubeObjects) > 0 {
			resp := &pb.CellResponse{
				CubeObjects: cubeObjects,
			}
			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("failed to send CellResponse: %w", err)
			}
		}

		return nil
	}

	// ---------- Axis positions ----------
	if err := initXYZAxes(stream.Context(), s.db, &axisX, &axisY, &axisZ); err != nil {
		return err
	}

	sqlstr = qg.GenerateSQLQueryForState(
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	traceSQL("GetCell.exec(state)", sqlstr)
	rows, err := s.db.QueryContext(stream.Context(), sqlstr)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
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
			return fmt.Errorf("scan state row: %w", err)
		}

		// map the axis‐IDs through the position maps
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		// build and send one CellResponse per row
		resp := &pb.CellResponse{
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
			return fmt.Errorf("send state response: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	return nil
}
