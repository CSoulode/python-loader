package index

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Model struct {
	Table          string
	Dim            int
	DistanceMetric string
}

type RebuildResult struct {
	IndexName   string
	BuildTimeS  float64
	IndexSizeMB float64
}

type Manager struct {
	db *sql.DB
}

func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

func (m *Manager) RebuildIndex(ctx context.Context, model Model, spec Spec) (*RebuildResult, error) {
	if err := m.DropEmbeddingIndexes(ctx, model.Table); err != nil {
		return nil, err
	}
	if spec.Type == NoIndex {
		if err := m.vacuumAnalyze(ctx, model.Table); err != nil {
			return nil, err
		}
		return &RebuildResult{}, nil
	}

	statement, indexName, err := m.createIndexSQL(ctx, model, spec)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	if _, err := m.db.ExecContext(ctx, statement); err != nil {
		return nil, err
	}
	if err := m.vacuumAnalyze(ctx, model.Table); err != nil {
		return nil, err
	}
	sizeMB, err := m.IndexSizeMB(ctx, indexName)
	if err != nil {
		return nil, err
	}
	return &RebuildResult{
		IndexName:   indexName,
		BuildTimeS:  time.Since(start).Seconds(),
		IndexSizeMB: sizeMB,
	}, nil
}

func (m *Manager) DropEmbeddingIndexes(ctx context.Context, table string) error {
	schemaName, tableName := splitTableRef(table)
	rows, err := m.db.QueryContext(ctx, `
SELECT indexname
FROM pg_indexes
WHERE schemaname = $1
  AND tablename = $2
  AND (
    indexdef LIKE '% USING hnsw %'
    OR indexdef LIKE '% USING ivfflat %'
    OR indexdef LIKE '% USING diskann %'
  )`, schemaName, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexNames := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		indexNames = append(indexNames, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range indexNames {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, quoteIdent(name))); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) IndexSizeMB(ctx context.Context, indexName string) (float64, error) {
	var bytes float64
	if err := m.db.QueryRowContext(ctx, `SELECT pg_relation_size($1::regclass)::float8`, indexName).Scan(&bytes); err != nil {
		return 0, err
	}
	return bytes / 1024.0 / 1024.0, nil
}

func (m *Manager) CaptureExplain(ctx context.Context, query string, args ...any) (string, bool, error) {
	return m.CaptureExplainWithSetup(ctx, nil, query, args...)
}

func (m *Manager) CaptureExplainWithSetup(ctx context.Context, setup []string, query string, args ...any) (string, bool, error) {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback() }()

	for _, statement := range setup {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return "", false, err
		}
	}

	rows, err := tx.QueryContext(ctx, "EXPLAIN ANALYZE "+query, args...)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()

	lines := make([]string, 0)
	usedIndex := false
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", false, err
		}
		if strings.Contains(line, "Index Scan") || strings.Contains(line, "Index Only Scan") || strings.Contains(line, "Bitmap Index Scan") {
			usedIndex = true
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return strings.Join(lines, "\n"), usedIndex, tx.Commit()
}

func (m *Manager) vacuumAnalyze(ctx context.Context, table string) error {
	_, err := m.db.ExecContext(ctx, fmt.Sprintf(`VACUUM ANALYZE %s`, normalizeTableRef(table)))
	return err
}

func (m *Manager) createIndexSQL(ctx context.Context, model Model, spec Spec) (string, string, error) {
	opclass, err := metricOpClass(model.DistanceMetric, spec.Precision, spec.Type)
	if err != nil {
		return "", "", err
	}
	expr := embeddingExpr(model, spec.Precision)
	name := indexName(model, spec)
	switch spec.Type {
	case HNSW:
		return fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s %s) WITH (m = 16, ef_construction = 64)`,
			quoteIdent(name), normalizeTableRef(model.Table), expr, opclass), name, nil
	case IVFFlat:
		lists, err := m.estimateIVFFlatLists(ctx, model.Table)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING ivfflat (%s %s) WITH (lists = %d)`,
			quoteIdent(name), normalizeTableRef(model.Table), expr, opclass, lists), name, nil
	case DiskANN:
		return fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING diskann (%s %s) WITH (num_neighbors = 50, storage_layout = 'memory_optimized')`,
			quoteIdent(name), normalizeTableRef(model.Table), expr, opclass), name, nil
	default:
		return "", "", fmt.Errorf("unsupported index type %s", spec.Type)
	}
}

func indexName(model Model, spec Spec) string {
	suffix := strings.ReplaceAll(normalizeTableRef(model.Table), ".", "_")
	if spec.Precision == HalfPrecision {
		return fmt.Sprintf("idx_%s_%s_half_%s", suffix, spec.Type, model.DistanceMetric)
	}
	return fmt.Sprintf("idx_%s_%s_%s", suffix, spec.Type, model.DistanceMetric)
}

func embeddingExpr(model Model, precision Precision) string {
	if precision == HalfPrecision {
		return fmt.Sprintf("(embedding::halfvec(%d))", model.Dim)
	}
	return "embedding"
}

func metricOpClass(metric string, precision Precision, indexType ANNIndexType) (string, error) {
	if precision == HalfPrecision && indexType == DiskANN {
		return "", fmt.Errorf("diskann does not support halfvec opclasses in this environment")
	}
	switch strings.TrimSpace(metric) {
	case "cosine":
		if precision == HalfPrecision {
			return "halfvec_cosine_ops", nil
		}
		return "vector_cosine_ops", nil
	case "l2":
		if precision == HalfPrecision {
			return "halfvec_l2_ops", nil
		}
		return "vector_l2_ops", nil
	default:
		return "", fmt.Errorf("unsupported metric %q", metric)
	}
}

func normalizeTableRef(table string) string {
	if strings.Contains(table, ".") {
		return table
	}
	return "public." + table
}

func splitTableRef(table string) (string, string) {
	normalized := normalizeTableRef(table)
	parts := strings.SplitN(normalized, ".", 2)
	return parts[0], parts[1]
}

func (m *Manager) estimateIVFFlatLists(ctx context.Context, table string) (int64, error) {
	var rows int64
	if err := m.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT GREATEST(COUNT(*) / 1000, 1) FROM %s`, normalizeTableRef(table))).Scan(&rows); err != nil {
		return 0, err
	}
	return rows, nil
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
