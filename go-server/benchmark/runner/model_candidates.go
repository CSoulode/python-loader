package runner

import (
	"context"
	"database/sql"
	"fmt"
)

func CountModelCandidates(ctx context.Context, db *sql.DB, query *BenchmarkQuery) (int64, error) {
	sqlStr, err := modelCandidateSQL(query)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+sqlStr+") q").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func LoadModelCandidateIDs(ctx context.Context, db *sql.DB, query *BenchmarkQuery) ([]int32, error) {
	sqlStr, err := modelCandidateSQL(query)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]int32, 0, 1024)
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func totalModelObjectCount(ctx context.Context, db *sql.DB, model BenchmarkModel) (int64, error) {
	query := fmt.Sprintf(`
SELECT COUNT(DISTINCT tg.object_id)
FROM %s v
JOIN public.taggings tg ON tg.tag_id = v.id
WHERE %s`, model.Table, modelTagsetCondition(model, "v"))
	var total int64
	err := db.QueryRowContext(ctx, query).Scan(&total)
	return total, err
}

func modelCandidateSQL(query *BenchmarkQuery) (string, error) {
	baseSQL, err := candidateSQL(query.ParsedFilters, query.ParsedAxes)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
SELECT DISTINCT base.object_id
FROM (%s) base
JOIN public.taggings tg ON tg.object_id = base.object_id
JOIN %s v ON v.id = tg.tag_id
WHERE %s
ORDER BY base.object_id ASC`, baseSQL, query.Model.Table, modelTagsetCondition(query.Model, "v")), nil
}
