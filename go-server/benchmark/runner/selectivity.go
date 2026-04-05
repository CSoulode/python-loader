package runner

import (
	"context"
	"database/sql"
	"fmt"

	qg "m3.dataloader/server/querygen"
)

type DynamicTagFilter struct {
	TagID          int32
	Selectivity    float64
	CandidateCount int64
}

func PickTagFilterNearSelectivity(
	ctx context.Context,
	db *sql.DB,
	model BenchmarkModel,
	filters []qg.ParsedFilter,
	axes []qg.ParsedAxis,
	target float64,
) (*DynamicTagFilter, error) {
	if target <= 0 || target > 1 {
		return nil, fmt.Errorf("target selectivity must be in (0,1], got %f", target)
	}

	baseSQL, err := modelCandidateSQL(&BenchmarkQuery{
		Model:         model,
		ParsedFilters: filters,
		ParsedAxes:    axes,
	})
	if err != nil {
		return nil, err
	}
	const query = `
WITH base AS (
  %s
),
domain AS (
  SELECT COUNT(DISTINCT tg.object_id)::float8 AS total
  FROM %s v
  JOIN public.taggings tg ON tg.tag_id = v.id
  WHERE %s
)
SELECT t.id,
       COUNT(DISTINCT base.object_id)::float8 / domain.total AS sel,
       COUNT(DISTINCT base.object_id)::bigint AS candidate_count
FROM public.tags t
JOIN public.taggings tg ON tg.tag_id = t.id
JOIN base ON base.object_id = tg.object_id
JOIN public.tag_types tt ON tt.id = t.tagtype_id
JOIN domain ON true
WHERE tt.description <> 'vector'
GROUP BY t.id, domain.total
HAVING COUNT(DISTINCT base.object_id) > 0
ORDER BY ABS((COUNT(DISTINCT base.object_id)::float8 / domain.total) - $1),
         COUNT(DISTINCT base.object_id) DESC,
         t.id
LIMIT 1`

	filter := &DynamicTagFilter{}
	sqlStr := fmt.Sprintf(query, baseSQL, model.Table, modelTagsetCondition(model, "v"))
	if err := db.QueryRowContext(ctx, sqlStr, target).Scan(&filter.TagID, &filter.Selectivity, &filter.CandidateCount); err != nil {
		return nil, err
	}
	return filter, nil
}

func SelectReferenceObjectID(
	ctx context.Context,
	db *sql.DB,
	query *BenchmarkQuery,
	offset int,
	candidateCount int64,
) (int32, error) {
	if candidateCount <= 0 {
		return 0, fmt.Errorf("no model candidates for reference selection")
	}
	sqlStr, err := modelCandidateSQL(query)
	if err != nil {
		return 0, err
	}
	index := normalizeReferenceOffset(offset, candidateCount)
	var objectID int32
	err = db.QueryRowContext(
		ctx,
		"SELECT object_id FROM ("+sqlStr+") q OFFSET $1 LIMIT 1",
		index,
	).Scan(&objectID)
	return objectID, err
}

func normalizeReferenceOffset(offset int, candidateCount int64) int {
	if offset < 0 {
		return 0
	}
	return int(int64(offset) % candidateCount)
}
