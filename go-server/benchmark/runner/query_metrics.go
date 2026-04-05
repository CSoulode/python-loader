package runner

import (
	"context"
	"database/sql"

	qg "m3.dataloader/server/querygen"
)

func MeasureQueryActuals(
	ctx context.Context,
	db *sql.DB,
	model BenchmarkModel,
	filters []qg.ParsedFilter,
	axes []qg.ParsedAxis,
) (int64, float64, error) {
	query := &BenchmarkQuery{
		Model:         model,
		ParsedFilters: filters,
		ParsedAxes:    axes,
	}
	candidateCount, err := CountModelCandidates(ctx, db, query)
	if err != nil {
		return 0, 0, err
	}
	total, err := totalModelObjectCount(ctx, db, model)
	if err != nil {
		return 0, 0, err
	}
	if total == 0 {
		return candidateCount, 0, nil
	}
	return candidateCount, float64(candidateCount) / float64(total), nil
}
