package runner

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type Neighbor struct {
	ObjectID int32
	Distance float64
}

func RecallAtK(approx []Neighbor, exact []Neighbor, k int) float64 {
	if k <= 0 {
		return 0
	}
	if len(approx) > k {
		approx = approx[:k]
	}
	if len(exact) > k {
		exact = exact[:k]
	}
	if len(exact) == 0 {
		return 0
	}
	exactSet := make(map[int32]struct{}, len(exact))
	for _, item := range exact {
		exactSet[item.ObjectID] = struct{}{}
	}
	matches := 0
	for _, item := range approx {
		if _, ok := exactSet[item.ObjectID]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(exact))
}

func ExactKNN(ctx context.Context, db *sql.DB, model BenchmarkModel, vector []float32, k int) ([]Neighbor, error) {
	return runExactQuery(ctx, db, exactKNNQuery(model), vectorLiteral(vector), nil, k)
}

func ExactFilteredKNN(ctx context.Context, db *sql.DB, model BenchmarkModel, vector []float32, candidateIDs []int32, k int) ([]Neighbor, error) {
	return runExactQuery(ctx, db, exactFilteredKNNQuery(model), vectorLiteral(vector), candidateIDs, k)
}

func exactKNNQuery(model BenchmarkModel) string {
	return fmt.Sprintf(`
SELECT tg.object_id, (%s %s $1::vector) AS distance
FROM %s v
JOIN public.taggings tg ON tg.tag_id = v.id
WHERE %s
ORDER BY %s %s $1::vector, tg.object_id ASC
LIMIT $2`, vectorExpr(model, false), distanceOperator(model), model.Table, modelTagsetCondition(model, "v"), vectorExpr(model, false), distanceOperator(model))
}

func exactFilteredKNNQuery(model BenchmarkModel) string {
	return fmt.Sprintf(`
SELECT tg.object_id, (%s %s $1::vector) AS distance
FROM %s v
JOIN public.taggings tg ON tg.tag_id = v.id
WHERE tg.object_id = ANY($2::integer[]) AND %s
ORDER BY %s %s $1::vector, tg.object_id ASC
LIMIT $3`, vectorExpr(model, false), distanceOperator(model), model.Table, modelTagsetCondition(model, "v"), vectorExpr(model, false), distanceOperator(model))
}

func runExactQuery(ctx context.Context, db *sql.DB, query string, vectorValue string, candidateIDs []int32, k int) ([]Neighbor, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if candidateIDs == nil {
		rows, err = db.QueryContext(ctx, query, vectorValue, k)
	} else {
		rows, err = db.QueryContext(ctx, query, vectorValue, pq.Array(int32SliceToInts(candidateIDs)), k)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Neighbor, 0, k)
	for rows.Next() {
		var neighbor Neighbor
		if err := rows.Scan(&neighbor.ObjectID, &neighbor.Distance); err != nil {
			return nil, err
		}
		out = append(out, neighbor)
	}
	return out, rows.Err()
}

func vectorLiteral(values []float32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(float64(value), 'f', -1, 32))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func vectorExpr(model BenchmarkModel, half bool) string {
	if half {
		return fmt.Sprintf("v.embedding::halfvec(%d)", model.Dim)
	}
	return "v.embedding"
}

func distanceOperator(model BenchmarkModel) string {
	switch strings.TrimSpace(model.DistanceMetric) {
	case "l2":
		return "<->"
	default:
		return "<=>"
	}
}

func int32SliceToInts(values []int32) []int {
	out := make([]int, 0, len(values))
	for _, value := range values {
		out = append(out, int(value))
	}
	return out
}
