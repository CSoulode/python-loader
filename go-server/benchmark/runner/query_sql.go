package runner

import (
	"fmt"
	"strings"

	benchindex "m3.dataloader/benchmark/index"
	pb "m3.dataloader/dataloader"
)

func ApproxKNNQuery(model BenchmarkModel, precision benchindex.Precision) string {
	expr := vectorExprForPrecision(model, precision)
	op := distanceOperator(model)
	cast := vectorCastForPrecision(model, precision)
	return fmt.Sprintf(`
SELECT tg.object_id, (%s %s $1%s) AS distance
FROM %s v
JOIN public.taggings tg ON tg.tag_id = v.id
WHERE %s
ORDER BY %s %s $1%s, tg.object_id ASC
LIMIT $2`, expr, op, cast, model.Table, modelTagsetCondition(model, "v"), expr, op, cast)
}

func ApproxFilteredKNNQuery(model BenchmarkModel, precision benchindex.Precision) string {
	expr := vectorExprForPrecision(model, precision)
	op := distanceOperator(model)
	cast := vectorCastForPrecision(model, precision)
	return fmt.Sprintf(`
SELECT tg.object_id, (%s %s $1%s) AS distance
FROM %s v
JOIN public.taggings tg ON tg.tag_id = v.id
WHERE tg.object_id = ANY($2::integer[]) AND %s
ORDER BY %s %s $1%s, tg.object_id ASC
LIMIT $3`, expr, op, cast, model.Table, modelTagsetCondition(model, "v"), expr, op, cast)
}

func ApproxQueryArg(vector []float32, precision benchindex.Precision) any {
	if precision == benchindex.HalfPrecision {
		return vectorLiteral(vector)
	}
	return vectorLiteral(vector)
}

func vectorExprForPrecision(model BenchmarkModel, precision benchindex.Precision) string {
	if precision == benchindex.HalfPrecision {
		return fmt.Sprintf("v.embedding::halfvec(%d)", model.Dim)
	}
	return "v.embedding"
}

func vectorCastForPrecision(model BenchmarkModel, precision benchindex.Precision) string {
	if precision == benchindex.HalfPrecision {
		return fmt.Sprintf("::halfvec(%d)", model.Dim)
	}
	return "::vector"
}

func strategyForSelectivity(selectivity float64) HybridStrategyPlan {
	if selectivity >= 1.0 {
		return HybridStrategyPlan{Name: "post_filter", Enum: pb.HybridStrategy_POST_FILTER}
	}
	return HybridStrategyPlan{Name: "pre_filter", Enum: pb.HybridStrategy_PRE_FILTER}
}

type HybridStrategyPlan struct {
	Name string
	Enum pb.HybridStrategy
}

func precisionLabel(precision benchindex.Precision) string {
	return strings.TrimSpace(string(precision))
}
