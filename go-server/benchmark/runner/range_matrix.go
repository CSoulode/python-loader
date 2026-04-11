package runner

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	pb "m3.dataloader/dataloader"
)

type RangeMatrixCase struct {
	Plan          VectorQueryPlan
	RangePosition string
	WidthLabel    string
}

var rangePositionSpecs = []struct {
	Label string
	Ratio float64
}{
	{Label: "near_zero", Ratio: 0.30},
	{Label: "mid", Ratio: 0.55},
	{Label: "far", Ratio: 0.80},
}

var rangeWidthSpecs = []struct {
	Label string
	Ratio float64
}{
	{Label: "0.1", Ratio: 0.10},
	{Label: "0.3", Ratio: 0.30},
	{Label: "0.5", Ratio: 0.50},
}

func BuildRangeMatrixCases(
	ctx context.Context,
	db *sql.DB,
	query *BenchmarkQuery,
	vector []float32,
	candidateIDs []int32,
	maxResults int32,
) ([]RangeMatrixCase, error) {
	exact, err := exactNeighborsForPlanCalibration(ctx, db, query, vector, candidateIDs, int(maxResults))
	if err != nil {
		return nil, err
	}
	return buildRangeMatrixCasesFromNeighbors(exact, maxResults)
}

func buildRangeMatrixCasesFromNeighbors(exact []Neighbor, maxResults int32) ([]RangeMatrixCase, error) {
	if len(exact) < 8 {
		return nil, fmt.Errorf("range matrix requires at least 8 exact neighbors, got %d", len(exact))
	}

	out := make([]RangeMatrixCase, 0, len(rangePositionSpecs)*len(rangeWidthSpecs)*2)
	for _, position := range rangePositionSpecs {
		for _, width := range rangeWidthSpecs {
			if ball := buildBallRangeCase(exact, position.Label, position.Ratio, width.Label, width.Ratio, maxResults); ball != nil {
				out = append(out, *ball)
			}
			if ring := buildRingRangeCase(exact, position.Label, position.Ratio, width.Label, width.Ratio, maxResults); ring != nil {
				out = append(out, *ring)
			}
		}
	}
	return out, nil
}

func buildBallRangeCase(exact []Neighbor, positionLabel string, positionRatio float64, widthLabel string, widthRatio float64, maxResults int32) *RangeMatrixCase {
	ratio := clampRangeRatio(positionRatio + widthRatio/2)
	maxIndex := rangeBoundaryIndex(len(exact), ratio)
	maxDistance := exact[maxIndex].Distance
	if maxDistance <= 0 {
		return nil
	}
	return &RangeMatrixCase{
		Plan: VectorQueryPlan{
			Mode:       QueryModeRangeBall,
			MaxResults: maxResults,
			DistanceRange: &pb.DistanceRange{
				MinDistance: 0,
				MaxDistance: float32(maxDistance),
			},
		},
		RangePosition: positionLabel,
		WidthLabel:    widthLabel,
	}
}

func buildRingRangeCase(exact []Neighbor, positionLabel string, positionRatio float64, widthLabel string, widthRatio float64, maxResults int32) *RangeMatrixCase {
	halfWindow := widthRatio / 2
	minRatio := clampRangeRatio(positionRatio - halfWindow)
	maxRatio := clampRangeRatio(positionRatio + halfWindow)
	minIndex := rangeBoundaryIndex(len(exact), minRatio)
	maxIndex := rangeBoundaryIndex(len(exact), maxRatio)
	if maxIndex <= minIndex {
		maxIndex = min(len(exact)-1, minIndex+1)
	}
	minDistance := exact[minIndex].Distance
	maxDistance := exact[maxIndex].Distance
	if minDistance <= 0 || maxDistance <= minDistance {
		return nil
	}
	return &RangeMatrixCase{
		Plan: VectorQueryPlan{
			Mode:       QueryModeRangeRing,
			MaxResults: maxResults,
			DistanceRange: &pb.DistanceRange{
				MinDistance: float32(minDistance),
				MaxDistance: float32(maxDistance),
			},
		},
		RangePosition: positionLabel,
		WidthLabel:    widthLabel,
	}
}

func rangeBoundaryIndex(length int, ratio float64) int {
	if length <= 1 {
		return 0
	}
	index := int(math.Round(float64(length-1) * clampRangeRatio(ratio)))
	return max(0, min(length-1, index))
}

func clampRangeRatio(value float64) float64 {
	return math.Min(0.98, math.Max(0.02, value))
}

func planDistanceBoundsCSV(plan VectorQueryPlan) (string, string) {
	if plan.DistanceRange == nil {
		return "", ""
	}
	return FormatFloat(plan.DistanceMin()), FormatFloat(plan.DistanceMax())
}

func planKForCSV(plan VectorQueryPlan) string {
	if plan.Mode != QueryModeKNN {
		return ""
	}
	return fmt.Sprintf("%d", plan.MaxResults)
}
