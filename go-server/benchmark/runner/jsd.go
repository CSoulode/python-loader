package runner

import (
	"math"
	"time"
)

type JSDPoint struct {
	BatchIdx      int
	ElapsedMS     float64
	JSD           float64
	CellsReceived int
	CellsTotal    int
}

func BuildJSDSeries(
	states []map[string]CellState,
	elapsed []time.Duration,
	batchSize int,
	maxCells int,
) []JSDPoint {
	if len(states) == 0 || len(states) != len(elapsed) || batchSize <= 0 || maxCells <= 0 {
		return nil
	}
	finalState := states[len(states)-1]
	cellsTotal := len(finalState)
	if cellsTotal == 0 {
		return nil
	}

	pointCount := (maxCells + batchSize - 1) / batchSize
	points := make([]JSDPoint, 0, pointCount)
	startIndex := 0
	for batchIdx := 1; batchIdx <= pointCount; batchIdx++ {
		threshold := minInt(batchIdx*batchSize, cellsTotal)
		index := len(states) - 1
		if threshold < cellsTotal {
			index = firstStateAtCellCount(states, startIndex, threshold)
			if index < 0 {
				return nil
			}
		}
		points = append(points, jsdPoint(batchIdx, elapsed[index], states[index], finalState))
		startIndex = index
	}
	return points
}

func firstStateAtCellCount(states []map[string]CellState, startIndex int, minCells int) int {
	for index := startIndex; index < len(states); index++ {
		if len(states[index]) >= minCells {
			return index
		}
	}
	return -1
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func jsdPoint(
	batchIdx int,
	elapsed time.Duration,
	state map[string]CellState,
	finalState map[string]CellState,
) JSDPoint {
	return JSDPoint{
		BatchIdx:      batchIdx,
		ElapsedMS:     durationToMS(elapsed),
		JSD:           JensenShannonDivergence(state, finalState),
		CellsReceived: len(state),
		CellsTotal:    len(finalState),
	}
}

func JensenShannonDivergence(current map[string]CellState, final map[string]CellState) float64 {
	p := normalizeCellState(current)
	q := normalizeCellState(final)
	keys := unionKeys(p, q)
	var divergence float64
	for _, key := range keys {
		pv := p[key]
		qv := q[key]
		mv := 0.5 * (pv + qv)
		if pv > 0 {
			divergence += 0.5 * pv * math.Log2(pv/mv)
		}
		if qv > 0 {
			divergence += 0.5 * qv * math.Log2(qv/mv)
		}
	}
	return divergence
}

func normalizeCellState(state map[string]CellState) map[string]float64 {
	out := make(map[string]float64, len(state))
	var total float64
	for _, cell := range state {
		total += float64(cell.Count)
	}
	if almostZero(total) {
		return out
	}
	for key, cell := range state {
		out[key] = float64(cell.Count) / total
	}
	return out
}

func unionKeys(left map[string]float64, right map[string]float64) []string {
	keys := make([]string, 0, len(left)+len(right))
	seen := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range right {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}
