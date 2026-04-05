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
	flushes []BenchEvent,
) []JSDPoint {
	if len(states) == 0 || len(flushes) == 0 {
		return nil
	}
	finalState := states[len(states)-1]
	points := make([]JSDPoint, 0, len(flushes))
	index := 0
	for _, flush := range flushes {
		index += flush.BatchSize
		if index <= 0 || index > len(states) {
			continue
		}
		state := states[index-1]
		points = append(points, JSDPoint{
			BatchIdx:      flush.BatchIdx,
			ElapsedMS:     durationToMS(elapsed[index-1]),
			JSD:           JensenShannonDivergence(state, finalState),
			CellsReceived: len(state),
			CellsTotal:    len(finalState),
		})
	}
	return points
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
