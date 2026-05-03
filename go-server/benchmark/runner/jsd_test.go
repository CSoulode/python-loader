package runner

import (
	"testing"
	"time"
)

func TestBuildJSDSeriesUsesCoverageThresholds(t *testing.T) {
	const batchSize = 2
	states := []map[string]CellState{
		stateWithCells(1),
		stateWithCells(1),
		stateWithCells(2),
		stateWithCells(3),
		stateWithCells(4),
		stateWithCells(5),
	}
	elapsed := elapsedMilliseconds(len(states))

	points := BuildJSDSeries(states, elapsed, batchSize, 5)
	wantCells := []int{2, 4, 5}
	if len(points) != len(wantCells) {
		t.Fatalf("point count = %d, want %d", len(points), len(wantCells))
	}
	for index, want := range wantCells {
		if points[index].BatchIdx != index+1 {
			t.Fatalf("point %d batch_idx = %d, want %d", index, points[index].BatchIdx, index+1)
		}
		if points[index].CellsReceived != want {
			t.Fatalf("point %d cells_received = %d, want %d", index, points[index].CellsReceived, want)
		}
	}
	if points[len(points)-1].JSD != 0 {
		t.Fatalf("final point JSD = %f, want 0", points[len(points)-1].JSD)
	}
}

func TestBuildJSDSeriesDoesNotDuplicateExactFinalThreshold(t *testing.T) {
	const batchSize = 2
	states := []map[string]CellState{
		stateWithCells(1),
		stateWithCells(2),
		stateWithCells(3),
		stateWithCells(4),
	}

	points := BuildJSDSeries(states, elapsedMilliseconds(len(states)), batchSize, 4)
	if len(points) != 2 {
		t.Fatalf("point count = %d, want 2", len(points))
	}
	if points[1].CellsReceived != 4 {
		t.Fatalf("final cells_received = %d, want 4", points[1].CellsReceived)
	}
}

func TestBuildJSDSeriesClampsAfterStreamCompletion(t *testing.T) {
	const batchSize = 2
	states := []map[string]CellState{
		stateWithCells(1),
		stateWithCells(2),
		stateWithCells(3),
	}

	points := BuildJSDSeries(states, elapsedMilliseconds(len(states)), batchSize, 6)
	wantCells := []int{2, 3, 3}
	if len(points) != len(wantCells) {
		t.Fatalf("point count = %d, want %d", len(points), len(wantCells))
	}
	for index, want := range wantCells {
		if points[index].CellsReceived != want {
			t.Fatalf("point %d cells_received = %d, want %d", index, points[index].CellsReceived, want)
		}
	}
}

func stateWithCells(count int) map[string]CellState {
	state := make(map[string]CellState, count)
	for index := 0; index < count; index++ {
		state[fmtInt32(int32(index))] = CellState{Count: 1}
	}
	return state
}

func elapsedMilliseconds(count int) []time.Duration {
	elapsed := make([]time.Duration, count)
	for index := range elapsed {
		elapsed[index] = time.Duration(index+1) * time.Millisecond
	}
	return elapsed
}
