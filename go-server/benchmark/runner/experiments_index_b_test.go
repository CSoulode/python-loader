package runner

import (
	"testing"

	benchindex "m3.dataloader/benchmark/index"
)

func TestVectorExplainSetup(t *testing.T) {
	t.Run("hnsw uses ef_search and iterative settings", func(t *testing.T) {
		setup := vectorExplainSetup(benchindex.Spec{
			Type:          benchindex.HNSW,
			Precision:     benchindex.FullPrecision,
			IterativeMode: benchindex.IterativeStrict,
		}, 500)
		if len(setup) != 3 {
			t.Fatalf("setup len = %d, want 3", len(setup))
		}
		if setup[0] != "SET LOCAL hnsw.ef_search = 500" {
			t.Fatalf("setup[0] = %q", setup[0])
		}
		if setup[1] != "SET LOCAL hnsw.iterative_scan = strict_order" {
			t.Fatalf("setup[1] = %q", setup[1])
		}
	})

	t.Run("hnsw caps ef_search for non-iterative k5000", func(t *testing.T) {
		setup := vectorExplainSetup(benchindex.Spec{
			Type:          benchindex.HNSW,
			Precision:     benchindex.HalfPrecision,
			IterativeMode: benchindex.IterativeOff,
		}, 5000)
		if len(setup) != 1 || setup[0] != "SET LOCAL hnsw.ef_search = 1000" {
			t.Fatalf("setup = %v", setup)
		}
	})

	t.Run("ivfflat uses probes", func(t *testing.T) {
		setup := vectorExplainSetup(benchindex.Spec{
			Type:          benchindex.IVFFlat,
			Precision:     benchindex.FullPrecision,
			IterativeMode: benchindex.IterativeOff,
		}, 500)
		if len(setup) != 1 || setup[0] != "SET LOCAL ivfflat.probes = 14" {
			t.Fatalf("setup = %v", setup)
		}
	})

	t.Run("diskann has no setup statements", func(t *testing.T) {
		setup := vectorExplainSetup(benchindex.Spec{
			Type:          benchindex.DiskANN,
			Precision:     benchindex.FullPrecision,
			IterativeMode: benchindex.IterativeOff,
		}, 500)
		if setup != nil {
			t.Fatalf("setup = %v, want nil", setup)
		}
	})
}

func TestPrecisionExplainPlanFilename(t *testing.T) {
	name := precisionExplainPlanFilename(
		DatasetPilot94,
		benchindex.Spec{Type: benchindex.HNSW, Precision: benchindex.HalfPrecision},
		500,
	)
	if name != "pilot_94k_hnsw_half_k0500.txt" {
		t.Fatalf("name = %q", name)
	}
}
