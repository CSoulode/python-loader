package runner

import (
	"context"
	"fmt"
	"path/filepath"

	benchindex "m3.dataloader/benchmark/index"
)

const benchmarkMaxK = "5000"

type BenchRunner struct {
	opts          Options
	paths         OutputPaths
	baseEnv       map[string]string
	catalog       *ModelCatalog
	indexSizeRows [][]string
}

func Run(ctx context.Context, opts Options) error {
	if err := opts.Validate(); err != nil {
		return err
	}

	paths := NewOutputPaths(opts.OutputRoot)
	if err := ensureOutputTree(paths); err != nil {
		return err
	}

	baseEnv, err := ParseDotEnv(opts.GoServerEnvFile)
	if err != nil {
		return err
	}
	catalog, err := LoadModelCatalog(opts.VectorModelsFile)
	if err != nil {
		return err
	}
	if err := BuildBinaries(ctx, opts.RootDir, paths); err != nil {
		return err
	}

	runner := &BenchRunner{
		opts:    opts,
		paths:   paths,
		baseEnv: baseEnv,
		catalog: catalog,
	}
	if err := runner.writeStaticDocs(); err != nil {
		return err
	}
	if err := runner.runExperiments(ctx); err != nil {
		return err
	}
	return runner.writeIndexSizes()
}

func ensureOutputTree(paths OutputPaths) error {
	for _, path := range []string{
		paths.Root,
		paths.RawDir,
		paths.AnalysisDir,
		paths.FigureDir,
		paths.ExplainDir,
		paths.LogDir,
		paths.ScratchDir,
	} {
		if err := EnsureDir(path); err != nil {
			return err
		}
	}
	return nil
}

func (r *BenchRunner) runExperiments(ctx context.Context) error {
	for _, experiment := range r.opts.Experiments {
		var err error
		switch experiment {
		case Experiment1:
			err = r.RunExperiment1(ctx)
		case Experiment2:
			err = r.RunExperiment2(ctx)
		case Experiment3:
			err = r.RunExperiment3(ctx)
		case Experiment4:
			err = r.RunExperiment4(ctx)
		case Experiment5:
			err = r.RunExperiment5(ctx)
		case Experiment6:
			err = r.RunExperiment6(ctx)
		case Experiment7:
			err = r.RunExperiment7(ctx)
		case Experiment8:
			err = r.RunExperiment8(ctx)
		case Experiment9:
			err = r.RunExperiment9(ctx)
		case Experiment10:
			err = r.RunExperiment10(ctx)
		case Experiment11:
			err = r.RunExperiment11(ctx)
		default:
			err = fmt.Errorf("unsupported experiment %s", experiment)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BenchRunner) runtimeConfig(dataset DatasetID, vectorEnv map[string]string) RuntimeConfig {
	return r.runtimeConfigWithServerEnv(dataset, vectorEnv, nil)
}

func (r *BenchRunner) runtimeConfigWithServerEnv(
	dataset DatasetID,
	vectorEnv map[string]string,
	serverEnv map[string]string,
) RuntimeConfig {
	return RuntimeConfig{
		Dataset:              r.opts.DatasetConfig(dataset),
		DatabaseURL:          r.opts.DatasetDBURL(dataset),
		VectorEnv:            vectorEnv,
		GoServerBaseEnv:      r.baseEnv,
		GoServerEnvOverrides: serverEnv,
		Paths:                r.paths,
		RootDir:              r.opts.RootDir,
	}
}

func (r *BenchRunner) vectorEnvForSpec(spec benchindex.Spec) map[string]string {
	env := map[string]string{
		"VECTOR_MODELS_FILE": r.opts.VectorModelsFile,
		"ANN_PRECISION":      string(spec.Precision),
		"MAX_K":              benchmarkMaxK,
	}
	switch spec.Type {
	case benchindex.IVFFlat:
		env["ANN_INDEX"] = string(benchindex.IVFFlat)
		env["IVFFLAT_PROBES"] = "14"
	case benchindex.DiskANN:
		env["ANN_INDEX"] = string(benchindex.DiskANN)
	case benchindex.NoIndex:
		env["ANN_INDEX"] = string(benchindex.HNSW)
	default:
		env["ANN_INDEX"] = string(benchindex.HNSW)
	}

	switch spec.Type {
	case benchindex.HNSW:
		setIterativeEnv(env, "HNSW_ITERATIVE_SCAN", spec.IterativeMode)
	case benchindex.IVFFlat:
		setIterativeEnv(env, "IVFFLAT_ITERATIVE_SCAN", spec.IterativeMode)
	}
	return env
}

func setIterativeEnv(env map[string]string, key string, mode benchindex.IterativeMode) {
	if mode == benchindex.IterativeOff {
		env[key] = ""
		return
	}
	env[key] = string(mode)
}

func (r *BenchRunner) addIndexSnapshot(
	dataset DatasetID,
	model BenchmarkModel,
	spec benchindex.Spec,
	result *benchindex.RebuildResult,
) {
	if result == nil || result.IndexName == "" {
		return
	}
	r.indexSizeRows = append(r.indexSizeRows, []string{
		string(dataset),
		r.opts.DatasetConfig(dataset).SizeLabel(),
		model.Name,
		string(spec.Type),
		string(spec.Precision),
		result.IndexName,
		FormatFloat(result.IndexSizeMB),
		FormatFloat(result.BuildTimeS),
	})
}

func (r *BenchRunner) writeIndexSizes() error {
	if len(r.indexSizeRows) == 0 {
		return nil
	}
	return WriteCSV(
		filepath.Join(r.paths.RawDir, "index_sizes.csv"),
		[]string{"dataset_label", "dataset_size", "model", "index_type", "precision", "index_name", "index_size_mb", "build_time_s"},
		r.indexSizeRows,
	)
}
