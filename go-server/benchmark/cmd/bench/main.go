package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"m3.dataloader/benchmark/runner"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	root, err := runner.DetectWorkspaceRoot(cwd)
	if err != nil {
		return err
	}

	defaultOutputRoot := filepath.Join(root, "docs", "experiments", "phase_d4_range_"+time.Now().UTC().Format("2006-01-02"))
	defaultEnvFile := filepath.Join(root, "python-loader", "go-server", ".env")
	defaultModelsFile := filepath.Join(root, "vectorkv", "config", "models.json")

	var (
		experimentsValue string
		datasetsValue    string
		datasetConfig    string
		outputRoot       string
		envFile          string
		modelsFile       string
		keepServices     bool
	)
	flag.StringVar(&experimentsValue, "experiments", "", "comma-separated experiment ids: exp1..exp9")
	flag.StringVar(&datasetsValue, "datasets", "", "comma-separated dataset labels from --dataset-config (defaults: 182k,725k)")
	flag.StringVar(&datasetConfig, "dataset-config", "", "comma-separated dataset specs: label:size:dsn_env")
	flag.StringVar(&outputRoot, "output-root", defaultOutputRoot, "output root for raw/ figures/ analysis/")
	flag.StringVar(&envFile, "go-server-env-file", defaultEnvFile, "path to go-server .env")
	flag.StringVar(&modelsFile, "vector-models-file", defaultModelsFile, "path to vectorkv models.json")
	flag.BoolVar(&keepServices, "keep-services", false, "leave managed services running after benchmark completes")
	flag.Parse()

	catalog, err := runner.ParseDatasetCatalog(datasetConfig)
	if err != nil {
		return err
	}
	datasets, err := runner.ParseDatasets(datasetsValue, catalog)
	if err != nil {
		return err
	}
	experiments, err := runner.ParseExperiments(experimentsValue)
	if err != nil {
		return err
	}

	opts := runner.Options{
		RootDir:          root,
		OutputRoot:       outputRoot,
		GoServerEnvFile:  envFile,
		VectorModelsFile: modelsFile,
		KeepServices:     keepServices,
		Datasets:         datasets,
		Experiments:      experiments,
		DatasetCatalog:   catalog,
	}
	return runner.Run(context.Background(), opts)
}
