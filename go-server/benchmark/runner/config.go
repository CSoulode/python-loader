package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type DatasetID string

const (
	Dataset182K    DatasetID = "182k"
	Dataset725K    DatasetID = "725k"
	DatasetPilot94 DatasetID = "pilot_94k"
)

type DatasetConfig struct {
	ID       DatasetID
	Size     int
	DSNEnv   string
	PortSlot int
}

func (c DatasetConfig) Label() string {
	return string(c.ID)
}

func (c DatasetConfig) SizeLabel() string {
	return strconv.Itoa(c.Size)
}

type DatasetCatalog struct {
	order   []DatasetID
	configs map[DatasetID]DatasetConfig
}

func DefaultDatasetCatalog() DatasetCatalog {
	return DatasetCatalog{
		order: []DatasetID{Dataset182K, Dataset725K},
		configs: map[DatasetID]DatasetConfig{
			Dataset182K: {ID: Dataset182K, Size: 182000, DSNEnv: "BENCH_DB_URL_182K", PortSlot: 0},
			Dataset725K: {ID: Dataset725K, Size: 725000, DSNEnv: "BENCH_DB_URL_725K", PortSlot: 1},
		},
	}
}

func ParseDatasetCatalog(value string) (DatasetCatalog, error) {
	if strings.TrimSpace(value) == "" {
		return DefaultDatasetCatalog(), nil
	}

	items := splitCSV(value)
	if len(items) == 0 {
		return DatasetCatalog{}, fmt.Errorf("dataset config is empty")
	}

	catalog := DatasetCatalog{
		order:   make([]DatasetID, 0, len(items)),
		configs: make(map[DatasetID]DatasetConfig, len(items)),
	}
	for index, item := range items {
		parts := strings.Split(item, ":")
		if len(parts) != 3 {
			return DatasetCatalog{}, fmt.Errorf("invalid dataset config %q; expected label:size:dsn_env", item)
		}

		label := DatasetID(strings.TrimSpace(parts[0]))
		if label == "" {
			return DatasetCatalog{}, fmt.Errorf("dataset label is required in %q", item)
		}
		size, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return DatasetCatalog{}, fmt.Errorf("dataset size invalid in %q: %w", item, err)
		}
		if size <= 0 {
			return DatasetCatalog{}, fmt.Errorf("dataset size must be > 0 in %q", item)
		}
		dsnEnv := strings.TrimSpace(parts[2])
		if dsnEnv == "" {
			return DatasetCatalog{}, fmt.Errorf("dataset DSN env is required in %q", item)
		}
		if _, exists := catalog.configs[label]; exists {
			return DatasetCatalog{}, fmt.Errorf("duplicate dataset label %q", label)
		}

		catalog.order = append(catalog.order, label)
		catalog.configs[label] = DatasetConfig{
			ID:       label,
			Size:     size,
			DSNEnv:   dsnEnv,
			PortSlot: index,
		}
	}
	return catalog, nil
}

func (c DatasetCatalog) Labels() []DatasetID {
	return append([]DatasetID(nil), c.order...)
}

func (c DatasetCatalog) Config(id DatasetID) (DatasetConfig, error) {
	cfg, ok := c.configs[id]
	if !ok {
		return DatasetConfig{}, fmt.Errorf("unsupported dataset %q", id)
	}
	return cfg, nil
}

type ExperimentID string

const (
	Experiment1  ExperimentID = "exp1"
	Experiment2  ExperimentID = "exp2"
	Experiment3  ExperimentID = "exp3"
	Experiment4  ExperimentID = "exp4"
	Experiment5  ExperimentID = "exp5"
	Experiment6  ExperimentID = "exp6"
	Experiment7  ExperimentID = "exp7"
	Experiment8  ExperimentID = "exp8"
	Experiment9  ExperimentID = "exp9"
	Experiment10 ExperimentID = "exp10"
	Experiment11 ExperimentID = "exp11"
)

type Options struct {
	RootDir          string
	OutputRoot       string
	GoServerEnvFile  string
	VectorModelsFile string
	KeepServices     bool
	Datasets         []DatasetID
	Experiments      []ExperimentID
	DatasetCatalog   DatasetCatalog
}

func (o Options) Validate() error {
	if strings.TrimSpace(o.RootDir) == "" {
		return fmt.Errorf("root dir is required")
	}
	if strings.TrimSpace(o.OutputRoot) == "" {
		return fmt.Errorf("output root is required")
	}
	if len(o.Datasets) == 0 {
		return fmt.Errorf("at least one dataset is required")
	}
	if len(o.Experiments) == 0 {
		return fmt.Errorf("at least one experiment is required")
	}
	for _, dataset := range o.Datasets {
		cfg, err := o.DatasetCatalog.Config(dataset)
		if err != nil {
			return err
		}
		if strings.TrimSpace(os.Getenv(cfg.DSNEnv)) == "" {
			return fmt.Errorf("missing database URL for dataset %s via env %s", dataset, cfg.DSNEnv)
		}
	}
	return nil
}

func (o Options) DatasetDBURL(id DatasetID) string {
	cfg, err := o.DatasetCatalog.Config(id)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(os.Getenv(cfg.DSNEnv))
}

func (o Options) DatasetConfig(id DatasetID) DatasetConfig {
	cfg, err := o.DatasetCatalog.Config(id)
	if err != nil {
		panic(err)
	}
	return cfg
}

func (o Options) DatasetLabel(id DatasetID) string {
	return o.DatasetConfig(id).Label()
}

func (o Options) DatasetSizeLabel(id DatasetID) string {
	return o.DatasetConfig(id).SizeLabel()
}

func (o Options) DatasetCSVPrefix(id DatasetID) []string {
	return []string{o.DatasetLabel(id), o.DatasetSizeLabel(id)}
}

func ParseDatasets(value string, catalog DatasetCatalog) ([]DatasetID, error) {
	items := splitCSV(value)
	if len(items) == 0 {
		return catalog.Labels(), nil
	}
	out := make([]DatasetID, 0, len(items))
	for _, item := range items {
		datasetID := DatasetID(item)
		if _, err := catalog.Config(datasetID); err != nil {
			return nil, fmt.Errorf("unsupported dataset %q", item)
		}
		out = append(out, datasetID)
	}
	return out, nil
}

func ParseExperiments(value string) ([]ExperimentID, error) {
	items := splitCSV(value)
	if len(items) == 0 {
		return orderedExperiments(), nil
	}
	out := make([]ExperimentID, 0, len(items))
	for _, item := range items {
		switch ExperimentID(item) {
		case Experiment1, Experiment2, Experiment3, Experiment4, Experiment5, Experiment6, Experiment7, Experiment8, Experiment9, Experiment10, Experiment11:
			out = append(out, ExperimentID(item))
		default:
			return nil, fmt.Errorf("unsupported experiment %q", item)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return experimentRank(out[i]) < experimentRank(out[j])
	})
	return out, nil
}

func orderedExperiments() []ExperimentID {
	return []ExperimentID{
		Experiment2,
		Experiment1,
		Experiment3,
		Experiment4,
		Experiment8,
		Experiment9,
		Experiment5,
		Experiment6,
		Experiment7,
		Experiment10,
		Experiment11,
	}
}

func experimentRank(id ExperimentID) int {
	for index, item := range orderedExperiments() {
		if item == id {
			return index
		}
	}
	return len(orderedExperiments())
}

func splitCSV(value string) []string {
	parts := strings.Split(strings.TrimSpace(value), ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

type OutputPaths struct {
	Root        string
	RawDir      string
	FigureDir   string
	AnalysisDir string
	ExplainDir  string
	LogDir      string
	ScratchDir  string
	Environment string
	Summary     string
	Feasibility string
	Comparison  string
	Report      string
	Readme      string
}

func NewOutputPaths(root string) OutputPaths {
	return OutputPaths{
		Root:        root,
		RawDir:      filepath.Join(root, "raw"),
		FigureDir:   filepath.Join(root, "figures"),
		AnalysisDir: filepath.Join(root, "analysis"),
		ExplainDir:  filepath.Join(root, "explain_plans"),
		LogDir:      filepath.Join(root, "logs"),
		ScratchDir:  filepath.Join(root, ".tmp"),
		Environment: filepath.Join(root, "environment.md"),
		Summary:     filepath.Join(root, "summary.md"),
		Feasibility: filepath.Join(root, "diskann_label_feasibility.md"),
		Comparison:  filepath.Join(root, "comparison.md"),
		Report:      filepath.Join(root, "report.md"),
		Readme:      filepath.Join(root, "README.md"),
	}
}
