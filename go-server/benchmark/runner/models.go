package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ModelCatalog struct {
	DefaultModel string           `json:"default_model"`
	Models       []BenchmarkModel `json:"models"`
}

type BenchmarkModel struct {
	Name           string `json:"name"`
	TagsetName     string `json:"tagset_name"`
	Table          string `json:"table"`
	Dim            int    `json:"dim"`
	DistanceMetric string `json:"distance_metric"`
}

func LoadModelCatalog(path string) (*ModelCatalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var catalog ModelCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, err
	}
	if len(catalog.Models) == 0 {
		return nil, fmt.Errorf("models catalog is empty")
	}
	return &catalog, nil
}

func (c *ModelCatalog) Model(name string) (BenchmarkModel, error) {
	for _, model := range c.Models {
		if strings.EqualFold(strings.TrimSpace(model.Name), strings.TrimSpace(name)) {
			return model, nil
		}
	}
	return BenchmarkModel{}, fmt.Errorf("model %q not found", name)
}

func (c *ModelCatalog) Default() (BenchmarkModel, error) {
	if strings.TrimSpace(c.DefaultModel) != "" {
		return c.Model(c.DefaultModel)
	}
	return c.Models[0], nil
}
