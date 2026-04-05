package main

import kvstorev1 "vectorkv/api/kvstore/v1/gen"

type SearchKind int

const (
	searchKindUnknown SearchKind = iota
	searchKindGlobalKNN
	searchKindFilteredKNN
	searchKindHybridIntersection
)

func (k SearchKind) String() string {
	switch k {
	case searchKindGlobalKNN:
		return "global_knn"
	case searchKindFilteredKNN:
		return "filtered_knn"
	case searchKindHybridIntersection:
		return "hybrid_intersection"
	default:
		return "unknown"
	}
}

type searchResult struct {
	RawNeighbors   []Neighbor
	DistanceMetric string
	Kind           SearchKind
}

type vectorSearchInputs struct {
	ModelInfo   *kvstorev1.ModelInfo
	QueryVector []float32
}

type Neighbor struct {
	ObjectID int32
	Distance float64
}

func limitSearchResult(result searchResult, reqK int32) searchResult {
	if reqK <= 0 || len(result.RawNeighbors) <= int(reqK) {
		return result
	}

	limited := cloneNeighbors(result.RawNeighbors[:reqK])
	return searchResult{
		RawNeighbors:   limited,
		DistanceMetric: result.DistanceMetric,
		Kind:           result.Kind,
	}
}

func cloneSearchResult(result searchResult) searchResult {
	return searchResult{
		RawNeighbors:   cloneNeighbors(result.RawNeighbors),
		DistanceMetric: result.DistanceMetric,
		Kind:           result.Kind,
	}
}

func cloneNeighbors(neighbors []Neighbor) []Neighbor {
	if len(neighbors) == 0 {
		return nil
	}

	cloned := make([]Neighbor, len(neighbors))
	copy(cloned, neighbors)
	return cloned
}
