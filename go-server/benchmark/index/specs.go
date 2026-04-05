package index

type ANNIndexType string

const (
	HNSW    ANNIndexType = "hnsw"
	IVFFlat ANNIndexType = "ivfflat"
	DiskANN ANNIndexType = "diskann"
	NoIndex ANNIndexType = "no_index"
)

type Precision string

const (
	FullPrecision Precision = "full"
	HalfPrecision Precision = "half"
)

type IterativeMode string

const (
	IterativeOff     IterativeMode = "off"
	IterativeStrict  IterativeMode = "strict_order"
	IterativeRelaxed IterativeMode = "relaxed_order"
)

type Spec struct {
	Type          ANNIndexType
	Precision     Precision
	IterativeMode IterativeMode
}
