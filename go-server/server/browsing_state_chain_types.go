package main

import (
	"time"

	pb "m3.dataloader/dataloader"
)

const (
	defaultBrowsingStateChainMaxNodes  = 4096
	defaultBrowsingStateChainTTL       = 5 * time.Minute
	browsingStateHeaderID              = "x-browsing-state-id"
	browsingStateHeaderCache           = "x-browsing-state-cache"
	browsingStateHeaderCachePath       = "x-browsing-cache-path"
	browsingStateHeaderDelta           = "x-browsing-delta-kind"
	browsingStateHeaderReusedFragments = "x-browsing-reused-fragments"
	browsingStateParentHeaderID        = "x-parent-browsing-state-id"
	browsingStateHeaderAncestorETags   = "x-browsing-ancestor-etags"
	browsingStateHeaderAncestorID      = "x-browsing-ancestor-state-id"
	browsingStateHeaderAncestorETag    = "x-browsing-ancestor-etag"
	browsingStateGRPCETag              = "etag"
	browsingStateGRPCIfNoneMatch       = "if-none-match"
	browsingStateGRPCAncestorETag      = "ancestor-if-none-match"
	browsingStateGRPCNotModified       = "bs-not-modified"
	browsingStateGRPCAncestorID        = "bs-ancestor-state-id"
	browsingStateGRPCAncestorMatch     = "bs-ancestor-etag"
)

type StateID string

type DeltaKind uint8

const (
	DeltaRoot DeltaKind = iota
	DeltaAddFilter
	DeltaRemoveFilter
	DeltaAddVectorDim
	DeltaRemoveVectorDim
	DeltaChangeAxisNonVector
	DeltaRebucketOnly
	DeltaChangeForcedStrategy
)

func (k DeltaKind) String() string {
	switch k {
	case DeltaAddFilter:
		return "AddFilter"
	case DeltaRemoveFilter:
		return "RemoveFilter"
	case DeltaAddVectorDim:
		return "AddVectorDim"
	case DeltaRemoveVectorDim:
		return "RemoveVectorDim"
	case DeltaChangeAxisNonVector:
		return "ChangeAxisNonVector"
	case DeltaRebucketOnly:
		return "RebucketOnly"
	case DeltaChangeForcedStrategy:
		return "ChangeForcedStrategy"
	default:
		return "Root"
	}
}

type TransitionDelta struct {
	Kind    DeltaKind
	Payload string
}

type browsingStateLookupMode string

const (
	chainLookupFullHit       browsingStateLookupMode = "full_hit"
	chainLookupAncestorReuse browsingStateLookupMode = "ancestor_reuse"
	chainLookupColdMiss      browsingStateLookupMode = "cold_miss"
)

type bsCellGrid struct {
	Responses        []*pb.BrowsingStateResponse
	HTTPBody         []byte
	HasGRPCResponses bool
	HasHTTPBody      bool
}

type browsingStateFragments struct {
	Candidates []int32
	VectorDims map[string]browsingStateVectorFragment
}

type browsingStateVectorFragment struct {
	RefHash      uint64
	FilterHash   uint64
	Signature    string
	SearchResult searchResult
	Result       *vectorDimensionResult
	Strategy     HybridStrategy
	Kind         SearchKind
}

type BrowsingStateSnapshot struct {
	FullKey         string
	ReuseKey        string
	AxisKey         string
	AxisDomainKey   string
	FilterKey       string
	FilterCount     int
	FilterItems     []string
	MetadataKey     string
	VectorSearchKey string
	VectorBucketKey string
	StrategyKey     string
	VectorDims      map[string]string
	Dependencies    []browsingStateDependency
}

type browsingStateDependency struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

const (
	browsingStateDepTagset      = "tagset"
	browsingStateDepTag         = "tag"
	browsingStateDepNode        = "node"
	browsingStateDepVectorModel = "vector_model"
)

type StateNode struct {
	ID                 StateID
	ParentID           StateID
	ReuseKey           string
	Delta              TransitionDelta
	Snapshot           *BrowsingStateSnapshot
	CellGrid           *bsCellGrid
	Fragments          *browsingStateFragments
	ETag               string
	Dependencies       []browsingStateDependency
	NodeDependencies   []browsingStateDependency
	CreatedAt          time.Time
	LastAccessAt       time.Time
	SubtreeLastAccess  time.Time
	ChildCount         int32
	lruElementAttached bool
}

type browsingStateChainLookup struct {
	Mode               browsingStateLookupMode
	TargetID           StateID
	ParentID           StateID
	ReuseKey           string
	Delta              TransitionDelta
	TargetETags        []string
	AncestorETags      []string
	Dependencies       []browsingStateDependency
	NodeDependencies   []browsingStateDependency
	Snapshot           *BrowsingStateSnapshot
	Fragments          *browsingStateFragments
	Collector          *browsingStateFragmentCollector
	InflightKey        string
	Node               *StateNode
	Ancestor           *StateNode
	CachePath          string
	Reusable           []string
	FallbackReason     string
	TargetETagMatch    bool
	AncestorETagMatch  bool
	MatchedAncestorID  StateID
	MatchedAncestorTag string
}

type browsingStateInflight struct {
	done chan struct{}
	node *StateNode
	err  error
}
