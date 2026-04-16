package runner

import pb "m3.dataloader/dataloader"

type phaseFSessionPlan struct {
	Type     string
	ID       string
	Requests []phaseFPlannedRequest
}

type phaseFPlannedRequest struct {
	Kind       string
	Variant    phaseFVariant
	Request    *pb.GetBrowsingStateRequest
	CacheState phaseFL0State
	Cache      *phaseFL0Cache
}

type phaseFVariantLookup map[string]phaseFVariant

func buildPhaseFSessionPlans(variants []phaseFVariant) []phaseFSessionPlan {
	lookup := buildPhaseFVariantLookup(variants)
	sharedCache := newPhaseFL0Cache()
	return []phaseFSessionPlan{
		buildLinearExploreSession(lookup, sharedCache),
		buildBacktrackHeavySession(lookup, sharedCache),
		buildRebucketTuningSession(lookup, sharedCache),
		buildMultiTabSession(lookup),
	}
}

func buildPhaseFVariantLookup(variants []phaseFVariant) phaseFVariantLookup {
	lookup := make(phaseFVariantLookup, len(variants))
	for _, variant := range variants {
		lookup[variant.ID] = variant
	}
	return lookup
}

func buildLinearExploreSession(
	lookup phaseFVariantLookup,
	cache *phaseFL0Cache,
) phaseFSessionPlan {
	requests := make([]phaseFPlannedRequest, 0, 20)
	for _, key := range orderedPhaseFVariantIDs() {
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], cache))
	}
	for _, item := range linearExploreExtras(lookup, cache) {
		requests = append(requests, item)
	}
	return phaseFSessionPlan{Type: "linear_explore", ID: "s01", Requests: requests}
}

func linearExploreExtras(
	lookup phaseFVariantLookup,
	cache *phaseFL0Cache,
) []phaseFPlannedRequest {
	return []phaseFPlannedRequest{
		newPhaseFRebucketRequest(lookup["LW-1d"], cache, RebucketVariants()[0]),
		newPhaseFRebucketRequest(lookup["GH-1d"], cache, RebucketVariants()[1]),
		newPhaseFRebucketRequest(lookup["RH-2d"], cache, RebucketVariants()[2]),
		newPhaseFRebucketRequest(lookup["HW-2d"], cache, RebucketVariants()[3]),
		newPhaseFRebucketRequest(lookup["LW-1d"], cache, RebucketVariants()[4]),
		newPhaseFRebucketRequest(lookup["GH-2d"], cache, RebucketVariants()[5]),
		newPhaseFRebucketRequest(lookup["RH-1d"], cache, RebucketVariants()[6]),
		newPhaseFRebucketRequest(lookup["HW-2d"], cache, RebucketVariants()[7]),
	}
}

func buildBacktrackHeavySession(
	lookup phaseFVariantLookup,
	cache *phaseFL0Cache,
) phaseFSessionPlan {
	order := []string{
		"LW-0d", "LW-1d", "GH-1d", "RH-2d",
		"LW-0d", "GH-1d", "HW-0d", "RH-2d",
		"LW-1d", "LW-0d", "GH-1d", "HW-0d",
		"RH-2d", "LW-0d", "GH-1d", "HW-0d",
	}
	requests := make([]phaseFPlannedRequest, 0, len(order))
	for _, key := range order {
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], cache))
	}
	return phaseFSessionPlan{Type: "backtrack_heavy", ID: "s02", Requests: requests}
}

func buildRebucketTuningSession(
	lookup phaseFVariantLookup,
	cache *phaseFL0Cache,
) phaseFSessionPlan {
	requests := make([]phaseFPlannedRequest, 0, 16)
	for _, key := range []string{"LW-0d", "LW-1d", "GH-1d", "RH-2d", "HW-2d"} {
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], cache))
	}
	for _, key := range []string{"LW-1d", "GH-1d", "RH-2d", "HW-2d"} {
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], cache, RebucketVariants()[0]))
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], cache, RebucketVariants()[1]))
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], cache, RebucketVariants()[0]))
	}
	return phaseFSessionPlan{Type: "rebucket_tuning", ID: "s03", Requests: requests}
}

func buildMultiTabSession(lookup phaseFVariantLookup) phaseFSessionPlan {
	tabA := newPhaseFL0Cache()
	tabB := newPhaseFL0Cache()
	keys := []string{"LW-0d", "GH-1d", "RH-2d", "HW-0d", "LW-1d", "GH-2d"}
	requests := make([]phaseFPlannedRequest, 0, len(keys)*3)
	for _, key := range keys {
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], tabA))
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], tabB))
		requests = append(requests, newPhaseFPlannedRequest("browsing_state", lookup[key], tabA))
	}
	return phaseFSessionPlan{Type: "multi_tab_shared", ID: "s04", Requests: requests}
}

func orderedPhaseFVariantIDs() []string {
	return []string{
		"LW-0d", "LW-1d", "LW-2d",
		"GH-0d", "GH-1d", "GH-2d",
		"RH-0d", "RH-1d", "RH-2d",
		"HW-0d", "HW-1d", "HW-2d",
	}
}

func newPhaseFPlannedRequest(
	kind string,
	variant phaseFVariant,
	cache *phaseFL0Cache,
) phaseFPlannedRequest {
	return phaseFPlannedRequest{
		Kind:       kind,
		Variant:    variant,
		Request:    clonePhaseFRequest(variant.Request),
		CacheState: variant.CacheState,
		Cache:      cache,
	}
}

func newPhaseFRebucketRequest(
	variant phaseFVariant,
	cache *phaseFL0Cache,
	bucketCfg *pb.BucketConfig,
) phaseFPlannedRequest {
	request := clonePhaseFRequest(variant.Request)
	setPhaseFBucketConfig(request, bucketCfg)
	request.RebucketOnly = variant.VectorDimCount > 0
	return phaseFPlannedRequest{
		Kind:       phaseFRequestKind(request.RebucketOnly),
		Variant:    variant,
		Request:    request,
		CacheState: buildPhaseFL0State(request.Filters, phaseFRequestVectorDimensions(request)),
		Cache:      cache,
	}
}

func clonePhaseFRequest(request *pb.GetBrowsingStateRequest) *pb.GetBrowsingStateRequest {
	if request == nil {
		return &pb.GetBrowsingStateRequest{}
	}
	cloned := &pb.GetBrowsingStateRequest{
		Filters:        cloneAxisFilters(request.GetFilters()),
		All:            request.GetAll(),
		Timeline:       request.GetTimeline(),
		RebucketOnly:   request.GetRebucketOnly(),
		HybridStrategy: request.GetHybridStrategy(),
	}
	if request.GetVectorDimension() != nil {
		cloned.VectorDimension = clonePhaseFVectorDimension(request.GetVectorDimension())
	}
	for _, vectorDimension := range request.GetVectorDimensions() {
		cloned.VectorDimensions = append(cloned.VectorDimensions, clonePhaseFVectorDimension(vectorDimension))
	}
	return cloned
}

func clonePhaseFVectorDimension(
	vectorDimension *pb.VectorSearchDimension,
) *pb.VectorSearchDimension {
	if vectorDimension == nil {
		return nil
	}
	cloned := *vectorDimension
	cloned.BucketCfg = cloneBucketConfig(vectorDimension.GetBucketCfg())
	return &cloned
}

func setPhaseFBucketConfig(request *pb.GetBrowsingStateRequest, bucketCfg *pb.BucketConfig) {
	if request.GetVectorDimension() != nil {
		request.VectorDimension.BucketCfg = cloneBucketConfig(bucketCfg)
	}
	for _, vectorDimension := range request.GetVectorDimensions() {
		vectorDimension.BucketCfg = cloneBucketConfig(bucketCfg)
	}
}

func phaseFRequestVectorDimensions(
	request *pb.GetBrowsingStateRequest,
) []*pb.VectorSearchDimension {
	if request.GetVectorDimension() != nil {
		return []*pb.VectorSearchDimension{request.GetVectorDimension()}
	}
	return request.GetVectorDimensions()
}

func phaseFRequestKind(rebucketOnly bool) string {
	if rebucketOnly {
		return "rebucket_only"
	}
	return "browsing_state"
}
