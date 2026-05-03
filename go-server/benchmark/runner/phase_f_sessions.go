package runner

import pb "m3.dataloader/dataloader"

type phaseFSessionPlan struct {
	Type     string
	ID       string
	Requests []phaseFRequest
}

type phaseFVariantLookup map[string]phaseFVariant

type phaseFRequestOptions struct {
	ParentKey string
	Tab       string
	DeltaKind string
}

func buildPhaseFSessionPlans(variants []phaseFVariant) []phaseFSessionPlan {
	lookup := buildPhaseFVariantLookup(variants)
	return []phaseFSessionPlan{
		buildLinearExploreSession(lookup),
		buildBacktrackHeavySession(lookup),
		buildRebucketTuningSession(lookup),
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

func buildLinearExploreSession(lookup phaseFVariantLookup) phaseFSessionPlan {
	requests := make([]phaseFRequest, 0, 22)
	for _, key := range orderedPhaseFVariantIDs() {
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "main"}))
	}
	requests = append(requests, newPhaseFAddFilterRequest(lookup["LW-1d"], "LW-1d"))
	requests = append(requests, linearExploreRebucketRequests(lookup)...)
	return phaseFSessionPlan{Type: "linear_explore", ID: "s01", Requests: requests}
}

func linearExploreRebucketRequests(lookup phaseFVariantLookup) []phaseFRequest {
	keys := []string{"LW-1d", "GH-1d", "RH-2d", "HW-2d"}
	requests := make([]phaseFRequest, 0, len(keys))
	for index, key := range keys {
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], key, RebucketVariants()[index]))
	}
	return requests
}

func buildBacktrackHeavySession(lookup phaseFVariantLookup) phaseFSessionPlan {
	order := []string{
		"LW-0d", "LW-1d", "GH-1d", "RH-2d",
		"LW-0d", "GH-1d", "HW-0d", "RH-2d",
		"LW-1d", "LW-0d", "GH-1d", "HW-0d",
		"RH-2d", "LW-0d", "GH-1d", "HW-0d",
	}
	requests := make([]phaseFRequest, 0, len(order))
	for _, key := range order {
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "main"}))
	}
	return phaseFSessionPlan{Type: "backtrack_heavy", ID: "s02", Requests: requests}
}

func buildRebucketTuningSession(lookup phaseFVariantLookup) phaseFSessionPlan {
	requests := make([]phaseFRequest, 0, 17)
	for _, key := range []string{"LW-0d", "LW-1d", "GH-1d", "RH-2d", "HW-2d"} {
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "main"}))
	}
	for _, key := range []string{"LW-1d", "GH-1d", "RH-2d", "HW-2d"} {
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], key, RebucketVariants()[0]))
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], key, RebucketVariants()[1]))
		requests = append(requests, newPhaseFRebucketRequest(lookup[key], key, RebucketVariants()[0]))
	}
	return phaseFSessionPlan{Type: "rebucket_tuning", ID: "s03", Requests: requests}
}

func buildMultiTabSession(lookup phaseFVariantLookup) phaseFSessionPlan {
	keys := []string{"LW-0d", "GH-1d", "RH-2d", "HW-0d", "LW-1d", "GH-2d"}
	requests := make([]phaseFRequest, 0, len(keys)*3)
	for _, key := range keys {
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "tab-a"}))
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "tab-b"}))
		requests = append(requests, newPhaseFVariantRequest(lookup, key, phaseFRequestOptions{Tab: "tab-a"}))
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

func newPhaseFVariantRequest(lookup phaseFVariantLookup, key string, options phaseFRequestOptions) phaseFRequest {
	variant := lookup[key]
	return phaseFRequest{
		key: key, parentKey: options.ParentKey, tab: options.Tab, deltaKind: options.DeltaKind,
		complexity: variant.Complexity, vectorDimCount: variant.VectorDimCount,
		req: clonePhaseFRequest(variant.Request),
	}
}

func newPhaseFAddFilterRequest(variant phaseFVariant, parentKey string) phaseFRequest {
	request := clonePhaseFRequest(variant.Request)
	request.Filters = append(request.Filters, &pb.AxisFilter{
		AxisFilterType: pb.AxisType_FILTER,
		Value:          phaseFMetadataTagA,
		ValueType:      pb.FilterValueType_TAG,
	})
	return phaseFRequest{
		key: variant.ID + "-add-filter", parentKey: parentKey, tab: "main", deltaKind: "AddFilter",
		complexity: variant.Complexity, vectorDimCount: variant.VectorDimCount, req: request,
	}
}

func newPhaseFRebucketRequest(variant phaseFVariant, parentKey string, bucketCfg *pb.BucketConfig) phaseFRequest {
	request := clonePhaseFRequest(variant.Request)
	setPhaseFBucketConfig(request, bucketCfg)
	request.RebucketOnly = variant.VectorDimCount > 0
	return phaseFRequest{
		key: variant.ID + "-rebucket-" + phaseFBucketKey(bucketCfg), parentKey: parentKey, tab: "main",
		deltaKind: "RebucketOnly", complexity: variant.Complexity,
		vectorDimCount: variant.VectorDimCount, req: request,
	}
}

func setPhaseFBucketConfig(request *pb.GetBrowsingStateRequest, bucketCfg *pb.BucketConfig) {
	if request.GetVectorDimension() != nil {
		request.VectorDimension.BucketCfg = cloneBucketConfig(bucketCfg)
	}
	for _, vectorDimension := range request.GetVectorDimensions() {
		vectorDimension.BucketCfg = cloneBucketConfig(bucketCfg)
	}
}

func phaseFBucketKey(bucketCfg *pb.BucketConfig) string {
	return bucketCfg.GetStrategy().String() + "-" + FormatFloat(float64(bucketCfg.GetCount()))
}
