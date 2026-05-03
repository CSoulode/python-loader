package main

func canReuseAddFilterCandidates(parent *BrowsingStateSnapshot, target *BrowsingStateSnapshot) bool {
	if parent == nil || target == nil {
		return false
	}
	if parent.AxisKey != target.AxisKey || parent.VectorSearchKey != target.VectorSearchKey {
		return false
	}
	if target.FilterCount <= parent.FilterCount {
		return false
	}
	return stringSliceSubset(parent.FilterItems, target.FilterItems)
}

func canReuseAxisCandidates(parent *BrowsingStateSnapshot, target *BrowsingStateSnapshot) bool {
	if parent == nil || target == nil {
		return false
	}
	return parent.AxisDomainKey != "" && parent.AxisDomainKey == target.AxisDomainKey
}

func stringSliceSubset(left []string, right []string) bool {
	if len(left) == 0 {
		return true
	}
	if len(right) == 0 {
		return false
	}
	values := make(map[string]struct{}, len(right))
	for _, item := range right {
		values[item] = struct{}{}
	}
	for _, item := range left {
		if _, ok := values[item]; !ok {
			return false
		}
	}
	return true
}

func fragmentsHaveReusable(fragments *browsingStateFragments) bool {
	if fragments == nil {
		return false
	}
	return len(fragments.Candidates) > 0 || len(fragments.VectorDims) > 0
}

func cloneCandidateFragments(src *browsingStateFragments) *browsingStateFragments {
	if src == nil || len(src.Candidates) == 0 {
		return nil
	}
	return &browsingStateFragments{Candidates: append([]int32(nil), src.Candidates...)}
}

func cloneVectorFragments(src *browsingStateFragments) *browsingStateFragments {
	if src == nil || len(src.VectorDims) == 0 {
		return nil
	}
	return &browsingStateFragments{VectorDims: cloneVectorFragmentMap(src.VectorDims)}
}

func cloneVectorFragmentMap(src map[string]browsingStateVectorFragment) map[string]browsingStateVectorFragment {
	cloned := make(map[string]browsingStateVectorFragment, len(src))
	for key, fragment := range src {
		cloned[key] = cloneBrowsingStateVectorFragment(fragment)
	}
	return cloned
}
