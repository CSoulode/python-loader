package main

import (
	"testing"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func TestComputeFilterHashIgnoresOrder(t *testing.T) {
	filtersA := []qg.ParsedFilter{
		{Type: "tag", Ids: []int{2, 1}},
		{Type: "numrange", Ids: []int{42}, Ranges: [][]string{{"2020", "2022"}}},
	}
	axesA := []qg.ParsedAxis{
		{Type: "node", Id: 9},
		{Type: "tagset", Id: 3},
	}

	filtersB := []qg.ParsedFilter{
		{Type: "numrange", Ids: []int{42}, Ranges: [][]string{{"2020", "2022"}}},
		{Type: "tag", Ids: []int{1, 2}},
	}
	axesB := []qg.ParsedAxis{
		{Type: "tagset", Id: 3},
		{Type: "node", Id: 9},
	}

	hashA, err := computeFilterHash(filtersA, axesA)
	if err != nil {
		t.Fatalf("computeFilterHash(A): %v", err)
	}
	hashB, err := computeFilterHash(filtersB, axesB)
	if err != nil {
		t.Fatalf("computeFilterHash(B): %v", err)
	}
	if hashA != hashB {
		t.Fatalf("hash mismatch: %d != %d", hashA, hashB)
	}
}

func TestComputeFilterHashIncludesRangeBounds(t *testing.T) {
	base := []qg.ParsedFilter{{
		Type:   "numrange",
		Ids:    []int{42},
		Ranges: [][]string{{"2020", "2022"}},
	}}
	other := []qg.ParsedFilter{{
		Type:   "numrange",
		Ids:    []int{42},
		Ranges: [][]string{{"2023", "2025"}},
	}}

	hashA, err := computeFilterHash(base, nil)
	if err != nil {
		t.Fatalf("computeFilterHash(base): %v", err)
	}
	hashB, err := computeFilterHash(other, nil)
	if err != nil {
		t.Fatalf("computeFilterHash(other): %v", err)
	}
	if hashA == hashB {
		t.Fatalf("expected distinct hashes for different range bounds, got %d", hashA)
	}
}

func TestChooseStrategyThresholds(t *testing.T) {
	if got := chooseStrategy(1, 100, false, strategyConfig(100), false, Auto); got != PostFilter {
		t.Fatalf("no metadata strategy = %v, want PostFilter", got)
	}
	if got := chooseStrategy(4999, 10000, true, strategyConfig(100), false, Auto); got != PreFilter {
		t.Fatalf("strict filter strategy = %v, want PreFilter", got)
	}
	if got := chooseStrategy(5000, 10000, true, strategyConfig(100), false, Auto); got != PostFilter {
		t.Fatalf("boundary strategy = %v, want PostFilter", got)
	}
	if got := chooseStrategy(2499, 10000, true, strategyConfig(1001), false, Auto); got != PreFilter {
		t.Fatalf("large-k prefilter strategy = %v, want PreFilter", got)
	}
	if got := chooseStrategy(2500, 10000, true, strategyConfig(1001), false, Auto); got != Hybrid {
		t.Fatalf("large-k boundary strategy = %v, want Hybrid", got)
	}
}

func strategyConfig(maxResults int32) *pb.VectorSearchDimension {
	return &pb.VectorSearchDimension{MaxResults: maxResults}
}
