package runner

import (
	"testing"
	"time"
)

func TestPhaseFL0CacheKeyTreatsFilterAndVectorOrderAsEquivalent(t *testing.T) {
	first := phaseFL0State{
		XAxis: &phaseFL0Axis{Type: "tagset", ID: 7},
		YAxis: &phaseFL0Axis{Type: "tagset", ID: 9},
		Filters: []phaseFL0Filter{
			{Type: "tag", ID: 5, GroupID: 9},
			{Type: "tag", ID: 1, GroupID: 7},
		},
		VectorDimensions: []phaseFL0VectorDimension{
			{Axis: "z", Model: "siglip2", ObjectID: 2, BucketCount: 2, BucketStrategy: "equal_width", MaxResults: 10, QueryMode: "knn"},
			{Axis: "x", Model: "siglip2", ObjectID: 1, BucketCount: 3, BucketStrategy: "custom", MaxResults: 20, QueryMode: "range", CustomBreaks: []float32{0, 0.3, 0.8, 1}},
		},
	}
	second := phaseFL0State{
		XAxis: first.XAxis,
		YAxis: first.YAxis,
		Filters: []phaseFL0Filter{
			first.Filters[1],
			first.Filters[0],
		},
		VectorDimensions: []phaseFL0VectorDimension{
			first.VectorDimensions[1],
			first.VectorDimensions[0],
		},
	}

	if got, want := phaseFL0CacheKey(first), phaseFL0CacheKey(second); got != want {
		t.Fatalf("phase F L0 cache keys differ:\nfirst=%s\nsecond=%s", got, want)
	}
}

func TestPhaseFL0CacheKeySeparatesRangeSemantics(t *testing.T) {
	distance := phaseFL0State{
		VectorDimensions: []phaseFL0VectorDimension{{
			Axis:           "x",
			Model:          "siglip2",
			ObjectID:       11,
			BucketCount:    4,
			BucketStrategy: "equal_width",
			MaxResults:     25,
			QueryMode:      "range",
			RangeSemantics: stringPtr("distance"),
		}},
	}
	similarity := phaseFL0State{
		VectorDimensions: []phaseFL0VectorDimension{{
			Axis:           "x",
			Model:          "siglip2",
			ObjectID:       11,
			BucketCount:    4,
			BucketStrategy: "equal_width",
			MaxResults:     25,
			QueryMode:      "range",
			RangeSemantics: stringPtr("similarity"),
		}},
	}

	if phaseFL0CacheKey(distance) == phaseFL0CacheKey(similarity) {
		t.Fatal("expected different range semantics to produce different L0 keys")
	}
}

func TestPhaseFL0CacheExpiresButReturnsStaleEntry(t *testing.T) {
	cache := newPhaseFL0Cache()
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	state := phaseFL0State{}

	cache.Put(state, `"etag-1"`, 42)
	now = now.Add(6 * time.Minute)

	if _, ok := cache.Get(state); ok {
		t.Fatal("expected fresh cache miss after ttl")
	}
	stale, ok := cache.GetStale(state)
	if !ok {
		t.Fatal("expected stale entry to remain available")
	}
	if stale.ETag != `"etag-1"` || stale.PayloadBytes != 42 {
		t.Fatalf("unexpected stale entry: %+v", stale)
	}
}

func TestPhaseFL0CacheRefreshTTL(t *testing.T) {
	cache := newPhaseFL0Cache()
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	state := phaseFL0State{}

	cache.Put(state, `"etag-1"`, 42)
	now = now.Add(6 * time.Minute)
	cache.RefreshTTL(state)

	if _, ok := cache.Get(state); !ok {
		t.Fatal("expected refresh ttl to reactivate entry")
	}
}

func stringPtr(value string) *string {
	return &value
}
