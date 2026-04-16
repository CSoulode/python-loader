package runner

import (
	"container/list"
	"encoding/json"
	"sort"
	"time"
)

const (
	phaseFL0DefaultMaxEntries = 32
	phaseFL0DefaultTTL        = 5 * time.Minute
)

type phaseFL0Axis struct {
	Type string `json:"type"`
	ID   int32  `json:"id"`
}

type phaseFL0Filter struct {
	Type    string `json:"type"`
	ID      int32  `json:"id"`
	GroupID int32  `json:"groupId"`
}

type phaseFL0VectorDimension struct {
	Axis           string    `json:"axis"`
	Model          string    `json:"model"`
	ObjectID       int32     `json:"objectId"`
	BucketCount    int32     `json:"bucketCount"`
	BucketStrategy string    `json:"bucketStrategy"`
	MaxResults     int32     `json:"maxResults"`
	DistMin        *float32  `json:"distMin"`
	DistMax        *float32  `json:"distMax"`
	CustomBreaks   []float32 `json:"customBreaks"`
	QueryMode      string    `json:"queryMode"`
	RangeMin       *float32  `json:"rangeMin"`
	RangeMax       *float32  `json:"rangeMax"`
	RangeSemantics *string   `json:"rangeSemantics"`
}

type phaseFL0State struct {
	XAxis            *phaseFL0Axis
	YAxis            *phaseFL0Axis
	Filters          []phaseFL0Filter
	VectorDimensions []phaseFL0VectorDimension
}

type phaseFL0StaleEntry struct {
	ETag         string
	PayloadBytes int
}

type phaseFL0Cache struct {
	entries    map[string]*list.Element
	lru        *list.List
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
}

type phaseFL0CacheEntry struct {
	Key          string
	ETag         string
	PayloadBytes int
	CreatedAt    time.Time
}

type phaseFL0KeyPayload struct {
	Axes             phaseFL0KeyAxes           `json:"axes"`
	Filters          []phaseFL0Filter          `json:"filters"`
	VectorDimensions []phaseFL0VectorDimension `json:"vectorDimensions"`
}

type phaseFL0KeyAxes struct {
	X *phaseFL0Axis `json:"x"`
	Y *phaseFL0Axis `json:"y"`
}

func newPhaseFL0Cache() *phaseFL0Cache {
	return &phaseFL0Cache{
		entries:    make(map[string]*list.Element, phaseFL0DefaultMaxEntries),
		lru:        list.New(),
		maxEntries: phaseFL0DefaultMaxEntries,
		ttl:        phaseFL0DefaultTTL,
		now:        time.Now,
	}
}

func (c *phaseFL0Cache) Get(state phaseFL0State) (phaseFL0StaleEntry, bool) {
	entry, ok := c.lookup(state)
	if !ok || c.isExpired(entry) {
		return phaseFL0StaleEntry{}, false
	}
	return phaseFL0StaleEntry{ETag: entry.ETag, PayloadBytes: entry.PayloadBytes}, true
}

func (c *phaseFL0Cache) GetStale(state phaseFL0State) (phaseFL0StaleEntry, bool) {
	entry, ok := c.lookup(state)
	if !ok {
		return phaseFL0StaleEntry{}, false
	}
	return phaseFL0StaleEntry{ETag: entry.ETag, PayloadBytes: entry.PayloadBytes}, true
}

func (c *phaseFL0Cache) Put(state phaseFL0State, etag string, payloadBytes int) {
	key := phaseFL0CacheKey(state)
	now := c.now()
	if element, ok := c.entries[key]; ok {
		entry := element.Value.(*phaseFL0CacheEntry)
		entry.ETag = etag
		entry.PayloadBytes = payloadBytes
		entry.CreatedAt = now
		c.lru.MoveToFront(element)
		return
	}

	element := c.lru.PushFront(&phaseFL0CacheEntry{
		Key:          key,
		ETag:         etag,
		PayloadBytes: payloadBytes,
		CreatedAt:    now,
	})
	c.entries[key] = element
	c.evictOverflow()
}

func (c *phaseFL0Cache) RefreshTTL(state phaseFL0State) {
	entry, ok := c.lookup(state)
	if !ok {
		return
	}
	entry.CreatedAt = c.now()
}

func (c *phaseFL0Cache) Clear() {
	c.entries = make(map[string]*list.Element, c.maxEntries)
	c.lru.Init()
}

func (c *phaseFL0Cache) lookup(state phaseFL0State) (*phaseFL0CacheEntry, bool) {
	key := phaseFL0CacheKey(state)
	element, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(element)
	return element.Value.(*phaseFL0CacheEntry), true
}

func (c *phaseFL0Cache) evictOverflow() {
	for len(c.entries) > c.maxEntries {
		element := c.lru.Back()
		if element == nil {
			return
		}
		entry := element.Value.(*phaseFL0CacheEntry)
		delete(c.entries, entry.Key)
		c.lru.Remove(element)
	}
}

func (c *phaseFL0Cache) isExpired(entry *phaseFL0CacheEntry) bool {
	return entry != nil && c.now().Sub(entry.CreatedAt) >= c.ttl
}

func phaseFL0CacheKey(state phaseFL0State) string {
	payload := phaseFL0KeyPayload{
		Axes: phaseFL0KeyAxes{
			X: state.XAxis,
			Y: state.YAxis,
		},
		Filters:          sortPhaseFL0Filters(state.Filters),
		VectorDimensions: sortPhaseFL0VectorDimensions(state.VectorDimensions),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func sortPhaseFL0Filters(filters []phaseFL0Filter) []phaseFL0Filter {
	cloned := append([]phaseFL0Filter(nil), filters...)
	sortPhaseFL0FilterSlice(cloned)
	return cloned
}

func sortPhaseFL0VectorDimensions(
	dimensions []phaseFL0VectorDimension,
) []phaseFL0VectorDimension {
	cloned := append([]phaseFL0VectorDimension(nil), dimensions...)
	sortPhaseFL0VectorDimensionSlice(cloned)
	return cloned
}

func sortPhaseFL0FilterSlice(filters []phaseFL0Filter) {
	sort.Slice(filters, func(left int, right int) bool {
		return filters[left].Type < filters[right].Type ||
			(filters[left].Type == filters[right].Type &&
				(filters[left].GroupID < filters[right].GroupID ||
					(filters[left].GroupID == filters[right].GroupID &&
						filters[left].ID < filters[right].ID)))
	})
}

func sortPhaseFL0VectorDimensionSlice(dimensions []phaseFL0VectorDimension) {
	sort.Slice(dimensions, func(left int, right int) bool {
		return dimensions[left].Axis < dimensions[right].Axis
	})
}
