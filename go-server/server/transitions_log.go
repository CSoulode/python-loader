package main

import (
	"sync"
	"sync/atomic"
	"time"
)

const defaultTransitionLogCapacity = 4096

type TransitionEvent struct {
	ParentStateID        StateID   `json:"parent_state_id"`
	TargetStateID        StateID   `json:"target_state_id"`
	DeltaKind            string    `json:"delta_kind"`
	EstimatedSelectivity *float64  `json:"estimated_selectivity,omitempty"`
	ActualCandidateCount *int64    `json:"actual_candidate_count,omitempty"`
	ElapsedMillis        *float64  `json:"elapsed_ms,omitempty"`
	AncestorReuseStages  []string  `json:"ancestor_reuse_stages,omitempty"`
	RecordedAt           time.Time `json:"recorded_at"`
}

type TransitionEventLog struct {
	mu        sync.RWMutex
	events    []TransitionEvent
	next      int
	count     int
	capacity  int
	appended  atomic.Int64
	rejected  atomic.Int64
	overwrite atomic.Int64
}

func newTransitionEventLog(capacity int) *TransitionEventLog {
	if capacity <= 0 {
		capacity = defaultTransitionLogCapacity
	}
	return &TransitionEventLog{events: make([]TransitionEvent, capacity), capacity: capacity}
}

func (l *TransitionEventLog) Append(event TransitionEvent) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if event.RecordedAt.IsZero() {
		event.RecordedAt = time.Now().UTC()
	}
	if l.count == l.capacity {
		l.overwrite.Add(1)
	} else {
		l.count++
	}
	l.events[l.next] = event
	l.next = (l.next + 1) % l.capacity
	l.appended.Add(1)
}

func (l *TransitionEventLog) List() []TransitionEvent {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	items := make([]TransitionEvent, 0, l.count)
	start := (l.next - l.count + l.capacity) % l.capacity
	for index := 0; index < l.count; index++ {
		items = append(items, l.events[(start+index)%l.capacity])
	}
	return items
}

func (l *TransitionEventLog) Reject() {
	if l != nil {
		l.rejected.Add(1)
	}
}

func (l *TransitionEventLog) Snapshot() map[string]int64 {
	if l == nil {
		return map[string]int64{"events": 0}
	}
	return map[string]int64{
		"events":    int64(len(l.List())),
		"appended":  l.appended.Load(),
		"rejected":  l.rejected.Load(),
		"overwrite": l.overwrite.Load(),
		"capacity":  int64(l.capacity),
	}
}

func (s *DataLoaderServer) ensureTransitionEventLog() *TransitionEventLog {
	if s.transitionLog == nil {
		s.transitionLog = newTransitionEventLog(defaultTransitionLogCapacity)
		setTransitionEventLogForExpvar(s.transitionLog)
	}
	return s.transitionLog
}
