package main

import (
	"log"
	"sync/atomic"
	"time"
)

type StreamMetrics struct {
	Name  string
	Start time.Time

	// “production” side
	RowsRead      int64 // db rows scanned
	ItemsProduced int64 // responses or tuples enqueued/created

	// “consumption” side
	ItemsSent   int64 // successful stream.Send calls
	DupsSkipped int64 // if applicable

	// Backpressure / bottleneck evidence
	QueueDepth    int64
	MaxQueueDepth int64

	// Latency markers (stored as nanoseconds since Start)
	FirstRowNS  int64
	FirstSendNS int64
	DoneGroupNS int64 // when grouping finished (if applicable)
}

func NewStreamMetrics(name string) *StreamMetrics {
	return &StreamMetrics{Name: name, Start: time.Now()}
}

func (m *StreamMetrics) MarkFirstRow() {
	if atomic.LoadInt64(&m.FirstRowNS) != 0 {
		return
	}
	atomic.CompareAndSwapInt64(&m.FirstRowNS, 0, time.Since(m.Start).Nanoseconds())
}

func (m *StreamMetrics) MarkFirstSend() {
	if atomic.LoadInt64(&m.FirstSendNS) != 0 {
		return
	}
	atomic.CompareAndSwapInt64(&m.FirstSendNS, 0, time.Since(m.Start).Nanoseconds())
}

func (m *StreamMetrics) MarkGroupDone() {
	atomic.StoreInt64(&m.DoneGroupNS, time.Since(m.Start).Nanoseconds())
}

func (m *StreamMetrics) IncQueue(delta int64) {
	cur := atomic.AddInt64(&m.QueueDepth, delta)
	for {
		prev := atomic.LoadInt64(&m.MaxQueueDepth)
		if cur <= prev {
			return
		}
		if atomic.CompareAndSwapInt64(&m.MaxQueueDepth, prev, cur) {
			return
		}
	}
}

func (m *StreamMetrics) LogSummary(extra string) {
	firstRow := time.Duration(atomic.LoadInt64(&m.FirstRowNS))
	firstSend := time.Duration(atomic.LoadInt64(&m.FirstSendNS))
	doneGroup := time.Duration(atomic.LoadInt64(&m.DoneGroupNS))
	total := time.Since(m.Start)

	log.Printf(
		"%s metrics: rowsRead=%d produced=%d sent=%d dupsSkipped=%d maxQueueDepth=%d firstRow=%s firstSend=%s groupDone=%s total=%s %s",
		m.Name,
		atomic.LoadInt64(&m.RowsRead),
		atomic.LoadInt64(&m.ItemsProduced),
		atomic.LoadInt64(&m.ItemsSent),
		atomic.LoadInt64(&m.DupsSkipped),
		atomic.LoadInt64(&m.MaxQueueDepth),
		firstRow,
		firstSend,
		doneGroup,
		total,
		extra,
	)
}
