// Put this in the same package as your gRPC handlers (e.g. in stream_sender.go).
// It’s intentionally small and predictable: FIFO ordering, one goroutine owns stream.Send.

package main

import (
	"context"
	"sync"
	"time"

	pb "m3.dataloader/dataloader"
)

// ---------- Batcher (pure batching, no gRPC knowledge) ----------

type Batcher struct {
	// knobs
	batchSize     int
	flushInterval time.Duration
	maxFirstFlush time.Duration // 0 disables

	// state
	pending        []*pb.BrowsingStateResponse
	startedAt      time.Time
	lastFlushAt    time.Time
	hasFlushedOnce bool
}

type BatcherOpts struct {
	BatchSize     int
	FlushInterval time.Duration
	MaxFirstFlush time.Duration // 0 disables
	// Optional: reserve capacity for fewer allocs
	InitialCap int
}

func NewBatcher(now time.Time, opts BatcherOpts) *Batcher {
	capacity := opts.InitialCap
	if capacity <= 0 {
		capacity = opts.BatchSize
		if capacity <= 0 {
			capacity = 64
		}
	}
	b := &Batcher{
		batchSize:     opts.BatchSize,
		flushInterval: opts.FlushInterval,
		maxFirstFlush: opts.MaxFirstFlush,
		pending:       make([]*pb.BrowsingStateResponse, 0, capacity),
		startedAt:     now,
		lastFlushAt:   now,
	}
	return b
}

func (b *Batcher) PendingLen() int { return len(b.pending) }

func (b *Batcher) Add(resp *pb.BrowsingStateResponse) {
	if resp == nil {
		return
	}
	b.pending = append(b.pending, resp)
}

func (b *Batcher) ShouldFlush(now time.Time) bool {
	if len(b.pending) == 0 {
		return false
	}
	if b.batchSize > 0 && len(b.pending) >= b.batchSize {
		return true
	}
	if b.flushInterval > 0 && now.Sub(b.lastFlushAt) >= b.flushInterval {
		return true
	}
	if !b.hasFlushedOnce && b.maxFirstFlush > 0 && now.Sub(b.startedAt) >= b.maxFirstFlush {
		return true
	}
	return false
}

// Flush returns the current pending batch and resets internal state.
// If force=true, it flushes even if empty (returns nil).
func (b *Batcher) Flush(now time.Time, force bool) []*pb.BrowsingStateResponse {
	if len(b.pending) == 0 && !force {
		return nil
	}
	out := b.pending
	b.pending = make([]*pb.BrowsingStateResponse, 0, cap(b.pending))
	b.lastFlushAt = now
	b.hasFlushedOnce = true
	return out
}

// ---------- StreamSender (owns stream.Send in one goroutine) ----------

type SendFn func(batch []*pb.BrowsingStateResponse) error

// DefaultSendFn sends each response as-is (preserves your current semantics).
func DefaultSendFn(stream interface {
	Send(*pb.BrowsingStateResponse) error
}) SendFn {
	return func(batch []*pb.BrowsingStateResponse) error {
		for _, r := range batch {
			if err := stream.Send(r); err != nil {
				return err
			}
		}
		return nil
	}
}

type StreamSender struct {
	ctx    context.Context
	cancel context.CancelFunc

	in   chan *pb.BrowsingStateResponse
	done chan struct{}

	sendBatch SendFn
	batcher   *Batcher

	mu  sync.Mutex
	err error
}

type StreamSenderOpts struct {
	ChanSize int

	Batcher BatcherOpts

	// If true, we also flush on every tick even if the batcher doesn’t think it should.
	// Usually false; leaving it here if you ever want a “heartbeat flush”.
	ForceFlushOnTick bool
}

// StartStreamSender starts a goroutine that is the ONLY code path allowed to call stream.Send.
func StartStreamSender(parent context.Context, sendBatch SendFn, opts StreamSenderOpts) *StreamSender {
	if opts.ChanSize <= 0 {
		opts.ChanSize = 1024
	}

	ctx, cancel := context.WithCancel(parent)
	now := time.Now()

	s := &StreamSender{
		ctx:       ctx,
		cancel:    cancel,
		in:        make(chan *pb.BrowsingStateResponse, opts.ChanSize),
		done:      make(chan struct{}),
		sendBatch: sendBatch,
		batcher:   NewBatcher(now, opts.Batcher),
	}

	go s.loop(opts)
	return s
}

func (s *StreamSender) loop(opts StreamSenderOpts) {
	defer close(s.done)

	// Ticker is optional; if FlushInterval==0 you can still flush on batch size or Close().
	var ticker *time.Ticker
	if opts.Batcher.FlushInterval > 0 {
		ticker = time.NewTicker(opts.Batcher.FlushInterval / 2) // tick faster than interval for smoother flushing
		defer ticker.Stop()
	}

	flush := func(now time.Time, force bool) bool {
		batch := s.batcher.Flush(now, force)
		if len(batch) == 0 {
			return false
		}
		if err := s.sendBatch(batch); err != nil {
			s.setErr(err)
			s.cancel() // stop producer side quickly
			return true
		}
		return false
	}

	for {
		select {
		case <-s.ctx.Done():
			// Context canceled (client gone, send error, handler aborted, etc.)
			_ = flush(time.Now(), true)
			return

		case r, ok := <-s.in:
			if !ok {
				// Producer closed channel: final flush and exit.
				_ = flush(time.Now(), true)
				return
			}
			s.batcher.Add(r)
			now := time.Now()
			if s.batcher.ShouldFlush(now) {
				if flush(now, false) {
					return
				}
			}

		case <-func() <-chan time.Time {
			if ticker == nil {
				// never fires
				return make(chan time.Time)
			}
			return ticker.C
		}():
			now := time.Now()
			if opts.ForceFlushOnTick {
				if flush(now, false) {
					return
				}
			} else {
				if s.batcher.ShouldFlush(now) {
					if flush(now, false) {
						return
					}
				}
			}
		}
	}
}

func (s *StreamSender) setErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *StreamSender) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Enqueue queues a response to be sent later by the sender goroutine.
// Returns an error if the sender is already in an error/canceled state.
func (s *StreamSender) Enqueue(resp *pb.BrowsingStateResponse) error {
	if resp == nil {
		return nil
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.in <- resp:
		return nil
	}
}

// CloseAndWait tells the sender there will be no more messages, waits for flush, returns send error if any.
func (s *StreamSender) CloseAndWait() error {
	// Closing the channel triggers a final flush.
	close(s.in)
	<-s.done
	// If ctx was canceled because of a send error, return the stored error.
	if err := s.Err(); err != nil {
		return err
	}
	// Otherwise: propagate cancellation if caller cares.
	return nil
}

// Cancel cancels the sender (e.g., if the handler hits a DB error and wants to stop immediately).
func (s *StreamSender) Cancel() { s.cancel() }

// ---------- Example usage in a handler ----------
//
// 1) Create sender:
//    sender := StartStreamSender(stream.Context(), DefaultSendFn(stream), StreamSenderOpts{
//        ChanSize: 2048,
//        Batcher: BatcherOpts{
//            BatchSize:     batchSize,
//            FlushInterval: flushInterval,
//            MaxFirstFlush: maxFirstFlush, // or 0 if not used
//            InitialCap:    batchSize,
//        },
//    })
//    defer sender.Cancel() // safety
//
// 2) Produce responses (NO stream.Send here):
//    if err := sender.Enqueue(&pb.BrowsingStateResponse{CellsChunk: chunk}); err != nil { return err }
//
// 3) End:
//    if err := sender.CloseAndWait(); err != nil { return err }
//    return nil
//
// To use without goroutine right now, use Batcher alone:
//
//    b := NewBatcher(time.Now(), BatcherOpts{BatchSize: batchSize, FlushInterval: flushInterval, MaxFirstFlush: maxFirstFlush})
//    send := DefaultSendFn(stream)
//    ...
//    b.Add(resp)
//    if b.ShouldFlush(time.Now()) { _ = send(b.Flush(time.Now(), false)) }
//    ...
//    _ = send(b.Flush(time.Now(), true))
