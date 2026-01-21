package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "m3.dataloader/dataloader" // TODO
)

// Reuse your existing types if they already exist globally.
// If these already exist, delete these duplicates.
type cellKey struct{ x, y, z int32 }

// cellAgg matches your old semantics.
// NOTE: keep seen map + rev exactly like before to avoid drops.
type cellAgg struct {
	count    int32
	repID    int32
	fileURI  string
	thumbURI string
	seen     map[int32]struct{}
	rev      uint64
}

// snap is what we send.
type snap struct {
	k        cellKey
	count    int32
	repID    int32
	fileURI  string
	thumbURI string
	rev      uint64
}

// SendStream is satisfied by your gRPC stream type.
type SendStream interface {
	Send(*pb.BrowsingStateResponse) error
}

// -------------------- CellAggregator --------------------

type CellAggregator struct {
	mu     sync.RWMutex
	cells  map[cellKey]*cellAgg
	queued map[cellKey]bool
}

type CellAggregatorOpts struct {
	InitialCap int
	SeenCap    int // initial cap for per-cell seen map
}

func NewCellAggregator(opts CellAggregatorOpts) *CellAggregator {
	capacity := opts.InitialCap
	if capacity <= 0 {
		capacity = 65536 // keep old default unless you decide otherwise
	}
	return &CellAggregator{
		cells:  make(map[cellKey]*cellAgg, capacity),
		queued: make(map[cellKey]bool, capacity),
	}
}

// ApplyRow updates cell state and returns (key, shouldEnqueue).
// shouldEnqueue == true means the caller should send key to dirtyCh (once until acked).
func (a *CellAggregator) ApplyRow(
	key cellKey,
	objectID int32,
	fileURI string,
	thumbURI string,
	seenCap int,
) (cellKey, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	agg := a.cells[key]
	if agg == nil {
		if seenCap <= 0 {
			seenCap = 16
		}
		agg = &cellAgg{seen: make(map[int32]struct{}, seenCap)}
		a.cells[key] = agg
	}

	changed := false

	// DISTINCT object per cell
	if _, ok := agg.seen[objectID]; !ok {
		agg.seen[objectID] = struct{}{}
		agg.count++
		changed = true
	}

	// Representative policy: max object_id
	if objectID > agg.repID {
		agg.repID = objectID
		agg.fileURI = fileURI
		agg.thumbURI = thumbURI

		changed = true
	}

	if changed {
		agg.rev++
	}

	// Only enqueue key once until it gets acknowledged by the flusher.
	shouldEnqueue := !a.queued[key]
	if shouldEnqueue {
		a.queued[key] = true
	}
	return key, shouldEnqueue
}

// Snapshot returns snapshots for the given keys under a read lock.
func (a *CellAggregator) Snapshot(keys []cellKey) []snap {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]snap, 0, len(keys))
	for _, k := range keys {
		agg := a.cells[k]
		if agg == nil {
			continue
		}
		out = append(out, snap{
			k:        k,
			count:    agg.count,
			repID:    agg.repID,
			fileURI:  agg.fileURI,
			thumbURI: agg.thumbURI,
			rev:      agg.rev,
		})
	}
	return out
}

// AckSent implements your "rev changed after snapshot" correctness.
// Returns true if it changed after the snapshot (meaning caller should keep it pending).
func (a *CellAggregator) AckSent(k cellKey, sentRev uint64) (changedAfter bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cur := a.cells[k]
	if cur != nil && cur.rev > sentRev {
		// It changed after snapshot / while sending; keep queued=true (do not clear)
		return true
	}

	// No changes since snapshot: allow scanner to enqueue again later
	a.queued[k] = false
	return false
}

// -------------------- Key Flusher --------------------

// SnapToResp builds the outgoing message from a snap. Keeps reusability across handlers.
type SnapToResp func(s snap) *pb.BrowsingStateResponse

// RunKeyFlusher owns: ticker, pending set, selection up to streamBatchSize, snapshot, Send, ack.
// It is the ONLY place that calls stream.Send for this request.
func RunKeyFlusher(
	ctx context.Context,
	stream SendStream,
	dirtyCh <-chan cellKey,
	agg *CellAggregator,
	flushInterval time.Duration,
	streamBatchSize int,
	pendingCap int,
	makeResp SnapToResp,
) error {
	if pendingCap <= 0 {
		pendingCap = 65536 // match old behavior by default
	}
	pending := make(map[cellKey]struct{}, pendingCap)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func(force bool) error {
		if !force && len(pending) == 0 {
			return nil
		}

		// Choose keys to send this round.
		keys := make([]cellKey, 0, len(pending))
		for k := range pending {
			keys = append(keys, k)
			if !force && streamBatchSize > 0 && len(keys) >= streamBatchSize {
				break
			}
		}

		// Snapshot under RLock (no locks held during network I/O)
		snaps := agg.Snapshot(keys)

		// Send + ack
		for _, sn := range snaps {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			resp := makeResp(sn)
			if err := stream.Send(resp); err != nil {
				return err
			}

			// remove from pending then decide whether it needs to stay pending
			delete(pending, sn.k)

			if agg.AckSent(sn.k, sn.rev) {
				// Changed after snapshot; keep pending so it will be re-sent
				pending[sn.k] = struct{}{}
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case k, ok := <-dirtyCh:
			if !ok {
				// Scanner finished: flush remaining
				return flush(true)
			}
			pending[k] = struct{}{}

			// Old "early flush" behavior
			if streamBatchSize > 0 && len(pending) >= streamBatchSize {
				if err := flush(false); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := flush(false); err != nil {
				return err
			}
		}
	}
}

func RunKeyFlusherWithMetrics(
	ctx context.Context,
	stream SendStream,
	dirtyCh <-chan cellKey,
	agg *CellAggregator,
	flushInterval time.Duration,
	streamBatchSize int,
	pendingCap int,
	makeResp SnapToResp,
	m *StreamMetrics,
) error {
	if pendingCap <= 0 {
		pendingCap = 65536
	}
	pending := make(map[cellKey]struct{}, pendingCap)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func(force bool) error {
		if !force && len(pending) == 0 {
			return nil
		}

		keys := make([]cellKey, 0, len(pending))
		for k := range pending {
			keys = append(keys, k)
			if !force && streamBatchSize > 0 && len(keys) >= streamBatchSize {
				break
			}
		}

		snaps := agg.Snapshot(keys)

		for _, sn := range snaps {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			resp := makeResp(sn)
			if err := stream.Send(resp); err != nil {
				return err
			}
			if m != nil {
				m.MarkFirstSend()
				atomic.AddInt64(&m.ItemsSent, 1)
			}

			delete(pending, sn.k)
			if agg.AckSent(sn.k, sn.rev) {
				pending[sn.k] = struct{}{}
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case k, ok := <-dirtyCh:
			if !ok {
				return flush(true)
			}
			if m != nil {
				// dirty key consumed => queue depth decreases
				m.IncQueue(-1)
			}
			pending[k] = struct{}{}

			if streamBatchSize > 0 && len(pending) >= streamBatchSize {
				if err := flush(false); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := flush(false); err != nil {
				return err
			}
		}
	}
}

// RunQueueSender drains respCh and sends each response to the stream.
// - Only this goroutine should call stream.Send.
// - Producer must close(respCh) to signal completion.
// - Returns when respCh is closed+drained, ctx is cancelled, or Send fails.
func RunQueueSender(ctx context.Context, stream SendStream, respCh <-chan *pb.BrowsingStateResponse) error {
	for resp := range respCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func RunQueueSenderWithMetrics(
	ctx context.Context,
	stream SendStream,
	respCh <-chan *pb.BrowsingStateResponse,
	m *StreamMetrics,
) error {
	for resp := range respCh {
		if m != nil {
			// Item consumed from queue
			m.IncQueue(-1)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if m != nil {
			m.MarkFirstSend()
		}

		if err := stream.Send(resp); err != nil {
			return err
		}

		if m != nil {
			atomic.AddInt64(&m.ItemsSent, 1)
		}
	}
	return nil
}

// DedupTuple is what the scanner enqueues when dedup happens in the sender goroutine.
type DedupTuple struct {
	Key      cellKey
	ObjectID int32
	FileURI  string
	ThumbURI string
}

// RunQueueSenderDedup drains items from itemCh, deduplicates per cell, and sends one response per unique (cell, object_id).
// Producer must close(itemCh) to signal completion.
func RunQueueSenderDedup(
	ctx context.Context,
	stream SendStream,
	itemCh <-chan DedupTuple,
	seenCapPerCell int,
	cellCap int,
) error {
	if seenCapPerCell <= 0 {
		seenCapPerCell = 16
	}
	if cellCap <= 0 {
		cellCap = 65536
	}

	seen := make(map[cellKey]map[int32]struct{}, cellCap)

	for it := range itemCh {
		// Dedup per cell
		sset := seen[it.Key]
		if sset == nil {
			sset = make(map[int32]struct{}, seenCapPerCell)
			seen[it.Key] = sset
		}
		if _, dup := sset[it.ObjectID]; dup {
			continue
		}
		sset[it.ObjectID] = struct{}{}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp := &pb.BrowsingStateResponse{
			X:     it.Key.x,
			Y:     it.Key.y,
			Z:     it.Key.z,
			Count: 1,
			CubeObjects: []*pb.CubeObject{{
				Id:           it.ObjectID,
				FileUri:      it.FileURI,
				ThumbnailUri: it.ThumbURI,
			}},
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func RunQueueSenderDedupWithMetrics(
	ctx context.Context,
	stream SendStream,
	itemCh <-chan DedupTuple,
	seenCapPerCell int,
	cellCap int,
	m *StreamMetrics,
) error {
	if seenCapPerCell <= 0 {
		seenCapPerCell = 16
	}
	if cellCap <= 0 {
		cellCap = 65536
	}

	seen := make(map[cellKey]map[int32]struct{}, cellCap)

	for it := range itemCh {
		if m != nil {
			// item consumed from queue
			m.IncQueue(-1)
		}

		// Dedup per cell
		sset := seen[it.Key]
		if sset == nil {
			sset = make(map[int32]struct{}, seenCapPerCell)
			seen[it.Key] = sset
		}
		if _, dup := sset[it.ObjectID]; dup {
			if m != nil {
				atomic.AddInt64(&m.DupsSkipped, 1)
			}
			continue
		}
		sset[it.ObjectID] = struct{}{}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp := &pb.BrowsingStateResponse{
			X:     it.Key.x,
			Y:     it.Key.y,
			Z:     it.Key.z,
			Count: 1,
			CubeObjects: []*pb.CubeObject{{
				Id:           it.ObjectID,
				FileUri:      it.FileURI,
				ThumbnailUri: it.ThumbURI,
			}},
		}

		if m != nil {
			m.MarkFirstSend()
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		if m != nil {
			atomic.AddInt64(&m.ItemsSent, 1)
		}
	}
	return nil
}
