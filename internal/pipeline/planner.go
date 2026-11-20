package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// StateLookup reads committed source_state rows from the sink.
type StateLookup func(ctx context.Context, keys []string) (map[string]store.SourceState, error)

// GenLoader loads a generation by id from the sink.
type GenLoader func(ctx context.Context, id string) (*store.Generation, error)

type window struct {
	gen       string
	job       int64
	changed   map[string]bool
	truncated bool
	dirty     bool
}

type overlayEntry struct {
	fields  map[string]*string
	deleted bool
	seq     uint64
}

// Planner converts committed transactions into ordered units. It is the only
// goroutine that interprets markers, so window bookkeeping is single-threaded
// and follows stream order exactly.
type Planner struct {
	Cfg       *config.Config
	KeyKind   keys.Kind
	Tr        *transform.Transformer
	Registry  *Registry
	Lookup    StateLookup
	LoadGen   GenLoader
	ModelOf   func(g *store.Generation) transform.ModelIdentity
	Log       *slog.Logger
	Live      map[string]*GenInfo // live generations at startup
	ActiveJob map[int64]*string   // running/pending backfill jobs -> planned last key

	windows map[string]*window
	seq     uint64

	mu      sync.Mutex
	overlay map[string]*overlayEntry
}

// Run consumes items until in is closed or a pause is emitted.
func (p *Planner) Run(ctx context.Context, in <-chan Item, out chan<- *Unit) error {
	defer close(out)
	p.windows = map[string]*window{}
	p.overlay = map[string]*overlayEntry{}
	var peek *Item
	for {
		var first Item
		if peek != nil {
			first, peek = *peek, nil
		} else {
			select {
			case it, ok := <-in:
				if !ok {
					return nil
				}
				first = it
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		p.seq++
		u := newUnit(p.seq)
		for id, g := range p.Live {
			u.Gens[id] = g
		}
		if err := p.add(ctx, u, first); err != nil {
			return err
		}
		if u.Pause == nil {
		drain:
			for len(u.Txns) < p.Cfg.Pipeline.MaxBatchTxns && u.docs < p.Cfg.Pipeline.MaxBatchDocs {
				select {
				case it, ok := <-in:
					if !ok {
						break drain
					}
					if it.Txn != nil && it.Txn.Err != nil {
						// Commit everything before the offending transaction first.
						peek = &it
						break drain
					}
					if err := p.add(ctx, u, it); err != nil {
						return err
					}
				default:
					break drain
				}
			}
		}
		metrics.QueueDepth.WithLabelValues("planner_in").Set(float64(len(in)))
		select {
		case out <- u:
		case <-ctx.Done():
			return ctx.Err()
		}
		if u.Pause != nil {
			return u.Pause
		}
	}
}

func (p *Planner) add(ctx context.Context, u *Unit, it Item) error {
	if it.Txn == nil {
		if it.IdleLSN > u.EndLSN {
			u.EndLSN = it.IdleLSN
		}
		return nil
	}
	txn := it.Txn
	if txn.Err != nil {
		if pe, ok := IsPause(txn.Err); ok {
			u.Pause = pe
		} else {
			u.Pause = &PauseError{Reason: "unsupported_input", Detail: txn.Err.Error()}
		}
		// The paused unit carries no effects and must not advance the checkpoint.
		return nil
	}
	for i := range txn.Changes {
		ch := &txn.Changes[i]
		var err error
		switch ch.Kind {
		case KMarker:
			err = p.marker(ctx, u, txn, ch.Marker)
		case KTruncate:
			p.truncate(u)
		default:
			err = p.row(ctx, u, txn, ch)
		}
		if err != nil {
			return err
		}
		if u.Pause != nil {
			return nil
		}
	}
	u.EndLSN = txn.EndLSN
	u.Txns = append(u.Txns, TxnMeta{EndLSN: txn.EndLSN, CommitTime: txn.CommitTime})
	return nil
}

func (p *Planner) truncate(u *Unit) {
	u.Truncate = true
	u.Stream = map[string]*DocEffect{}
	u.Snapshot = map[string]map[string]*DocEffect{}
	u.RangeDeletes = nil
	for _, w := range p.windows {
		w.truncated = true
	}
	p.mu.Lock()
	// Every pre-truncate state is gone; mark it deleted so TOAST lookups cannot
	// read a sink row that this unit is about to delete.
	for k, e := range p.overlay {
		e.fields, e.deleted, e.seq = nil, true, u.Seq
		p.overlay[k] = e
	}
	p.overlay[truncateSentinel] = &overlayEntry{deleted: true, seq: u.Seq}
	p.mu.Unlock()
}

// truncateSentinel marks that a not-yet-committed truncate hides every sink state row.
const truncateSentinel = "\x00truncate"

func (p *Planner) markChanged(u *Unit, key string) {
	for _, w := range p.windows {
		w.changed[key] = true
	}
	for _, m := range u.Snapshot {
		delete(m, key)
	}
}

func (p *Planner) setOverlay(key string, fields map[string]*string, deleted bool, seq uint64) {
	p.mu.Lock()
	p.overlay[key] = &overlayEntry{fields: fields, deleted: deleted, seq: seq}
	p.mu.Unlock()
}

// Release drops overlay entries whose unit has committed.
func (p *Planner) Release(seq uint64) {
	p.mu.Lock()
	for k, e := range p.overlay {
		if e.seq <= seq {
			delete(p.overlay, k)
		}
	}
	p.mu.Unlock()
}

// state returns the latest known fields for a key: uncommitted overlay first, then the sink.
func (p *Planner) state(ctx context.Context, key string) (map[string]*string, bool, error) {
	p.mu.Lock()
	e, ok := p.overlay[key]
	_, truncPending := p.overlay[truncateSentinel]
	p.mu.Unlock()
	if ok {
		if e.deleted {
			return nil, false, nil
		}
		return e.fields, true, nil
	}
	if truncPending {
		return nil, false, nil
	}
	m, err := p.Lookup(ctx, []string{key})
	if err != nil {
		return nil, false, err
	}
	st, ok := m[key]
	if !ok {
		return nil, false, nil
	}
	return st.Fields, true, nil
}

func (p *Planner) effect(key string, fields map[string]*string, lsn LSN, ct time.Time) *DocEffect {
	e := &DocEffect{Key: key, Fields: fields, LSN: lsn, CommitTime: ct}
	vals := make(map[string]string, len(fields))
	for k, v := range fields {
		if v != nil {
			vals[k] = *v
		}
	}
	text, err := p.Tr.Render(vals)
	switch {
	case err != nil:
		e.Poison, e.PoisonErr = "render_error", err.Error()
	case len(text) > p.Cfg.Pipeline.MaxDocumentBytes:
		e.Poison, e.PoisonErr = "document_too_large", fmt.Sprintf("rendered document is %d bytes; pipeline.max_document_bytes=%d", len(text), p.Cfg.Pipeline.MaxDocumentBytes)
	default:
		e.Text = text
	}
	return e
}

func (p *Planner) row(ctx context.Context, u *Unit, txn *Txn, ch *Change) error {
	switch ch.Kind {
	case KDelete:
		p.markChanged(u, ch.Key)
		u.Stream[ch.Key] = &DocEffect{Key: ch.Key, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlay(ch.Key, nil, true, u.Seq)
		u.docs++
		return nil
	case KInsert, KUpdate:
	default:
		return fmt.Errorf("unexpected change kind %d", ch.Kind)
	}
	fields := ch.Fields
	if len(ch.Unchanged) > 0 {
		base := ch.Key
		if ch.OldKey != "" {
			base = ch.OldKey
		}
		prev, ok, err := p.state(ctx, base)
		if err != nil {
			return err
		}
		if !ok {
			return p.deferUnchanged(u, ch, txn.EndLSN)
		}
		merged := make(map[string]*string, len(prev)+len(fields))
		for k, v := range prev {
			merged[k] = v
		}
		for k, v := range fields {
			merged[k] = v
		}
		fields = merged
	}
	if ch.OldKey != "" {
		p.markChanged(u, ch.OldKey)
		u.Stream[ch.OldKey] = &DocEffect{Key: ch.OldKey, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlay(ch.OldKey, nil, true, u.Seq)
		u.docs++
	}
	p.markChanged(u, ch.Key)
	u.Stream[ch.Key] = p.effect(ch.Key, fields, txn.EndLSN, txn.CommitTime)
	p.setOverlay(ch.Key, fields, false, u.Seq)
	u.docs++
	return nil
}

// deferUnchanged handles an UPDATE whose unchanged TOAST value is unknown
// because backfill has not reached the key yet. The future snapshot window
// will capture the full row. If a window is open the snapshot may predate this
// update, so the window is discarded and retried. If no backfill can cover the
// key, the pipeline pauses rather than guess.
func (p *Planner) deferUnchanged(u *Unit, ch *Change, lsn LSN) error {
	for _, last := range p.ActiveJob {
		if last == nil || p.KeyKind.Compare(ch.Key, *last) > 0 {
			for _, w := range p.windows {
				w.dirty = true
			}
			if ch.OldKey != "" {
				p.markChanged(u, ch.OldKey)
				u.Stream[ch.OldKey] = &DocEffect{Key: ch.OldKey, Delete: true, LSN: lsn}
				p.setOverlay(ch.OldKey, nil, true, u.Seq)
			}
			return nil
		}
	}
	u.Pause = &PauseError{Reason: "unsupported_input", Detail: "update of a row with an unchanged TOAST column, but no state is known for it and no backfill will cover it; run `embedsync backfill` for the affected generation"}
	return nil
}

func (p *Planner) marker(ctx context.Context, u *Unit, txn *Txn, m Marker) error {
	switch m.Kind {
	case "start":
		if _, ok := p.Live[m.Gen]; ok {
			return nil
		}
		g, err := p.LoadGen(ctx, m.Gen)
		if err != nil {
			return err
		}
		if g == nil || (g.State != store.StatePending && !store.IsLive(g.State)) {
			return nil
		}
		gi := &GenInfo{Gen: g, Model: p.ModelOf(g)}
		p.Live[m.Gen] = gi
		u.Gens[m.Gen] = gi
		u.GenStarts = append(u.GenStarts, GenStart{Gen: m.Gen, LSN: txn.EndLSN})
		if m.JobID != 0 {
			if _, ok := p.ActiveJob[m.JobID]; !ok {
				p.ActiveJob[m.JobID] = nil
			}
		}
	case "low":
		p.windows[m.WindowID] = &window{gen: m.Gen, job: m.JobID, changed: map[string]bool{}}
		if _, ok := p.ActiveJob[m.JobID]; !ok && m.JobID != 0 {
			p.ActiveJob[m.JobID] = nil
		}
	case "high":
		w := p.windows[m.WindowID]
		delete(p.windows, m.WindowID)
		entry := p.Registry.Take(m.WindowID)
		gi := p.Live[m.Gen]
		if w == nil || entry == nil || gi == nil {
			if entry != nil {
				entry.Finish("discarded")
			}
			return nil
		}
		if w.dirty {
			metrics.BackfillWindows.WithLabelValues("discarded").Inc()
			entry.Finish("discarded")
			return nil
		}
		p.applyWindow(u, gi, w, entry, txn.EndLSN)
	case "retire":
		delete(p.Live, m.Gen)
		u.Retires = append(u.Retires, m.Gen)
	case "barrier":
	}
	return nil
}

// applyWindow emits a window's snapshot rows; lsn is the high marker
// transaction's end LSN (the version stamp of everything the window writes).
func (p *Planner) applyWindow(u *Unit, gi *GenInfo, w *window, entry *WindowRows, lsn LSN) {
	gen := gi.Gen.ID
	u.SnapGens[gen] = gi
	snap := u.Snapshot[gen]
	if snap == nil {
		snap = map[string]*DocEffect{}
		u.Snapshot[gen] = snap
	}
	keep := make([]string, 0, len(entry.Rows)+len(w.changed))
	for k := range w.changed {
		keep = append(keep, k)
	}
	if !w.truncated {
		for _, r := range entry.Rows {
			if w.changed[r.Key] {
				continue
			}
			snap[r.Key] = p.effect(r.Key, r.Fields, lsn, time.Time{})
			p.setOverlay(r.Key, r.Fields, false, u.Seq)
			keep = append(keep, r.Key)
			u.docs++
		}
	}
	var hi *string
	if !entry.Final && len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		hi = &k
	}
	u.RangeDeletes = append(u.RangeDeletes, RangeDelete{Gen: gen, Lo: entry.Lo, Hi: hi, Keep: keep})
	last := entry.Lo
	if len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		last = &k
	}
	u.Progress = append(u.Progress, WindowProgress{JobID: entry.JobID, Gen: gen, LastKey: last, Rows: len(entry.Rows), Final: entry.Final, Entry: entry})
	if entry.Final {
		delete(p.ActiveJob, entry.JobID)
	} else {
		p.ActiveJob[entry.JobID] = last
	}
	metrics.BackfillRows.WithLabelValues(gen).Add(float64(len(entry.Rows)))
	metrics.BackfillWindows.WithLabelValues("applied").Inc()
}
