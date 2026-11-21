package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// StateLookup reads one pipeline's committed source_state rows from the sink.
type StateLookup func(ctx context.Context, pipeline string, keys []string) (map[string]store.SourceState, error)

// GenLoader loads a generation by id from the sink.
type GenLoader func(ctx context.Context, id string) (*store.Generation, error)

// JobLoader loads backfill jobs: one by id, or every active job of a generation.
type JobLoader interface {
	BackfillJobByID(ctx context.Context, id int64) (*store.BackfillJob, error)
	ActiveBackfillJobs(ctx context.Context, genID string) ([]store.BackfillJob, error)
}

// JobCursor is the planner's view of an active backfill job: its pipeline
// and the last key of its last applied window (nil: nothing applied yet, so
// the job will still cover every key of the pipeline).
type JobCursor struct {
	Pipeline string
	Last     *string
	Kind     string // store.JobEmbed, JobState or JobRender
}

// window tracks one open backfill window. Its pipeline is only known when
// the executor's snapshot arrives at the high marker, so changes and
// truncates are recorded for every pipeline and filtered then.
type window struct {
	gen       string
	job       int64
	changed   map[DocID]bool
	truncated map[string]bool // pipelines truncated while the window was open
	dirty     map[string]bool // pipelines with a deferred unchanged-TOAST update
}

type overlayEntry struct {
	fields  map[string]*string
	refs    map[string]string // view component rows: ref name -> referenced key
	deleted bool
	seq     uint64
}

// Planner converts committed transactions into ordered units. It is the only
// goroutine that interprets markers, so window bookkeeping is single-threaded
// and follows stream order exactly.
type Planner struct {
	Cfg       *config.Config
	Specs     Specs
	Registry  *Registry
	Lookup    StateLookup
	RefLookup RefLookup
	RefKeys   RefKeysLookup
	RootKeys  RootKeysLookup
	LoadGen   GenLoader
	Jobs      JobLoader
	ModelOf   func(g *store.Generation) transform.ModelIdentity
	Log       *slog.Logger
	Live      map[string]*GenInfo  // live generations at startup
	ActiveJob map[int64]*JobCursor // running/pending backfill jobs

	windows  map[string]*window
	seq      uint64
	txnDirty map[string]map[string]bool // documents dirtied by the current source transaction, per view

	mu      sync.Mutex
	overlay map[DocID]*overlayEntry
}

// Run consumes items until in is closed or a pause is emitted.
func (p *Planner) Run(ctx context.Context, in <-chan Item, out chan<- *Unit) error {
	defer close(out)
	p.windows = map[string]*window{}
	p.overlay = map[DocID]*overlayEntry{}
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
		if u.Pause == nil {
			if err := p.renderViews(ctx, u); err != nil {
				return err
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
	p.txnDirty = map[string]map[string]bool{}
	for i := range txn.Changes {
		ch := &txn.Changes[i]
		var err error
		switch ch.Kind {
		case KMarker:
			err = p.marker(ctx, u, txn, ch.Marker)
		case KTruncate:
			if spec := p.Specs[ch.Pipeline]; spec != nil && spec.IsComponent() {
				err = p.truncateComponent(ctx, u, txn, spec)
			} else {
				p.truncate(u, ch.Pipeline)
			}
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

// truncate drops every earlier effect of the pipeline in this unit; the
// committer applies the truncate before the unit's remaining effects.
func (p *Planner) truncate(u *Unit, pipe string) {
	u.Truncates[pipe] = true
	for id := range u.Stream {
		if id.Pipeline == pipe {
			delete(u.Stream, id)
		}
	}
	for id := range u.State {
		if id.Pipeline == pipe {
			delete(u.State, id)
		}
	}
	for _, m := range u.Snapshot {
		for id := range m {
			if id.Pipeline == pipe {
				delete(m, id)
			}
		}
	}
	rds := u.RangeDeletes[:0]
	for _, rd := range u.RangeDeletes {
		if rd.Pipeline != pipe {
			rds = append(rds, rd)
		}
	}
	u.RangeDeletes = rds
	for _, w := range p.windows {
		w.truncated[pipe] = true
	}
	p.mu.Lock()
	// Every pre-truncate state of the pipeline is gone; mark it deleted so
	// TOAST lookups cannot read a sink row that this unit is about to delete.
	for id, e := range p.overlay {
		if id.Pipeline == pipe {
			e.fields, e.deleted, e.seq = nil, true, u.Seq
		}
	}
	p.overlay[truncateSentinel(pipe)] = &overlayEntry{deleted: true, seq: u.Seq}
	p.mu.Unlock()
}

// truncateSentinel marks that a not-yet-committed truncate hides every sink
// state row of the pipeline (source keys can never contain NUL).
func truncateSentinel(pipe string) DocID { return DocID{Pipeline: pipe, Key: "\x00truncate"} }

func (p *Planner) markChanged(u *Unit, id DocID) {
	for _, w := range p.windows {
		w.changed[id] = true
	}
	for _, m := range u.Snapshot {
		delete(m, id)
	}
}

func (p *Planner) setOverlay(id DocID, fields map[string]*string, deleted bool, seq uint64) {
	p.mu.Lock()
	p.overlay[id] = &overlayEntry{fields: fields, deleted: deleted, seq: seq}
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

// state returns the latest known fields for a document: uncommitted overlay first, then the sink.
func (p *Planner) state(ctx context.Context, id DocID) (map[string]*string, bool, error) {
	p.mu.Lock()
	e, ok := p.overlay[id]
	_, truncPending := p.overlay[truncateSentinel(id.Pipeline)]
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
	m, err := p.Lookup(ctx, id.Pipeline, []string{id.Key})
	if err != nil {
		return nil, false, err
	}
	st, ok := m[id.Key]
	if !ok {
		return nil, false, nil
	}
	return st.Fields, true, nil
}

func (p *Planner) effect(id DocID, fields map[string]*string, lsn LSN, ct time.Time) *DocEffect {
	e := &DocEffect{ID: id, Fields: fields, LSN: lsn, CommitTime: ct}
	vals := make(map[string]string, len(fields))
	for k, v := range fields {
		if v != nil {
			vals[k] = *v
		}
	}
	spec := p.Specs[id.Pipeline]
	if spec == nil {
		e.Poison, e.PoisonErr = "render_error", "unknown pipeline "+id.Pipeline
		return e
	}
	text, err := spec.Tr.Render(vals)
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
	if spec := p.Specs[ch.Pipeline]; spec != nil && spec.IsComponent() {
		return p.componentRow(ctx, u, txn, ch, spec)
	}
	id := DocID{Pipeline: ch.Pipeline, Key: ch.Key}
	switch ch.Kind {
	case KDelete:
		p.markChanged(u, id)
		u.Stream[id] = &DocEffect{ID: id, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlay(id, nil, true, u.Seq)
		u.docs++
		return nil
	case KInsert, KUpdate:
	default:
		return fmt.Errorf("unexpected change kind %d", ch.Kind)
	}
	fields := ch.Fields
	if len(ch.Unchanged) > 0 {
		base := id
		if ch.OldKey != "" {
			base.Key = ch.OldKey
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
		old := DocID{Pipeline: ch.Pipeline, Key: ch.OldKey}
		p.markChanged(u, old)
		u.Stream[old] = &DocEffect{ID: old, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlay(old, nil, true, u.Seq)
		u.docs++
	}
	p.markChanged(u, id)
	u.Stream[id] = p.effect(id, fields, txn.EndLSN, txn.CommitTime)
	p.setOverlay(id, fields, false, u.Seq)
	u.docs++
	return nil
}

// deferUnchanged handles an UPDATE whose unchanged TOAST value is unknown
// because backfill has not reached the key yet. The future snapshot window
// will capture the full row. If a window is open the snapshot may predate this
// update, so the window is discarded (for this pipeline) and retried. If no
// backfill of the pipeline can cover the key, the pipeline pauses rather than
// guess.
func (p *Planner) deferUnchanged(u *Unit, ch *Change, lsn LSN) error {
	spec := p.Specs[ch.Pipeline]
	for _, cur := range p.ActiveJob {
		if cur == nil || cur.Pipeline != ch.Pipeline || spec == nil {
			continue
		}
		if cur.Last == nil || spec.Codec.Compare(ch.Key, *cur.Last) > 0 {
			for _, w := range p.windows {
				w.dirty[ch.Pipeline] = true
			}
			if ch.OldKey != "" {
				old := DocID{Pipeline: ch.Pipeline, Key: ch.OldKey}
				p.markChanged(u, old)
				if spec.IsComponent() {
					u.State[old] = &DocEffect{ID: old, Delete: true, LSN: lsn}
				} else {
					u.Stream[old] = &DocEffect{ID: old, Delete: true, LSN: lsn}
				}
				p.setOverlay(old, nil, true, u.Seq)
			}
			return nil
		}
	}
	u.Pause = &PauseError{Reason: "unsupported_input", Detail: fmt.Sprintf("pipeline %s: update of a row with an unchanged TOAST column, but no state is known for it and no backfill will cover it; run `embedsync backfill --pipeline %s` for the affected generation", ch.Pipeline, ch.Pipeline)}
	return nil
}

// trackJob registers an active job the planner has not seen yet (queued
// after startup), loading its pipeline from the sink.
func (p *Planner) trackJob(ctx context.Context, id int64) error {
	if id == 0 {
		return nil
	}
	if _, ok := p.ActiveJob[id]; ok {
		return nil
	}
	j, err := p.Jobs.BackfillJobByID(ctx, id)
	if err != nil {
		return err
	}
	if j == nil || (j.State != "pending" && j.State != "running") {
		return nil
	}
	p.ActiveJob[id] = &JobCursor{Pipeline: j.Pipeline, Last: j.LastKey, Kind: j.Kind}
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
		// From here the generation consumes the stream; every pipeline's job
		// for it will cover keys whose unchanged TOAST values are unknown.
		jobs, err := p.Jobs.ActiveBackfillJobs(ctx, m.Gen)
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if _, ok := p.ActiveJob[j.ID]; !ok {
				p.ActiveJob[j.ID] = &JobCursor{Pipeline: j.Pipeline, Last: j.LastKey, Kind: j.Kind}
			}
		}
	case "low":
		p.windows[m.WindowID] = &window{gen: m.Gen, job: m.JobID, changed: map[DocID]bool{}, truncated: map[string]bool{}, dirty: map[string]bool{}}
		if err := p.trackJob(ctx, m.JobID); err != nil {
			return err
		}
	case "high":
		w := p.windows[m.WindowID]
		delete(p.windows, m.WindowID)
		entry := p.Registry.Take(m.WindowID)
		if entry != nil && entry.Kind == store.JobState {
			if w == nil || w.dirty[entry.Pipeline] {
				metrics.BackfillWindows.WithLabelValues("discarded").Inc()
				entry.Finish("discarded")
				return nil
			}
			p.applyStateWindow(u, w, entry, txn.EndLSN)
			return nil
		}
		gi := p.Live[m.Gen]
		if entry != nil && entry.Kind == store.JobRender {
			if w == nil || gi == nil {
				entry.Finish("discarded")
				return nil
			}
			// Resolved at the end of the unit, against the state after it.
			u.renders = append(u.renders, renderReq{gi: gi, entry: entry})
			return nil
		}
		if w == nil || entry == nil || gi == nil {
			if entry != nil {
				entry.Finish("discarded")
			}
			return nil
		}
		if w.dirty[entry.Pipeline] {
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
	pipe := entry.Pipeline
	u.SnapGens[gen] = gi
	snap := u.Snapshot[gen]
	if snap == nil {
		snap = map[DocID]*DocEffect{}
		u.Snapshot[gen] = snap
	}
	keep := make([]string, 0, len(entry.Rows)+len(w.changed))
	for id := range w.changed {
		if id.Pipeline == pipe {
			keep = append(keep, id.Key)
		}
	}
	if !w.truncated[pipe] {
		for _, r := range entry.Rows {
			id := DocID{Pipeline: pipe, Key: r.Key}
			if w.changed[id] {
				continue
			}
			snap[id] = p.effect(id, r.Fields, lsn, time.Time{})
			p.setOverlay(id, r.Fields, false, u.Seq)
			keep = append(keep, r.Key)
			u.docs++
		}
	}
	var hi *string
	if !entry.Final && len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		hi = &k
	}
	u.RangeDeletes = append(u.RangeDeletes, RangeDelete{Gen: gen, Pipeline: pipe, Lo: entry.Lo, Hi: hi, Keep: keep})
	last := entry.Lo
	if len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		last = &k
	}
	u.Progress = append(u.Progress, WindowProgress{JobID: entry.JobID, Gen: gen, Pipeline: pipe, LastKey: last, Rows: len(entry.Rows), Final: entry.Final, Entry: entry})
	if entry.Final {
		delete(p.ActiveJob, entry.JobID)
	} else {
		p.ActiveJob[entry.JobID] = &JobCursor{Pipeline: pipe, Last: last, Kind: store.JobEmbed}
	}
	metrics.BackfillRows.WithLabelValues(gen).Add(float64(len(entry.Rows)))
	metrics.BackfillWindows.WithLabelValues("applied").Inc()
}
