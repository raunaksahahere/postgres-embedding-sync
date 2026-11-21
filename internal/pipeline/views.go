package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// View documents (phase 1B).
//
// A view's tables never get queried by the stream path. Every component row
// change updates the component's state (u.State, plus the uncommitted
// overlay), and marks the documents it affects as dirty: a root row its own
// document (old and new key), a child or link row its parent under the old
// and the new reference (the old one read from state before the overlay is
// updated, because pgoutput does not send unchanged columns of a default
// replica identity), a target row every document linked to it. At the end of
// the unit each dirty document is assembled once from the state after the
// unit (overlay over the sink) and rendered, so any number of edits in a
// transaction or unit yield one final representation, committed with the
// unit's checkpoint like every other effect.

// RefLookup reads the reference index of a component (source_refs).
type RefLookup func(ctx context.Context, pipeline, ref string, parents []string) (map[string][]string, error)

// RefKeysLookup lists the distinct keys a component's reference points at.
type RefKeysLookup func(ctx context.Context, pipeline, ref string) ([]string, error)

// RootKeysLookup lists up to limit keys of a component after lo, in key order.
type RootKeysLookup func(ctx context.Context, pipeline string, codec keys.Codec, lo *string, limit int) ([]string, error)

// viewReady reports whether every state backfill of the view is complete.
// Until then nothing is rendered (a render job follows the state jobs).
func (p *Planner) viewReady(view string) bool {
	prefix := view + ":"
	for _, c := range p.ActiveJob {
		if c != nil && c.Kind == store.JobState && strings.HasPrefix(c.Pipeline, prefix) {
			return false
		}
	}
	return true
}

func (p *Planner) markDirty(u *Unit, txn *Txn, view, key string) {
	if key == "" {
		return
	}
	m := u.dirty[view]
	if m == nil {
		m = map[string]dirtyMark{}
		u.dirty[view] = m
	}
	m[key] = dirtyMark{lsn: txn.EndLSN, ct: txn.CommitTime}
	if !p.viewReady(view) || u.Pause != nil {
		return
	}
	t := p.txnDirty[view]
	if t == nil {
		t = map[string]bool{}
		p.txnDirty[view] = t
	}
	t[key] = true
	if v := p.Specs[view]; v != nil && v.ViewDef != nil && len(t) > v.ViewDef.MaxFanoutPerTxn {
		u.Pause = &PauseError{Reason: "fanout_limit", Detail: fmt.Sprintf(
			"view %s: source transaction ending at %s changes more than max_fanout_per_txn=%d documents; raise the limit and restart (the transaction is applied atomically or not at all)",
			view, txn.EndLSN, v.ViewDef.MaxFanoutPerTxn)}
	}
}

// componentRow applies a change of a view component's table.
func (p *Planner) componentRow(ctx context.Context, u *Unit, txn *Txn, ch *Change, spec *Spec) error {
	comp := spec.Comp
	id := DocID{Pipeline: ch.Pipeline, Key: ch.Key}
	oldID := id
	if ch.OldKey != "" {
		oldID.Key = ch.OldKey
	}
	oldFields, oldOK, err := p.state(ctx, oldID)
	if err != nil {
		return err
	}
	var oldRefs map[string]string
	if oldOK {
		oldRefs = viewdoc.Refs(comp, spec.Codec, oldID.Key, oldFields)
	}
	if ch.Kind == KDelete {
		p.markChanged(u, id)
		u.State[id] = &DocEffect{ID: id, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlayRefs(id, nil, nil, true, u.Seq)
		u.docs++
		return p.dirtyFor(ctx, u, txn, spec, oldID.Key, "", oldRefs, nil)
	}
	fields := ch.Fields
	if len(ch.Unchanged) > 0 {
		if !oldOK {
			return p.deferUnchanged(u, ch, txn.EndLSN)
		}
		merged := make(map[string]*string, len(oldFields)+len(fields))
		for k, v := range oldFields {
			merged[k] = v
		}
		for k, v := range fields {
			merged[k] = v
		}
		fields = merged
	}
	if ch.OldKey != "" {
		p.markChanged(u, oldID)
		u.State[oldID] = &DocEffect{ID: oldID, Delete: true, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
		p.setOverlayRefs(oldID, nil, nil, true, u.Seq)
		u.docs++
	}
	refs := viewdoc.Refs(comp, spec.Codec, id.Key, fields)
	p.markChanged(u, id)
	u.State[id] = &DocEffect{ID: id, Fields: fields, Refs: refs, LSN: txn.EndLSN, CommitTime: txn.CommitTime}
	p.setOverlayRefs(id, fields, refs, false, u.Seq)
	u.docs++
	return p.dirtyFor(ctx, u, txn, spec, oldID.Key, id.Key, oldRefs, refs)
}

// dirtyFor marks the documents a component row change affects.
func (p *Planner) dirtyFor(ctx context.Context, u *Unit, txn *Txn, spec *Spec, oldKey, newKey string, oldRefs, newRefs map[string]string) error {
	comp := spec.Comp
	view := comp.View.Name
	switch comp.Kind {
	case config.CompRoot:
		p.markDirty(u, txn, view, oldKey)
		p.markDirty(u, txn, view, newKey)
	case config.CompChild, config.CompLink:
		p.markDirty(u, txn, view, oldRefs["parent"])
		p.markDirty(u, txn, view, newRefs["parent"])
	case config.CompTarget:
		parents, err := p.linkedParents(ctx, comp, []string{oldKey, newKey})
		if err != nil {
			return err
		}
		for _, k := range parents {
			p.markDirty(u, txn, view, k)
		}
	}
	return nil
}

// linkedParents returns the documents whose link rows point at the given
// target keys (as of the overlay over the sink).
func (p *Planner) linkedParents(ctx context.Context, target *config.Component, targetKeys []string) ([]string, error) {
	linkName := config.ComponentName(target.View.Name, target.Link.Name)
	link := p.Specs[linkName]
	if link == nil {
		return nil, fmt.Errorf("view %s: link component %s missing", target.View.Name, linkName)
	}
	var tk []string
	for _, k := range targetKeys {
		if k != "" {
			tk = append(tk, k)
		}
	}
	src := plannerSource{p}
	byTarget, err := src.Children(ctx, linkName, "target", tk)
	if err != nil {
		return nil, err
	}
	var linkKeys []string
	for _, ks := range byTarget {
		linkKeys = append(linkKeys, ks...)
	}
	st, err := src.States(ctx, linkName, linkKeys)
	if err != nil {
		return nil, err
	}
	var out []string
	for k, f := range st {
		if parent, ok := viewdoc.Refs(link.Comp, link.Codec, k, f)["parent"]; ok {
			out = append(out, parent)
		}
	}
	sort.Strings(out)
	return out, nil
}

// truncateComponent handles TRUNCATE of a view's table: the documents that
// referenced its rows become dirty (computed before the truncate hides the
// rows), then the component's state is cleared. Truncating the root table
// deletes every document of the view.
func (p *Planner) truncateComponent(ctx context.Context, u *Unit, txn *Txn, spec *Spec) error {
	comp := spec.Comp
	view := comp.View.Name
	switch comp.Kind {
	case config.CompRoot:
		p.truncate(u, spec.Name)
		p.truncate(u, view)
		return nil
	case config.CompChild, config.CompLink:
		parents, err := p.allRefKeys(ctx, spec.Name, "parent")
		if err != nil {
			return err
		}
		for _, k := range parents {
			p.markDirty(u, txn, view, k)
		}
	case config.CompTarget:
		// Every document with a link row pointing at any target.
		linkName := config.ComponentName(view, comp.Link.Name)
		parents, err := p.allRefKeys(ctx, linkName, "parent")
		if err != nil {
			return err
		}
		for _, k := range parents {
			p.markDirty(u, txn, view, k)
		}
	}
	p.truncate(u, spec.Name)
	return nil
}

// allRefKeys lists every key a component's reference points at: the sink's
// reference index plus uncommitted rows.
func (p *Planner) allRefKeys(ctx context.Context, pipeline, ref string) ([]string, error) {
	ov, trunc := p.overlayOf(pipeline)
	seen := map[string]bool{}
	if !trunc {
		ks, err := p.RefKeys(ctx, pipeline, ref)
		if err != nil {
			return nil, err
		}
		for _, k := range ks {
			seen[k] = true
		}
	}
	for _, e := range ov {
		if !e.deleted && e.refs[ref] != "" {
			seen[e.refs[ref]] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// overlayOf snapshots the uncommitted rows of one pipeline and whether an
// uncommitted truncate hides its sink rows. Callers read the overlay before
// the sink: an entry released after its unit committed is then visible in
// the sink.
func (p *Planner) overlayOf(pipeline string) (map[string]overlayEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]overlayEntry{}
	for id, e := range p.overlay {
		if id.Pipeline == pipeline && id != truncateSentinel(pipeline) {
			out[id.Key] = *e
		}
	}
	_, trunc := p.overlay[truncateSentinel(pipeline)]
	return out, trunc
}

func (p *Planner) setOverlayRefs(id DocID, fields map[string]*string, refs map[string]string, deleted bool, seq uint64) {
	p.mu.Lock()
	p.overlay[id] = &overlayEntry{fields: fields, refs: refs, deleted: deleted, seq: seq}
	p.mu.Unlock()
}

// plannerSource is view state as of the end of everything planned so far:
// uncommitted units (overlay) over the committed sink.
type plannerSource struct{ p *Planner }

func (s plannerSource) States(ctx context.Context, pipeline string, ks []string) (map[string]map[string]*string, error) {
	out := map[string]map[string]*string{}
	if len(ks) == 0 {
		return out, nil
	}
	ov, trunc := s.p.overlayOf(pipeline)
	var miss []string
	for _, k := range ks {
		if e, ok := ov[k]; ok {
			if !e.deleted {
				out[k] = e.fields
			}
			continue
		}
		if !trunc {
			miss = append(miss, k)
		}
	}
	if len(miss) > 0 {
		st, err := s.p.Lookup(ctx, pipeline, miss)
		if err != nil {
			return nil, err
		}
		for k, v := range st {
			out[k] = v.Fields
		}
	}
	return out, nil
}

func (s plannerSource) Children(ctx context.Context, pipeline, ref string, parents []string) (map[string][]string, error) {
	out := map[string][]string{}
	if len(parents) == 0 {
		return out, nil
	}
	ov, trunc := s.p.overlayOf(pipeline)
	want := map[string]bool{}
	for _, k := range parents {
		want[k] = true
	}
	if !trunc {
		sink, err := s.p.RefLookup(ctx, pipeline, ref, parents)
		if err != nil {
			return nil, err
		}
		for parent, kids := range sink {
			for _, k := range kids {
				if _, shadowed := ov[k]; !shadowed {
					out[parent] = append(out[parent], k)
				}
			}
		}
	}
	for k, e := range ov {
		if !e.deleted && want[e.refs[ref]] {
			out[e.refs[ref]] = append(out[e.refs[ref]], k)
		}
	}
	for parent := range out {
		sort.Strings(out[parent])
	}
	return out, nil
}

// renderDoc turns an assembled document (nil: root row absent) into an effect.
func (p *Planner) renderDoc(v *Spec, key string, doc *viewdoc.Doc, lsn LSN, ct time.Time) *DocEffect {
	id := DocID{Pipeline: v.Name, Key: key}
	if doc == nil {
		return &DocEffect{ID: id, Delete: true, LSN: lsn, CommitTime: ct}
	}
	e := &DocEffect{ID: id, LSN: lsn, CommitTime: ct}
	if name, n := doc.MaxList(); n > v.ViewDef.MaxChildren {
		e.Poison, e.PoisonErr = "too_many_children", fmt.Sprintf("%s has %d rows; max_children=%d", name, n, v.ViewDef.MaxChildren)
		return e
	}
	text, err := v.Tr.RenderData(doc.Data())
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

// renderViews runs at the end of a unit: every dirty document of a ready
// view is rendered once from the state after the unit, then render backfill
// windows are resolved against the same state.
func (p *Planner) renderViews(ctx context.Context, u *Unit) error {
	src := plannerSource{p}
	views := make([]string, 0, len(u.dirty))
	for v := range u.dirty {
		views = append(views, v)
	}
	sort.Strings(views)
	for _, view := range views {
		spec := p.Specs[view]
		if spec == nil || spec.View == nil || !p.viewReady(view) {
			continue
		}
		marks := u.dirty[view]
		ks := make([]string, 0, len(marks))
		for k := range marks {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		docs, err := viewdoc.Assemble(ctx, src, spec.View, ks)
		if err != nil {
			return fmt.Errorf("view %s: assemble: %w", view, err)
		}
		for _, k := range ks {
			id := DocID{Pipeline: view, Key: k}
			u.Stream[id] = p.renderDoc(spec, k, docs[k], marks[k].lsn, marks[k].ct)
			u.docs++
		}
	}
	for _, r := range u.renders {
		if err := p.applyRenderWindow(ctx, u, src, r); err != nil {
			return err
		}
	}
	return nil
}

// applyRenderWindow renders the next batch of a view's documents into one
// generation: the root keys after the job's cursor, from the state after
// the unit, plus a range delete of every other document in that range.
func (p *Planner) applyRenderWindow(ctx context.Context, u *Unit, src plannerSource, r renderReq) error {
	entry, gi := r.entry, r.gi
	spec := p.Specs[entry.Pipeline]
	if spec == nil || spec.View == nil || !p.viewReady(entry.Pipeline) {
		metrics.BackfillWindows.WithLabelValues("discarded").Inc()
		entry.Finish("discarded")
		return nil
	}
	root := spec.View.Root
	limit := max(entry.Limit, 1)
	keysAfter, final, err := p.rootKeysAfter(ctx, root.Comp.Def.Name, root.Codec, entry.Lo, limit)
	if err != nil {
		return err
	}
	docs, err := viewdoc.Assemble(ctx, src, spec.View, keysAfter)
	if err != nil {
		return fmt.Errorf("view %s: assemble: %w", spec.Name, err)
	}
	gen := gi.Gen.ID
	u.SnapGens[gen] = gi
	snap := u.Snapshot[gen]
	if snap == nil {
		snap = map[DocID]*DocEffect{}
		u.Snapshot[gen] = snap
	}
	for _, k := range keysAfter {
		snap[DocID{Pipeline: spec.Name, Key: k}] = p.renderDoc(spec, k, docs[k], u.EndLSN, time.Time{})
		u.docs++
	}
	var hi *string
	last := entry.Lo
	if len(keysAfter) > 0 {
		k := keysAfter[len(keysAfter)-1]
		last = &k
		if !final {
			hi = &k
		}
	}
	u.RangeDeletes = append(u.RangeDeletes, RangeDelete{Gen: gen, Pipeline: spec.Name, Lo: entry.Lo, Hi: hi, Keep: keysAfter})
	u.Progress = append(u.Progress, WindowProgress{JobID: entry.JobID, Gen: gen, Pipeline: spec.Name, LastKey: last, Rows: len(keysAfter), Final: final, Entry: entry})
	if final {
		delete(p.ActiveJob, entry.JobID)
	} else {
		p.ActiveJob[entry.JobID] = &JobCursor{Pipeline: spec.Name, Last: last, Kind: store.JobRender}
	}
	metrics.BackfillRows.WithLabelValues(gen).Add(float64(len(keysAfter)))
	metrics.BackfillWindows.WithLabelValues("applied").Inc()
	return nil
}

// rootKeysAfter lists up to limit root keys after lo (overlay over the
// sink) in key order; final reports that no key follows.
func (p *Planner) rootKeysAfter(ctx context.Context, pipeline string, codec keys.Codec, lo *string, limit int) ([]string, bool, error) {
	ov, trunc := p.overlayOf(pipeline)
	set := map[string]bool{}
	exhausted := true
	if !trunc {
		want := limit + len(ov)
		ks, err := p.RootKeys(ctx, pipeline, codec, lo, want)
		if err != nil {
			return nil, false, err
		}
		exhausted = len(ks) < want
		for _, k := range ks {
			set[k] = true
		}
	}
	for k, e := range ov {
		if lo != nil && codec.Compare(k, *lo) <= 0 {
			continue
		}
		if e.deleted {
			delete(set, k)
		} else {
			set[k] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return codec.Compare(out[i], out[j]) < 0 })
	// The sink returned limit+len(overlay) keys; overlay deletes remove at
	// most len(overlay) of them, so when the sink was not exhausted at least
	// limit returned sink keys survive and the first limit merged keys are
	// exactly the first limit keys after lo.
	if len(out) > limit {
		return out[:limit], false, nil
	}
	return out, exhausted, nil
}

// applyStateWindow applies a state backfill window of a view component:
// snapshot rows go to state only (no embedding), with a range delete of the
// component's state rows in the window that were neither scanned nor
// changed during it.
func (p *Planner) applyStateWindow(u *Unit, w *window, entry *WindowRows, lsn LSN) {
	pipe := entry.Pipeline
	spec := p.Specs[pipe]
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
			var refs map[string]string
			if spec != nil && spec.Comp != nil {
				refs = viewdoc.Refs(spec.Comp, spec.Codec, r.Key, r.Fields)
			}
			u.State[id] = &DocEffect{ID: id, Fields: r.Fields, Refs: refs, LSN: lsn}
			p.setOverlayRefs(id, r.Fields, refs, false, u.Seq)
			keep = append(keep, r.Key)
			u.docs++
		}
	}
	var hi *string
	if !entry.Final && len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		hi = &k
	}
	u.RangeDeletes = append(u.RangeDeletes, RangeDelete{Pipeline: pipe, Lo: entry.Lo, Hi: hi, Keep: keep})
	last := entry.Lo
	if len(entry.Rows) > 0 {
		k := entry.Rows[len(entry.Rows)-1].Key
		last = &k
	}
	u.Progress = append(u.Progress, WindowProgress{JobID: entry.JobID, Pipeline: pipe, LastKey: last, Rows: len(entry.Rows), Final: entry.Final, Entry: entry})
	if entry.Final {
		delete(p.ActiveJob, entry.JobID)
	} else {
		p.ActiveJob[entry.JobID] = &JobCursor{Pipeline: pipe, Last: last, Kind: store.JobState}
	}
	metrics.BackfillWindows.WithLabelValues("applied").Inc()
}
