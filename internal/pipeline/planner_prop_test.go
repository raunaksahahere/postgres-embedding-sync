package pipeline

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// The property: for any interleaving of source writes to several tables
// (inserts, updates, updates with unchanged TOAST columns, key changes,
// deletes, truncates, transactions that touch several tables) with watermark
// backfill windows of one pipeline after another, and any grouping of
// transactions into units, applying the planner's units yields a sink equal
// to the source, for every pipeline.
//
// Pipelines: "a" and "c" both have single-column integer keys drawn from the
// same range (the same id exists in both tables), "b" has a composite
// (int, text) key whose text column orders bytewise.

type simSink struct {
	state map[DocID]map[string]*string
	docs  map[DocID]string // document -> rendered text in generation "g"
}

func inRange(c keys.Codec, k string, lo, hi *string) bool {
	if lo != nil && c.Compare(k, *lo) <= 0 {
		return false
	}
	if hi != nil && c.Compare(k, *hi) > 0 {
		return false
	}
	return true
}

// apply mirrors Committer.applyState/applyGen ordering: truncates, range
// deletes, stream effects, snapshot effects (overriding).
func (s *simSink) apply(t *testing.T, specs Specs, u *Unit) {
	if u.Pause != nil {
		t.Fatalf("unexpected pause: %v", u.Pause)
	}
	for pipe := range u.Truncates {
		for id := range s.state {
			if id.Pipeline == pipe {
				delete(s.state, id)
			}
		}
		for id := range s.docs {
			if id.Pipeline == pipe {
				delete(s.docs, id)
			}
		}
	}
	for _, rd := range u.RangeDeletes {
		keep := map[string]bool{}
		for _, k := range rd.Keep {
			keep[k] = true
		}
		codec := specs[rd.Pipeline].Codec
		for id := range s.state {
			if id.Pipeline == rd.Pipeline && inRange(codec, id.Key, rd.Lo, rd.Hi) && !keep[id.Key] {
				delete(s.state, id)
			}
		}
		for id := range s.docs {
			if id.Pipeline == rd.Pipeline && inRange(codec, id.Key, rd.Lo, rd.Hi) && !keep[id.Key] {
				delete(s.docs, id)
			}
		}
	}
	put := func(d *DocEffect, gen bool) {
		if d.Delete {
			delete(s.state, d.ID)
			if gen {
				delete(s.docs, d.ID)
			}
			return
		}
		s.state[d.ID] = d.Fields
		if gen {
			s.docs[d.ID] = d.Text
		}
	}
	_, streamGen := u.Gens["g"]
	for id, d := range u.Stream {
		if d.ID != id {
			t.Fatalf("stream effect stored under %v has id %v", id, d.ID)
		}
		if _, overridden := u.Snapshot["g"][id]; overridden {
			put(d, false)
			continue
		}
		put(d, streamGen)
	}
	for _, d := range u.Snapshot["g"] {
		// Snapshot rows carry the high marker's LSN: never 0/0, never beyond
		// the unit (regression: they used to take the previous unit's LSN).
		if d.LSN == 0 || d.LSN > u.EndLSN {
			panic(fmt.Sprintf("snapshot effect for %v has LSN %d (unit end %d)", d.ID, d.LSN, u.EndLSN))
		}
		put(d, true)
	}
}

func str(s string) *string { return &s }

var discarded int

// fakeJobs serves the backfill jobs the planner looks up at the start marker.
type fakeJobs struct{ jobs []store.BackfillJob }

func (f *fakeJobs) BackfillJobByID(_ context.Context, id int64) (*store.BackfillJob, error) {
	for i := range f.jobs {
		if f.jobs[i].ID == id {
			j := f.jobs[i]
			return &j, nil
		}
	}
	return nil, nil
}

func (f *fakeJobs) ActiveBackfillJobs(_ context.Context, gen string) ([]store.BackfillJob, error) {
	return f.jobs, nil
}

var textIDs = []string{"a", "B", "b", "Z", "aa", "é", "10", "9", "x,y", `q"`}

func runInterleaving(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed*7919))
	cfg := &config.Config{}
	cfg.Pipeline.MaxBatchTxns, cfg.Pipeline.MaxBatchDocs, cfg.Pipeline.MaxDocumentBytes = 1000, 1000, 1<<20
	pipes := []string{"a", "b", "c"}
	specs := Specs{}
	for _, name := range pipes {
		tmpl := "{{.title}}|{{.body}}"
		codec := keys.NewCodec(keys.Int)
		if name == "b" {
			tmpl, codec = "{{.title}}#{{.body}}", keys.NewCodec(keys.Int, keys.Text)
		}
		tr, err := transform.New("public."+name, tmpl, []string{"title", "body"}, 50, 5, "fp-"+name)
		if err != nil {
			t.Fatal(err)
		}
		specs[name] = &Spec{Name: name, Codec: codec, Tr: tr}
	}
	sink := &simSink{state: map[DocID]map[string]*string{}, docs: map[DocID]string{}}
	reg := NewRegistry()
	jobs := &fakeJobs{}
	for i, name := range pipes {
		jobs.jobs = append(jobs.jobs, store.BackfillJob{ID: int64(i + 1), GenerationID: "g", Pipeline: name, State: "pending"})
	}
	p := &Planner{Cfg: cfg, Specs: specs, Registry: reg, Jobs: jobs,
		Lookup: func(_ context.Context, pipe string, ks []string) (map[string]store.SourceState, error) {
			out := map[string]store.SourceState{}
			for _, k := range ks {
				if f, ok := sink.state[DocID{pipe, k}]; ok {
					out[k] = store.SourceState{Key: k, Fields: f}
				}
			}
			return out, nil
		},
		LoadGen: func(_ context.Context, id string) (*store.Generation, error) {
			return &store.Generation{ID: id, State: store.StatePending}, nil
		},
		ModelOf: func(*store.Generation) transform.ModelIdentity {
			return transform.ModelIdentity{Name: "m", Revision: "r"}
		},
		Live:      map[string]*GenInfo{},
		ActiveJob: map[int64]*JobCursor{},
		windows:   map[string]*window{},
		overlay:   map[DocID]*overlayEntry{},
	}
	source := map[DocID]map[string]*string{}
	nextKey := 1
	// newKey draws a fresh key. For a key change (rekey) the key sorts after
	// every existing key: a row whose unchanged TOAST value is unknown can
	// only be re-keyed forward of the backfill cursor (moving it behind the
	// cursor pauses the real pipeline, by design).
	newKey := func(pipe string, rekey bool) string {
		n := nextKey
		nextKey++
		if rekey {
			if pipe != "b" {
				return strconv.Itoa(n)
			}
			k, _ := specs["b"].Codec.Encode([]string{strconv.Itoa(1000 + n), textIDs[rng.IntN(len(textIDs))]})
			return k
		}
		if pipe != "b" {
			// a and c draw from one counter but also reuse each other's ids.
			if rng.IntN(3) == 0 {
				other := "a"
				if pipe == "a" {
					other = "c"
				}
				var cands []string
				for id := range source {
					if id.Pipeline == other {
						if _, taken := source[DocID{pipe, id.Key}]; !taken {
							cands = append(cands, id.Key)
						}
					}
				}
				if len(cands) > 0 {
					sort.Strings(cands)
					return cands[rng.IntN(len(cands))]
				}
			}
			return strconv.Itoa(n)
		}
		k, _ := specs["b"].Codec.Encode([]string{strconv.Itoa([]int{-3, 1, 2, 10}[rng.IntN(4)]), textIDs[rng.IntN(len(textIDs))] + strconv.Itoa(n)})
		return k
	}
	// Leftover chunks in the generation from an earlier build: documents that
	// no longer exist and documents with outdated content. Only range deletes
	// and snapshots can repair them.
	for i := 0; i < rng.IntN(8); i++ {
		pipe := pipes[rng.IntN(len(pipes))]
		k := strconv.Itoa(1 + rng.IntN(60))
		if pipe == "b" {
			k, _ = specs["b"].Codec.Encode([]string{strconv.Itoa(rng.IntN(3)), textIDs[rng.IntN(len(textIDs))]})
		}
		sink.docs[DocID{pipe, k}] = "stale leftover"
	}
	// Rows that exist before the slot: only backfill can deliver them.
	for i := 0; i < 10+rng.IntN(40); i++ {
		pipe := pipes[rng.IntN(len(pipes))]
		k := newKey(pipe, false)
		source[DocID{pipe, k}] = map[string]*string{"title": str("t" + k), "body": str("pre " + k)}
	}
	var lsn LSN = 100
	var pending []Item
	emit := func(changes ...Change) {
		lsn += 10
		pending = append(pending, Item{Txn: &Txn{EndLSN: lsn, Changes: changes}})
	}
	var seq uint64
	committedLast := map[string]*string{}
	jobIdx := 0 // index into pipes of the job being backfilled; len(pipes) = all done
	flush := func(all bool) {
		for len(pending) > 0 {
			n := 1 + rng.IntN(4)
			if n > len(pending) {
				n = len(pending)
			}
			seq++
			u := newUnit(seq)
			for id, g := range p.Live {
				u.Gens[id] = g
			}
			for _, it := range pending[:n] {
				if err := p.add(context.Background(), u, it); err != nil {
					t.Fatal(err)
				}
			}
			pending = pending[n:]
			sink.apply(t, specs, u)
			p.Release(u.Seq)
			for _, pr := range u.Progress {
				committedLast[pr.Pipeline] = pr.LastKey
				if pr.Final {
					jobIdx++
				}
				pr.Entry.Finish("applied")
			}
			if !all && rng.IntN(2) == 0 {
				return
			}
		}
	}
	emit(Change{Kind: KMarker, Marker: Marker{Kind: "start", Gen: "g", JobID: 1}})
	const limit = 4
	phase, windowN := 0, 0
	var windowID string
	var snap []SnapRow
	var entry *WindowRows
	sourceKeys := func(pipe string) []string {
		var ks []string
		for id := range source {
			if id.Pipeline == pipe {
				ks = append(ks, id.Key)
			}
		}
		c := specs[pipe].Codec
		sort.Slice(ks, func(i, j int) bool { return c.Compare(ks[i], ks[j]) < 0 })
		return ks
	}
	randomExisting := func() (DocID, bool) {
		ids := make([]DocID, 0, len(source))
		for id := range source {
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			return DocID{}, false
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i].Less(ids[j]) })
		return ids[rng.IntN(len(ids))], true
	}
	// change builds one random source change and applies it to the model.
	change := func(step int) (Change, bool) {
		switch r := rng.IntN(100); {
		case r < 30:
			pipe := pipes[rng.IntN(len(pipes))]
			k := newKey(pipe, false)
			if _, taken := source[DocID{pipe, k}]; taken {
				return Change{}, false
			}
			f := map[string]*string{"title": str("new " + k), "body": str(fmt.Sprintf("inserted %d %d", step, rng.IntN(1000)))}
			if rng.IntN(10) == 0 {
				f["title"] = nil
			}
			source[DocID{pipe, k}] = f
			return Change{Kind: KInsert, Pipeline: pipe, Key: k, Fields: f}, true
		case r < 58:
			id, ok := randomExisting()
			if !ok {
				return Change{}, false
			}
			if phase != 0 && jobIdx < len(pipes) && rng.IntN(2) == 0 {
				// prefer a key of the pipeline being backfilled at or beyond its cursor
				pipe := pipes[jobIdx]
				c := specs[pipe].Codec
				for _, k := range sourceKeys(pipe) {
					if last := committedLast[pipe]; last == nil || c.Compare(k, *last) > 0 {
						id = DocID{pipe, k}
						break
					}
				}
			}
			old := source[id]
			nf := map[string]*string{"title": str(fmt.Sprintf("upd %d", step)), "body": old["body"]}
			ch := Change{Kind: KUpdate, Pipeline: id.Pipeline, Key: id.Key, Fields: map[string]*string{"title": nf["title"]}}
			if rng.IntN(2) == 0 {
				ch.Unchanged = []string{"body"} // unchanged TOAST value not sent
			} else {
				ch.Fields["body"] = nf["body"]
			}
			if rng.IntN(5) == 0 && len(ch.Unchanged) == 0 {
				nf["body"] = str(fmt.Sprintf("rewritten %d", step))
				ch.Fields["body"] = nf["body"]
			}
			source[id] = nf
			return ch, true
		case r < 70:
			id, ok := randomExisting()
			if !ok {
				return Change{}, false
			}
			nk := newKey(id.Pipeline, true)
			if _, taken := source[DocID{id.Pipeline, nk}]; taken || nk == id.Key {
				return Change{}, false
			}
			f := source[id]
			delete(source, id)
			source[DocID{id.Pipeline, nk}] = f
			ch := Change{Kind: KUpdate, Pipeline: id.Pipeline, Key: nk, OldKey: id.Key, Fields: map[string]*string{"title": f["title"]}}
			if rng.IntN(2) == 0 {
				ch.Unchanged = []string{"body"}
			} else {
				ch.Fields["body"] = f["body"]
			}
			return ch, true
		case r < 94:
			id, ok := randomExisting()
			if !ok {
				return Change{}, false
			}
			delete(source, id)
			return Change{Kind: KDelete, Pipeline: id.Pipeline, Key: id.Key}, true
		default:
			pipe := pipes[rng.IntN(len(pipes))]
			for id := range source {
				if id.Pipeline == pipe {
					delete(source, id)
				}
			}
			return Change{Kind: KTruncate, Pipeline: pipe}, true
		}
	}
	extraOps := 0
	extraLimit := rng.IntN(20) // stop at a random point after backfill so later edits do not mask errors
	mixed := 0
	for step := 0; step < 30000; step++ {
		jobsDone := jobIdx >= len(pipes)
		if jobsDone && (phase == 0 || phase == 3) {
			if extraOps > extraLimit {
				break
			}
			extraOps++
		}
		switch r := rng.IntN(100); {
		case r < 35 && (!jobsDone || (phase != 0 && phase != 3)):
			pipe := pipes[min(jobIdx, len(pipes)-1)]
			job := int64(min(jobIdx, len(pipes)-1) + 1)
			switch phase {
			case 0:
				windowN++
				windowID = fmt.Sprintf("w%d", windowN)
				emit(Change{Kind: KMarker, Marker: Marker{Kind: "low", Gen: "g", JobID: job, WindowID: windowID}})
				phase = 1
			case 1:
				snap = nil
				c := specs[pipe].Codec
				for _, k := range sourceKeys(pipe) {
					if last := committedLast[pipe]; last != nil && c.Compare(k, *last) <= 0 {
						continue
					}
					f := map[string]*string{}
					for n, v := range source[DocID{pipe, k}] {
						f[n] = v
					}
					snap = append(snap, SnapRow{Key: k, Fields: f})
					if len(snap) == limit {
						break
					}
				}
				entry = &WindowRows{WindowID: windowID, Gen: "g", Pipeline: pipe, JobID: job, Lo: committedLast[pipe], Rows: snap, Final: len(snap) < limit}
				reg.Put(entry)
				phase = 2
			case 2:
				emit(Change{Kind: KMarker, Marker: Marker{Kind: "high", Gen: "g", JobID: job, WindowID: windowID}})
				phase = 3
			case 3:
				// The executor waits for the window outcome; the stream keeps
				// flowing, so later changes can share a unit with the high marker.
				select {
				case <-entry.done:
					if entry.outcome == "discarded" {
						discarded++
					}
					phase = 0
				default:
					flush(false)
				}
			}
		case r < 92:
			// One source transaction with one to three changes, often
			// touching several tables.
			var chs []Change
			for i := 0; i < 1+rng.IntN(3); i++ {
				if ch, ok := change(step); ok {
					chs = append(chs, ch)
				}
			}
			if len(chs) == 0 {
				continue
			}
			seen := map[string]bool{}
			for _, ch := range chs {
				seen[ch.Pipeline] = true
			}
			if len(seen) > 1 {
				mixed++
			}
			emit(chs...)
		default:
			flush(false)
		}
	}
	if phase == 1 || phase == 2 {
		if phase == 1 {
			pipe := pipes[min(jobIdx, len(pipes)-1)]
			reg.Put(&WindowRows{WindowID: windowID, Gen: "g", Pipeline: pipe, JobID: int64(min(jobIdx, len(pipes)-1) + 1), Lo: committedLast[pipe], Final: false})
		}
		emit(Change{Kind: KMarker, Marker: Marker{Kind: "high", Gen: "g", WindowID: windowID}})
	}
	flush(true)
	if jobIdx < len(pipes) {
		t.Fatalf("seed %d: backfill did not finish (job %d of %d, windows=%d)", seed, jobIdx+1, len(pipes), windowN)
	}
	if mixed == 0 {
		t.Fatalf("seed %d: no transaction touched two tables", seed)
	}
	for id, f := range source {
		vals := map[string]string{}
		for n, v := range f {
			if v != nil {
				vals[n] = *v
			}
		}
		want, _ := specs[id.Pipeline].Tr.Render(vals)
		got, ok := sink.docs[id]
		if !ok {
			t.Fatalf("seed %d: missing document %v", seed, id)
		}
		if got != want {
			t.Fatalf("seed %d: stale document %v: got %q want %q", seed, id, got, want)
		}
		if st, ok := sink.state[id]; !ok || len(st) != len(f) {
			t.Fatalf("seed %d: source_state mismatch for %v", seed, id)
		}
	}
	for id := range sink.docs {
		if _, ok := source[id]; !ok {
			t.Fatalf("seed %d: extra document %v in sink", seed, id)
		}
	}
	if len(sink.state) != len(source) {
		t.Fatalf("seed %d: source_state has %d rows, source %d", seed, len(sink.state), len(source))
	}
}

func TestPropertyBackfillInterleavings(t *testing.T) {
	n := 1000
	if testing.Short() {
		n = 100
	}
	discarded = 0
	for seed := uint64(1); seed <= uint64(n); seed++ {
		runInterleaving(t, seed)
	}
	t.Logf("%d interleavings over 3 pipelines, %d windows discarded after unchanged-TOAST deferral", n, discarded)
	if discarded == 0 {
		t.Fatal("the deferral path was never exercised")
	}
}

// A truncate of one table must not touch another pipeline's documents, open
// windows or same-key rows, and must not split a transaction's other effects.
func TestTruncateIsScopedToItsPipeline(t *testing.T) {
	cfg := &config.Config{}
	cfg.Pipeline.MaxBatchTxns, cfg.Pipeline.MaxBatchDocs, cfg.Pipeline.MaxDocumentBytes = 100, 100, 1<<20
	specs := Specs{}
	for _, n := range []string{"a", "c"} {
		tr, _ := transform.New("public."+n, "{{.body}}", []string{"body"}, 50, 5, "fp")
		specs[n] = &Spec{Name: n, Codec: keys.NewCodec(keys.Int), Tr: tr}
	}
	p := &Planner{Cfg: cfg, Specs: specs, Registry: NewRegistry(), Live: map[string]*GenInfo{}, ActiveJob: map[int64]*JobCursor{},
		windows: map[string]*window{}, overlay: map[DocID]*overlayEntry{}}
	u := newUnit(1)
	txn := &Txn{EndLSN: 10, Changes: []Change{
		{Kind: KInsert, Pipeline: "a", Key: "5", Fields: map[string]*string{"body": str("a5")}},
		{Kind: KInsert, Pipeline: "c", Key: "5", Fields: map[string]*string{"body": str("c5")}},
		{Kind: KTruncate, Pipeline: "a"},
		{Kind: KInsert, Pipeline: "a", Key: "6", Fields: map[string]*string{"body": str("a6")}},
	}}
	if err := p.add(context.Background(), u, Item{Txn: txn}); err != nil {
		t.Fatal(err)
	}
	if !u.Truncates["a"] || u.Truncates["c"] {
		t.Fatalf("truncates %v", u.Truncates)
	}
	if _, ok := u.Stream[DocID{"a", "5"}]; ok {
		t.Fatal("a/5 precedes the truncate of a and must be dropped")
	}
	if d := u.Stream[DocID{"c", "5"}]; d == nil || d.Text != "c5" {
		t.Fatal("c/5 (same key, other table) must survive the truncate of a")
	}
	if d := u.Stream[DocID{"a", "6"}]; d == nil || d.Text != "a6" {
		t.Fatal("a/6 follows the truncate and must be applied")
	}
}
