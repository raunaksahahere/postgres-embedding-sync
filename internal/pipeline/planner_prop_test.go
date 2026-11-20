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

// The property: for any interleaving of source writes (inserts, updates,
// updates with unchanged TOAST columns, key changes, deletes, truncates) with
// watermark backfill windows, and any grouping of transactions into units,
// applying the planner's units yields a sink equal to the source.

type simSink struct {
	state map[string]map[string]*string
	docs  map[string]string // key -> rendered text in generation "g"
}

func inRange(kk keys.Kind, k string, lo, hi *string) bool {
	if lo != nil && kk.Compare(k, *lo) <= 0 {
		return false
	}
	if hi != nil && kk.Compare(k, *hi) > 0 {
		return false
	}
	return true
}

// apply mirrors Committer.applyState/applyGen ordering: truncate, range
// deletes, stream effects, snapshot effects (overriding).
func (s *simSink) apply(t *testing.T, kk keys.Kind, u *Unit) {
	if u.Pause != nil {
		t.Fatalf("unexpected pause: %v", u.Pause)
	}
	if u.Truncate {
		s.state = map[string]map[string]*string{}
		s.docs = map[string]string{}
	}
	for _, rd := range u.RangeDeletes {
		keep := map[string]bool{}
		for _, k := range rd.Keep {
			keep[k] = true
		}
		for k := range s.state {
			if inRange(kk, k, rd.Lo, rd.Hi) && !keep[k] {
				delete(s.state, k)
			}
		}
		for k := range s.docs {
			if inRange(kk, k, rd.Lo, rd.Hi) && !keep[k] {
				delete(s.docs, k)
			}
		}
	}
	put := func(d *DocEffect, gen bool) {
		if d.Delete {
			delete(s.state, d.Key)
			if gen {
				delete(s.docs, d.Key)
			}
			return
		}
		s.state[d.Key] = d.Fields
		if gen {
			s.docs[d.Key] = d.Text
		}
	}
	_, streamGen := u.Gens["g"]
	for _, d := range u.Stream {
		if _, overridden := u.Snapshot["g"][d.Key]; overridden {
			put(d, false)
			continue
		}
		put(d, streamGen)
	}
	for _, d := range u.Snapshot["g"] {
		// Snapshot rows carry the high marker's LSN: never 0/0, never beyond
		// the unit (regression: they used to take the previous unit's LSN).
		if d.LSN == 0 || d.LSN > u.EndLSN {
			panic(fmt.Sprintf("snapshot effect for %s has LSN %d (unit end %d)", d.Key, d.LSN, u.EndLSN))
		}
		put(d, true)
	}
}

func str(s string) *string { return &s }

var discarded int

func runInterleaving(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed*7919))
	kk := keys.Int
	cfg := &config.Config{}
	cfg.Pipeline.MaxBatchTxns, cfg.Pipeline.MaxBatchDocs, cfg.Pipeline.MaxDocumentBytes = 1000, 1000, 1<<20
	tr, err := transform.New("public.documents", "{{.title}}|{{.body}}", []string{"title", "body"}, 50, 5, "fp")
	if err != nil {
		t.Fatal(err)
	}
	sink := &simSink{state: map[string]map[string]*string{}, docs: map[string]string{}}
	reg := NewRegistry()
	p := &Planner{Cfg: cfg, KeyKind: kk, Tr: tr, Registry: reg,
		Lookup: func(_ context.Context, ks []string) (map[string]store.SourceState, error) {
			out := map[string]store.SourceState{}
			for _, k := range ks {
				if f, ok := sink.state[k]; ok {
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
		ActiveJob: map[int64]*string{1: nil},
		windows:   map[string]*window{},
		overlay:   map[string]*overlayEntry{},
	}
	source := map[string]map[string]*string{}
	nextKey := 1
	// Leftover chunks in the generation from an earlier build: keys that no
	// longer exist and keys with outdated content. Only range deletes and
	// snapshots can repair them.
	for i := 0; i < rng.IntN(8); i++ {
		sink.docs[strconv.Itoa(1+rng.IntN(60))] = "stale leftover"
	}
	// Rows that exist before the slot: only backfill can deliver them.
	for i := 0; i < 10+rng.IntN(40); i++ {
		source[strconv.Itoa(nextKey)] = map[string]*string{"title": str(fmt.Sprintf("t%d", nextKey)), "body": str(fmt.Sprintf("pre %d", nextKey))}
		nextKey++
	}
	var lsn LSN = 100
	var pending []Item
	emit := func(changes ...Change) {
		lsn += 10
		pending = append(pending, Item{Txn: &Txn{EndLSN: lsn, Changes: changes}})
	}
	var seq uint64
	var committedLast *string
	jobDone := false
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
			sink.apply(t, kk, u)
			p.Release(u.Seq)
			for _, pr := range u.Progress {
				committedLast = pr.LastKey
				if pr.Final {
					jobDone = true
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
	sourceKeys := func() []string {
		ks := make([]string, 0, len(source))
		for k := range source {
			ks = append(ks, k)
		}
		sort.Slice(ks, func(i, j int) bool { return kk.Compare(ks[i], ks[j]) < 0 })
		return ks
	}
	randomExisting := func() (string, bool) {
		ks := sourceKeys()
		if len(ks) == 0 {
			return "", false
		}
		return ks[rng.IntN(len(ks))], true
	}
	extraOps := 0
	extraLimit := rng.IntN(20) // stop at a random point after backfill so later edits do not mask errors
	for step := 0; step < 20000; step++ {
		if jobDone && (phase == 0 || phase == 3) {
			if extraOps > extraLimit {
				break
			}
			extraOps++
		}
		switch r := rng.IntN(100); {
		case r < 35 && (!jobDone || (phase != 0 && phase != 3)):
			switch phase {
			case 0:
				windowN++
				windowID = fmt.Sprintf("w%d", windowN)
				emit(Change{Kind: KMarker, Marker: Marker{Kind: "low", Gen: "g", JobID: 1, WindowID: windowID}})
				phase = 1
			case 1:
				snap = nil
				for _, k := range sourceKeys() {
					if committedLast != nil && kk.Compare(k, *committedLast) <= 0 {
						continue
					}
					f := map[string]*string{}
					for n, v := range source[k] {
						f[n] = v
					}
					snap = append(snap, SnapRow{Key: k, Fields: f})
					if len(snap) == limit {
						break
					}
				}
				entry = &WindowRows{WindowID: windowID, Gen: "g", JobID: 1, Lo: committedLast, Rows: snap, Final: len(snap) < limit}
				reg.Put(entry)
				phase = 2
			case 2:
				emit(Change{Kind: KMarker, Marker: Marker{Kind: "high", Gen: "g", JobID: 1, WindowID: windowID}})
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
		case r < 55:
			k := strconv.Itoa(nextKey)
			nextKey++
			f := map[string]*string{"title": str("new " + k), "body": str(fmt.Sprintf("inserted %d %d", step, rng.IntN(1000)))}
			if rng.IntN(10) == 0 {
				f["title"] = nil
			}
			source[k] = f
			emit(Change{Kind: KInsert, Key: k, Fields: f})
		case r < 75:
			k, ok := randomExisting()
			if !ok {
				continue
			}
			if phase != 0 && rng.IntN(2) == 0 {
				// prefer a key at or beyond the backfill cursor
				for _, c := range sourceKeys() {
					if committedLast == nil || kk.Compare(c, *committedLast) > 0 {
						k = c
						break
					}
				}
			}
			old := source[k]
			nf := map[string]*string{"title": str(fmt.Sprintf("upd %d", step)), "body": old["body"]}
			ch := Change{Kind: KUpdate, Key: k, Fields: map[string]*string{"title": nf["title"]}}
			if rng.IntN(2) == 0 {
				ch.Unchanged = []string{"body"} // unchanged TOAST value not sent
			} else {
				ch.Fields["body"] = nf["body"]
			}
			if rng.IntN(5) == 0 && len(ch.Unchanged) == 0 {
				nf["body"] = str(fmt.Sprintf("rewritten %d", step))
				ch.Fields["body"] = nf["body"]
			}
			source[k] = nf
			emit(ch)
		case r < 82:
			k, ok := randomExisting()
			if !ok {
				continue
			}
			nk := strconv.Itoa(nextKey)
			nextKey++
			f := source[k]
			delete(source, k)
			source[nk] = f
			ch := Change{Kind: KUpdate, Key: nk, OldKey: k, Fields: map[string]*string{"title": f["title"]}}
			if rng.IntN(2) == 0 {
				ch.Unchanged = []string{"body"}
			} else {
				ch.Fields["body"] = f["body"]
			}
			emit(ch)
		case r < 95:
			k, ok := randomExisting()
			if !ok {
				continue
			}
			delete(source, k)
			emit(Change{Kind: KDelete, Key: k})
		case r < 97:
			source = map[string]map[string]*string{}
			emit(Change{Kind: KTruncate})
		default:
			flush(false)
		}
	}
	if phase == 1 || phase == 2 {
		if phase == 1 {
			reg.Put(&WindowRows{WindowID: windowID, Gen: "g", JobID: 1, Lo: committedLast, Final: false})
		}
		emit(Change{Kind: KMarker, Marker: Marker{Kind: "high", Gen: "g", JobID: 1, WindowID: windowID}})
	}
	flush(true)
	if !jobDone {
		t.Fatalf("seed %d: backfill did not finish (windows=%d)", seed, windowN)
	}
	for k, f := range source {
		vals := map[string]string{}
		for n, v := range f {
			if v != nil {
				vals[n] = *v
			}
		}
		want, _ := tr.Render(vals)
		got, ok := sink.docs[k]
		if !ok {
			t.Fatalf("seed %d: missing key %s", seed, k)
		}
		if got != want {
			t.Fatalf("seed %d: stale key %s: got %q want %q", seed, k, got, want)
		}
		if st, ok := sink.state[k]; !ok || len(st) != len(f) {
			t.Fatalf("seed %d: source_state mismatch for %s", seed, k)
		}
	}
	for k := range sink.docs {
		if _, ok := source[k]; !ok {
			t.Fatalf("seed %d: extra key %s in sink", seed, k)
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
	t.Logf("%d interleavings, %d windows discarded after unchanged-TOAST deferral", n, discarded)
	if discarded == 0 {
		t.Fatal("the deferral path was never exercised")
	}
}
