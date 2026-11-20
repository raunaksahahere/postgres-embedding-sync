//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// topics give every document a distinctive word so lexical (bag-of-words)
// encoders retrieve it, while the noise encoder cannot.
var topics = strings.Fields(`aardvark bison cobra dingo egret ferret gecko heron ibis jackal koala lemur marmot newt ocelot
 puffin quokka raven stoat tapir urchin vole walrus yak zebra badger camel donkey eland finch gopher hyena impala jaguar kiwi
 llama moose narwhal otter panda quail rhino skunk toucan`)

// goodTopics are topics whose query clearly prefers its own document under
// the fake bag-of-words encoder (64 signed hash buckets can cancel a query's
// only overlap with its document, leaving a tie with every row that ANN would
// break arbitrarily). bowalt is a permutation of bow, so the same holds there.
func goodTopics() map[int]bool {
	cos := func(a, b []float32) float64 {
		s := 0.0
		for i := range a {
			s += float64(a[i]) * float64(b[i])
		}
		return s
	}
	docs := make([][]float32, len(topics))
	for i := range topics {
		title, body := topicDoc(i)
		docs[i] = encode("fake/bow", title+" "+body)
	}
	good := map[int]bool{}
	for i, w := range topics {
		q := encode("fake/bow", "facts about the "+w)
		own, best := cos(q, docs[i]), -1.0
		for j := range topics {
			if j != i {
				best = max(best, cos(q, docs[j]))
			}
		}
		if own >= 0.3 && own > best+0.05 {
			good[i] = true
		}
	}
	return good
}

func writeEval(t *testing.T, dir string) (string, string) {
	q := filepath.Join(dir, "queries.jsonl")
	r := filepath.Join(dir, "qrels.tsv")
	var qb, rb strings.Builder
	rb.WriteString("query-id\tcorpus-id\tscore\n")
	good := goodTopics()
	if len(good) < 12 {
		t.Fatalf("only %d usable evaluation topics", len(good))
	}
	for i, w := range topics {
		if !good[i] {
			continue
		}
		fmt.Fprintf(&qb, `{"_id":"q%d","text":"facts about the %s"}`+"\n", i, w)
		fmt.Fprintf(&rb, "q%d\t%d\t1\n", i, i+1)
	}
	os.WriteFile(q, []byte(qb.String()), 0o600)
	os.WriteFile(r, []byte(rb.String()), 0o600)
	return q, r
}

// fillers make non-relevant documents differ from each other, so ranks below
// the relevant document are not one big tie.
var fillers = strings.Fields(`amber basin cliff delta ember fjord grove hollow inlet jetty knoll ledge mesa nook
 oasis plateau quarry ridge shoal thicket upland valley wharf yard zenith`)

func topicDoc(i int) (title, body string) {
	w := topics[i]
	var extra []string
	for j := 0; j < 1+i%4; j++ {
		extra = append(extra, fillers[(i*7+j*3)%len(fillers)])
	}
	return w, fmt.Sprintf("the %s lives here near the %s and %s habits are described", w, strings.Join(extra, " "), w)
}

func seedTopics(e *env) {
	for i := range topics {
		title, body := topicDoc(i)
		e.exec(`INSERT INTO documents (id, title, body) VALUES ($1, $2, $3)`, i+1, title, body)
	}
}

type searchResp struct {
	Generation string `json:"generation"`
	Hits       []struct {
		Key string `json:"key"`
	} `json:"hits"`
	Error string `json:"error"`
}

type cutoverReader struct {
	stop     chan struct{}
	wg       sync.WaitGroup
	ok, errs atomic.Int64
	mu       sync.Mutex
	gens     []string
	maxLat   time.Duration
	wrongTop atomic.Int64
	examples []string
}

// probeTopics are topic words whose hash bucket no other word in the corpus,
// the writers' vocabulary or the query template uses. For them the topic
// document is the unambiguous top result under bow and under bowalt (a
// permutation of the same buckets), so a wrong top-1 can only come from
// pairing a query encoder with the wrong generation's index.
func probeTopics(t *testing.T) []string {
	bucket := func(w string) uint64 { return (hash("a", w) >> 1) % dim }
	taken := map[uint64]int{}
	others := append(append(append([]string{}, vocab...), fillers...),
		strings.Fields("facts about the lives here near and habits are described")...)
	for _, w := range others {
		taken[bucket(w)] = -1
	}
	for _, w := range topics {
		if taken[bucket(w)] >= 0 {
			taken[bucket(w)]++
		}
	}
	good := goodTopics()
	var out []string
	for i, w := range topics {
		if taken[bucket(w)] == 1 && good[i] {
			out = append(out, w)
		}
	}
	if len(out) < 3 {
		t.Fatalf("only %d collision-free probe topics", len(out))
	}
	return out
}

// startReader issues searches continuously and records errors, latency and
// which generation answered (cutover time = first answer from the new one).
func (e *env) startReader() *cutoverReader {
	probes := probeTopics(e.t)
	r := &cutoverReader{stop: make(chan struct{})}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		i := 0
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			w := probes[i%len(probes)]
			i++
			start := time.Now()
			// Exact search through the serving path: this reader checks query
			// encoder pinning and the serving switch. ANN recall on the toy
			// encoder (a few isolated topic rows among hundreds of similar
			// churn rows) is what the candidate's ANN-vs-exact gate measures.
			code, body := e.httpGet("/search?k=3&exact=true&q=" + url.QueryEscape("facts about the "+w))
			lat := time.Since(start)
			var sr searchResp
			_ = json.Unmarshal(body, &sr)
			if code != 200 || sr.Error != "" {
				r.errs.Add(1)
				continue
			}
			r.ok.Add(1)
			want := fmt.Sprint(indexOf(w) + 1)
			if len(sr.Hits) == 0 || sr.Hits[0].Key != want {
				if r.wrongTop.Add(1) <= 5 {
					r.mu.Lock()
					r.examples = append(r.examples, fmt.Sprintf("%s q=%q want=%s got=%s", sr.Generation, w, want, body))
					r.mu.Unlock()
				}
			}
			r.mu.Lock()
			if len(r.gens) == 0 || r.gens[len(r.gens)-1] != sr.Generation {
				r.gens = append(r.gens, sr.Generation)
			}
			if lat > r.maxLat {
				r.maxLat = lat
			}
			r.mu.Unlock()
		}
	}()
	return r
}

func indexOf(w string) int {
	for i, x := range topics {
		if x == w {
			return i
		}
	}
	return -1
}

func (r *cutoverReader) halt() { close(r.stop); r.wg.Wait() }

// Promote an equal-dimension model, reject a regressed one, and roll back,
// while a reader searches continuously.
func TestMigrationPromoteRejectRollback(t *testing.T) {
	dir := t.TempDir()
	q, qr := writeEval(t, dir)
	e := newEnv(t, envOpts{evalQueries: q, evalQrels: qr})
	seedTopics(e)
	p := e.start()
	e.waitReady()
	e.requireConsistent("v1 built")
	reader := e.startReader()
	w := e.startPacedWriters(1, 300, 77, 20*time.Millisecond, 1000) // churn keys above the topic docs during migration
	_ = w

	out, err := e.cli("migrate", "--to", "v2", "--json")
	if err != nil {
		t.Fatalf("migrate v2: %v\n%s", err, out)
	}
	if s := e.scalar(`SELECT generation_id || ',' || previous_generation_id FROM embedsync.serving`); s != "v2,v1" {
		t.Fatalf("serving after promote = %s", s)
	}
	// The pinned query encoder for v2 must be the bowalt model with its query prompt.
	e.emb.mu.Lock()
	sawV2Query := false
	for _, l := range e.emb.log {
		if strings.HasPrefix(l, "fake/bowalt query") {
			sawV2Query = true
		}
	}
	e.emb.mu.Unlock()
	if !sawV2Query {
		t.Fatal("no query embedding was requested from the v2 encoder")
	}
	if n := e.emb.prefixMiss.Load(); n != 0 {
		t.Fatalf("%d v2 inputs were sent without the configured query/passage prompt", n)
	}

	// Regression: the noise encoder must be rejected; v2 keeps serving.
	out, err = e.cli("migrate", "--to", "bad")
	if err == nil || !strings.Contains(out, "REJECTED") {
		t.Fatalf("expected rejection, got err=%v\n%s", err, out)
	}
	if s := e.scalar(`SELECT state FROM embedsync.generations WHERE id = 'bad'`); s != "rejected" {
		t.Fatalf("bad generation state %s", s)
	}
	st, _ := e.cli("migrate", "status")
	if !strings.Contains(st, "gate_failed") || !strings.Contains(st, "delta_ndcg_at_10_ci_lower") {
		t.Fatalf("failed gate must be inspectable:\n%s", st)
	}

	// Rollback while both generations are current.
	if out, err := e.cli("migrate", "rollback"); err != nil {
		t.Fatalf("rollback: %v %s", err, out)
	}
	time.Sleep(500 * time.Millisecond)
	w.halt()
	reader.halt()
	r := e.requireConsistent("after promote/reject/rollback")
	gens := map[string]bool{}
	for _, g := range r.Generations {
		gens[g.Generation] = true
	}
	if !gens["v1"] || !gens["v2"] || gens["bad"] {
		t.Fatalf("both v1 and v2 must stay current during the rollback window; got %v", gens)
	}
	t.Logf("reader: ok=%d errors=%d wrong_top1=%d max_latency=%s generation sequence=%v",
		reader.ok.Load(), reader.errs.Load(), reader.wrongTop.Load(), reader.maxLat, reader.gens)
	if reader.errs.Load() != 0 {
		t.Fatalf("reader saw %d errors during cutover", reader.errs.Load())
	}
	if reader.wrongTop.Load() != 0 {
		t.Fatalf("reader saw %d wrong top results (encoder/index mismatch?), e.g.\n%s", reader.wrongTop.Load(), strings.Join(reader.examples, "\n"))
	}
	seq := strings.Join(reader.gens, ">")
	if !strings.Contains(seq, "v1>v2") || !strings.HasSuffix(seq, "v2>v1") {
		t.Fatalf("unexpected cutover sequence %s", seq)
	}
	if s := e.scalar(`SELECT generation_id FROM embedsync.serving`); s != "v1" {
		t.Fatalf("serving after rollback = %s", s)
	}
	// Finalize retires the previous generation.
	if out, err := e.cli("migrate", "finalize", "--force"); err != nil || !strings.Contains(out, "retired v2") {
		t.Fatalf("finalize: %v %s", err, out)
	}
	p.stop(t)
}

// An interrupted migration resumes from its persisted step.
func TestMigrationResume(t *testing.T) {
	dir := t.TempDir()
	q, qr := writeEval(t, dir)
	e := newEnv(t, envOpts{evalQueries: q, evalQrels: qr, batch: 5})
	seedTopics(e)
	e.seed(0, 0)
	e.emb.delayMS.Store(40) // slow backfill so the migration is interrupted mid-way
	p := e.start()
	e.waitReady()
	cmd := e.cliCmd("migrate", "--to", "v2")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "candidate registered", func() bool {
		return e.scalar(`SELECT coalesce((SELECT step FROM embedsync.migrations ORDER BY id DESC LIMIT 1), '')`) == "backfill"
	})
	cmd.Process.Kill()
	cmd.Wait()
	e.emb.delayMS.Store(0)
	out, err := e.cli("migrate", "--to", "v2")
	if err != nil || !strings.Contains(out, "resuming migration") {
		t.Fatalf("resume: %v\n%s", err, out)
	}
	if s := e.scalar(`SELECT generation_id FROM embedsync.serving`); s != "v2" {
		t.Fatalf("serving %s", s)
	}
	e.requireConsistent("after resumed migration")
	p.stop(t)
}

// Regressions from review: a failing --gate-only run keeps the candidate; a
// second promotion inside the rollback window retires the older previous
// generation (and cancels its jobs) instead of maintaining it forever.
func TestMigrationGateOnlyAndSecondPromotion(t *testing.T) {
	dir := t.TempDir()
	q, qr := writeEval(t, dir)
	e := newEnv(t, envOpts{evalQueries: q, evalQrels: qr})
	seedTopics(e)
	p := e.start()
	e.waitReady()

	out, err := e.cli("migrate", "--to", "bad", "--gate-only")
	if err == nil || !strings.Contains(out, "REJECTED") {
		t.Fatalf("gate-only on a regression must fail: %v\n%s", err, out)
	}
	if s := e.scalar(`SELECT state FROM embedsync.generations WHERE id = 'bad'`); s != "ready" {
		t.Fatalf("gate-only must keep the candidate, state %s", s)
	}

	if out, err := e.cli("migrate", "--to", "v2"); err != nil {
		t.Fatalf("migrate v2: %v\n%s", err, out)
	}
	if out, err := e.cli("migrate", "--to", "v2b"); err != nil {
		t.Fatalf("migrate v2b: %v\n%s", err, out)
	}
	if s := e.scalar(`SELECT generation_id || ',' || previous_generation_id FROM embedsync.serving`); s != "v2b,v2" {
		t.Fatalf("serving after second promotion = %s", s)
	}
	if s := e.scalar(`SELECT state FROM embedsync.generations WHERE id = 'v1'`); s != "retired" {
		t.Fatalf("older previous generation must be retired, state %s", s)
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.backfill_jobs WHERE generation_id = 'v1' AND state IN ('pending','running')`); n != "0" {
		t.Fatalf("retired generation still has %s active backfill jobs", n)
	}
	// A backfill queued for the rollback target, then finalized away, must be
	// cancelled rather than block later jobs (and verify) forever.
	if out, err := e.cli("backfill", "--generation", "v2", "--wait=false"); err != nil {
		t.Fatalf("queue backfill: %v %s", err, out)
	}
	if out, err := e.cli("migrate", "finalize", "--force"); err != nil || !strings.Contains(out, "retired v2") {
		t.Fatalf("finalize: %v %s", err, out)
	}
	e.exec(`UPDATE documents SET title = title || ' edited' WHERE id = 3`)
	r := e.requireConsistent("after second promotion and finalize")
	for _, g := range r.Generations {
		if g.Generation == "v1" || g.Generation == "v2" {
			t.Fatalf("retired generation %s still verified as live", g.Generation)
		}
	}
	p.stop(t)
}
