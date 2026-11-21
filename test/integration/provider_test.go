//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func (e *env) searchProvider(q string) (gen, provider string) {
	e.t.Helper()
	_, b := e.httpGet("/search?q=" + strings.ReplaceAll(q, " ", "+"))
	var r struct{ Generation, Provider string }
	if err := json.Unmarshal(b, &r); err != nil {
		e.t.Fatalf("search: %v %s", err, b)
	}
	return r.Generation, r.Provider
}

// A hosted-style provider that throttles (429 with Retry-After), goes down
// and returns malformed batches: the pipeline backs off, acknowledges
// nothing it has not durably applied, and converges with nothing
// quarantined. Also crashes immediately before and after a sink commit
// while the provider is throttling.
func TestProviderFaultsAndCheckpointSafety(t *testing.T) {
	e := newEnv(t, envOpts{initialGen: "pv1"})
	e.seed(60, 30)
	p := e.start()
	e.waitReady()
	e.requireConsistent("built by the mock provider")
	if g, prov := e.searchProvider("seed body word 7"); g != "pv1" || prov != "mock" {
		t.Fatalf("search must route to the generation's provider: %s/%s", g, prov)
	}
	w := e.startPacedWriters(2, 120, 5, 30*time.Millisecond, 0)

	// 429 with Retry-After: 1 (four times).
	e.emb.retryAfter.Store(1)
	e.emb.throttleN.Store(4)
	waitFor(t, 30*time.Second, "throttling consumed", func() bool { return e.emb.throttleN.Load() == 0 })
	e.assertFeedbackNotAhead()

	// Outage.
	e.emb.outage.Store(true)
	time.Sleep(3 * time.Second)
	e.assertFeedbackNotAhead()
	stalled := e.checkpointLSN()
	e.emb.outage.Store(false)

	// Malformed batches (one embedding short).
	e.emb.malformedN.Store(5)
	waitFor(t, 30*time.Second, "malformed batches consumed", func() bool { return e.emb.malformedN.Load() == 0 })
	e.assertFeedbackNotAhead()

	// Crashes around the sink commit while the provider throttles.
	p.kill()
	for _, fp := range []string{"before_sink_commit=2", "after_commit_before_feedback=2"} {
		e.emb.throttleN.Store(2)
		pc := e.start("EMBEDSYNC_FAILPOINTS=" + fp)
		if code := pc.wait(t, 60*time.Second); code != 86 {
			t.Fatalf("%s: expected failpoint exit 86, got %d", fp, code)
		}
		e.assertFeedbackNotAhead()
	}
	w.halt()
	p = e.start()
	e.waitReady()
	r := e.requireConsistent("after throttling, outage, malformed batches and crashes")
	if e.scalar(`SELECT ($1::pg_lsn < (SELECT lsn FROM embedsync.checkpoints))::text`, stalled) != "true" {
		t.Fatal("the checkpoint did not advance after the outage")
	}
	if dlq := e.scalar(`SELECT count(*)::text FROM embedsync.dlq`); dlq != "0" || r.Generations[0].Quarantined != 0 {
		t.Fatalf("transient provider faults must not quarantine anything (dlq=%s)", dlq)
	}
	throttled := e.metric(`embedsync_provider_retries_total{provider="mock",reason="throttled"}`)
	unavailable := e.metric(`embedsync_provider_retries_total{provider="mock",reason="unavailable"}`)
	invalid := e.metric(`embedsync_provider_retries_total{provider="mock",reason="invalid"}`)
	tokens := e.metric(`embedsync_provider_tokens_total{provider="mock",source="reported"}`)
	cost := e.metric(`embedsync_provider_estimated_cost_usd_total{pricing_source="integration test fixture (not a real price)",provider="mock"}`)
	t.Logf("this process: throttled retries=%v unavailable=%v invalid=%v reported tokens=%v estimated cost=%v; commits=%d",
		throttled, unavailable, invalid, tokens, cost, w.commits.Load())
	if tokens == 0 || cost == 0 {
		t.Fatal("token usage and estimated cost must be recorded")
	}
	p.stop(t)
}

// The first process of the fault test exited by SIGKILL, so its retry
// metrics are checked here in one process: throttling honours Retry-After.
func TestProviderRetryAfterIsHonoured(t *testing.T) {
	e := newEnv(t, envOpts{initialGen: "pv1"})
	e.seed(5, 5)
	p := e.start()
	e.waitReady()
	e.emb.retryAfter.Store(1)
	e.emb.throttleN.Store(3)
	start := time.Now()
	e.exec(`UPDATE documents SET title = 'after throttling' WHERE id = 1`)
	waitFor(t, 30*time.Second, "edit applied", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.chunks_pv1 WHERE content LIKE 'after throttling%'`) != "0"
	})
	took := time.Since(start)
	retries := e.metric(`embedsync_provider_retries_total{provider="mock",reason="throttled"}`)
	waited := e.metric(`embedsync_provider_retry_wait_seconds_total{provider="mock"}`)
	t.Logf("3 x 429 Retry-After: 1 -> %v throttled retries, %.2fs waited, edit searchable after %s", retries, waited, took)
	if retries < 3 || waited < 3 || took < 3*time.Second {
		t.Fatalf("Retry-After not honoured: retries=%v waited=%v took=%s", retries, waited, took)
	}
	e.requireConsistent("after throttled edit")
	p.stop(t)
}

// A cross-provider model migration goes through the quality gate: the
// built-in embedder (v1) is migrated to a model served by the mock
// provider (pv2, with query/passage prompts) and promoted; queries follow
// the serving generation's provider; rollback returns them to the built-in
// embedder; a regressing provider model is rejected.
func TestCrossProviderMigrationGate(t *testing.T) {
	dir := t.TempDir()
	q, qr := writeEval(t, dir)
	e := newEnv(t, envOpts{evalQueries: q, evalQrels: qr})
	seedTopics(e)
	p := e.start()
	e.waitReady()
	e.requireConsistent("v1 built by the built-in embedder")
	reader := e.startReader()

	out, err := e.cli("migrate", "--to", "pv2", "--json")
	if err != nil {
		t.Fatalf("migrate to the provider model: %v\n%s", err, out)
	}
	var rep struct {
		Active, Candidate struct{ Provider string }
		Pass              bool
	}
	if err := json.Unmarshal([]byte(out[strings.Index(out, "{"):]), &rep); err != nil {
		t.Fatalf("gate report: %v\n%s", err, out)
	}
	if !rep.Pass || rep.Active.Provider != "local" || rep.Candidate.Provider != "mock" {
		t.Fatalf("gate report must name both providers and pass: %+v", rep)
	}
	if g, prov := e.searchProvider("facts about the bison"); g != "pv2" || prov != "mock" {
		t.Fatalf("after promotion search must use pv2's provider, got %s/%s", g, prov)
	}
	if n := e.emb.prefixMiss.Load(); n != 0 {
		t.Fatalf("%d provider inputs lacked the configured prompts", n)
	}
	e.emb.mu.Lock()
	sawQuery := false
	for _, l := range e.emb.log {
		sawQuery = sawQuery || strings.HasPrefix(l, "openai fake/bowalt query")
	}
	e.emb.mu.Unlock()
	if !sawQuery {
		t.Fatal("no query was embedded by the provider after promotion")
	}

	out, err = e.cli("migrate", "--to", "pbad")
	if err == nil || !strings.Contains(out, "REJECTED") {
		t.Fatalf("a regressing provider model must be rejected, got err=%v\n%s", err, out)
	}
	if g, prov := e.searchProvider("facts about the bison"); g != "pv2" || prov != "mock" {
		t.Fatalf("a rejected candidate must not change serving: %s/%s", g, prov)
	}
	if out, err := e.cli("migrate", "rollback"); err != nil {
		t.Fatalf("rollback: %v %s", err, out)
	}
	if g, prov := e.searchProvider("facts about the bison"); g != "v1" || prov != "local" {
		t.Fatalf("after rollback search must use the built-in embedder: %s/%s", g, prov)
	}
	time.Sleep(500 * time.Millisecond)
	reader.halt()
	t.Logf("reader: ok=%d errors=%d wrong_top1=%d sequence=%v", reader.ok.Load(), reader.errs.Load(), reader.wrongTop.Load(), reader.gens)
	if reader.errs.Load() != 0 || reader.wrongTop.Load() != 0 {
		t.Fatalf("reader saw errors or wrong results across providers: %s", strings.Join(reader.examples, "\n"))
	}
	r := e.requireConsistent("after cross-provider promote, reject and rollback")
	gens := map[string]bool{}
	for _, g := range r.Generations {
		gens[g.Generation] = true
	}
	if !gens["v1"] || !gens["pv2"] {
		t.Fatalf("both providers' generations must stay current in the rollback window: %v", gens)
	}
	if s := e.scalar(`SELECT (provider->>'name') || '/' || (provider->>'kind') FROM embedsync.generations WHERE id = 'pv2'`); s != "mock/openai" {
		t.Fatalf("pv2 provider identity %q", s)
	}
	p.stop(t)
	_ = fmt.Sprint
}
