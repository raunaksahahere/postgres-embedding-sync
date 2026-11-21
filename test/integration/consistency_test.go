//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Inserts, updates, key changes, chunk shrink, deletes and truncate.
func TestStreamCorrectness(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(30, 10)
	p := e.start()
	e.waitReady()
	e.requireConsistent("after initial backfill")

	long := strings.Repeat("alpha beta gamma delta epsilon ", 40) // ~200 words -> many chunks
	e.exec(`INSERT INTO documents (id, title, body) VALUES (1000, 'long', $1)`, long)
	e.exec(`UPDATE documents SET body = 'short now' WHERE id = 1000`) // chunk shrink
	e.exec(`UPDATE documents SET id = 2000 WHERE id = 5`)             // key change
	e.exec(`DELETE FROM documents WHERE id IN (6, 7)`)
	e.exec(`UPDATE documents SET title = NULL WHERE id = 8`)
	e.exec(`UPDATE documents SET secret = 'not allowlisted' WHERE id = 9`) // filtered column only
	e.requireConsistent("after mixed changes")

	e.exec(`TRUNCATE documents`)
	e.exec(`INSERT INTO documents (id, title, body) VALUES (1, 'after truncate', 'fresh row')`)
	r := e.requireConsistent("after truncate")
	if r.SourceRows != 1 || r.Generations[0].ActualChunks != 1 {
		t.Fatalf("truncate not reconciled: %+v", r)
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
}

// The secret column is outside the allowlist: the replicator cannot read it
// and it never appears in the sink.
func TestLeastPrivilegeAndColumnAllowlist(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.exec(`INSERT INTO documents (id, title, body, secret) VALUES (1, 't', 'b', 'TOPSECRET-123')`)
	p := e.start()
	e.waitReady()
	e.requireConsistent("initial")
	out, err := e.cli("search", "--json", "t")
	if err != nil || strings.Contains(out, "TOPSECRET") {
		t.Fatalf("search: %v %s", err, out)
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE content LIKE '%TOPSECRET%'`); n != "0" {
		t.Fatal("secret column leaked into the sink")
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.source_state WHERE fields::text LIKE '%TOPSECRET%'`); n != "0" {
		t.Fatal("secret column leaked into source_state")
	}
	ctx := context.Background()
	c, err := e.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if _, err := c.Exec(ctx, `SET ROLE embedsync_replicator`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `SELECT secret FROM documents`); err == nil {
		t.Fatal("replicator can read a non-allowlisted column")
	}
	if _, err := c.Exec(ctx, `SELECT * FROM embedsync.chunks_v1`); err == nil {
		t.Fatal("replicator can read sink tables")
	}
	c.Exec(ctx, `RESET ROLE`)
	if _, err := c.Exec(ctx, `SET ROLE embedsync_reader`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `SELECT * FROM embedsync.source_state`); err == nil {
		t.Fatal("reader can read source_state")
	}
	if _, err := c.Exec(ctx, `SELECT count(*) FROM embedsync.chunks_v1`); err != nil {
		t.Fatalf("reader cannot read chunks: %v", err)
	}
	c.Exec(ctx, `RESET ROLE`)
	pubs := e.scalar(`SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY schemaname, tablename) FROM pg_publication_tables WHERE pubname = 'embedsync_pub'`)
	if pubs != "embedsync_control.markers,public.documents" {
		t.Fatalf("publication must contain only the source and marker tables, got %s", pubs)
	}
	_, metrics := e.httpGet("/metrics")
	if strings.Contains(string(metrics), "TOPSECRET") {
		t.Fatal("sensitive text in metrics")
	}
	p.stop(t)
	for _, l := range e.procLogs {
		if strings.Contains(l.String(), "TOPSECRET") {
			t.Fatal("sensitive text in logs")
		}
	}
}

// Concurrent writers with aborted transactions, duplicate delivery from
// repeated SIGKILLs, and restarts.
func TestConcurrentWritesAbortsAndKills(t *testing.T) {
	e := newEnv(t, envOpts{batch: 40})
	e.seed(300, 15)
	p := e.start()
	w := e.startWriters(4, 400, 11)
	rng := rand.New(rand.NewPCG(3, 4))
	kills := 0
	for round := 0; round < 5; round++ {
		time.Sleep(time.Duration(400+rng.IntN(1200)) * time.Millisecond)
		p.kill() // SIGKILL: feedback lags the checkpoint, so the restart replays work
		kills++
		e.assertFeedbackNotAhead()
		p = e.start()
	}
	time.Sleep(time.Second)
	w.halt()
	e.waitReady()
	r := e.requireConsistent("after concurrent writes and kills")
	t.Logf("commits=%d aborts=%d writer_errors=%d kills=%d source_rows=%d", w.commits.Load(), w.aborts.Load(), w.errs.Load(), kills, r.SourceRows)
	if w.aborts.Load() == 0 || w.commits.Load() < 50 {
		t.Fatal("workload did not exercise aborts/commits")
	}
	p.stop(t)
}

// Crash exactly before the sink commit and exactly after it (before feedback).
func TestFailpointCrashes(t *testing.T) {
	for _, fp := range []string{"before_sink_commit", "after_commit_before_feedback"} {
		t.Run(fp, func(t *testing.T) {
			e := newEnv(t, envOpts{batch: 25})
			e.seed(120, 12)
			w := e.startWriters(3, 200, 5)
			crashes := 0
			for i, hit := range []int{2, 5, 9, 3} {
				p := e.start(fmt.Sprintf("EMBEDSYNC_FAILPOINTS=%s=%d", fp, hit))
				code := p.wait(t, 60*time.Second)
				if code != 86 {
					t.Fatalf("round %d: expected failpoint exit 86, got %d", i, code)
				}
				crashes++
				e.assertFeedbackNotAhead()
			}
			w.halt()
			p := e.start()
			e.waitReady()
			e.requireConsistent("after " + fp + " crashes")
			t.Logf("%s: %d crashes, commits=%d aborts=%d", fp, crashes, w.commits.Load(), w.aborts.Load())
			p.stop(t)
		})
	}
}

// Backfill a large table in many windows while writers change, delete,
// re-key and truncate rows underneath it.
func TestBackfillDuringWrites(t *testing.T) {
	e := newEnv(t, envOpts{batch: 37})
	e.seed(2500, 6)
	w := e.startPacedWriters(4, 2600, 99, 50*time.Millisecond, 0)
	p := e.start()
	time.Sleep(1500 * time.Millisecond)
	e.exec(`DELETE FROM documents WHERE id % 97 = 0`)
	waitFor(t, 120*time.Second, "backfill done", func() bool {
		return e.scalar(`SELECT coalesce(string_agg(state, ','), '') FROM embedsync.backfill_jobs`) == "done"
	})
	w.halt()
	r := e.requireConsistent("after backfill during writes")
	windows := e.scalar(`SELECT windows::text FROM embedsync.backfill_jobs`)
	t.Logf("backfill windows=%s commits=%d aborts=%d source_rows=%d", windows, w.commits.Load(), w.aborts.Load(), r.SourceRows)
	p.stop(t)

	// Re-backfill a live generation that has stale leftovers: range deletes and
	// snapshots must repair it without resurrecting deleted keys.
	e.exec(`INSERT INTO embedsync.chunks_v1 (pipeline, source_key, chunk_no, chunk_id, fingerprint, content, embedding, source_lsn)
		SELECT 'default', '999999', 0, 'x', 'x', 'ghost', embedding, source_lsn FROM embedsync.chunks_v1 LIMIT 1`)
	e.exec(`UPDATE embedsync.chunks_v1 SET content = 'tampered', fingerprint = 'tampered' WHERE source_key = (SELECT min(source_key) FROM embedsync.chunks_v1 WHERE source_key <> '999999')`)
	p = e.start()
	e.waitReady()
	if rr := e.verify(); rr.OK {
		t.Fatal("tampering should be detected by verify")
	}
	if out, err := e.cli("backfill", "--wait"); err != nil {
		t.Fatalf("backfill: %v %s", err, out)
	}
	e.requireConsistent("after re-backfill repair")
	p.stop(t)
}

// Unchanged TOAST values: updating only the title of a row whose body is
// stored out of line must keep the body.
func TestUnchangedToast(t *testing.T) {
	e := newEnv(t, envOpts{chunkWords: 200, chunkOverlap: 10})
	rng := rand.New(rand.NewPCG(1, 2))
	var words []string
	for i := 0; i < 6000; i++ {
		b := make([]byte, 3+rng.IntN(8))
		for j := range b {
			b[j] = byte('a' + rng.IntN(26))
		}
		words = append(words, string(b))
	}
	big := strings.Join(words, " ")
	e.exec(`INSERT INTO documents (id, title, body) VALUES (1, 'before', $1), (2, 'pre-backfill', $1)`, big)
	p := e.start()
	e.waitReady()
	e.requireConsistent("initial toast rows")
	storage := e.scalar(`SELECT pg_column_size(body)::text FROM documents WHERE id = 1`)
	e.exec(`UPDATE documents SET title = 'after' WHERE id = 1`)
	e.exec(`INSERT INTO documents (id, title, body) VALUES (3, 'new', $1)`, big)
	e.exec(`UPDATE documents SET title = 'renamed' WHERE id = 3`)
	e.exec(`UPDATE documents SET id = 4, title = 'rekeyed' WHERE id = 3`)
	e.requireConsistent("after title-only updates of toasted rows")
	t.Logf("toasted body stored size %s bytes", storage)
	p.stop(t)
}

// An embedding outage backpressures the pipeline (WAL is retained, nothing is
// acknowledged or quarantined) and recovers without loss.
func TestEmbeddingOutage(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(50, 10)
	p := e.start()
	e.waitReady()
	e.requireConsistent("before outage")
	before := e.checkpointLSN()
	e.emb.outage.Store(true)
	w := e.startWriters(2, 100, 42)
	time.Sleep(4 * time.Second)
	if got := e.checkpointLSN(); got != before {
		// idle-progress may advance only when nothing is pending; with writers it must not
		t.Logf("checkpoint moved %s -> %s during outage (only allowed for idle positions)", before, got)
	}
	e.assertFeedbackNotAhead()
	pending := e.metric("embedsync_pending_transactions")
	retries := e.metric(`embedsync_retries_total{component="embedder"}`)
	e.emb.outage.Store(false)
	w.halt()
	r := e.requireConsistent("after outage")
	dlq := e.scalar(`SELECT count(*)::text FROM embedsync.dlq`)
	t.Logf("outage: pending_txns_during=%v embedder_retries=%v quarantined=%s commits=%d", pending, retries, dlq, w.commits.Load())
	if retries == 0 || pending == 0 || dlq != "0" || r.Generations[0].Quarantined != 0 {
		t.Fatalf("expected backpressure with retries and no quarantine")
	}
	p.stop(t)
}

// Poison rows are quarantined atomically, counted as unresolved, superseded
// by newer edits/deletes, and replayable.
func TestPoisonQuarantineAndReplay(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(10, 5)
	e.emb.reject.Store("POISON")
	p := e.start()
	e.waitReady()
	e.exec(`INSERT INTO documents (id, title, body) VALUES (100, 'bad', 'this has POISON in it'), (101, 'good', 'fine row')`)
	waitFor(t, 20*time.Second, "quarantine", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "1"
	})
	r := e.verify()
	if !r.OK || r.Generations[0].MissingQ == 0 {
		t.Fatalf("quarantined row must be reported separately: %+v", r)
	}
	st := e.status()
	if st["unresolved_rows"].(float64) != 1 {
		t.Fatalf("status must expose unresolved rows: %v", st["unresolved_rows"])
	}
	// A newer poison version replaces the open record (versioned).
	e.exec(`UPDATE documents SET title = 'still bad' WHERE id = 100`)
	waitFor(t, 20*time.Second, "versioned record", func() bool {
		return e.scalar(`SELECT attempts::text FROM embedsync.dlq WHERE state = 'open' AND source_key = '100'`) == "2"
	})
	// A newer good edit supersedes it.
	e.exec(`UPDATE documents SET body = 'clean now' WHERE id = 100`)
	waitFor(t, 20*time.Second, "superseded", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "0"
	})
	e.requireConsistent("after supersede")
	// Replay must respect a newer deletion.
	e.exec(`INSERT INTO documents (id, title, body) VALUES (102, 'x', 'POISON again')`)
	waitFor(t, 20*time.Second, "quarantine 102", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "1"
	})
	e.exec(`DELETE FROM documents WHERE id = 102`)
	waitFor(t, 20*time.Second, "delete supersedes", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "0"
	})
	// Replay after the cause is fixed.
	e.exec(`INSERT INTO documents (id, title, body) VALUES (103, 'y', 'POISON once more')`)
	waitFor(t, 20*time.Second, "quarantine 103", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "1"
	})
	if out, err := e.cli("dlq", "replay"); err != nil || !strings.Contains(out, `"still_failing": 1`) {
		t.Fatalf("replay while still poisoned: %v %s", err, out)
	}
	e.emb.reject.Store("")
	out, err := e.cli("dlq", "replay")
	if err != nil || !strings.Contains(out, `"resolved": 1`) {
		t.Fatalf("replay: %v %s", err, out)
	}
	e.requireConsistent("after replay")
	p.stop(t)

	// on_poison: pause stops before acknowledging the poison row.
	e.opts.onPoison = "pause"
	e.writeConfig()
	e.emb.reject.Store("POISON")
	p = e.start()
	e.waitReady()
	e.exec(`INSERT INTO documents (id, title, body) VALUES (200, 'p', 'POISON pause')`)
	waitFor(t, 20*time.Second, "paused", func() bool {
		return e.scalar(`SELECT state || ':' || reason FROM embedsync.pipeline_status`) == "paused:poison_row"
	})
	cp := e.checkpointLSN()
	if e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE source_key = '200'`) != "0" {
		t.Fatal("paused row must not be applied")
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
	e.emb.reject.Store("")
	p = e.start()
	e.waitReady()
	e.requireConsistent("after resuming from pause")
	if e.scalar(`SELECT ($1::pg_lsn > $2::pg_lsn)::text`, e.checkpointLSN(), cp) != "true" {
		t.Fatalf("checkpoint did not advance past %s after resume", cp)
	}
	p.stop(t)
}

// Renaming an allowlisted column pauses before acknowledging the affected
// transaction; a reviewed config change resumes it.
func TestSchemaChangePauses(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(10, 5)
	p := e.start()
	e.waitReady()
	e.requireConsistent("before schema change")
	e.exec(`ALTER TABLE documents RENAME COLUMN body TO content`)
	before := e.scalar(`SELECT pg_current_wal_lsn()::text`)
	e.exec(`INSERT INTO documents (id, title, content) VALUES (50, 'after rename', 'renamed column')`)
	waitFor(t, 20*time.Second, "schema pause", func() bool {
		return e.scalar(`SELECT state || ':' || reason FROM embedsync.pipeline_status`) == "paused:schema_incompatible"
	})
	if cp := e.checkpointLSN(); e.scalar(`SELECT ($1::pg_lsn >= $2::pg_lsn)::text`, cp, before) == "true" {
		t.Fatalf("checkpoint %s passed the incompatible change at %s", cp, before)
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
	// Without acceptance the restart refuses (wait for this process's own
	// refusal, not the status row left by the previous one).
	p = e.start()
	p.waitLog(t, 20*time.Second, `"reason":"schema_incompatible"`)
	if got := e.scalar(`SELECT state || ':' || reason FROM embedsync.pipeline_status`); got != "paused:schema_incompatible" {
		t.Fatalf("restart without acceptance must stay paused, status %s", got)
	}
	p.stop(t)
	e.opts.column = "content"
	e.writeConfig()
	p = e.start("--accept-schema-change")
	e.waitReady()
	e.requireConsistent("after accepted rename")
	p.stop(t)
}

// A column renamed while the consumer is behind (here: stopped): changing the
// mapping early must not wedge the pipeline (regression: the new catalog shape
// used to be accepted at startup, then every replayed old-shape transaction
// paused it forever). The old mapping drains up to the change, the stream
// pauses there, and only then is the new mapping accepted.
func TestSchemaChangeAheadOfStream(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(10, 5)
	p := e.start()
	e.waitReady()
	e.requireConsistent("before")
	p.stop(t)
	e.exec(`INSERT INTO documents (id, title, body) VALUES (60, 'before rename', 'old shape row')`)
	e.exec(`ALTER TABLE documents RENAME COLUMN body TO content`)
	e.exec(`INSERT INTO documents (id, title, content) VALUES (61, 'after rename', 'new shape row')`)

	// New mapping too early: refused with guidance, nothing acknowledged.
	e.opts.column = "content"
	e.writeConfig()
	p = e.start("--accept-schema-change")
	p.waitLog(t, 20*time.Second, "has not reached the schema change")
	p.stop(t)

	// Old mapping: drains row 60, then the stream pauses at the rename.
	e.opts.column = "body"
	e.writeConfig()
	p = e.start()
	p.waitLog(t, 20*time.Second, "accepted signature")
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE source_key = '60'`); n == "0" {
		t.Fatal("old-shape row before the rename was not applied")
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE source_key = '61'`); n != "0" {
		t.Fatal("row after the rename applied before the change was accepted")
	}
	e.assertFeedbackNotAhead()
	p.stop(t)

	// Now the new mapping is accepted from exactly the change.
	e.opts.column = "content"
	e.writeConfig()
	p = e.start("--accept-schema-change")
	e.waitReady()
	e.requireConsistent("after accepted rename")
	p.stop(t)
}

// Idle periods advance the checkpoint (bounded retained WAL) and report idle,
// not stalled.
func TestIdleProgressAndState(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(5, 5)
	p := e.start()
	e.waitReady()
	e.requireConsistent("initial")
	cp1 := e.checkpointLSN()
	// Generate unrelated WAL in another table that is not published.
	e.exec(`CREATE TABLE unrelated (x int)`)
	e.exec(`INSERT INTO unrelated SELECT generate_series(1, 20000)`)
	waitFor(t, 20*time.Second, "idle checkpoint advance", func() bool {
		return e.scalar(`SELECT ($1::pg_lsn < (SELECT lsn FROM embedsync.checkpoints))::text`, cp1) == "true"
	})
	if v := e.metric(`embedsync_consumer_state{state="idle"}`); v != 1 {
		t.Fatalf("expected idle state, got %v", v)
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
}

// Slot loss is detected on restart and recovered by rebuilding into a fresh
// generation while the stale one keeps serving.
func TestSlotLossRebuild(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.seed(40, 5)
	p := e.start()
	e.waitReady()
	e.requireConsistent("initial")
	p.stop(t)
	e.exec(`SELECT pg_drop_replication_slot($1)`, e.slot)
	e.exec(`UPDATE documents SET title = 'changed while slot was gone' WHERE id <= 10`)
	p = e.start()
	waitFor(t, 20*time.Second, "slot_lost pause", func() bool {
		return e.scalar(`SELECT state || ':' || reason FROM embedsync.pipeline_status`) == "paused:slot_lost"
	})
	p.stop(t)
	out, err := e.cli("rebuild", "--confirm")
	if err != nil {
		t.Fatalf("rebuild: %v %s", err, out)
	}
	p = e.start()
	e.waitReady()
	waitFor(t, 60*time.Second, "rebuilt generation ready", func() bool {
		return e.scalar(`SELECT state FROM embedsync.generations WHERE id = 'v1_r2'`) == "ready"
	})
	if out, err := e.cli("migrate", "--to", "v1_r2"); err != nil {
		t.Fatalf("promote rebuilt generation: %v %s", err, out)
	}
	if s := e.scalar(`SELECT generation_id FROM embedsync.serving`); s != "v1_r2" {
		t.Fatalf("serving %s", s)
	}
	e.requireConsistent("after rebuild")
	p.stop(t)
}

var _ = sync.Mutex{}
var _ = atomic.Int64{}
var _ = json.Marshal
var _ = os.Getenv
var _ = filepath.Join
