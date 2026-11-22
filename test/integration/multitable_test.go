//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
)

// Three pipelines through one slot: documents (id), notes (id, the same ids
// as documents) and support.tickets (tenant_id, id).
var multiSetup = []string{
	`CREATE TABLE public.notes (id bigint PRIMARY KEY, body text, private text)`,
	`CREATE SCHEMA support`,
	`CREATE TABLE support.tickets (tenant_id int NOT NULL, id bigint NOT NULL, subject text, body text, internal text,
		PRIMARY KEY (tenant_id, id))`,
}

const multiPipelines = `  - name: docs
    table: public.documents
    key: id
    fields: [{name: title, column: title}, {name: body, column: body}]
  - name: notes
    table: public.notes
    key: id
    fields: [{name: body, column: body}]
    transform: {template: "note {{.body}}", chunk_words: 20, chunk_overlap: 4}
  - name: tickets
    table: support.tickets
    key: [tenant_id, id]
    fields: [{name: subject, column: subject}, {name: body, column: body}]
    transform: {template: "ticket {{.subject}}\n{{.body}}", chunk_words: 20, chunk_overlap: 4}
`

func (e *env) seedMulti(docs, tenants, perTenant int) {
	e.t.Helper()
	e.seed(docs, 8)
	e.exec(fmt.Sprintf(`INSERT INTO notes (id, body, private) SELECT g, 'note body ' || g || ' harbor lantern', 'private ' || g FROM generate_series(1, %d) g`, docs))
	e.exec(fmt.Sprintf(`INSERT INTO support.tickets (tenant_id, id, subject, body, internal)
		SELECT t, g, 'subject ' || t || '/' || g, repeat('ticket body ' || t || ' ' || g || ' ', 6), 'internal ' || g
		FROM generate_series(1, %d) t, generate_series(1, %d) g`, tenants, perTenant))
}

// crossWriter issues transactions that touch several tables: updates of
// overlapping ids in documents and notes, ticket inserts/updates/deletes,
// ticket key changes (tenant moves and id changes) and document key changes.
// ~20 % of transactions roll back.
func (e *env) startCrossWriters(n int, seed uint64, pace time.Duration) *writer {
	w := &writer{stop: make(chan struct{})}
	for i := 0; i < n; i++ {
		w.wg.Add(1)
		go func(i int) {
			defer w.wg.Done()
			r := &mrand{x: seed + uint64(i)*104729 + 1}
			ctx := context.Background()
			for {
				select {
				case <-w.stop:
					return
				default:
				}
				tx, err := e.admin.Begin(ctx)
				if err != nil {
					w.errs.Add(1)
					time.Sleep(50 * time.Millisecond)
					continue
				}
				failed := false
				for j := 0; j < 1+r.Intn(4) && !failed; j++ {
					id := 1 + r.Intn(300)
					tenant := 1 + r.Intn(4)
					var err error
					switch op := r.Intn(12); {
					case op < 2: // same id in two tables, one transaction
						if _, err = tx.Exec(ctx, `UPDATE documents SET title = $2 WHERE id = $1`, id, sentence(r, 4)); err == nil {
							_, err = tx.Exec(ctx, `INSERT INTO notes (id, body) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body`, id, sentence(r, 6))
						}
					case op < 4:
						_, err = tx.Exec(ctx, `INSERT INTO support.tickets (tenant_id, id, subject, body) VALUES ($1, $2, $3, $4)
							ON CONFLICT (tenant_id, id) DO UPDATE SET subject = EXCLUDED.subject, body = EXCLUDED.body`, tenant, id, sentence(r, 3), sentence(r, 5+r.Intn(40)))
					case op < 6:
						_, err = tx.Exec(ctx, `UPDATE support.tickets SET subject = $3 WHERE tenant_id = $1 AND id = $2`, tenant, id, sentence(r, 4))
					case op < 7:
						_, err = tx.Exec(ctx, `DELETE FROM support.tickets WHERE tenant_id = $1 AND id = $2`, tenant, id)
					case op < 8: // composite key change: move a ticket to another tenant
						_, err = tx.Exec(ctx, `UPDATE support.tickets SET tenant_id = $3 WHERE tenant_id = $1 AND id = $2
							AND NOT EXISTS (SELECT 1 FROM support.tickets WHERE tenant_id = $3 AND id = $2)`, tenant, id, 1+r.Intn(4))
					case op < 9: // composite key change: second column
						_, err = tx.Exec(ctx, `UPDATE support.tickets SET id = $3 WHERE tenant_id = $1 AND id = $2
							AND NOT EXISTS (SELECT 1 FROM support.tickets WHERE tenant_id = $1 AND id = $3)`, tenant, id, 300+r.Intn(300))
					case op < 10:
						_, err = tx.Exec(ctx, `DELETE FROM notes WHERE id = $1`, id)
					case op < 11:
						_, err = tx.Exec(ctx, `UPDATE documents SET id = $2 WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM documents WHERE id = $2)`, id, 300+r.Intn(300))
					default:
						_, err = tx.Exec(ctx, `INSERT INTO documents (id, title, body) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body`,
							id, sentence(r, 3), sentence(r, 5+r.Intn(40)))
					}
					if err != nil {
						failed = true
					}
				}
				if failed || r.Intn(5) == 0 {
					tx.Rollback(ctx)
					w.aborts.Add(1)
				} else if err := tx.Commit(ctx); err != nil {
					w.errs.Add(1)
				} else {
					w.commits.Add(1)
				}
				time.Sleep(pace/2 + time.Duration(r.Intn(int(pace/time.Millisecond)+1))*time.Millisecond)
			}
		}(i)
	}
	return w
}

func (e *env) jobsDone() bool {
	return e.scalar(`SELECT (count(*) FILTER (WHERE state <> 'done') = 0 AND count(*) > 0)::text FROM embedsync.backfill_jobs`) == "true"
}

func requirePipelines(t *testing.T, r verifyReport, names ...string) {
	t.Helper()
	for _, n := range names {
		p, ok := r.Pipelines[n]
		if !ok || p.SourceRows == 0 {
			t.Fatalf("verify did not cover pipeline %s with rows: %s", n, r)
		}
		for _, g := range r.Generations {
			c, ok := g.Pipelines[n]
			if !ok || c.ActualChunks == 0 || c.Missing+c.Extra+c.Stale != 0 {
				t.Fatalf("generation %s pipeline %s: %+v", g.Generation, n, c)
			}
		}
	}
}

// The 1A acceptance scenario: three tables through one slot with overlapping
// ids and a (tenant_id, id) key; backfill with deletes and key changes under
// way; transactions spanning tables; crashes immediately before and after the
// sink commit; then every pipeline verified at a barrier and feedback checked
// against the durable checkpoint after every crash.
func TestMultiPipelineAcceptance(t *testing.T) {
	e := newEnv(t, envOpts{batch: 23, pipelines: multiPipelines, setupSQL: multiSetup})
	e.seedMulti(300, 3, 120)
	// Slow embeddings down so the backfill is still running when the
	// deletes and key changes below happen.
	e.emb.delayMS.Store(15)
	w := e.startCrossWriters(3, 17, 40*time.Millisecond)
	p := e.start()
	waitFor(t, 30*time.Second, "backfill under way", func() bool {
		return e.scalar(`SELECT coalesce(sum(windows), 0)::text FROM embedsync.backfill_jobs`) != "0"
	})
	if e.jobsDone() {
		t.Fatal("backfill finished before the concurrent deletes; slow it down further")
	}
	// Deletes and key changes while windows are being applied.
	e.exec(`DELETE FROM support.tickets WHERE id % 13 = 0`)
	e.exec(`DELETE FROM documents WHERE id % 17 = 0`)
	e.exec(`DELETE FROM notes WHERE id % 19 = 0`)
	e.exec(`UPDATE support.tickets SET tenant_id = tenant_id + 10 WHERE id % 29 = 0`)
	e.exec(`UPDATE documents SET id = id + 5000 WHERE id % 31 = 0`)
	running := e.scalar(`SELECT count(*)::text FROM embedsync.backfill_jobs WHERE state <> 'done'`)
	t.Logf("deletes and key changes issued while %s backfill job(s) were still running", running)
	if running == "0" {
		t.Fatal("deletes did not overlap the backfill")
	}
	e.emb.delayMS.Store(0)
	waitFor(t, 180*time.Second, "backfill of every pipeline", e.jobsDone)
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.backfill_jobs`); n != "3" {
		t.Fatalf("expected one backfill job per pipeline, got %s", n)
	}
	p.kill()
	e.assertFeedbackNotAhead()

	crashes := 0
	for _, fp := range []string{"before_sink_commit", "after_commit_before_feedback"} {
		for _, hit := range []int{2, 5, 3} {
			p := e.start(fmt.Sprintf("EMBEDSYNC_FAILPOINTS=%s=%d", fp, hit))
			if code := p.wait(t, 60*time.Second); code != 86 {
				t.Fatalf("%s=%d: expected failpoint exit 86, got %d", fp, hit, code)
			}
			crashes++
			e.assertFeedbackNotAhead()
		}
	}
	w.halt()
	p = e.start()
	e.waitReady()
	r := e.requireConsistent("after cross-table writes, backfill deletes and commit crashes")
	requirePipelines(t, r, "docs", "notes", "tickets")
	e.assertFeedbackNotAhead()
	t.Logf("crashes=%d commits=%d aborts=%d writer_errors=%d source_rows=%d", crashes, w.commits.Load(), w.aborts.Load(), w.errs.Load(), r.SourceRows)
	if w.commits.Load() < 50 || w.aborts.Load() == 0 {
		t.Fatal("workload did not exercise commits and aborts")
	}

	// Overlapping ids are distinct documents: id 2 exists in documents and in
	// notes (same encoded key "2"), and as (1, 2) in tickets.
	e.exec(`INSERT INTO documents (id, title, body) VALUES (2, 'zephyr doc', 'zephyr obsidian xylophone') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body`)
	e.exec(`INSERT INTO notes (id, body) VALUES (2, 'zephyr obsidian xylophone') ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body`)
	e.exec(`INSERT INTO support.tickets (tenant_id, id, subject, body) VALUES (1, 2, 'zephyr', 'zephyr obsidian xylophone')
		ON CONFLICT (tenant_id, id) DO UPDATE SET subject = EXCLUDED.subject, body = EXCLUDED.body`)
	e.requireConsistent("after overlapping-id writes")
	n := e.scalar(`SELECT string_agg(pipeline || '=' || source_key, ' ' ORDER BY pipeline) FROM embedsync.chunks_v1
		WHERE chunk_no = 0 AND (source_key = '2' OR source_key = '["1","2"]')`)
	if n != `docs=2 notes=2 tickets=["1","2"]` {
		t.Fatalf("overlapping ids must be separate documents per pipeline, got %s", n)
	}
	_, body := e.httpGet("/search?q=zephyr+obsidian+xylophone&k=10&exact=true") // words the writers never use; exact, so ANN recall cannot hide a hit
	var res struct {
		Hits []struct{ Pipeline, Key string }
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("search: %v %s", err, body)
	}
	got := map[string]bool{}
	for _, h := range res.Hits {
		got[h.Pipeline+"="+h.Key] = true
	}
	for _, want := range []string{"docs=2", "notes=2", `tickets=["1","2"]`} {
		if !got[want] {
			t.Fatalf("search must return %s as its own hit: %s", want, body)
		}
	}
	// The column allowlists hold per pipeline.
	for _, secret := range []string{"private ", "internal "} {
		if c := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE content LIKE $1`, "%"+secret+"%"); c != "0" {
			t.Fatalf("non-allowlisted column text %q reached the sink", secret)
		}
	}
	pubs := e.scalar(`SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY schemaname, tablename) FROM pg_publication_tables WHERE pubname = 'embedsync_pub'`)
	if pubs != "embedsync_control.markers,public.documents,public.notes,support.tickets" {
		t.Fatalf("publication tables: %s", pubs)
	}
	if slots := e.scalar(`SELECT count(*)::text FROM pg_replication_slots WHERE database = $1`, e.db); slots != "1" {
		t.Fatalf("expected one replication slot, got %s", slots)
	}

	// A model migration covers every pipeline: the candidate is ready only
	// after all three backfills (one job each), and the coverage gate checks
	// every table. Writers keep running across all tables meanwhile.
	e.emb.delayMS.Store(5)
	w2 := e.startCrossWriters(2, 99, 60*time.Millisecond)
	done := make(chan struct{})
	var early atomic.Value
	go func() {
		defer close(done)
		for {
			// The candidate must not be ready while one of its jobs is open.
			st := e.scalar(`SELECT coalesce((SELECT state FROM embedsync.generations WHERE id = 'v2'), '') || ':' ||
				(SELECT count(*) FROM embedsync.backfill_jobs WHERE generation_id = 'v2' AND state IN ('pending','running'))::text`)
			if strings.HasPrefix(st, "ready:") && st != "ready:0" {
				early.CompareAndSwap(nil, st)
			}
			if strings.HasPrefix(st, "ready:0") || strings.HasPrefix(st, "active:") || strings.HasPrefix(st, "rejected:") {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	out, err := e.cli("migrate", "--to", "v2")
	<-done
	e.emb.delayMS.Store(0)
	w2.halt()
	if v := early.Load(); v != nil {
		t.Fatalf("candidate was ready with backfill jobs still open (%v)", v)
	}
	if err != nil {
		t.Fatalf("migrate across pipelines: %v\n%s", err, out)
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.backfill_jobs WHERE generation_id = 'v2'`); n != "3" {
		t.Fatalf("expected one v2 backfill job per pipeline, got %s", n)
	}
	r = e.requireConsistent("after migrating every pipeline to v2")
	requirePipelines(t, r, "docs", "notes", "tickets")
	if s := e.scalar(`SELECT generation_id FROM embedsync.serving`); s != "v2" {
		t.Fatalf("serving %s", s)
	}
	p.stop(t)
}

// A source transaction that touches two tables commits at the sink as one
// unit even when the embedding of one of its documents finishes long after
// the other's (and after later transactions' embeddings): a reader never sees
// one table's new version next to the other table's old version.
func TestCrossTableTransactionBoundary(t *testing.T) {
	e := newEnv(t, envOpts{pipelines: multiPipelines, setupSQL: multiSetup, embedBatch: 1})
	e.exec(`INSERT INTO documents (id, title, body) VALUES (1, 'pair', 'version 0')`)
	e.exec(`INSERT INTO support.tickets (tenant_id, id, subject, body) VALUES (7, 1, 'pair', 'version 0')`)
	p := e.start()
	e.waitReady()
	e.requireConsistent("initial pair")
	e.emb.slow.Store("slowpoke")
	e.emb.slowMS.Store(250)
	e.emb.recordDone.Store(true)

	verRe := regexp.MustCompile(`version (\d+)`)
	var samples, mismatches atomic.Int64
	var firstBad atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// One statement: one snapshot of both pipelines' documents.
			var d, tk *string
			err := e.admin.QueryRow(ctx, `SELECT
				(SELECT content FROM embedsync.chunks_v1 WHERE pipeline = 'docs' AND source_key = '1' AND chunk_no = 0),
				(SELECT content FROM embedsync.chunks_v1 WHERE pipeline = 'tickets' AND source_key = '["7","1"]' AND chunk_no = 0)`).Scan(&d, &tk)
			if err != nil || d == nil || tk == nil {
				continue
			}
			dv, tv := verRe.FindStringSubmatch(*d), verRe.FindStringSubmatch(*tk)
			samples.Add(1)
			if dv == nil || tv == nil || dv[1] != tv[1] {
				mismatches.Add(1)
				firstBad.CompareAndSwap(nil, fmt.Sprintf("docs=%q tickets=%q", *d, *tk))
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	const rounds = 24
	for n := 1; n <= rounds; n++ {
		// Odd rounds make the document's embedding slow, even rounds the
		// ticket's; each transaction updates both.
		dSlow, tSlow := "", ""
		if n%2 == 1 {
			dSlow = " slowpoke"
		} else {
			tSlow = " slowpoke"
		}
		tx, err := e.admin.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `UPDATE documents SET body = $1 WHERE id = 1`, fmt.Sprintf("version %d%s", n, dSlow)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `UPDATE support.tickets SET body = $1 WHERE tenant_id = 7 AND id = 1`, fmt.Sprintf("version %d%s", n, tSlow)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(20+rand.IntN(60)) * time.Millisecond)
	}
	waitFor(t, 60*time.Second, "last pair applied", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE content LIKE $1`, fmt.Sprintf("%%version %d%%", rounds)) == "2"
	})
	close(stop)
	wg.Wait()
	e.requireConsistent("after paired updates")
	e.assertFeedbackNotAhead()

	// Show that embeddings did finish out of order: some fast input of a
	// round completed before the slow input of the same round.
	e.emb.mu.Lock()
	done := append([]string(nil), e.emb.doneLog...)
	e.emb.mu.Unlock()
	pos := map[string]int{}
	for i, s := range done {
		if m := verRe.FindStringSubmatch(s); m != nil {
			key := m[1] + map[bool]string{true: "slow", false: "fast"}[strings.Contains(s, "slowpoke")]
			if _, ok := pos[key]; !ok {
				pos[key] = i
			}
		}
	}
	outOfOrder := 0
	for n := 1; n <= rounds; n++ {
		f, ok1 := pos[strconv.Itoa(n)+"fast"]
		s, ok2 := pos[strconv.Itoa(n)+"slow"]
		if ok1 && ok2 && f < s {
			outOfOrder++
		}
	}
	t.Logf("reader samples=%d mismatched=%d; rounds whose fast embedding finished before the slow one: %d/%d", samples.Load(), mismatches.Load(), outOfOrder, rounds)
	if mismatches.Load() != 0 {
		t.Fatalf("a reader saw a torn cross-table transaction: %v", firstBad.Load())
	}
	if samples.Load() < 50 || outOfOrder == 0 {
		t.Fatal("the test did not exercise out-of-order embedding under a concurrent reader")
	}
	p.stop(t)
}

// Composite-key order: PostgreSQL's row comparison over typed columns (int,
// then text with COLLATE "C"), the backfill scan's windows, the sink-side
// range predicate over encoded keys and keys.Codec.Compare must all agree,
// including negative ints, mixed case, UTF-8, digits-as-text and characters
// that need JSON escaping.
func TestCompositeKeyOrderAgreesWithPostgres(t *testing.T) {
	setup := []string{`CREATE TABLE public.labels (tenant int NOT NULL, slug text NOT NULL, body text, PRIMARY KEY (tenant, slug))`}
	pipes := `  - name: labels
    table: public.labels
    key: [tenant, slug]
    fields: [{name: body, column: body}]
    transform: {template: "label {{.body}}", chunk_words: 20, chunk_overlap: 4}
`
	e := newEnv(t, envOpts{batch: 3, pipelines: pipes, setupSQL: setup})
	slugs := []string{"a", "B", "b", "Z", "aa", "é", "e", "10", "9", "x,y", `q"`, `back\slash`, "<&>", "", " lead", "日本"}
	ctx := context.Background()
	var all []string
	codec := keys.NewCodec(keys.Int, keys.Text)
	for _, tenant := range []int{-7, -1, 0, 1, 2, 10, 11} {
		for _, s := range slugs {
			e.exec(`INSERT INTO labels (tenant, slug, body) VALUES ($1, $2, $3)`, tenant, s, fmt.Sprintf("label %d %s", tenant, s))
			k, err := codec.Encode([]string{strconv.Itoa(tenant), s})
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, k)
		}
	}
	sort.Slice(all, func(i, j int) bool { return codec.Compare(all[i], all[j]) < 0 })

	// 1. Backfill scan windows, as the executor issues them.
	def := &config.PipelineDef{Name: "labels", Table: "public.labels", Key: config.KeyColumns{"tenant", "slug"}, Fields: []config.Field{{Name: "body", Column: "body"}}}
	var scanned []string
	var lo *string
	for i := 0; i < 1000; i++ {
		q, args, err := pipeline.ScanQuery(def, codec, lo, 5)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := e.admin.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		n := 0
		for rows.Next() {
			var tenant, slug string
			var body *string
			if err := rows.Scan(&tenant, &slug, &body); err != nil {
				t.Fatal(err)
			}
			k, _ := codec.Encode([]string{tenant, slug})
			scanned = append(scanned, k)
			lo = &k
			n++
		}
		rows.Close()
		if n < 5 {
			break
		}
	}
	if strings.Join(scanned, "\n") != strings.Join(all, "\n") {
		t.Fatalf("PostgreSQL window order differs from keys.Codec order:\n pg: %v\ngo: %v", scanned, all)
	}

	// 2. The sink-side range predicate over encoded keys.
	e.exec(`CREATE TABLE enc (k text)`)
	for _, k := range all {
		e.exec(`INSERT INTO enc VALUES ($1)`, k)
	}
	rng := rand.New(rand.NewPCG(5, 6))
	for i := 0; i < 200; i++ {
		a, b := all[rng.IntN(len(all))], all[rng.IntN(len(all))]
		var args []any
		p1, err := codec.SinkCompare("k", ">", a, &args)
		if err != nil {
			t.Fatal(err)
		}
		p2, _ := codec.SinkCompare("k", "<=", b, &args)
		var got int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM enc WHERE `+p1+` AND `+p2, args...).Scan(&got); err != nil {
			t.Fatal(err)
		}
		want := 0
		for _, k := range all {
			if codec.Compare(k, a) > 0 && codec.Compare(k, b) <= 0 {
				want++
			}
		}
		if got != want {
			t.Fatalf("range (%s, %s]: PostgreSQL counts %d, keys.Codec %d", a, b, got, want)
		}
	}

	// 3. End to end: tiny windows, with deletes and key changes racing them.
	e.emb.delayMS.Store(20)
	p := e.start()
	waitFor(t, 30*time.Second, "first windows", func() bool {
		return e.scalar(`SELECT coalesce(sum(windows), 0)::text FROM embedsync.backfill_jobs`) != "0"
	})
	e.exec(`DELETE FROM labels WHERE slug IN ('B', 'é', '')`)
	e.exec(`UPDATE labels SET tenant = tenant + 100 WHERE slug = 'Z'`)
	e.exec(`UPDATE labels SET slug = slug || '~' WHERE slug = 'aa'`)
	e.emb.delayMS.Store(0)
	waitFor(t, 120*time.Second, "backfill", e.jobsDone)
	r := e.requireConsistent("composite text keys")
	requirePipelines(t, r, "labels")
	p.stop(t)
}

// legacyBinary builds embedsync at EMBEDSYNC_IT_LEGACY_REF (default v0.2.0,
// the last single-table release) from this repository's git history.
func legacyBinary(t *testing.T) string {
	ref := os.Getenv("EMBEDSYNC_IT_LEGACY_REF")
	if ref == "" {
		ref = "v0.2.0"
	}
	return binaryAt(t, ref)
}

// binaryAt builds embedsync at a git ref of this repository.
func binaryAt(t *testing.T, ref string) string {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	archive := exec.Command("sh", "-c", fmt.Sprintf(`git -C ../.. archive --format=tar %s | tar -x -C %s`, ref, src))
	if out, err := archive.CombinedOutput(); err != nil {
		t.Fatalf("export %s from git (fetch tags: git fetch --tags): %v\n%s", ref, err, out)
	}
	bin := filepath.Join(dir, "embedsync-legacy")
	build := exec.Command("go", "build", "-o", bin, "./cmd/embedsync")
	build.Dir = src
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", ref, err, out)
	}
	out, _ := exec.Command(bin, "version").CombinedOutput()
	t.Logf("legacy binary from %s: %s", ref, strings.TrimSpace(string(out)))
	return bin
}

type chunkRow struct{ Key, No, FP, Emb, LSN, Updated string }

func (e *env) chunkRows(table, pipelineFilter string) []chunkRow {
	e.t.Helper()
	q := `SELECT source_key, chunk_no::text, fingerprint, embedding::text, source_lsn::text, updated_at::text FROM embedsync.` + table
	if pipelineFilter != "" {
		q += ` WHERE pipeline = '` + pipelineFilter + `'`
	}
	rows, err := e.admin.Query(context.Background(), q+` ORDER BY pipeline, source_key, chunk_no`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []chunkRow
	for rows.Next() {
		var r chunkRow
		if err := rows.Scan(&r.Key, &r.No, &r.FP, &r.Emb, &r.LSN, &r.Updated); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func (e *env) searchKeys(q, gen string) string {
	e.t.Helper()
	args := []string{"search", "--json", "--k", "8"}
	if gen != "" {
		args = append(args, "--generation", gen)
	}
	out, err := e.cli(append(args, q)...)
	if err != nil {
		e.t.Fatalf("search: %v %s", err, out)
	}
	var res struct {
		Generation string
		Hits       []struct {
			Pipeline string
			Key      string
			Score    float64
		}
	}
	if err := json.Unmarshal([]byte(out[strings.Index(out, "{"):]), &res); err != nil {
		e.t.Fatalf("search json: %v %s", err, out)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s:", res.Generation)
	for _, h := range res.Hits {
		fmt.Fprintf(&b, " %s(%.4f)", h.Key, h.Score)
		if h.Pipeline != "" && h.Pipeline != "default" {
			e.t.Fatalf("legacy documents must be served as pipeline default, got %q", h.Pipeline)
		}
	}
	return b.String()
}

// A populated installation created by the last single-table release keeps
// serving its original documents after the upgrade: same chunks (not
// re-embedded or rewritten), same search results, its DLQ record, migration
// history and rollback target intact. Then a composite-key table is added as
// a second pipeline in place.
func TestLegacyUpgradeServesOriginalDocuments(t *testing.T) {
	legacy := legacyBinary(t)
	e := newEnv(t, envOpts{binary: legacy})
	e.seed(60, 12)
	p := e.start()
	e.waitReady()
	if out, err := e.cli("migrate", "--to", "v2"); err != nil {
		t.Fatalf("legacy migrate: %v %s", err, out)
	}
	// v2 serves, v1 is the rollback target; a poison row is quarantined in both.
	e.emb.reject.Store("POISON")
	e.exec(`INSERT INTO documents (id, title, body) VALUES (500, 'bad', 'POISON row'), (501, 'tricky', 'harbor violin quartz')`)
	waitFor(t, 20*time.Second, "quarantine", func() bool {
		return e.scalar(`SELECT count(*)::text FROM embedsync.dlq WHERE state = 'open'`) == "2"
	})
	e.requireConsistent("legacy installation")
	queries := []string{"seed body word 17", "harbor violin quartz", "title 42"}
	before := map[string]string{}
	for _, q := range queries {
		before[q+"|"] = e.searchKeys(q, "")
		before[q+"|v1"] = e.searchKeys(q, "v1")
	}
	chunksV1, chunksV2 := e.chunkRows("chunks_v1", ""), e.chunkRows("chunks_v2", "")
	p.stop(t)
	e.emb.reject.Store("")

	// The new binary refuses to touch the old schema until `init` upgrades it.
	e.bin = binary
	out, err := e.cli("status")
	if err == nil || !strings.Contains(out, "run `embedsync init`") {
		t.Fatalf("status against an un-upgraded sink must ask for init: %v %s", err, out)
	}
	out, err = e.cli("init")
	if err != nil || !strings.Contains(out, "sink: applied sink/0002_pipelines.sql") {
		t.Fatalf("upgrade init: %v %s", err, out)
	}
	embedsBefore := e.emb.embeds.Load()
	p = e.start()
	e.waitReady()
	time.Sleep(3 * time.Second)
	if n := e.emb.embeds.Load() - embedsBefore; n != 0 {
		t.Fatalf("the upgrade re-embedded %d inputs; existing chunks must be reused as they are", n)
	}
	if got := e.chunkRows("chunks_v1", "default"); fmt.Sprint(got) != fmt.Sprint(chunksV1) {
		t.Fatalf("chunks_v1 changed by the upgrade")
	}
	if got := e.chunkRows("chunks_v2", "default"); fmt.Sprint(got) != fmt.Sprint(chunksV2) {
		t.Fatalf("chunks_v2 changed by the upgrade")
	}
	for _, q := range queries {
		if got := e.searchKeys(q, ""); got != before[q+"|"] {
			t.Fatalf("serving results for %q changed:\n before %s\n after  %s", q, before[q+"|"], got)
		}
		if got := e.searchKeys(q, "v1"); got != before[q+"|v1"] {
			t.Fatalf("v1 results for %q changed", q)
		}
	}
	r := e.requireConsistent("after upgrade")
	for _, g := range r.Generations {
		if g.Quarantined != 1 {
			t.Fatalf("the open DLQ records must survive the upgrade: %s", r)
		}
	}
	st := e.status()
	pipes := st["pipelines"].([]any)
	if len(pipes) != 1 || pipes[0].(map[string]any)["name"] != "default" || pipes[0].(map[string]any)["key_kinds"] != "int" {
		t.Fatalf("status pipelines: %v", st["pipelines"])
	}
	if out, err := e.cli("dlq", "list"); err != nil || !strings.Contains(out, "pipeline=default key=500") {
		t.Fatalf("dlq list: %v %s", err, out)
	}
	if out, err := e.cli("dlq", "replay"); err != nil || !strings.Contains(out, `"resolved": 2`) {
		t.Fatalf("dlq replay after upgrade: %v %s", err, out)
	}
	if out, err := e.cli("migrate", "rollback"); err != nil || !strings.Contains(out, "serving v1 again") {
		t.Fatalf("rollback to the legacy previous generation: %v %s", err, out)
	}
	e.exec(`UPDATE documents SET body = 'edited after upgrade' WHERE id = 3`)
	e.requireConsistent("after replay, rollback and an edit")
	p.stop(t)

	// Add a composite-key table as a second pipeline: the documents table
	// becomes the explicitly named "default" pipeline.
	e.exec(`CREATE SCHEMA support`)
	e.exec(`CREATE TABLE support.tickets (tenant_id int, id bigint, subject text, body text, PRIMARY KEY (tenant_id, id))`)
	e.exec(`INSERT INTO support.tickets SELECT t, g, 'subject ' || g, 'ticket body ' || t || ' ' || g FROM generate_series(1, 3) t, generate_series(1, 40) g`)
	e.opts.pipelines = `  - name: default
    table: public.documents
    key: id
    fields: [{name: title, column: title}, {name: body, column: body}]
  - name: tickets
    table: support.tickets
    key: [tenant_id, id]
    fields: [{name: subject, column: subject}, {name: body, column: body}]
    transform: {template: "ticket {{.subject}}\n{{.body}}", chunk_words: 20, chunk_overlap: 4}
`
	e.writeConfig()
	out, err = e.cli("init")
	if err != nil || !strings.Contains(out, "publication embedsync_pub: added support.tickets") {
		t.Fatalf("init after adding a pipeline: %v %s", err, out)
	}
	defaultV1 := e.chunkRows("chunks_v1", "default")
	p = e.start()
	e.waitReady()
	waitFor(t, 60*time.Second, "new pipeline backfilled into both live generations", e.jobsDone)
	e.exec(`UPDATE support.tickets SET id = 99 WHERE tenant_id = 2 AND id = 5`)
	r = e.requireConsistent("after adding the tickets pipeline")
	requirePipelines(t, r, "default", "tickets")
	if len(r.Generations) != 2 {
		t.Fatalf("both live generations must cover the new pipeline: %s", r)
	}
	if got := e.chunkRows("chunks_v1", "default"); fmt.Sprint(got) != fmt.Sprint(defaultV1) {
		t.Fatal("adding a pipeline rewrote the existing pipeline's chunks")
	}
	p.stop(t)

	// Dropping the pipeline from the config must not drop its table from the
	// publication: restoring it later would otherwise leave a gap in its
	// change stream that nothing backfills.
	full := e.opts.pipelines
	e.opts.pipelines = full[:strings.Index(full, "  - name: tickets")]
	e.writeConfig()
	out, err = e.cli("init")
	if err != nil || !strings.Contains(out, "still publishes support.tickets") {
		t.Fatalf("init without the tickets pipeline: %v %s", err, out)
	}
	if n := e.scalar(`SELECT count(*)::text FROM pg_publication_tables WHERE pubname = 'embedsync_pub' AND tablename = 'tickets'`); n != "1" {
		t.Fatal("init removed a synced table from the publication")
	}
	e.opts.pipelines = full
	e.writeConfig()
	e.exec(`UPDATE support.tickets SET subject = 'changed while unconfigured' WHERE id = 7`)
	p = e.start()
	e.waitReady()
	e.requireConsistent("after removing and restoring the tickets pipeline")
	p.stop(t)
}
