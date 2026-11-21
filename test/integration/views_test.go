//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A knowledge base: documents with ordered sections and tags through a link
// table, every table keyed by tenant first. No foreign keys, so tests can
// create orphans and apply changes in any order.
var kbSetup = []string{
	`CREATE TABLE public.kb_documents (tenant_id int, id bigint, title text, secret text, PRIMARY KEY (tenant_id, id))`,
	`CREATE TABLE public.kb_sections (tenant_id int, id bigint, document_id bigint, position int, heading text, body text, PRIMARY KEY (tenant_id, id))`,
	`CREATE TABLE public.kb_tags (tenant_id int, id bigint, name text, PRIMARY KEY (tenant_id, id))`,
	`CREATE TABLE public.kb_document_tags (tenant_id int, document_id bigint, tag_id bigint, PRIMARY KEY (tenant_id, document_id, tag_id))`,
}

const kbView = `  - name: kb
    tenant: tenant_id
    table: public.kb_documents
    key: [tenant_id, id]
    fields: [{name: title, column: title}]
    children:
      - name: sections
        table: public.kb_sections
        key: [tenant_id, id]
        parent: [tenant_id, document_id]
        order_by: [position]
        fields: [{name: heading, column: heading}, {name: body, column: body}]
    links:
      - name: tags
        table: public.kb_document_tags
        key: [tenant_id, document_id, tag_id]
        parent: [tenant_id, document_id]
        target: [tenant_id, tag_id]
        target_table: public.kb_tags
        target_key: [tenant_id, id]
        target_fields: [{name: name, column: name}]
        order_by: [name]
    transform: {template: "{{.title}}{{range .tags}} #{{.name}}{{end}}{{range .sections}} | {{.heading}}: {{.body}}{{end}}", chunk_words: 20, chunk_overlap: 4}
    max_children: 200
    max_fanout_per_txn: 40
`

// seedKB creates docs documents per tenant, each with sections sections and
// two of the tenant's five tags. Tenants reuse the same ids.
func (e *env) seedKB(tenants, docs, sections int) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO kb_documents SELECT t, d, 'doc ' || t || '/' || d, 'secret' FROM generate_series(1, %d) t, generate_series(1, %d) d`, tenants, docs))
	e.exec(fmt.Sprintf(`INSERT INTO kb_sections SELECT t, d * 100 + s, d, (s * 7) %% 5, 'h' || s, 'body of section ' || s || ' of doc ' || d
		FROM generate_series(1, %d) t, generate_series(1, %d) d, generate_series(1, %d) s`, tenants, docs, sections))
	e.exec(fmt.Sprintf(`INSERT INTO kb_tags SELECT t, g, 'tag' || g || 't' || t FROM generate_series(1, %d) t, generate_series(1, 5) g`, tenants))
	e.exec(fmt.Sprintf(`INSERT INTO kb_document_tags SELECT t, d, 1 + (d %% 5) FROM generate_series(1, %d) t, generate_series(1, %d) d
		UNION SELECT t, d, 1 + ((d + 2) %% 5) FROM generate_series(1, %d) t, generate_series(1, %d) d`, tenants, docs, tenants, docs))
}

func (e *env) kbDoc(gen, key string) string {
	e.t.Helper()
	return e.scalar(`SELECT coalesce(string_agg(content, ' ' ORDER BY chunk_no), '') FROM embedsync.chunks_`+gen+` WHERE pipeline = 'kb' AND source_key = $1`, key)
}

// The 1B acceptance changes, one by one, each checked against the
// independently assembled source at a barrier.
func TestViewAcceptance(t *testing.T) {
	e := newEnv(t, envOpts{batch: 7, views: kbView, setupSQL: kbSetup})
	e.seedKB(2, 12, 4)
	p := e.start()
	e.waitReady()
	waitFor(t, 120*time.Second, "state and render backfills", e.jobsDone)
	r := e.requireConsistent("after view backfill")
	if r.Generations[0].Pipelines["kb"].ActualChunks == 0 {
		t.Fatalf("no view documents: %s", r)
	}
	if strings.Contains(e.kbDoc("v1", `["1","1"]`), "secret") {
		t.Fatal("a non-allowlisted column reached a view document")
	}

	// Moving a section from document 1 to 2 updates both parents.
	e.exec(`UPDATE kb_sections SET heading = 'moved', document_id = 2 WHERE tenant_id = 1 AND id = 101`)
	e.requireConsistent("after moving a section")
	if a, b := e.kbDoc("v1", `["1","1"]`), e.kbDoc("v1", `["1","2"]`); strings.Contains(a, "moved") || !strings.Contains(b, "moved") {
		t.Fatalf("section move: doc1=%q doc2=%q", a, b)
	}

	// Renaming a shared tag updates every linked document, in one sink
	// transaction, and only within the tenant.
	before := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND content LIKE '%renamedtag%'`)
	e.exec(`UPDATE kb_tags SET name = 'renamedtag' WHERE tenant_id = 1 AND id = 2`)
	e.requireConsistent("after renaming a shared tag")
	linked := e.scalar(`SELECT count(*)::text FROM kb_document_tags WHERE tenant_id = 1 AND tag_id = 2`)
	got := e.scalar(`SELECT count(DISTINCT source_key)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND content LIKE '%renamedtag%'`)
	if before != "0" || got != linked || linked == "0" {
		t.Fatalf("tag rename reached %s documents, %s are linked", got, linked)
	}
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND source_key LIKE '["2",%' AND content LIKE '%renamedtag%'`); n != "0" {
		t.Fatal("a tag rename crossed tenants")
	}

	// Deleting a link, then a parent, removes the obsolete content.
	e.exec(`DELETE FROM kb_document_tags WHERE tenant_id = 1 AND document_id = 3`)
	e.requireConsistent("after deleting links")
	if d := e.kbDoc("v1", `["1","3"]`); strings.Contains(d, "#") {
		t.Fatalf("deleted links still rendered: %q", d)
	}
	e.exec(`DELETE FROM kb_documents WHERE tenant_id = 1 AND id = 4`)
	r = e.requireConsistent("after deleting a parent")
	if d := e.kbDoc("v1", `["1","4"]`); d != "" {
		t.Fatalf("deleted parent still searchable: %q", d)
	}
	if r.ViewOrphans == 0 {
		t.Fatal("the deleted parent's sections should be reported as orphans")
	}

	// Many child edits in one transaction: one final representation.
	lsnBefore := e.scalar(`SELECT max(source_lsn)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND source_key = '["2","5"]'`)
	tx, err := e.admin.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, q := range []string{
		`UPDATE kb_sections SET body = 'edit one' WHERE tenant_id = 2 AND id = 501`,
		`UPDATE kb_sections SET body = 'edit two', position = 9 WHERE tenant_id = 2 AND id = 502`,
		`INSERT INTO kb_sections VALUES (2, 599, 5, 0, 'new', 'inserted first')`,
		`DELETE FROM kb_sections WHERE tenant_id = 2 AND id = 503`,
		`UPDATE kb_documents SET title = 'retitled' WHERE tenant_id = 2 AND id = 5`,
		`INSERT INTO kb_document_tags VALUES (2, 5, 5)`,
		`UPDATE kb_tags SET name = 'aaa first' WHERE tenant_id = 2 AND id = 5`,
	} {
		if _, err := tx.Exec(context.Background(), q); err != nil {
			t.Fatalf("edit %d: %v", i, err)
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.requireConsistent("after many child edits in one transaction")
	doc := e.kbDoc("v1", `["2","5"]`)
	if !strings.HasPrefix(doc, "retitled #aaa first") || !strings.Contains(doc, "new: inserted first") || strings.Contains(doc, "body of section 3 of doc 5") {
		t.Fatalf("final representation: %q", doc)
	}
	lsns := e.scalar(`SELECT count(DISTINCT source_lsn)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND source_key = '["2","5"]'`)
	if lsns != "1" || e.scalar(`SELECT max(source_lsn)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND source_key = '["2","5"]'`) == lsnBefore {
		t.Fatalf("the document must be rewritten once, at the transaction's position (distinct lsns %s)", lsns)
	}
	st := e.status()
	var kinds []string
	for _, p := range st["pipelines"].([]any) {
		m := p.(map[string]any)
		kinds = append(kinds, fmt.Sprint(m["name"], "=", m["kind"]))
	}
	b, _ := json.Marshal(kinds)
	if !strings.Contains(string(b), "kb=view") || !strings.Contains(string(b), "kb:sections=view_table") {
		t.Fatalf("status pipelines: %s", b)
	}
	p.stop(t)
}

// kbWriters change every table of the view concurrently: section edits and
// moves between documents (within the tenant), inserts and deletes, link
// changes, tag renames, document deletes and re-inserts. ~20 % roll back.
func (e *env) kbWriters(n int, seed uint64, tenants, docs int, pace time.Duration) *writer {
	w := &writer{stop: make(chan struct{})}
	for i := 0; i < n; i++ {
		w.wg.Add(1)
		go func(i int) {
			defer w.wg.Done()
			r := &mrand{x: seed + uint64(i)*6151 + 1}
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
				for j := 0; j < 1+r.Intn(5) && !failed; j++ {
					t := 1 + r.Intn(tenants)
					d := 1 + r.Intn(docs)
					sec := d*100 + 1 + r.Intn(8)
					var err error
					switch op := r.Intn(14); {
					case op < 3:
						_, err = tx.Exec(ctx, `UPDATE kb_sections SET body = $3 WHERE tenant_id = $1 AND id = $2`, t, sec, sentence(r, 4+r.Intn(30)))
					case op < 5: // move a section to another document
						_, err = tx.Exec(ctx, `UPDATE kb_sections SET document_id = $3, position = $4 WHERE tenant_id = $1 AND id = $2`, t, sec, 1+r.Intn(docs), r.Intn(6))
					case op < 6:
						_, err = tx.Exec(ctx, `INSERT INTO kb_sections VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tenant_id, id) DO UPDATE SET heading = EXCLUDED.heading`,
							t, sec, d, r.Intn(6), sentence(r, 2), sentence(r, 10))
					case op < 7:
						_, err = tx.Exec(ctx, `DELETE FROM kb_sections WHERE tenant_id = $1 AND id = $2`, t, sec)
					case op < 8:
						_, err = tx.Exec(ctx, `INSERT INTO kb_document_tags VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, t, d, 1+r.Intn(5))
					case op < 9:
						_, err = tx.Exec(ctx, `DELETE FROM kb_document_tags WHERE tenant_id = $1 AND document_id = $2 AND tag_id = $3`, t, d, 1+r.Intn(5))
					case op < 10: // rename a tag shared by several documents
						_, err = tx.Exec(ctx, `UPDATE kb_tags SET name = $3 WHERE tenant_id = $1 AND id = $2`, t, 1+r.Intn(5), sentence(r, 1))
					case op < 11:
						_, err = tx.Exec(ctx, `UPDATE kb_documents SET title = $3 WHERE tenant_id = $1 AND id = $2`, t, d, sentence(r, 3))
					case op < 12:
						_, err = tx.Exec(ctx, `DELETE FROM kb_documents WHERE tenant_id = $1 AND id = $2`, t, d)
					case op < 13:
						_, err = tx.Exec(ctx, `INSERT INTO kb_documents VALUES ($1, $2, $3, 'x') ON CONFLICT DO NOTHING`, t, d, sentence(r, 3))
					default: // a document key change
						_, err = tx.Exec(ctx, `UPDATE kb_documents SET id = $3 WHERE tenant_id = $1 AND id = $2
							AND NOT EXISTS (SELECT 1 FROM kb_documents WHERE tenant_id = $1 AND id = $3)`, t, d, docs+1+r.Intn(docs))
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

// Concurrent writes to every table of the view during its state and render
// backfills, crashes immediately before and after the sink commit, a TOASTed
// section edited without touching its body, and a truncated child table:
// after restart every document equals the independently assembled source at
// a barrier, and feedback never passed the durable checkpoint.
func TestViewWritesBackfillAndCrashes(t *testing.T) {
	e := newEnv(t, envOpts{batch: 9, views: strings.Replace(kbView, "max_fanout_per_txn: 40", "max_fanout_per_txn: 100000", 1), setupSQL: kbSetup})
	e.seedKB(3, 40, 5)
	big := strings.Repeat("toasted section body word ", 400) // ~10 KB, stored out of line
	e.exec(`UPDATE kb_sections SET body = $1 WHERE id % 50 = 1`, big)
	e.emb.delayMS.Store(10)
	w := e.kbWriters(3, 21, 3, 40, 40*time.Millisecond)
	p := e.start()
	waitFor(t, 60*time.Second, "backfill under way", func() bool {
		return e.scalar(`SELECT coalesce(sum(windows), 0)::text FROM embedsync.backfill_jobs`) != "0"
	})
	if e.jobsDone() {
		t.Fatal("backfill finished before the concurrent writes; slow it down")
	}
	e.emb.delayMS.Store(0)
	waitFor(t, 180*time.Second, "state and render backfills", e.jobsDone)
	p.kill()
	e.assertFeedbackNotAhead()
	crashes := 0
	for _, fp := range []string{"before_sink_commit", "after_commit_before_feedback"} {
		for _, hit := range []int{2, 4, 3} {
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
	r := e.requireConsistent("after concurrent view writes, backfill and crashes")
	t.Logf("crashes=%d commits=%d aborts=%d writer_errors=%d", crashes, w.commits.Load(), w.aborts.Load(), w.errs.Load())
	if w.commits.Load() < 50 || r.Generations[0].Pipelines["kb"].ActualChunks == 0 {
		t.Fatal("workload too small")
	}

	// Unchanged TOAST: only the heading of a section with a large body.
	e.exec(`UPDATE kb_sections SET heading = 'toast heading' WHERE id % 50 = 1 AND tenant_id = 1`)
	e.requireConsistent("after heading-only updates of toasted sections")

	// Truncating a child table empties every document's sections.
	e.exec(`TRUNCATE kb_sections`)
	e.exec(`INSERT INTO kb_sections VALUES (1, 101, 1, 0, 'after truncate', 'fresh section')`)
	e.requireConsistent("after truncating the child table")
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE pipeline = 'kb' AND content LIKE '%body of section%'`); n != "0" {
		t.Fatal("truncated sections are still searchable")
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
}

// A transaction that changes more documents than max_fanout_per_txn pauses
// before acknowledging it; raising the limit resumes and applies it whole.
func TestViewFanoutLimitPauses(t *testing.T) {
	e := newEnv(t, envOpts{views: kbView, setupSQL: kbSetup})
	e.seedKB(1, 60, 1)
	e.exec(`INSERT INTO kb_document_tags SELECT 1, d, 5 FROM generate_series(1, 60) d ON CONFLICT DO NOTHING`)
	p := e.start()
	e.waitReady()
	waitFor(t, 60*time.Second, "backfill", e.jobsDone)
	e.requireConsistent("initial")
	cp := e.checkpointLSN()
	e.exec(`UPDATE kb_tags SET name = 'everywhere' WHERE tenant_id = 1 AND id = 5`) // 60 documents > 40
	waitFor(t, 20*time.Second, "fan-out pause", func() bool {
		return e.scalar(`SELECT state || ':' || reason FROM embedsync.pipeline_status`) == "paused:fanout_limit"
	})
	if n := e.scalar(`SELECT count(*)::text FROM embedsync.chunks_v1 WHERE content LIKE '%everywhere%'`); n != "0" {
		t.Fatal("a paused transaction was partly applied")
	}
	if e.scalar(`SELECT ($1::pg_lsn = (SELECT lsn FROM embedsync.checkpoints))::text`, cp) != "true" {
		t.Fatal("the checkpoint moved past the paused transaction")
	}
	e.assertFeedbackNotAhead()
	p.stop(t)
	e.opts.views = strings.Replace(kbView, "max_fanout_per_txn: 40", "max_fanout_per_txn: 100", 1)
	e.writeConfig()
	p = e.start()
	e.waitReady()
	e.requireConsistent("after raising the limit")
	if n := e.scalar(`SELECT count(DISTINCT source_key)::text FROM embedsync.chunks_v1 WHERE content LIKE '%everywhere%'`); n != "60" {
		t.Fatalf("rename reached %s of 60 documents", n)
	}
	p.stop(t)
}
