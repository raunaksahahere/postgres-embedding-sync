// Package verify recomputes the expected sink from source rows and compares it
// with the entire sink: every chunk of every document of every pipeline,
// including stale leftovers. Documents are identified by (pipeline, key).
package verify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// Row is one expected document (encoded key, allowlisted fields, nil = NULL).
type Row struct {
	Key    string
	Fields map[string]*string
}

// Rows holds each pipeline's rows.
type Rows map[string][]Row

// Count returns the total number of rows.
func (r Rows) Count() int {
	n := 0
	for _, rs := range r {
		n += len(rs)
	}
	return n
}

// GenReport compares one generation with the expected rows.
type GenReport struct {
	Generation      string   `json:"generation"`
	ExpectedChunks  int      `json:"expected_chunks"`
	ActualChunks    int      `json:"actual_chunks"`
	Missing         int      `json:"missing"`
	Extra           int      `json:"extra"`
	Stale           int      `json:"stale"`
	Quarantined     int      `json:"quarantined_keys"`
	MissingQuarant  int      `json:"missing_due_to_quarantine"`
	PoisonExpected  int      `json:"expected_poison_keys"`
	ExampleProblems []string `json:"example_problems,omitempty"`
	// Pipelines breaks the chunk counts down per pipeline.
	Pipelines map[string]*PipeCounts `json:"pipelines"`
}

// PipeCounts is one pipeline's share of a GenReport.
type PipeCounts struct {
	ExpectedChunks int `json:"expected_chunks"`
	ActualChunks   int `json:"actual_chunks"`
	Missing        int `json:"missing"`
	Extra          int `json:"extra"`
	Stale          int `json:"stale"`
}

// OK reports zero missing, extra and stale entries (quarantined rows are reported separately).
func (r GenReport) OK() bool { return r.Missing == 0 && r.Extra == 0 && r.Stale == 0 }

type chunkKey struct {
	pipeline string
	key      string
	no       int
}

type chunkVal struct{ fp, text string }

// Expected computes chunks for every pipeline's rows under one generation.
// Rows whose rendered document exceeds maxDocBytes are expected to be absent
// (quarantined).
func Expected(trs map[string]*transform.Transformer, m transform.ModelIdentity, rows Rows, maxDocBytes int) (map[chunkKey]chunkVal, map[chunkKey]bool, error) {
	out := map[chunkKey]chunkVal{}
	poison := map[chunkKey]bool{}
	for pipe, rs := range rows {
		tr := trs[pipe]
		if tr == nil {
			return nil, nil, fmt.Errorf("no transform for pipeline %q", pipe)
		}
		for _, r := range rs {
			vals := map[string]string{}
			for k, v := range r.Fields {
				if v != nil {
					vals[k] = *v
				}
			}
			text, err := tr.Render(vals)
			if err != nil || len(text) > maxDocBytes {
				poison[chunkKey{pipeline: pipe, key: r.Key}] = true
				continue
			}
			for _, c := range tr.Chunks(r.Key, text, m) {
				out[chunkKey{pipe, r.Key, c.No}] = chunkVal{c.Fingerprint, c.Text}
			}
		}
	}
	return out, poison, nil
}

// Querier is a pgx query interface (pool or tx).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ViewDocs holds a view's assembled documents (encoded root key -> doc).
type ViewDocs struct {
	Docs        map[string]*viewdoc.Doc
	MaxChildren int
}

// ExpectedViews computes chunks for assembled view documents under one
// generation; documents over max_children or max_document_bytes, or that
// fail to render, are expected to be absent (quarantined).
func ExpectedViews(trs map[string]*transform.Transformer, m transform.ModelIdentity, views map[string]ViewDocs, maxDocBytes int,
	out map[chunkKey]chunkVal, poison map[chunkKey]bool) error {
	for view, vd := range views {
		tr := trs[view]
		if tr == nil {
			return fmt.Errorf("no transform for view %q", view)
		}
		for key, d := range vd.Docs {
			if _, n := d.MaxList(); n > vd.MaxChildren {
				poison[chunkKey{pipeline: view, key: key}] = true
				continue
			}
			text, err := tr.RenderData(d.Data())
			if err != nil || len(text) > maxDocBytes {
				poison[chunkKey{pipeline: view, key: key}] = true
				continue
			}
			for _, c := range tr.Chunks(key, text, m) {
				out[chunkKey{view, key, c.No}] = chunkVal{c.Fingerprint, c.Text}
			}
		}
	}
	return nil
}

// Generation compares a generation's chunk table with every standalone
// pipeline's rows and every view's assembled documents.
func Generation(ctx context.Context, q Querier, schema string, g *store.Generation, trs map[string]*transform.Transformer, rows Rows, views map[string]ViewDocs, maxDocBytes int) (*GenReport, error) {
	m := transform.ModelIdentity{Name: g.ModelName, Revision: g.ModelRevision, DocumentPrefix: g.DocumentPrefix}
	exp, poison, err := Expected(trs, m, rows, maxDocBytes)
	if err != nil {
		return nil, err
	}
	if err := ExpectedViews(trs, m, views, maxDocBytes, exp, poison); err != nil {
		return nil, err
	}
	quarantined := map[chunkKey]bool{}
	qr, err := q.Query(ctx, `SELECT pipeline, source_key FROM `+pgx.Identifier{schema, "dlq"}.Sanitize()+` WHERE generation_id = $1 AND state = 'open'`, g.ID)
	if err != nil {
		return nil, err
	}
	for qr.Next() {
		var k chunkKey
		if err := qr.Scan(&k.pipeline, &k.key); err != nil {
			qr.Close()
			return nil, err
		}
		quarantined[k] = true
	}
	qr.Close()
	rs, err := q.Query(ctx, `SELECT pipeline, source_key, chunk_no, fingerprint, content FROM `+pgx.Identifier{schema, g.ChunkTable}.Sanitize())
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	rep := &GenReport{Generation: g.ID, ExpectedChunks: len(exp), Quarantined: len(quarantined), PoisonExpected: len(poison),
		Pipelines: map[string]*PipeCounts{}}
	pc := func(pipe string) *PipeCounts {
		c := rep.Pipelines[pipe]
		if c == nil {
			c = &PipeCounts{}
			rep.Pipelines[pipe] = c
		}
		return c
	}
	for pipe := range rows {
		pc(pipe)
	}
	for view := range views {
		pc(view)
	}
	for ck := range exp {
		pc(ck.pipeline).ExpectedChunks++
	}
	seen := map[chunkKey]bool{}
	var problems []string
	note := func(s string) {
		if len(problems) < 10 {
			problems = append(problems, s)
		}
	}
	for rs.Next() {
		var ck chunkKey
		var v chunkVal
		if err := rs.Scan(&ck.pipeline, &ck.key, &ck.no, &v.fp, &v.text); err != nil {
			return nil, err
		}
		rep.ActualChunks++
		pc(ck.pipeline).ActualChunks++
		seen[ck] = true
		want, ok := exp[ck]
		switch {
		case !ok:
			rep.Extra++
			pc(ck.pipeline).Extra++
			note(fmt.Sprintf("extra pipeline=%s key=%s chunk=%d", ck.pipeline, ck.key, ck.no))
		case want.fp != v.fp || want.text != v.text:
			rep.Stale++
			pc(ck.pipeline).Stale++
			note(fmt.Sprintf("stale pipeline=%s key=%s chunk=%d", ck.pipeline, ck.key, ck.no))
		}
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	for ck := range exp {
		if seen[ck] {
			continue
		}
		if quarantined[chunkKey{pipeline: ck.pipeline, key: ck.key}] {
			rep.MissingQuarant++
			continue
		}
		rep.Missing++
		pc(ck.pipeline).Missing++
		note(fmt.Sprintf("missing pipeline=%s key=%s chunk=%d", ck.pipeline, ck.key, ck.no))
	}
	sort.Strings(problems)
	rep.ExampleProblems = problems
	return rep, nil
}

// StateReport compares source_state with source rows.
type StateReport struct {
	Expected int `json:"expected"`
	Actual   int `json:"actual"`
	Missing  int `json:"missing"`
	Extra    int `json:"extra"`
	Stale    int `json:"stale"`
}

// OK reports exact equality.
func (r StateReport) OK() bool { return r.Missing == 0 && r.Extra == 0 && r.Stale == 0 }

func fieldsEqual(a, b map[string]*string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || (v == nil) != (w == nil) || (v != nil && *v != *w) {
			return false
		}
	}
	return true
}

// State compares source_state rows (loaded by the caller, pipeline -> key ->
// fields) with every pipeline's source rows.
func State(rows Rows, state map[string]map[string]map[string]*string) StateReport {
	rep := StateReport{Expected: rows.Count()}
	for pipe, st := range state {
		rep.Actual += len(st)
		seen := map[string]bool{}
		for _, r := range rows[pipe] {
			seen[r.Key] = true
		}
		for k := range st {
			if !seen[k] {
				rep.Extra++
			}
		}
	}
	for pipe, rs := range rows {
		for _, r := range rs {
			s, ok := state[pipe][r.Key]
			switch {
			case !ok:
				rep.Missing++
			case !fieldsEqual(s, r.Fields):
				rep.Stale++
			}
		}
	}
	return rep
}

// SourceRows reads the allowlisted projection of one pipeline's table, with
// keys encoded exactly as the stream and backfill encode them.
func SourceRows(ctx context.Context, q Querier, p *config.PipelineDef) ([]Row, error) {
	schema, table := p.SchemaTable()
	cols := []string{}
	for _, k := range p.Key {
		cols = append(cols, pgx.Identifier{k}.Sanitize()+"::text")
	}
	for _, f := range p.Fields {
		cols = append(cols, pgx.Identifier{f.Column}.Sanitize()+"::text")
	}
	rs, err := q.Query(ctx, `SELECT `+strings.Join(cols, ", ")+` FROM `+pgx.Identifier{schema, table}.Sanitize())
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	nk := len(p.Key)
	var out []Row
	for rs.Next() {
		vals := make([]*string, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		kv := make([]string, nk)
		for i := range kv {
			if vals[i] == nil {
				return nil, fmt.Errorf("pipeline %s: NULL primary key value", p.Name)
			}
			kv[i] = *vals[i]
		}
		key, err := keys.EncodeValues(kv)
		if err != nil {
			return nil, fmt.Errorf("pipeline %s: %w", p.Name, err)
		}
		r := Row{Key: key, Fields: map[string]*string{}}
		for i, f := range p.Fields {
			r.Fields[f.Name] = vals[nk+i]
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

// AllSourceRows reads every replicated table, standalone pipelines and view
// components alike (call inside one snapshot).
func AllSourceRows(ctx context.Context, q Querier, cfg *config.Config) (Rows, error) {
	out := Rows{}
	for _, p := range cfg.TableDefs() {
		rs, err := SourceRows(ctx, q, p)
		if err != nil {
			return nil, err
		}
		out[p.Name] = rs
	}
	return out, nil
}

// StateRows reads source_state as rows per pipeline, and as pipeline -> key -> fields.
func StateRows(ctx context.Context, q Querier, schema string) (Rows, map[string]map[string]map[string]*string, error) {
	rs, err := q.Query(ctx, `SELECT pipeline, source_key, fields FROM `+pgx.Identifier{schema, "source_state"}.Sanitize())
	if err != nil {
		return nil, nil, err
	}
	defer rs.Close()
	rows := Rows{}
	m := map[string]map[string]map[string]*string{}
	for rs.Next() {
		var pipe string
		var r Row
		if err := rs.Scan(&pipe, &r.Key, &r.Fields); err != nil {
			return nil, nil, err
		}
		rows[pipe] = append(rows[pipe], r)
		if m[pipe] == nil {
			m[pipe] = map[string]map[string]*string{}
		}
		m[pipe][r.Key] = r.Fields
	}
	return rows, m, rs.Err()
}
