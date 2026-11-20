// Package verify recomputes the expected sink from source rows and compares it
// with the entire sink: every chunk of every key, including stale leftovers.
package verify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// Row is one expected document (allowlisted fields, nil = NULL).
type Row struct {
	Key    string
	Fields map[string]*string
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
}

// OK reports zero missing, extra and stale entries (quarantined rows are reported separately).
func (r GenReport) OK() bool { return r.Missing == 0 && r.Extra == 0 && r.Stale == 0 }

type chunkKey struct {
	key string
	no  int
}

type chunkVal struct{ fp, text string }

// Expected computes chunks for rows under one generation. Rows whose rendered
// document exceeds maxDocBytes are expected to be absent (quarantined).
func Expected(tr *transform.Transformer, m transform.ModelIdentity, rows []Row, maxDocBytes int) (map[chunkKey]chunkVal, map[string]bool, error) {
	out := map[chunkKey]chunkVal{}
	poison := map[string]bool{}
	for _, r := range rows {
		vals := map[string]string{}
		for k, v := range r.Fields {
			if v != nil {
				vals[k] = *v
			}
		}
		text, err := tr.Render(vals)
		if err != nil || len(text) > maxDocBytes {
			poison[r.Key] = true
			continue
		}
		for _, c := range tr.Chunks(r.Key, text, m) {
			out[chunkKey{r.Key, c.No}] = chunkVal{c.Fingerprint, c.Text}
		}
	}
	return out, poison, nil
}

// Querier is a pgx query interface (pool or tx).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Generation compares a generation's chunk table with rows.
func Generation(ctx context.Context, q Querier, schema string, g *store.Generation, tr *transform.Transformer, rows []Row, maxDocBytes int) (*GenReport, error) {
	m := transform.ModelIdentity{Name: g.ModelName, Revision: g.ModelRevision, DocumentPrefix: g.DocumentPrefix}
	exp, poison, err := Expected(tr, m, rows, maxDocBytes)
	if err != nil {
		return nil, err
	}
	quarantined := map[string]bool{}
	qr, err := q.Query(ctx, `SELECT source_key FROM `+pgx.Identifier{schema, "dlq"}.Sanitize()+` WHERE generation_id = $1 AND state = 'open'`, g.ID)
	if err != nil {
		return nil, err
	}
	for qr.Next() {
		var k string
		if err := qr.Scan(&k); err != nil {
			qr.Close()
			return nil, err
		}
		quarantined[k] = true
	}
	qr.Close()
	rs, err := q.Query(ctx, `SELECT source_key, chunk_no, fingerprint, content FROM `+pgx.Identifier{schema, g.ChunkTable}.Sanitize())
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	rep := &GenReport{Generation: g.ID, ExpectedChunks: len(exp), Quarantined: len(quarantined), PoisonExpected: len(poison)}
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
		if err := rs.Scan(&ck.key, &ck.no, &v.fp, &v.text); err != nil {
			return nil, err
		}
		rep.ActualChunks++
		seen[ck] = true
		want, ok := exp[ck]
		switch {
		case !ok:
			rep.Extra++
			note(fmt.Sprintf("extra key=%s chunk=%d", ck.key, ck.no))
		case want.fp != v.fp || want.text != v.text:
			rep.Stale++
			note(fmt.Sprintf("stale key=%s chunk=%d", ck.key, ck.no))
		}
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	for ck := range exp {
		if seen[ck] {
			continue
		}
		if quarantined[ck.key] {
			rep.MissingQuarant++
			continue
		}
		rep.Missing++
		note(fmt.Sprintf("missing key=%s chunk=%d", ck.key, ck.no))
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

// State compares source_state rows (loaded by the caller) with source rows.
func State(rows []Row, state map[string]map[string]*string) StateReport {
	rep := StateReport{Expected: len(rows), Actual: len(state)}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Key] = true
		s, ok := state[r.Key]
		switch {
		case !ok:
			rep.Missing++
		case !fieldsEqual(s, r.Fields):
			rep.Stale++
		}
	}
	for k := range state {
		if !seen[k] {
			rep.Extra++
		}
	}
	return rep
}

// SourceRows reads the allowlisted projection of the source table.
func SourceRows(ctx context.Context, q Querier, cfg *config.Config) ([]Row, error) {
	schema, table := cfg.SourceSchemaTable()
	cols := []string{pgx.Identifier{cfg.Source.Key}.Sanitize() + "::text"}
	for _, f := range cfg.Source.Fields {
		cols = append(cols, pgx.Identifier{f.Column}.Sanitize()+"::text")
	}
	rs, err := q.Query(ctx, `SELECT `+strings.Join(cols, ", ")+` FROM `+pgx.Identifier{schema, table}.Sanitize())
	if err != nil {
		return nil, err
	}
	defer rs.Close()
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
		r := Row{Key: *vals[0], Fields: map[string]*string{}}
		for i, f := range cfg.Source.Fields {
			r.Fields[f.Name] = vals[i+1]
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

// StateRows reads source_state as rows.
func StateRows(ctx context.Context, q Querier, schema string) ([]Row, map[string]map[string]*string, error) {
	rs, err := q.Query(ctx, `SELECT source_key, fields FROM `+pgx.Identifier{schema, "source_state"}.Sanitize())
	if err != nil {
		return nil, nil, err
	}
	defer rs.Close()
	var rows []Row
	m := map[string]map[string]*string{}
	for rs.Next() {
		var r Row
		if err := rs.Scan(&r.Key, &r.Fields); err != nil {
			return nil, nil, err
		}
		rows = append(rows, r)
		m[r.Key] = r.Fields
	}
	return rows, m, rs.Err()
}
