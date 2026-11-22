// Package verify recomputes the expected sink from source rows and compares it
// with the entire sink: every chunk of every document of every pipeline,
// including stale leftovers. Documents are identified by (pipeline, key).
package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
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
	// MetaStale counts chunks whose meta differs from the document's
	// declared metadata; FTSIndexMissing reports that the full-text
	// expression index for search.text_config is missing or invalid. Both
	// are checked only when the generation's search columns are complete
	// for the configuration (SearchBuilding false); while they are still
	// being built in the background they are not counted.
	MetaStale       int  `json:"meta_stale"`
	FTSIndexMissing bool `json:"fts_index_missing"`
	SearchBuilding  bool `json:"search_columns_building"`
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
	MetaStale      int `json:"meta_stale"`
}

// OK reports zero missing, extra and stale entries, including stale search
// columns (quarantined rows are reported separately).
func (r GenReport) OK() bool {
	return r.Missing == 0 && r.Extra == 0 && r.Stale == 0 && r.MetaStale == 0 && !r.FTSIndexMissing
}

// SearchCheck is what the search columns of a generation are compared with.
type SearchCheck struct {
	// Fingerprint is the configuration's search fingerprint; the columns
	// are checked only for a generation whose SearchFingerprint equals it.
	Fingerprint string
	// TextConfig is the text search configuration the full-text index uses.
	TextConfig string
	// Meta returns the metadata declarations of a pipeline or view.
	Meta func(doc string) []config.MetaField
}

// SearchCheckFor returns the search check for a configuration.
func SearchCheckFor(cfg *config.Config) SearchCheck {
	return SearchCheck{Fingerprint: cfg.SearchFingerprint(), TextConfig: cfg.Search.TextConfig, Meta: cfg.MetaFor}
}

func (s SearchCheck) meta(doc string) []config.MetaField {
	if s.Meta == nil {
		return nil
	}
	return s.Meta(doc)
}

type chunkKey struct {
	pipeline string
	key      string
	no       int
}

// chunkVal is an expected chunk: fingerprint, text and its document's
// metadata JSON (as stored in meta).
type chunkVal struct{ fp, text, meta string }

// Expected computes chunks for every pipeline's rows under one generation.
// Rows whose rendered document exceeds maxDocBytes are expected to be absent
// (quarantined). meta returns a pipeline's metadata declarations (nil: none).
func Expected(trs map[string]*transform.Transformer, m transform.ModelIdentity, rows Rows, maxDocBytes int, meta func(string) []config.MetaField) (map[chunkKey]chunkVal, map[chunkKey]bool, error) {
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
			mj := pipeline.MetaJSON(metaDecls(meta, pipe), r.Fields)
			for _, c := range tr.Chunks(r.Key, text, m) {
				out[chunkKey{pipe, r.Key, c.No}] = chunkVal{c.Fingerprint, c.Text, mj}
			}
		}
	}
	return out, poison, nil
}

func metaDecls(meta func(string) []config.MetaField, doc string) []config.MetaField {
	if meta == nil {
		return nil
	}
	return meta(doc)
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
// fail to render, are expected to be absent (quarantined). A document's
// metadata comes from its root row (meta returns the view's declarations).
func ExpectedViews(trs map[string]*transform.Transformer, m transform.ModelIdentity, views map[string]ViewDocs, maxDocBytes int,
	meta func(string) []config.MetaField, out map[chunkKey]chunkVal, poison map[chunkKey]bool) error {
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
			mj := pipeline.MetaJSON(metaDecls(meta, view), d.Root)
			for _, c := range tr.Chunks(key, text, m) {
				out[chunkKey{view, key, c.No}] = chunkVal{c.Fingerprint, c.Text, mj}
			}
		}
	}
	return nil
}

// Generation compares a generation's chunk table with every standalone
// pipeline's rows and every view's assembled documents. When the
// generation's search columns are complete for sc.Fingerprint, every
// chunk's meta is compared and the full-text index checked; otherwise they are still being
// rebuilt and the report says so (SearchBuilding) without counting them.
func Generation(ctx context.Context, q Querier, schema string, g *store.Generation, trs map[string]*transform.Transformer, rows Rows, views map[string]ViewDocs, maxDocBytes int, sc SearchCheck) (*GenReport, error) {
	m := g.Encoder()
	exp, poison, err := Expected(trs, m, rows, maxDocBytes, sc.meta)
	if err != nil {
		return nil, err
	}
	if err := ExpectedViews(trs, m, views, maxDocBytes, sc.meta, exp, poison); err != nil {
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
	checkSearch := g.SearchReady(sc.Fingerprint)
	tbl := pgx.Identifier{schema, g.ChunkTable}.Sanitize()
	var rs pgx.Rows
	if checkSearch {
		rs, err = q.Query(ctx, `SELECT pipeline, source_key, chunk_no, fingerprint, content, meta::text FROM `+tbl)
	} else {
		rs, err = q.Query(ctx, `SELECT pipeline, source_key, chunk_no, fingerprint, content, '{}'::text FROM `+tbl)
	}
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	rep := &GenReport{Generation: g.ID, ExpectedChunks: len(exp), Quarantined: len(quarantined), PoisonExpected: len(poison),
		SearchBuilding: !checkSearch, Pipelines: map[string]*PipeCounts{}}
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
		if err := rs.Scan(&ck.pipeline, &ck.key, &ck.no, &v.fp, &v.text, &v.meta); err != nil {
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
		if !checkSearch {
			continue
		}
		if ok && !JSONEqual(want.meta, v.meta) {
			rep.MetaStale++
			pc(ck.pipeline).MetaStale++
			note(fmt.Sprintf("meta stale pipeline=%s key=%s chunk=%d: have %s want %s", ck.pipeline, ck.key, ck.no, v.meta, want.meta))
		}
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	rs.Close()
	if checkSearch {
		ir, err := q.Query(ctx, `SELECT coalesce(bool_and(i.indisvalid), false) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, schema, store.FTSIndexName(g.ChunkTable, sc.TextConfig))
		if err != nil {
			return nil, err
		}
		valid, err := pgx.CollectExactlyOneRow(ir, pgx.RowTo[bool])
		if err != nil {
			return nil, err
		}
		if !valid {
			rep.FTSIndexMissing = true
			note("full-text index " + store.FTSIndexName(g.ChunkTable, sc.TextConfig) + " is missing or invalid")
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

// JSONEqual reports whether two JSON texts hold the same value the way
// jsonb compares them: object key order and formatting are ignored and
// numbers compare by value (jsonb prints 1.7e+09 as 1700000000).
func JSONEqual(a, b string) bool {
	va, err := decodeJSON(a)
	if err != nil {
		return false
	}
	vb, err := decodeJSON(b)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

func decodeJSON(s string) (any, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return canonNumbers(v), nil
}

// jsonNum is a JSON number's exact value in canonical form (a distinct type,
// so the number 1 never equals the string "1").
type jsonNum string

// canonNumbers replaces every json.Number with its exact rational value
// rendered canonically, so equal numbers written differently compare equal.
func canonNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(string(t))
		if !ok {
			return jsonNum(t)
		}
		return jsonNum(r.RatString())
	case map[string]any:
		for k, x := range t {
			t[k] = canonNumbers(x)
		}
	case []any:
		for i, x := range t {
			t[i] = canonNumbers(x)
		}
	}
	return v
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
	fields := p.AllFields()
	for _, f := range fields {
		cols = append(cols, config.FieldText(f, pgx.Identifier{f.Column}.Sanitize()))
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
		for i, f := range fields {
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
