package verify

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// SourceViewDocs assembles every document of view v straight from the source
// tables, independently of the stream path and of viewdoc.SortRows: list
// order comes from SQL ORDER BY (order columns NULLS LAST, text compared
// COLLATE "C", ties broken by the child or target key). Call it inside the
// caller's REPEATABLE READ transaction so every query sees one snapshot.
//
// docs is keyed by the encoded root key. orphans counts child rows and links
// whose parent document does not exist; they are left out of docs. Links
// whose target row is missing are dropped by the join and not counted.
func SourceViewDocs(ctx context.Context, q Querier, v *config.ViewDef) (docs map[string]*viewdoc.Doc, orphans int, err error) {
	docs, err = viewRootDocs(ctx, q, v)
	if err != nil {
		return nil, 0, err
	}
	for i := range v.Children {
		n, err := addViewChildren(ctx, q, v, &v.Children[i], docs)
		if err != nil {
			return nil, 0, err
		}
		orphans += n
	}
	for i := range v.Links {
		n, err := addViewLinks(ctx, q, v, &v.Links[i], docs)
		if err != nil {
			return nil, 0, err
		}
		orphans += n
	}
	return docs, orphans, nil
}

func viewRootDocs(ctx context.Context, q Querier, v *config.ViewDef) (map[string]*viewdoc.Doc, error) {
	fieldCols := fieldColumns(v.Fields)
	sql := `SELECT ` + selectList("", v.Key, fieldCols) + ` FROM ` + tableIdent(v.Table)
	docs := map[string]*viewdoc.Doc{}
	err := queryText(ctx, q, sql, len(v.Key)+len(fieldCols), func(vals []*string) error {
		key, err := encodeKey(vals[:len(v.Key)])
		if err != nil {
			return fmt.Errorf("view %s root: %w", v.Name, err)
		}
		d := &viewdoc.Doc{Key: key, Root: map[string]*string{}, Lists: map[string][]viewdoc.Row{}}
		for i, f := range v.Fields {
			d.Root[f.Name] = vals[len(v.Key)+i]
		}
		for _, ch := range v.Children {
			d.Lists[ch.Name] = []viewdoc.Row{}
		}
		for _, l := range v.Links {
			d.Lists[l.Name] = []viewdoc.Row{}
		}
		docs[key] = d
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("view %s root %s: %w", v.Name, v.Table, err)
	}
	return docs, nil
}

func addViewChildren(ctx context.Context, q Querier, v *config.ViewDef, ch *config.ChildDef, docs map[string]*viewdoc.Doc) (int, error) {
	textCols, err := textColumns(ctx, q, ch.Table)
	if err != nil {
		return 0, fmt.Errorf("view %s child %s: %w", v.Name, ch.Name, err)
	}
	fieldCols := fieldColumns(ch.Fields)
	sql := childQuery(ch, fieldCols, textCols)
	nk, np := len(ch.Key), len(ch.Parent)
	orphans := 0
	err = queryText(ctx, q, sql, nk+np+len(fieldCols), func(vals []*string) error {
		d := parentDoc(docs, vals[nk:nk+np])
		if d == nil {
			orphans++
			return nil
		}
		key, err := encodeKey(vals[:nk])
		if err != nil {
			return err
		}
		r := viewdoc.Row{Key: key, Fields: map[string]*string{}}
		for i, f := range ch.Fields {
			r.Fields[f.Name] = vals[nk+np+i]
		}
		d.Lists[ch.Name] = append(d.Lists[ch.Name], r)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("view %s child %s (%s): %w", v.Name, ch.Name, ch.Table, err)
	}
	return orphans, nil
}

func addViewLinks(ctx context.Context, q Querier, v *config.ViewDef, l *config.LinkDef, docs map[string]*viewdoc.Doc) (int, error) {
	if len(l.Target) != len(l.TargetKey) {
		return 0, fmt.Errorf("view %s link %s: target has %d columns, target key %d", v.Name, l.Name, len(l.Target), len(l.TargetKey))
	}
	textCols, err := textColumns(ctx, q, l.TargetTable)
	if err != nil {
		return 0, fmt.Errorf("view %s link %s target: %w", v.Name, l.Name, err)
	}
	fieldCols := fieldColumns(l.TargetFields)
	colOf := map[string]string{}
	for _, f := range l.TargetFields {
		colOf[f.Name] = f.Column
	}
	orderCols := make([]string, len(l.OrderBy))
	for i, name := range l.OrderBy {
		c, ok := colOf[name]
		if !ok {
			return 0, fmt.Errorf("view %s link %s: order_by %q is not a target field", v.Name, l.Name, name)
		}
		orderCols[i] = c
	}
	on := make([]string, len(l.Target))
	for i := range l.Target {
		on[i] = pgx.Identifier{"k", l.Target[i]}.Sanitize() + " = " + pgx.Identifier{"t", l.TargetKey[i]}.Sanitize()
	}
	order := []string{orderByClause(l.Parent, "k", nil, false)}
	if len(orderCols) > 0 {
		order = append(order, orderByClause(orderCols, "t", textCols, true))
	}
	order = append(order, orderByClause(l.TargetKey, "t", textCols, false))
	sql := `SELECT ` + selectList("k", l.Parent) + `, ` + selectList("t", l.TargetKey, fieldCols) +
		` FROM ` + tableIdent(l.Table) + ` AS ` + pgx.Identifier{"k"}.Sanitize() +
		` JOIN ` + tableIdent(l.TargetTable) + ` AS ` + pgx.Identifier{"t"}.Sanitize() +
		` ON ` + strings.Join(on, " AND ") +
		` ORDER BY ` + strings.Join(order, ", ")
	np, nk := len(l.Parent), len(l.TargetKey)
	orphans := 0
	err = queryText(ctx, q, sql, np+nk+len(fieldCols), func(vals []*string) error {
		d := parentDoc(docs, vals[:np])
		if d == nil {
			orphans++
			return nil
		}
		key, err := encodeKey(vals[np : np+nk])
		if err != nil {
			return err
		}
		r := viewdoc.Row{Key: key, Fields: map[string]*string{}}
		for i, f := range l.TargetFields {
			r.Fields[f.Name] = vals[np+nk+i]
		}
		d.Lists[l.Name] = append(d.Lists[l.Name], r)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("view %s link %s (%s -> %s): %w", v.Name, l.Name, l.Table, l.TargetTable, err)
	}
	return orphans, nil
}

// parentDoc returns the document a reference points at, or nil if the
// reference has a NULL column or names no existing root row.
func parentDoc(docs map[string]*viewdoc.Doc, ref []*string) *viewdoc.Doc {
	vals := make([]string, len(ref))
	for i, p := range ref {
		if p == nil {
			return nil
		}
		vals[i] = *p
	}
	key, err := keys.EncodeValues(vals)
	if err != nil {
		return nil
	}
	return docs[key]
}

func encodeKey(vals []*string) (string, error) {
	kv := make([]string, len(vals))
	for i, p := range vals {
		if p == nil {
			return "", fmt.Errorf("NULL primary key value")
		}
		kv[i] = *p
	}
	return keys.EncodeValues(kv)
}

func fieldColumns(fs []config.Field) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Column
	}
	return out
}

func tableIdent(t string) string {
	schema, table, _ := strings.Cut(t, ".")
	return pgx.Identifier{schema, table}.Sanitize()
}

// selectList renders quoted columns (optionally alias-qualified) cast to text.
func selectList(alias string, groups ...[]string) string {
	var parts []string
	for _, cols := range groups {
		for _, c := range cols {
			parts = append(parts, colIdent(alias, c)+"::text")
		}
	}
	return strings.Join(parts, ", ")
}

func colIdent(alias, col string) string {
	if alias == "" {
		return pgx.Identifier{col}.Sanitize()
	}
	return pgx.Identifier{alias, col}.Sanitize()
}

// orderByClause renders an ORDER BY list (without the keywords) over cols:
// each quoted and prefixed with alias when given, COLLATE "C" for columns in
// textCols, and NULLS LAST on every column when nullsLast is set.
func orderByClause(cols []string, alias string, textCols map[string]bool, nullsLast bool) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		s := colIdent(alias, c)
		if textCols[c] {
			s += ` COLLATE "C"`
		}
		if nullsLast {
			s += " NULLS LAST"
		}
		parts[i] = s
	}
	return strings.Join(parts, ", ")
}

// textColumns returns the text and varchar columns of a "schema.table".
func textColumns(ctx context.Context, q Querier, table string) (map[string]bool, error) {
	rs, err := q.Query(ctx, `SELECT attname::text, atttypid::int8 FROM pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped`, tableIdent(table))
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	out := map[string]bool{}
	for rs.Next() {
		var name string
		var oid int64
		if err := rs.Scan(&name, &oid); err != nil {
			return nil, err
		}
		if oid == 25 || oid == 1043 {
			out[name] = true
		}
	}
	return out, rs.Err()
}

// queryText runs sql and calls fn with each row's n text values (nil = NULL).
func queryText(ctx context.Context, q Querier, sql string, n int, fn func([]*string) error) error {
	rs, err := q.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		vals := make([]*string, n)
		ptrs := make([]any, n)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return err
		}
		if err := fn(vals); err != nil {
			return err
		}
	}
	return rs.Err()
}

// stateSource reads view state from the sink inside the caller's snapshot.
type stateSource struct {
	q      Querier
	schema string
}

func (s stateSource) States(ctx context.Context, pipeline string, ks []string) (map[string]map[string]*string, error) {
	out := map[string]map[string]*string{}
	if len(ks) == 0 {
		return out, nil
	}
	rs, err := s.q.Query(ctx, `SELECT source_key, fields FROM `+pgx.Identifier{s.schema, "source_state"}.Sanitize()+
		` WHERE pipeline = $1 AND source_key = ANY($2)`, pipeline, ks)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	for rs.Next() {
		var k string
		var f map[string]*string
		if err := rs.Scan(&k, &f); err != nil {
			return nil, err
		}
		out[k] = f
	}
	return out, rs.Err()
}

func (s stateSource) Children(ctx context.Context, pipeline, ref string, parents []string) (map[string][]string, error) {
	out := map[string][]string{}
	if len(parents) == 0 {
		return out, nil
	}
	rs, err := s.q.Query(ctx, `SELECT ref_key, source_key FROM `+pgx.Identifier{s.schema, "source_refs"}.Sanitize()+
		` WHERE pipeline = $1 AND ref = $2 AND ref_key = ANY($3) ORDER BY 1, 2`, pipeline, ref, parents)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	for rs.Next() {
		var p, k string
		if err := rs.Scan(&p, &k); err != nil {
			return nil, err
		}
		out[p] = append(out[p], k)
	}
	return out, rs.Err()
}

// StateViewDocs assembles every view's documents from the sink's state (the
// same assembly the stream path uses), inside the caller's snapshot. rels
// are the accepted relations of the views' components; stateRows are the
// state rows already read in that snapshot (root keys come from them).
func StateViewDocs(ctx context.Context, q Querier, schema string, cfg *config.Config, rels map[string]viewdoc.Relation, stateRows Rows) (map[string]ViewDocs, error) {
	out := map[string]ViewDocs{}
	for i := range cfg.Views {
		v := &cfg.Views[i]
		vs, err := viewdoc.Build(v, rels)
		if err != nil {
			return nil, err
		}
		var roots []string
		for _, r := range stateRows[vs.Root.Comp.Def.Name] {
			roots = append(roots, r.Key)
		}
		docs, err := viewdoc.Assemble(ctx, stateSource{q: q, schema: schema}, vs, roots)
		if err != nil {
			return nil, err
		}
		out[v.Name] = ViewDocs{Docs: docs, MaxChildren: v.MaxChildren}
	}
	return out, nil
}

// SourceViews assembles every view's documents from the source tables
// (independently of the stream path), inside the caller's snapshot.
func SourceViews(ctx context.Context, q Querier, cfg *config.Config) (map[string]ViewDocs, int, error) {
	out := map[string]ViewDocs{}
	orphans := 0
	for i := range cfg.Views {
		v := &cfg.Views[i]
		docs, n, err := SourceViewDocs(ctx, q, v)
		if err != nil {
			return nil, 0, fmt.Errorf("view %s: %w", v.Name, err)
		}
		orphans += n
		out[v.Name] = ViewDocs{Docs: docs, MaxChildren: v.MaxChildren}
	}
	return out, orphans, nil
}

// OnlyPipelines keeps the rows of standalone pipelines (not view components).
func OnlyPipelines(rows Rows, cfg *config.Config) Rows {
	out := Rows{}
	for n, rs := range rows {
		if cfg.Pipe(n) != nil {
			out[n] = rs
		}
	}
	return out
}

// StateSource returns a view-state reader over the sink (pool or transaction).
func StateSource(q Querier, schema string) viewdoc.Source { return stateSource{q: q, schema: schema} }

// childQuery selects a child table's key, parent and field columns as text,
// ordered by parent, order columns and key. Every column is qualified with
// the table alias: an unqualified ORDER BY "id" would bind to the ::text
// output column and sort "803" after "3802".
func childQuery(ch *config.ChildDef, fieldCols []string, textCols map[string]bool) string {
	const a = "c"
	order := []string{orderByClause(ch.Parent, a, nil, false)}
	if len(ch.OrderBy) > 0 {
		order = append(order, orderByClause(ch.OrderBy, a, textCols, true))
	}
	order = append(order, orderByClause(ch.Key, a, textCols, false))
	return `SELECT ` + selectList(a, ch.Key, ch.Parent, fieldCols) + ` FROM ` + tableIdent(ch.Table) + ` AS ` + pgx.Identifier{a}.Sanitize() +
		` ORDER BY ` + strings.Join(order, ", ")
}
