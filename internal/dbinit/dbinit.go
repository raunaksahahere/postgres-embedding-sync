// Package dbinit applies the embedded, versioned SQL migrations.
package dbinit

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/migrations"
)

// Roles names the least-privilege roles (created by the DBA beforehand).
type Roles struct {
	Replicator string
	Sink       string
	Reader     string
}

var ident = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type vars struct {
	ControlSchema, Publication, ReplRole string
	Grants, PubTables                    string // every pipeline's table
	Schema, SinkRole, ReaderRole         string
}

// sourceGrants renders USAGE on each source schema and column-level SELECT on
// each pipeline's key and allowlisted columns.
func sourceGrants(cfg *config.Config, replRole string) string {
	var b strings.Builder
	schemas := map[string]bool{}
	for _, p := range cfg.TableDefs() {
		schema, _ := p.SchemaTable()
		if !schemas[schema] {
			schemas[schema] = true
			fmt.Fprintf(&b, "GRANT USAGE ON SCHEMA %s TO %s;\n", pgx.Identifier{schema}.Sanitize(), replRole)
		}
		fmt.Fprintf(&b, "GRANT SELECT (%s) ON %s TO %s;\n", columnList(p), pubTable(p), replRole)
	}
	return b.String()
}

func pubTable(p *config.PipelineDef) string {
	schema, table := p.SchemaTable()
	return pgx.Identifier{schema, table}.Sanitize()
}

func columnList(p *config.PipelineDef) string {
	cols := make([]string, 0)
	for _, c := range p.PublishedColumns() {
		cols = append(cols, pgx.Identifier{c}.Sanitize())
	}
	return strings.Join(cols, ", ")
}

// publicationTables renders "t1 (cols), t2 (cols)" for CREATE/ALTER PUBLICATION.
func publicationTables(cfg *config.Config) string {
	defs := cfg.TableDefs()
	parts := make([]string, 0, len(defs))
	for _, d := range defs {
		parts = append(parts, pubTable(d)+" ("+columnList(d)+")")
	}
	return strings.Join(parts, ", ")
}

func render(name, body string, v vars) (string, error) {
	t, err := template.New(name).Option("missingkey=error").Parse(body)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	if err := t.Execute(&b, v); err != nil {
		return "", err
	}
	return b.String(), nil
}

// Apply runs pending migrations for one side ("source" or "sink").
func Apply(ctx context.Context, conn *pgx.Conn, side string, cfg *config.Config, r Roles) ([]string, error) {
	for _, n := range []string{r.Replicator, r.Sink, r.Reader} {
		if !ident.MatchString(n) {
			return nil, fmt.Errorf("invalid role name %q", n)
		}
	}
	replRole := pgx.Identifier{r.Replicator}.Sanitize()
	v := vars{
		ControlSchema: pgx.Identifier{cfg.Source.ControlSchema}.Sanitize(),
		Grants:        sourceGrants(cfg, replRole), PubTables: publicationTables(cfg),
		Publication: cfg.Source.Publication, ReplRole: replRole,
		Schema: pgx.Identifier{cfg.Sink.Schema}.Sanitize(), SinkRole: pgx.Identifier{r.Sink}.Sanitize(), ReaderRole: pgx.Identifier{r.Reader}.Sanitize(),
	}
	var versionTable string
	if side == "source" {
		versionTable = v.ControlSchema + ".schema_migrations"
	} else {
		versionTable = v.Schema + ".schema_migrations"
	}
	files, err := fs.Glob(migrations.FS, side+"/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	applied := map[int]bool{}
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, versionTable).Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		rows, err := conn.Query(ctx, `SELECT version FROM `+versionTable)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var n int
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				return nil, err
			}
			applied[n] = true
		}
		rows.Close()
	}
	var done []string
	for _, f := range files {
		base := f[strings.LastIndex(f, "/")+1:]
		n, err := strconv.Atoi(strings.SplitN(base, "_", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("bad migration name %s", f)
		}
		if applied[n] {
			continue
		}
		body, err := fs.ReadFile(migrations.FS, f)
		if err != nil {
			return nil, err
		}
		sql, err := render(f, string(body), v)
		if err != nil {
			return nil, err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, sql); err != nil {
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("apply %s: %w", f, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+versionTable+` (version) VALUES ($1)`, n); err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		done = append(done, f)
	}
	return done, nil
}

// ReconcileSource makes the replicator grants and the publication follow the
// configured pipelines. It runs on every `embedsync init`, after the versioned
// migrations, so a pipeline added to an existing installation gets its column
// grants and joins the publication (with its column list), and a pipeline
// whose fields changed gets its new column list.
//
// It never removes a table from the publication. A table that no pipeline
// names may belong to a removed pipeline whose rows the sink still holds;
// dropping it would leave a gap in its change stream that nothing backfills
// if the pipeline is restored. Such tables stay published and are reported as
// warnings. Returns a description of each change and each warning.
func ReconcileSource(ctx context.Context, conn *pgx.Conn, cfg *config.Config, r Roles) (changes, warnings []string, err error) {
	if !ident.MatchString(r.Replicator) {
		return nil, nil, fmt.Errorf("invalid role name %q", r.Replicator)
	}
	if _, err := conn.Exec(ctx, sourceGrants(cfg, pgx.Identifier{r.Replicator}.Sanitize())); err != nil {
		return nil, nil, fmt.Errorf("grant source columns: %w", err)
	}
	markers := cfg.Source.ControlSchema + ".markers"
	want := map[string]string{markers: anyColumns}
	byTable := map[string]*config.PipelineDef{}
	for _, p := range cfg.TableDefs() {
		want[p.Table] = strings.Join(sortedCopy(p.PublishedColumns()), ",")
		byTable[p.Table] = p
	}
	rows, err := conn.Query(ctx, `SELECT schemaname, tablename, attnames FROM pg_publication_tables WHERE pubname = $1`, cfg.Source.Publication)
	if err != nil {
		return nil, nil, err
	}
	have := map[string]string{}
	quoted := map[string]string{} // published table -> quoted identifier
	for rows.Next() {
		var s, t string
		var cols []string
		if err := rows.Scan(&s, &t, &cols); err != nil {
			rows.Close()
			return nil, nil, err
		}
		have[s+"."+t] = strings.Join(sortedCopy(cols), ",")
		quoted[s+"."+t] = pgx.Identifier{s, t}.Sanitize()
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	adds, relist, extra := planPublication(want, have)

	pubName := cfg.Source.Publication
	pub := pgx.Identifier{pubName}.Sanitize()
	for _, t := range extra {
		warnings = append(warnings, fmt.Sprintf("publication %s still publishes %s, which no pipeline names: "+
			"embedsync never removes a table it may have synced (removing a pipeline is not supported in place); "+
			"if it was never synced, drop it with: ALTER PUBLICATION %s DROP TABLE %s", pubName, t, pub, quoted[t]))
	}
	if len(adds) == 0 && len(relist) == 0 {
		return nil, warnings, nil
	}
	// withColumns renders a wanted table with its column list for ADD TABLE.
	withColumns := func(t string) string {
		if t == markers {
			return pgx.Identifier{cfg.Source.ControlSchema, "markers"}.Sanitize()
		}
		return pubTable(byTable[t]) + " (" + columnList(byTable[t]) + ")"
	}
	var stmts []string
	for _, t := range adds {
		stmts = append(stmts, `ALTER PUBLICATION `+pub+` ADD TABLE `+withColumns(t))
		changes = append(changes, fmt.Sprintf("publication %s: added %s", pubName, t))
	}
	// A column list cannot be altered in place; dropping and re-adding the
	// table in one transaction changes it atomically, so no change is lost.
	for _, t := range relist {
		p := byTable[t]
		stmts = append(stmts, `ALTER PUBLICATION `+pub+` DROP TABLE `+pubTable(p), `ALTER PUBLICATION `+pub+` ADD TABLE `+withColumns(t))
		changes = append(changes, fmt.Sprintf("publication %s: column list of %s is now (%s)", pubName, t, strings.Join(p.PublishedColumns(), ", ")))
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q); err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, fmt.Errorf("update publication: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("update publication: %w", err)
	}
	return changes, warnings, nil
}

// anyColumns as a planPublication want entry accepts any column list.
const anyColumns = "*"

// planPublication diffs the wanted publication against the current one; both
// map a schema-qualified table to its sorted, comma-joined columns (or
// anyColumns in want). adds are wanted tables that are not published, relist
// are wanted tables published with a different column list, and extra are
// published tables nobody wants, which are reported, never removed. Each
// result is sorted.
func planPublication(want, have map[string]string) (adds, relist, extra []string) {
	for t, cols := range want {
		h, ok := have[t]
		switch {
		case !ok:
			adds = append(adds, t)
		case cols != anyColumns && h != cols:
			relist = append(relist, t)
		}
	}
	for t := range have {
		if _, ok := want[t]; !ok {
			extra = append(extra, t)
		}
	}
	sort.Strings(adds)
	sort.Strings(relist)
	sort.Strings(extra)
	return adds, relist, extra
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// SchemaVersion is the newest sink migration this binary ships.
func SchemaVersion() (int, error) {
	files, err := fs.Glob(migrations.FS, "sink/*.sql")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, f := range files {
		base := f[strings.LastIndex(f, "/")+1:]
		v, err := strconv.Atoi(strings.SplitN(base, "_", 2)[0])
		if err != nil {
			return 0, err
		}
		n = max(n, v)
	}
	return n, nil
}
