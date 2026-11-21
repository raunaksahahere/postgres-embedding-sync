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
	for i := range cfg.Pipelines {
		p := &cfg.Pipelines[i]
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
	parts := make([]string, 0, len(cfg.Pipelines))
	for i := range cfg.Pipelines {
		parts = append(parts, pubTable(&cfg.Pipelines[i])+" ("+columnList(&cfg.Pipelines[i])+")")
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

// ReconcileSource makes the replicator grants and the publication's table list
// match the configured pipelines. It runs on every `embedsync init`, after the
// versioned migrations, so a pipeline added to an existing installation gets
// its column grants and joins the publication (with its column list). It
// never removes a table that a pipeline still names; tables of removed
// pipelines leave the publication. Returns a description of each change.
func ReconcileSource(ctx context.Context, conn *pgx.Conn, cfg *config.Config, r Roles) ([]string, error) {
	if !ident.MatchString(r.Replicator) {
		return nil, fmt.Errorf("invalid role name %q", r.Replicator)
	}
	var changes []string
	if _, err := conn.Exec(ctx, sourceGrants(cfg, pgx.Identifier{r.Replicator}.Sanitize())); err != nil {
		return nil, fmt.Errorf("grant source columns: %w", err)
	}
	want := map[string]string{cfg.Source.ControlSchema + ".markers": "*"}
	for i := range cfg.Pipelines {
		p := &cfg.Pipelines[i]
		want[p.Table] = strings.Join(sortedCopy(p.PublishedColumns()), ",")
	}
	rows, err := conn.Query(ctx, `SELECT schemaname || '.' || tablename, attnames FROM pg_publication_tables WHERE pubname = $1`, cfg.Source.Publication)
	if err != nil {
		return nil, err
	}
	have := map[string]string{}
	for rows.Next() {
		var t string
		var cols []string
		if err := rows.Scan(&t, &cols); err != nil {
			rows.Close()
			return nil, err
		}
		have[t] = strings.Join(sortedCopy(cols), ",")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	same := len(have) == len(want)
	for t, cols := range want {
		h, ok := have[t]
		if !ok || (cols != "*" && h != cols) {
			same = false
		}
	}
	if same {
		return nil, nil
	}
	q := `ALTER PUBLICATION ` + pgx.Identifier{cfg.Source.Publication}.Sanitize() + ` SET TABLE ` + publicationTables(cfg) + `, ` +
		pgx.Identifier{cfg.Source.ControlSchema, "markers"}.Sanitize()
	if _, err := conn.Exec(ctx, q); err != nil {
		return nil, fmt.Errorf("update publication: %w", err)
	}
	tables := make([]string, 0, len(want))
	for t := range want {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	changes = append(changes, fmt.Sprintf("publication %s now publishes %s", cfg.Source.Publication, strings.Join(tables, ", ")))
	return changes, nil
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
