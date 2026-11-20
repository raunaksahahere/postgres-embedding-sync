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
	ControlSchema, SourceSchema, Table, Columns, Publication, ReplRole string
	Schema, SinkRole, ReaderRole                                       string
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
	schema, table := cfg.SourceSchemaTable()
	cols := make([]string, 0)
	for _, c := range cfg.PublishedColumns() {
		cols = append(cols, pgx.Identifier{c}.Sanitize())
	}
	v := vars{
		ControlSchema: pgx.Identifier{cfg.Source.ControlSchema}.Sanitize(), SourceSchema: pgx.Identifier{schema}.Sanitize(),
		Table: pgx.Identifier{schema, table}.Sanitize(), Columns: strings.Join(cols, ", "),
		Publication: cfg.Source.Publication, ReplRole: pgx.Identifier{r.Replicator}.Sanitize(),
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
