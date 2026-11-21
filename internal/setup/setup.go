package setup

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/dbinit"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/local"
)

// Options for `embedsync setup`. Any field set here skips the matching question.
type Options struct {
	ConfigPath     string
	AdminURL       string   // admin connection URL (superuser, or owner of the table with CREATEROLE)
	Table          string   // schema.table
	Columns        []string // columns to embed
	Model          string   // bundled model key, or Hugging Face id[@revision]
	QueryPrefix    string
	ResetPasswords bool // give existing embedsync roles new passwords
	Force          bool // overwrite an existing config
}

// Result tells the caller what was written.
type Result struct {
	ConfigPath string
	EnvPath    string
	Generation string
}

// Role names created by setup (the same defaults `embedsync init` uses).
const (
	roleReplicator = "embedsync_replicator"
	roleSink       = "embedsync_sink"
	roleReader     = "embedsync_reader"
)

var lowerIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// errStop ends setup after printing instructions the user must follow first.
type errStop struct{ msg string }

func (e errStop) Error() string { return e.msg }

// Run performs the guided setup.
func Run(ctx context.Context, p *Prompter, o Options) (*Result, error) {
	p.Say("embedsync setup: keeps a pgvector search index in sync with one PostgreSQL table.\n")
	if o.ConfigPath == "" {
		o.ConfigPath = local.DefaultConfigPath()
	}
	if _, err := os.Stat(o.ConfigPath); err == nil && !o.Force {
		ok, err := p.Confirm(fmt.Sprintf("A config already exists at %s. Replace it?", o.ConfigPath), false)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errStop{"kept the existing config; nothing changed (use --force to replace it non-interactively)"}
		}
	}

	// 1. Database connection and server checks.
	conn, admin, err := connectAdmin(ctx, p, o.AdminURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	if _, err := checkServer(ctx, p, conn); err != nil {
		return nil, err
	}

	// 2. Table, key and columns.
	tbl, err := chooseTable(ctx, p, conn, o.Table)
	if err != nil {
		return nil, err
	}
	if err := checkExistingSetup(ctx, conn, tbl); err != nil {
		return nil, err
	}
	cols, err := chooseColumns(ctx, p, conn, tbl, o.Columns)
	if err != nil {
		return nil, err
	}

	// 3. Embedding model(s).
	models, initial, err := chooseModel(ctx, p, o.Model, o.QueryPrefix)
	if err != nil {
		return nil, err
	}

	// 4. Least-privilege roles with generated passwords.
	pw, err := ensureRoles(ctx, p, conn, o.ResetPasswords)
	if err != nil {
		return nil, err
	}

	// 5. Config + credentials file.
	if err := os.MkdirAll(filepath.Dir(o.ConfigPath), 0o700); err != nil {
		return nil, err
	}
	yml := renderConfig(tbl, cols, models, initial)
	if _, err := config.Parse([]byte(yml)); err != nil {
		return nil, fmt.Errorf("internal error, generated config is invalid: %w", err)
	}
	envPath := config.EnvFilePath(o.ConfigPath)
	dsn := func(role string) string { return roleDSN(admin, role, pw[role]) }
	if err := config.WriteEnvFile(envPath, "embedsync credentials written by `embedsync setup`. Keep this file private.",
		[][2]string{{"EMBEDSYNC_SOURCE_DSN", dsn(roleReplicator)}, {"EMBEDSYNC_SINK_DSN", dsn(roleSink)},
			{"EMBEDSYNC_READER_DSN", dsn(roleReader)}}); err != nil {
		return nil, err
	}
	if err := os.WriteFile(o.ConfigPath, []byte(yml), 0o600); err != nil {
		return nil, err
	}
	cfg, err := config.Load(o.ConfigPath)
	if err != nil {
		return nil, err
	}

	// 6. Check the new roles can log in the way embedsync will connect.
	for _, r := range []string{roleReplicator, roleSink, roleReader} {
		if err := tryLogin(ctx, dsn(r), r == roleReplicator); err != nil {
			return nil, fmt.Errorf("role %s cannot connect: %w\n\nAllow password logins for it in pg_hba.conf "+
				"(e.g. `host all %s 127.0.0.1/32 scram-sha-256`), reload PostgreSQL and run setup again", r, err, r)
		}
	}

	// 7. Publication, control and sink schemas (`embedsync init`).
	roles := dbinit.Roles{Replicator: roleReplicator, Sink: roleSink, Reader: roleReader}
	for _, side := range []string{"source", "sink"} {
		applied, err := dbinit.Apply(ctx, conn, side, cfg, roles)
		if err != nil {
			return nil, fmt.Errorf("initialise %s: %w", side, err)
		}
		for _, f := range applied {
			p.Say("  applied %s", f)
		}
		if side == "source" {
			// Grants and publication membership follow the configured
			// pipelines, also when setup reuses an existing publication.
			changes, err := dbinit.ReconcileSource(ctx, conn, cfg, roles)
			if err != nil {
				return nil, fmt.Errorf("initialise source: %w", err)
			}
			for _, c := range changes {
				p.Say("  %s", c)
			}
		}
	}
	p.Say("\nDone.")
	p.Say("  config:      %s", o.ConfigPath)
	p.Say("  credentials: %s (readable only by you)", envPath)
	return &Result{ConfigPath: o.ConfigPath, EnvPath: envPath, Generation: initial}, nil
}

// IsStop reports whether err is a "follow these instructions, then rerun" stop.
func IsStop(err error) bool {
	var s errStop
	return errors.As(err, &s)
}

func connectAdmin(ctx context.Context, p *Prompter, adminURL string) (*pgx.Conn, *url.URL, error) {
	if adminURL == "" {
		adminURL = os.Getenv("EMBEDSYNC_ADMIN_URL")
	}
	if adminURL == "" && !p.Interactive {
		return nil, nil, fmt.Errorf("%w: pass --admin-url postgres://user:password@host:5432/dbname", ErrNeedInput)
	}
	for attempt := 0; ; attempt++ {
		var u *url.URL
		if adminURL != "" {
			var err error
			if u, err = url.Parse(adminURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
				return nil, nil, fmt.Errorf("--admin-url must look like postgres://user:password@host:5432/dbname")
			}
		} else {
			p.Say("Step 1/4: connect to PostgreSQL as an administrator (used once now, never stored).")
			host, err := p.Ask("  Host", "localhost")
			if err != nil {
				return nil, nil, err
			}
			port, err := p.Ask("  Port", "5432")
			if err != nil {
				return nil, nil, err
			}
			db, err := p.Ask("  Database that contains your table", "postgres")
			if err != nil {
				return nil, nil, err
			}
			user, err := p.Ask("  Admin user", "postgres")
			if err != nil {
				return nil, nil, err
			}
			pass, err := p.Secret("  Password (hidden; empty if none)")
			if err != nil {
				return nil, nil, err
			}
			u = &url.URL{Scheme: "postgres", Host: net.JoinHostPort(host, port), Path: "/" + db}
			if pass != "" {
				u.User = url.UserPassword(user, pass)
			} else {
				u.User = url.User(user)
			}
		}
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		conn, err := pgx.Connect(cctx, u.String())
		cancel()
		if err == nil {
			p.Say("  connected to %s as %s", hostDB(u), u.User.Username())
			return conn, u, nil
		}
		var pgErr *pgconn.PgError
		switch {
		case errors.As(err, &pgErr) && (pgErr.Code == "28P01" || pgErr.Code == "28000"):
			p.Say("  login failed: %s", pgErr.Message)
		case errors.As(err, &pgErr) && pgErr.Code == "3D000":
			p.Say("  %s", pgErr.Message)
		case unreachable(err):
			if postgresInstalled() {
				return nil, nil, errStop{fmt.Sprintf("cannot reach PostgreSQL at %s (%v).\n\n%s\nThen run `embedsync setup` again.",
					u.Host, rootCause(err), startPostgresHelp())}
			}
			return nil, nil, errStop{fmt.Sprintf("cannot reach PostgreSQL at %s and it does not seem to be installed.\n\n%s",
				u.Host, installPostgresHelp())}
		default:
			p.Say("  connection failed: %v", err)
		}
		if adminURL != "" || attempt >= 2 {
			return nil, nil, fmt.Errorf("connect to %s: %w", hostDB(u), err)
		}
		p.Say("  let's try again.")
	}
}

func hostDB(u *url.URL) string { return u.Host + u.Path }

func unreachable(err error) bool {
	var ne net.Error
	var oe *net.OpError
	var de *net.DNSError
	s := err.Error()
	return errors.As(err, &oe) || errors.As(err, &de) || (errors.As(err, &ne) && ne.Timeout()) ||
		strings.Contains(s, "connection refused") || strings.Contains(s, "no such host") ||
		strings.Contains(s, "No connection could be made") || strings.Contains(s, "i/o timeout")
}

func rootCause(err error) error {
	for {
		u := errors.Unwrap(err)
		if u == nil {
			return err
		}
		err = u
	}
}

func checkServer(ctx context.Context, p *Prompter, conn *pgx.Conn) (string, error) {
	var num int
	var walLevel, slots, senders string
	var super bool
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int, current_setting('wal_level'),
		current_setting('max_replication_slots'), current_setting('max_wal_senders'),
		(SELECT rolsuper FROM pg_roles WHERE rolname = current_user)`).Scan(&num, &walLevel, &slots, &senders, &super); err != nil {
		return "", err
	}
	major := strconv.Itoa(num / 10000)
	if num < 150000 {
		return "", errStop{fmt.Sprintf("PostgreSQL %s is too old: logical replication with column lists needs 15+ (embedsync is tested on 16, 17 and 18).", major)}
	}
	if num < 160000 {
		p.Say("  warning: PostgreSQL %s works but embedsync is tested on 16, 17 and 18", major)
	}
	var vector bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')`).Scan(&vector); err != nil {
		return "", err
	}
	if !vector {
		return "", errStop{fmt.Sprintf("the pgvector extension is not installed on this PostgreSQL %s server. Install it:\n\n%s\nThen run `embedsync setup` again.",
			major, pgvectorHelp(major))}
	}
	if slots == "0" || senders == "0" {
		return "", errStop{"max_replication_slots and max_wal_senders must be at least 1 (defaults are 10); raise them in postgresql.conf and restart PostgreSQL."}
	}
	if walLevel != "logical" {
		p.Say("  wal_level is %q; embedsync needs \"logical\" to follow changes.", walLevel)
		if !super {
			return "", errStop{"ask a superuser to run:\n\n  ALTER SYSTEM SET wal_level = 'logical';\n\nthen restart PostgreSQL and run `embedsync setup` again."}
		}
		ok, err := p.Confirm("  Set wal_level = logical now? (takes effect after a PostgreSQL restart)", true)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", errStop{"wal_level must be logical. Run `ALTER SYSTEM SET wal_level = 'logical'`, restart PostgreSQL and run setup again."}
		}
		if _, err := conn.Exec(ctx, `ALTER SYSTEM SET wal_level = 'logical'`); err != nil {
			return "", err
		}
		return "", errStop{fmt.Sprintf("wal_level set to logical. Restart PostgreSQL so it takes effect:\n\n%s\nThen run `embedsync setup` again.", restartHelp(major))}
	}
	p.Say("  PostgreSQL %s, wal_level=logical, pgvector available", major)
	return major, nil
}

type table struct {
	Schema, Name, Key string
	KeyType           string
}

func (t table) qualified() string { return t.Schema + "." + t.Name }

func chooseTable(ctx context.Context, p *Prompter, conn *pgx.Conn, want string) (table, error) {
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, a.attname, a.atttypid::int8, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
		WHERE i.indisprimary AND i.indnkeyatts = 1 AND c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'embedsync', 'embedsync_control')
		  AND n.nspname NOT LIKE 'pg\_%'
		ORDER BY 1, 2`)
	if err != nil {
		return table{}, err
	}
	var ts []table
	var skipped []string
	for rows.Next() {
		var t table
		var oid int64
		if err := rows.Scan(&t.Schema, &t.Name, &t.Key, &oid, &t.KeyType); err != nil {
			rows.Close()
			return table{}, err
		}
		if _, err := keys.FromTypeOID(uint32(oid)); err != nil || !lowerIdent.MatchString(t.Schema) ||
			!lowerIdent.MatchString(t.Name) || !lowerIdent.MatchString(t.Key) {
			skipped = append(skipped, t.qualified())
			continue
		}
		ts = append(ts, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return table{}, err
	}
	if want != "" {
		for _, t := range ts {
			if t.qualified() == want || (!strings.Contains(want, ".") && t.Schema == "public" && t.Name == want) {
				p.Say("  table %s (key %s %s)", t.qualified(), t.Key, t.KeyType)
				return t, nil
			}
		}
		return table{}, fmt.Errorf("table %q not found or not supported (needs a single-column primary key of type "+
			"integer, bigint, smallint, text, varchar or uuid, and lowercase names)", want)
	}
	if len(ts) == 0 {
		msg := "no supported table found in this database. embedsync needs a table with a single-column primary key " +
			"(integer, bigint, smallint, text, varchar or uuid) and lowercase names."
		if len(skipped) > 0 {
			msg += " Unsupported: " + strings.Join(skipped, ", ")
		}
		return table{}, errStop{msg}
	}
	if !p.Interactive {
		return table{}, fmt.Errorf("%w: pass --table schema.table", ErrNeedInput)
	}
	p.Say("\nStep 2/4: which table should be searchable?")
	opts := make([]string, len(ts))
	for i, t := range ts {
		opts[i] = fmt.Sprintf("%s  (key %s %s)", t.qualified(), t.Key, t.KeyType)
	}
	i, err := p.Choose("Tables with a supported primary key:", opts, 0)
	if err != nil {
		return table{}, err
	}
	return ts[i], nil
}

// checkExistingSetup refuses to silently re-point an existing embedsync setup
// in this database at a different table (the publication would keep the old one).
func checkExistingSetup(ctx context.Context, conn *pgx.Conn, t table) error {
	rows, err := conn.Query(ctx, `SELECT schemaname || '.' || tablename FROM pg_publication_tables
		WHERE pubname = 'embedsync_pub' AND schemaname <> 'embedsync_control'`)
	if err != nil {
		return err
	}
	var existing []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return err
		}
		existing = append(existing, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(existing) == 0 || (len(existing) == 1 && existing[0] == t.qualified()) {
		return nil
	}
	return errStop{fmt.Sprintf("this database is already set up for %s; the setup wizard configures one table.\n"+
		"To sync more tables, add them as `pipelines:` to the existing config and run `embedsync init` (see docs/operations.md).\n"+
		"To switch to %s instead, first stop `embedsync run`, then remove the old setup (this deletes its embeddings):\n\n"+
		"  SELECT pg_drop_replication_slot('embedsync_slot') FROM pg_replication_slots WHERE slot_name = 'embedsync_slot';\n"+
		"  DROP PUBLICATION embedsync_pub;\n  DROP SCHEMA embedsync CASCADE;\n  DROP SCHEMA embedsync_control CASCADE;\n\n"+
		"and run `embedsync setup` again.", strings.Join(existing, ", "), t.qualified())}
}

func chooseColumns(ctx context.Context, p *Prompter, conn *pgx.Conn, t table, want []string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT attname, format_type(atttypid, atttypmod), atttypid::regtype::text
		FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped
		ORDER BY attnum`, pgx.Identifier{t.Schema, t.Name}.Sanitize())
	if err != nil {
		return nil, err
	}
	var names, labels []string
	var def []int
	for rows.Next() {
		var name, typ, base string
		if err := rows.Scan(&name, &typ, &base); err != nil {
			rows.Close()
			return nil, err
		}
		if name == t.Key || !lowerIdent.MatchString(name) {
			continue
		}
		switch base {
		case "text", "character varying", "character", "citext", "name":
			def = append(def, len(names))
		}
		names = append(names, name)
		labels = append(labels, name+"  ("+typ+")")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(want) > 0 {
		have := map[string]bool{}
		for _, n := range names {
			have[n] = true
		}
		for _, w := range want {
			if !have[w] {
				return nil, fmt.Errorf("column %q not found in %s (or it is the key)", w, t.qualified())
			}
		}
		p.Say("  embedding columns: %s", strings.Join(want, ", "))
		return want, nil
	}
	if len(names) == 0 {
		return nil, errStop{fmt.Sprintf("%s has no columns besides its key to embed", t.qualified())}
	}
	if len(def) == 0 {
		def = []int{0}
	}
	if !p.Interactive {
		out := make([]string, len(def))
		for i, d := range def {
			out[i] = names[d]
		}
		p.Say("  embedding columns: %s", strings.Join(out, ", "))
		return out, nil
	}
	idx, err := p.ChooseMany("Which columns hold the text to search? (only these are ever read)", labels, def)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(idx))
	for i, d := range idx {
		out[i] = names[d]
	}
	return out, nil
}

// modelChoice is one model written into the config.
type modelChoice struct {
	Key         string
	Name        string
	Revision    string
	Dimension   int
	QueryPrefix string
}

func chooseModel(ctx context.Context, p *Prompter, want, queryPrefix string) ([]modelChoice, string, error) {
	man, err := local.LoadManifest()
	if err != nil {
		return nil, "", err
	}
	var bundled []modelChoice
	taken := map[string]bool{}
	for _, m := range man.Models {
		bundled = append(bundled, modelChoice{m.Key, m.Name, m.Revision, m.Dimension, m.QueryPrefix})
		taken[m.Key] = true
	}
	pickHF := func(spec string) ([]modelChoice, string, error) {
		mc, err := ResolveModel(ctx, p, spec, queryPrefix, taken)
		if err != nil {
			return nil, "", err
		}
		return append([]modelChoice{mc}, bundled...), mc.Key, nil
	}
	if want != "" {
		for _, b := range bundled {
			if want == b.Key || want == b.Name {
				p.Say("  model %s (bundled)", b.Name)
				return bundled, b.Key, nil
			}
		}
		return pickHF(want)
	}
	if !p.Interactive {
		if len(bundled) > 0 {
			p.Say("  model %s (bundled)", bundled[0].Name)
			return bundled, bundled[0].Key, nil
		}
		return nil, "", fmt.Errorf("%w: this package has no bundled model; pass --model <hugging face id>", ErrNeedInput)
	}
	p.Say("\nStep 3/4: choose the embedding model.")
	if len(bundled) == 0 {
		p.Say("This package does not include a model; any sentence-transformers model from Hugging Face works.")
		p.Say("Popular choices: sentence-transformers/all-MiniLM-L6-v2 (small, fast), BAAI/bge-small-en-v1.5 (better quality),")
		p.Say("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 (50+ languages).")
		spec, err := p.Ask("Hugging Face model id", "sentence-transformers/all-MiniLM-L6-v2")
		if err != nil {
			return nil, "", err
		}
		return pickHF(spec)
	}
	opts := make([]string, 0, len(bundled)+1)
	for _, b := range bundled {
		opts = append(opts, fmt.Sprintf("%s  (included, %d dimensions)", b.Name, b.Dimension))
	}
	opts = append(opts, "another model from Hugging Face (downloaded now)")
	i, err := p.Choose("Models:", opts, 0)
	if err != nil {
		return nil, "", err
	}
	if i < len(bundled) {
		return bundled, bundled[i].Key, nil
	}
	spec, err := p.Ask("Hugging Face model id (e.g. BAAI/bge-base-en-v1.5)", "")
	if err != nil {
		return nil, "", err
	}
	return pickHF(spec)
}

// ResolveModel pins a Hugging Face model, downloads it through the bundled
// embedder and measures its dimension.
func ResolveModel(ctx context.Context, p *Prompter, spec, queryPrefix string, taken map[string]bool) (modelChoice, error) {
	name, sha, err := ResolveHF(ctx, spec)
	if err != nil {
		return modelChoice{}, err
	}
	p.Say("  %s pinned to revision %s", name, sha[:12])
	if queryPrefix == "" && p.Interactive {
		queryPrefix, err = p.Ask("  Query prefix (only if the model card asks for one, e.g. BGE/E5 models; Enter for none)", "")
		if err != nil {
			return modelChoice{}, err
		}
	}
	p.Say("  downloading and loading the model (first time only; this can take a few minutes)...")
	ref := local.ModelRef{Name: name, Revision: sha}
	emb, err := local.Start(ctx, local.Options{Models: []local.ModelRef{ref},
		Progress: func(m string) { p.Say("  ...%s", m) }})
	if err != nil {
		return modelChoice{}, err
	}
	defer emb.Stop()
	dims, err := local.Dimensions(ctx, emb.URL)
	if err != nil {
		return modelChoice{}, err
	}
	dim := dims[ref.String()]
	if dim <= 0 {
		return modelChoice{}, fmt.Errorf("could not determine the vector dimension of %s", name)
	}
	if dim > 2000 {
		p.Say("  note: %d dimensions exceeds pgvector's HNSW limit of 2000 for vector; searches may be slow", dim)
	}
	p.Say("  %s ready (%d dimensions)", name, dim)
	return modelChoice{Key: ModelKey(name, taken), Name: name, Revision: sha, Dimension: dim, QueryPrefix: queryPrefix}, nil
}

func ensureRoles(ctx context.Context, p *Prompter, conn *pgx.Conn, reset bool) (map[string]string, error) {
	p.Say("\nStep 4/4: create database roles for embedsync (least privilege, random passwords).")
	pw := map[string]string{}
	for _, r := range []string{roleReplicator, roleSink, roleReader} {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, r).Scan(&exists); err != nil {
			return nil, err
		}
		secret, err := randomPassword()
		if err != nil {
			return nil, err
		}
		attrs := "LOGIN"
		if r == roleReplicator {
			attrs += " REPLICATION"
		}
		if exists && !reset {
			ok, err := p.Confirm(fmt.Sprintf("  Role %s already exists. Give it a new password? (anything else using it must be updated)", r), false)
			if err != nil {
				return nil, err
			}
			if !ok {
				if !p.Interactive {
					return nil, fmt.Errorf("%w: role %s exists; pass --reset-role-passwords to give it a new password", ErrNeedInput, r)
				}
				cur, err := p.Secret(fmt.Sprintf("  Current password of %s", r))
				if err != nil {
					return nil, err
				}
				pw[r] = cur
				continue
			}
		}
		verb := "CREATE"
		if exists {
			verb = "ALTER"
		}
		// The password is generated from [A-Za-z0-9] only, so quoting is safe.
		if _, err := conn.Exec(ctx, fmt.Sprintf("%s ROLE %s WITH %s PASSWORD '%s'", verb, pgx.Identifier{r}.Sanitize(), attrs, secret)); err != nil {
			return nil, fmt.Errorf("%s role %s: %w", strings.ToLower(verb), r, err)
		}
		pw[r] = secret
		p.Say("  %s role %s", map[string]string{"CREATE": "created", "ALTER": "updated"}[verb], r)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s, %s, %s",
		pgx.Identifier{conn.Config().Database}.Sanitize(), roleReplicator, roleSink, roleReader)); err != nil {
		return nil, err
	}
	return pw, nil
}

func randomPassword() (string, error) {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
	b := make([]byte, 32)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		b[i] = alphabet[n.Int64()]
	}
	return string(b), nil
}

func roleDSN(admin *url.URL, role, password string) string {
	u := *admin
	u.User = url.UserPassword(role, password)
	return u.String()
}

func tryLogin(ctx context.Context, dsn string, replication bool) error {
	if replication {
		u, err := url.Parse(dsn)
		if err != nil {
			return err
		}
		q := u.Query()
		q.Set("replication", "database")
		u.RawQuery = q.Encode()
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		c, err := pgconn.Connect(cctx, u.String())
		if err != nil {
			return err
		}
		return c.Close(context.Background())
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	c, err := pgx.Connect(cctx, dsn)
	if err != nil {
		return err
	}
	return c.Close(context.Background())
}

func renderConfig(t table, cols []string, models []modelChoice, initial string) string {
	var b strings.Builder
	b.WriteString(`# embedsync configuration written by ` + "`embedsync setup`" + `.
# Every option is documented in the project's examples/embedsync.yaml.
# Credentials are not stored here: the *_env fields name variables that
# embedsync.env (next to this file) provides.

source:
  dsn_env: EMBEDSYNC_SOURCE_DSN
  table: ` + t.qualified() + `
  key: ` + t.Key + `
  fields:                          # only these columns are ever read or published
`)
	tmpl := make([]string, len(cols))
	for i, c := range cols {
		fmt.Fprintf(&b, "    - { name: %s, column: %s }\n", c, c)
		tmpl[i] = "{{." + c + "}}"
	}
	fmt.Fprintf(&b, `
sink:
  dsn_env: EMBEDSYNC_SINK_DSN
  reader_dsn_env: EMBEDSYNC_READER_DSN
  schema: embedsync
  reader_role: %s

transform:
  template: %q
  chunk_words: 160
  chunk_overlap: 24

embedder:
  managed: true                    # embedsync starts its bundled embedder itself
  timeout: 60s
  batch_size: 32
  concurrency: 2
  requests_per_second: 40
  burst: 4

# Each key can become a generation (see `+"`embedsync migrate --to <key>`"+`).
models:
`, roleReader, strings.Join(tmpl, "\n\n"))
	for _, m := range models {
		fmt.Fprintf(&b, "  %s:\n    name: %s\n    revision: %s\n    dimension: %d\n    distance: cosine\n", m.Key, m.Name, m.Revision, m.Dimension)
		if m.QueryPrefix != "" {
			fmt.Fprintf(&b, "    query_prefix: %q\n", m.QueryPrefix)
		}
	}
	fmt.Fprintf(&b, `
initial_generation: %s

metrics:
  listen: 127.0.0.1:9187           # /metrics /status /search /readyz
`, initial)
	return b.String()
}
