// Command embedsync keeps pgvector embeddings in sync with a PostgreSQL table
// through logical replication.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/app"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/dbinit"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/local"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/migrate"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/version"
)

const usage = `embedsync %s — CDC-driven embedding sync for PostgreSQL + pgvector

Usage:
  embedsync <command> [flags]

Getting started:
  setup                    guided setup: connect to PostgreSQL, pick a table and a
                           model, create roles and write the config (run this first)
  doctor                   check the installation, database and embedder
  model list|add           list bundled/configured models, add one from Hugging Face

Commands:
  init [--admin-url URL]   apply source/sink SQL migrations (admin URL or admin DSNs);
                           run it after an upgrade, before starting embedsync run
  run                      stream changes, backfill, serve /metrics /status /search
  backfill                 queue a watermark backfill for a generation
  status                   show checkpoint, slot, generations, DLQ, migrations
  migrate --to <model>     build, gate and promote a new embedding generation
  migrate status|rollback|finalize
  search <query>           search the serving (or a given) generation
  verify                   compare the entire sink with the source at a boundary
  dlq list|inspect|replay  inspect and replay quarantined rows
  rebuild --confirm        recover from a lost slot into a fresh generation
  version                  print version

Every command accepts --config (default: $EMBEDSYNC_CONFIG, else ./embedsync.yaml
if present, else the file written by setup: %s).
Credentials are read only from environment variables named in the config; an
embedsync.env file next to the config (written by setup, mode 0600) provides
any that are not already set.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version.Version, local.DefaultConfigPath())
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch cmd {
	case "version", "--version", "-v":
		fmt.Println("embedsync", version.Version)
		return
	case "help", "-h", "--help":
		fmt.Printf(usage, version.Version, local.DefaultConfigPath())
		return
	case "setup":
		err = cmdSetup(ctx, args)
	case "doctor":
		err = cmdDoctor(ctx, args)
	case "model", "models":
		err = cmdModel(ctx, args)
	case "init":
		err = cmdInit(ctx, args)
	case "run":
		err = cmdRun(ctx, args)
	case "backfill":
		err = cmdBackfill(ctx, args)
	case "status":
		err = cmdStatus(ctx, args)
	case "migrate":
		err = cmdMigrate(ctx, args)
	case "search":
		err = cmdSearch(ctx, args)
	case "verify":
		err = cmdVerify(ctx, args)
	case "dlq":
		err = cmdDLQ(ctx, args)
	case "rebuild":
		err = cmdRebuild(ctx, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, cmd, version.Version, local.DefaultConfigPath())
		os.Exit(2)
	}
	runCleanups()
	if err != nil {
		var ec exitCode
		if errors.As(err, &ec) {
			os.Exit(int(ec))
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type exitCode int

func (e exitCode) Error() string { return "exit " + strconv.Itoa(int(e)) }

type common struct {
	fs     *flag.FlagSet
	config *string
	json   *bool
	level  *string
}

// cleanups run before the process exits (stopping a child embedder).
var cleanups []func()

func runCleanups() {
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
	cleanups = nil
}

// defaultConfig picks $EMBEDSYNC_CONFIG, ./embedsync.yaml, or the per-user file from setup.
func defaultConfig() string {
	if v := os.Getenv("EMBEDSYNC_CONFIG"); v != "" {
		return v
	}
	if _, err := os.Stat("embedsync.yaml"); err == nil {
		return "embedsync.yaml"
	}
	return local.DefaultConfigPath()
}

func newFlags(name string) *common {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	def := defaultConfig()
	return &common{fs: fs, config: fs.String("config", def, "path to embedsync.yaml"),
		json: fs.Bool("json", false, "machine-readable output"), level: fs.String("log-level", "info", "debug|info|warn|error")}
}

func (c *common) loadEnv() error { return config.LoadEnvFile(config.EnvFilePath(*c.config)) }

func (c *common) load() (*config.Config, *slog.Logger, error) {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*c.level))
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	if err := config.LoadEnvFile(config.EnvFilePath(*c.config)); err != nil {
		return nil, log, err
	}
	cfg, err := config.Load(*c.config)
	if errors.Is(err, os.ErrNotExist) {
		return nil, log, fmt.Errorf("no config at %s: run `embedsync setup` first (or pass --config)", *c.config)
	}
	return cfg, log, err
}

func open(ctx context.Context, c *common, needSource bool) (*app.Env, error) {
	e, _, err := openWith(ctx, c, needSource, false)
	return e, err
}

// openWithEmbedder also makes sure an embedder is reachable: with
// embedder.managed it reuses the one `embedsync run` started or starts the
// bundled embedder for the duration of this command.
func openWithEmbedder(ctx context.Context, c *common, needSource bool) (*app.Env, *local.Embedder, error) {
	return openWith(ctx, c, needSource, true)
}

func openWith(ctx context.Context, c *common, needSource, needEmbed bool) (*app.Env, *local.Embedder, error) {
	cfg, log, err := c.load()
	if err != nil {
		return nil, nil, err
	}
	var emb *local.Embedder
	if needEmbed && cfg.Embedder.Managed && len(configModels(cfg)) > 0 {
		emb, err = local.Ensure(ctx, local.Options{Models: configModels(cfg),
			Progress: func(m string) { log.Info("waiting for embedder", "status", m) }})
		if err != nil {
			return nil, nil, err
		}
		cleanups = append(cleanups, emb.Stop)
		cfg.Embedder.URL, cfg.Embedder.URLEnv = emb.URL, ""
	}
	e, err := app.Open(ctx, cfg, log, needSource)
	return e, emb, err
}

// configModels lists the models the built-in embedder serves (models of
// other providers are never loaded locally).
func configModels(cfg *config.Config) []local.ModelRef {
	keys := make([]string, 0, len(cfg.Models))
	for k, m := range cfg.Models {
		if m.ProviderName() == config.LocalProvider {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]local.ModelRef, 0, len(keys))
	for _, k := range keys {
		out = append(out, local.ModelRef{Name: cfg.Models[k].Name, Revision: cfg.Models[k].Revision})
	}
	return out
}

func cmdInit(ctx context.Context, args []string) error {
	c := newFlags("init")
	repl := c.fs.String("replicator-role", "embedsync_replicator", "role with REPLICATION that reads the source")
	sink := c.fs.String("sink-role", "embedsync_sink", "role that owns the sink schema")
	reader := c.fs.String("reader-role", "embedsync_reader", "read-only search role")
	srcEnv := c.fs.String("source-admin-dsn-env", "EMBEDSYNC_SOURCE_ADMIN_DSN", "env var with the source admin DSN")
	sinkEnv := c.fs.String("sink-admin-dsn-env", "EMBEDSYNC_SINK_ADMIN_DSN", "env var with the sink admin DSN")
	adminURL := c.fs.String("admin-url", "", "admin connection URL for a side whose admin DSN env var is not set, e.g. postgres://postgres:secret@localhost:5432/app (or $"+adminURLEnv+")")
	_ = c.fs.Parse(args)
	cfg, _, err := c.load()
	if err != nil {
		return err
	}
	// Reject a template that references fields outside its pipeline's
	// allowlist before changing either database.
	if _, err := pipeline.NewSpecs(cfg); err != nil {
		return err
	}
	roles := dbinit.Roles{Replicator: *repl, Sink: *sink, Reader: *reader}
	for _, side := range []struct{ name, env string }{{"source", *srcEnv}, {"sink", *sinkEnv}} {
		dsn, err := resolveAdminDSN(side.env, *adminURL, os.Getenv)
		if err != nil {
			return fmt.Errorf("%s: %w", side.name, err)
		}
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("%s admin connect: %w", side.name, err)
		}
		applied, err := dbinit.Apply(ctx, conn, side.name, cfg, roles)
		var changes, warnings []string
		if err == nil && side.name == "source" {
			// Grants and publication membership follow the configured
			// pipelines, also after the first init.
			changes, warnings, err = dbinit.ReconcileSource(ctx, conn, cfg, roles)
		}
		conn.Close(context.Background())
		if err != nil {
			return err
		}
		if len(applied) == 0 && len(changes) == 0 {
			fmt.Printf("%s: up to date\n", side.name)
		}
		for _, f := range applied {
			fmt.Printf("%s: applied %s\n", side.name, f)
		}
		for _, c := range changes {
			fmt.Printf("%s: %s\n", side.name, c)
		}
		for _, w := range warnings {
			fmt.Fprintf(os.Stderr, "%s: warning: %s\n", side.name, w)
		}
	}
	return nil
}

// adminURLEnv is the variable `embedsync setup` reads its admin URL from.
const adminURLEnv = "EMBEDSYNC_ADMIN_URL"

// resolveAdminDSN picks one side's admin DSN for `embedsync init`: the env var
// envName if it is set, else the --admin-url value, else $EMBEDSYNC_ADMIN_URL.
// Setup uses one database for source and sink, so one admin URL serves both.
// The error never contains a DSN (they carry passwords).
func resolveAdminDSN(envName, flagURL string, getenv func(string) string) (string, error) {
	if envName != "" {
		if v := getenv(envName); v != "" {
			return v, nil
		}
	}
	if flagURL != "" {
		return flagURL, nil
	}
	if v := getenv(adminURLEnv); v != "" {
		return v, nil
	}
	opts := "pass --admin-url postgres://user:password@host:5432/dbname or set " + adminURLEnv
	if envName != "" {
		opts = "set " + envName + ", " + opts
	}
	return "", errors.New("no admin connection: " + opts)
}

func cmdRun(ctx context.Context, args []string) error {
	c := newFlags("run")
	accept := c.fs.Bool("accept-schema-change", false, "accept a reviewed change to the source table's key/field columns")
	_ = c.fs.Parse(args)
	e, emb, err := openWithEmbedder(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	if emb != nil && emb.Exited() != nil {
		// Let `embedsync search`/`migrate` reuse this embedder, and stop if it dies.
		if err := emb.Publish(configModels(e.Cfg)); err != nil {
			e.Log.Warn("could not publish embedder state", "err", err)
		}
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		defer cancel(nil)
		go func() {
			select {
			case <-emb.Exited():
				cancel(fmt.Errorf("bundled embedder exited unexpectedly; log: %s", emb.LogPath))
			case <-ctx.Done():
			}
		}()
		e.Log.Info("bundled embedder ready", "url", emb.URL, "log", emb.LogPath)
	}
	err = app.Run(ctx, app.RunOptions{AcceptSchemaChange: *accept}, e)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

func cmdBackfill(ctx context.Context, args []string) error {
	c := newFlags("backfill")
	gen := c.fs.String("generation", "", "generation id (default: serving)")
	pipe := c.fs.String("pipeline", "", "pipeline name (default: every pipeline)")
	wait := c.fs.Bool("wait", true, "wait for completion")
	_ = c.fs.Parse(args)
	e, err := open(ctx, c, false)
	if err != nil {
		return err
	}
	defer e.Close()
	return app.Backfill(ctx, e, *gen, *pipe, *wait, os.Stdout)
}

func cmdStatus(ctx context.Context, args []string) error {
	c := newFlags("status")
	_ = c.fs.Parse(args)
	e, err := open(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	r, err := app.BuildStatus(ctx, e, "")
	if err != nil {
		return err
	}
	if *c.json {
		return app.PrintJSON(os.Stdout, r)
	}
	app.PrintStatus(os.Stdout, r)
	return nil
}

func cmdMigrate(ctx context.Context, args []string) error {
	sub := ""
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		sub, args = args[0], args[1:]
	}
	c := newFlags("migrate")
	to := c.fs.String("to", "", "model key (from config) or generation id to migrate to")
	restart := c.fs.Bool("restart", false, "build a fresh candidate after a rejected one")
	gateOnly := c.fs.Bool("gate-only", false, "evaluate gates without promoting")
	force := c.fs.Bool("force", false, "finalize before the rollback window ends")
	timeout := c.fs.Duration("catchup-timeout", 0, "override migration.catchup_timeout")
	_ = c.fs.Parse(args)
	var e *app.Env
	var err error
	if sub == "" {
		cfg, _, lerr := c.load()
		if lerr == nil && cfg.Embedder.Managed && local.RunPublished() && !local.RunCovers(configModels(cfg)) {
			return errors.New("`embedsync run` is running with an embedder that does not serve every configured model; " +
				"restart `embedsync run` (it loads the models in the config), then run migrate again")
		}
		e, _, err = openWithEmbedder(ctx, c, true)
	} else {
		e, err = open(ctx, c, true)
	}
	if err != nil {
		return err
	}
	defer e.Close()
	m := &migrate.Migrator{Cfg: e.Cfg, Store: e.Store, Source: e.Source, Trs: e.Trs(), Log: e.Log, Embed: e.Providers, Out: os.Stdout,
		Searcher: app.NewSearcher(e, e.Store.Pool)}
	if *c.json {
		m.Out = os.Stderr
	}
	switch sub {
	case "status":
		ms, err := m.List(ctx, 20)
		if err != nil {
			return err
		}
		return app.PrintJSON(os.Stdout, ms)
	case "rollback":
		g, err := m.Rollback(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("serving %s again\n", g)
		return nil
	case "finalize":
		g, err := m.Finalize(ctx, *force)
		if err != nil {
			return err
		}
		if g == "" {
			fmt.Println("nothing to finalize")
		} else {
			fmt.Printf("retired %s\n", g)
		}
		return nil
	case "":
	default:
		return fmt.Errorf("unknown migrate subcommand %q", sub)
	}
	if *to == "" {
		return errors.New("migrate requires --to <model>")
	}
	rep, err := m.Migrate(ctx, *to, migrate.Options{Restart: *restart, GateOnly: *gateOnly, WaitTimeout: *timeout})
	if *c.json && rep != nil {
		_ = app.PrintJSON(os.Stdout, rep)
	}
	if errors.Is(err, migrate.ErrGateFailed) {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(3)
	}
	return err
}

func cmdSearch(ctx context.Context, args []string) error {
	c := newFlags("search")
	k := c.fs.Int("k", 5, "results")
	gen := c.fs.String("generation", "", "generation id (default: serving)")
	exact := c.fs.Bool("exact", false, "exact search (no ANN index)")
	mode := c.fs.String("mode", "vector", "vector, text or hybrid")
	filt := c.fs.String("filter", "", `metadata filter as JSON, e.g. {"field":"lang","op":"eq","value":"en"}`)
	pipes := c.fs.String("pipeline", "", "comma-separated pipelines/views to search (default all)")
	tenant := c.fs.String("tenant", "", "tenant value (when search.tenant_filter is configured)")
	_ = c.fs.Parse(args)
	if c.fs.NArg() == 0 {
		return errors.New("usage: embedsync search [flags] <query>")
	}
	e, _, err := openWithEmbedder(ctx, c, false)
	if err != nil {
		return err
	}
	defer e.Close()
	pool := e.Store.Pool
	if env := e.Cfg.Sink.ReaderDSNEnv; env != "" {
		if dsn := os.Getenv(env); dsn != "" {
			rs, err := store.New(ctx, dsn, e.Cfg.Sink.Schema, 2)
			if err != nil {
				return fmt.Errorf("reader connect: %w", err)
			}
			defer rs.Close()
			pool = rs.Pool
		}
	}
	s := app.NewSearcher(e, pool)
	q := c.fs.Arg(0)
	for _, a := range c.fs.Args()[1:] {
		q += " " + a
	}
	o := search.Options{K: *k, Generation: *gen, Exact: *exact, Mode: search.Mode(*mode)}
	if *filt != "" {
		o.Filter = []byte(*filt)
	}
	if *pipes != "" {
		o.Pipelines = strings.Split(*pipes, ",")
	}
	if *tenant != "" {
		o.Tenant = tenant
	}
	res, err := s.Search(ctx, q, o)
	if err != nil {
		return err
	}
	if *c.json {
		return app.PrintJSON(os.Stdout, res)
	}
	fmt.Printf("generation %s (%s@%.10s) embed %.1fms query %.1fms\n", res.Generation, res.Model, res.Revision, res.EmbedMS, res.QueryMS)
	for i, h := range res.Hits {
		snippet := h.Content
		if len(snippet) > 110 {
			snippet = snippet[:110] + "…"
		}
		fmt.Printf("%2d. [%s] %.3f  %s\n", i+1, hitLabel(h), h.Score, snippet)
	}
	return nil
}

// hitLabel shows the key alone for the single-table pipeline ("default"), as
// before pipelines existed, and "<pipeline>:<key>" otherwise.
func hitLabel(h search.Hit) string {
	if h.Pipeline == config.DefaultPipeline {
		return h.Key
	}
	return h.Pipeline + ":" + h.Key
}

func cmdVerify(ctx context.Context, args []string) error {
	c := newFlags("verify")
	timeout := c.fs.Duration("timeout", 2*time.Minute, "max wait for the checkpoint to pass the barrier")
	_ = c.fs.Parse(args)
	e, err := open(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	rep, err := app.Verify(ctx, e, *timeout)
	if err != nil {
		return err
	}
	if *c.json {
		_ = app.PrintJSON(os.Stdout, rep)
	} else {
		fmt.Printf("boundary %s checkpoint %s source rows %d\n", rep.BoundaryLSN, rep.CheckpointLSN, rep.SourceRows)
		fmt.Printf("source_state: missing=%d extra=%d stale=%d\n", rep.SourceState.Missing, rep.SourceState.Extra, rep.SourceState.Stale)
		multi := len(e.Cfg.Pipelines) > 1
		if multi {
			for _, p := range e.Cfg.PipelineNames() {
				pr := rep.Pipelines[p]
				fmt.Printf("  pipeline %s: source rows %d, source_state missing=%d extra=%d stale=%d\n", p, pr.SourceRows, pr.SourceState.Missing, pr.SourceState.Extra, pr.SourceState.Stale)
			}
		}
		for _, g := range rep.Generations {
			fmt.Printf("%s: chunks expected=%d actual=%d missing=%d extra=%d stale=%d quarantined_keys=%d\n",
				g.Generation, g.ExpectedChunks, g.ActualChunks, g.Missing, g.Extra, g.Stale, g.Quarantined)
			if g.SearchBuilding {
				fmt.Printf("  search columns: building (meta/tsv not checked)\n")
			} else {
				fmt.Printf("  search columns: meta_stale=%d tsv_stale=%d\n", g.MetaStale, g.TSVStale)
			}
			if multi {
				for _, p := range e.Cfg.PipelineNames() {
					if c := g.Pipelines[p]; c != nil {
						fmt.Printf("  pipeline %s: chunks expected=%d actual=%d missing=%d extra=%d stale=%d meta_stale=%d tsv_stale=%d\n",
							p, c.ExpectedChunks, c.ActualChunks, c.Missing, c.Extra, c.Stale, c.MetaStale, c.TSVStale)
					}
				}
			}
		}
		fmt.Printf("ok=%v\n", rep.OK)
	}
	if !rep.OK {
		return exitCode(4)
	}
	return nil
}

func cmdDLQ(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: embedsync dlq list|inspect <id>|replay [--id N]")
	}
	sub, args := args[0], args[1:]
	c := newFlags("dlq")
	state := c.fs.String("state", "open", "filter by state (open|resolved|superseded|'')")
	id := c.fs.Int64("id", 0, "replay only this record")
	_ = c.fs.Parse(args)
	e, _, err := openWith(ctx, c, false, sub == "replay")
	if err != nil {
		return err
	}
	defer e.Close()
	switch sub {
	case "list":
		es, err := e.Store.DLQ(ctx, *state, 1000)
		if err != nil {
			return err
		}
		if *c.json {
			return app.PrintJSON(os.Stdout, es)
		}
		for _, d := range es {
			fmt.Printf("#%d %-8s gen=%s pipeline=%s key=%s lsn=%s reason=%s attempts=%d\n", d.ID, d.State, d.GenerationID, d.Pipeline, d.SourceKey, d.SourceLSN, d.Reason, d.Attempts)
		}
		return nil
	case "inspect":
		if c.fs.NArg() != 1 {
			return errors.New("usage: embedsync dlq inspect <id>")
		}
		want, _ := strconv.ParseInt(c.fs.Arg(0), 10, 64)
		es, err := e.Store.DLQ(ctx, "", 100000)
		if err != nil {
			return err
		}
		for _, d := range es {
			if d.ID == want {
				return app.PrintJSON(os.Stdout, d)
			}
		}
		return fmt.Errorf("dlq record %d not found", want)
	case "replay":
		res, err := app.ReplayDLQ(ctx, e, *id, os.Stdout)
		if err != nil {
			return err
		}
		return app.PrintJSON(os.Stdout, res)
	}
	return fmt.Errorf("unknown dlq subcommand %q", sub)
}

func cmdRebuild(ctx context.Context, args []string) error {
	c := newFlags("rebuild")
	confirm := c.fs.Bool("confirm", false, "required: drops the slot and checkpoint")
	_ = c.fs.Parse(args)
	if !*confirm {
		return errors.New("rebuild drops the replication slot and checkpoint; pass --confirm after reading docs/operations.md")
	}
	e, err := open(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	_, err = app.Rebuild(ctx, e, os.Stdout)
	return err
}
