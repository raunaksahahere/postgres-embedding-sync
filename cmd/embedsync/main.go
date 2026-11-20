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
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/app"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/dbinit"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/migrate"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/version"
)

const usage = `embedsync %s — CDC-driven embedding sync for PostgreSQL + pgvector

Usage:
  embedsync <command> [flags]

Commands:
  init                     apply source/sink SQL migrations (admin DSNs)
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

Every command accepts --config (default ./embedsync.yaml or $EMBEDSYNC_CONFIG).
Credentials are read only from environment variables named in the config.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version.Version)
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
		fmt.Printf(usage, version.Version)
		return
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
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, cmd, version.Version)
		os.Exit(2)
	}
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

func newFlags(name string) *common {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	def := os.Getenv("EMBEDSYNC_CONFIG")
	if def == "" {
		def = "embedsync.yaml"
	}
	return &common{fs: fs, config: fs.String("config", def, "path to embedsync.yaml"),
		json: fs.Bool("json", false, "machine-readable output"), level: fs.String("log-level", "info", "debug|info|warn|error")}
}

func (c *common) load() (*config.Config, *slog.Logger, error) {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*c.level))
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	cfg, err := config.Load(*c.config)
	return cfg, log, err
}

func open(ctx context.Context, c *common, needSource bool) (*app.Env, error) {
	cfg, log, err := c.load()
	if err != nil {
		return nil, err
	}
	return app.Open(ctx, cfg, log, needSource)
}

func cmdInit(ctx context.Context, args []string) error {
	c := newFlags("init")
	repl := c.fs.String("replicator-role", "embedsync_replicator", "role with REPLICATION that reads the source")
	sink := c.fs.String("sink-role", "embedsync_sink", "role that owns the sink schema")
	reader := c.fs.String("reader-role", "embedsync_reader", "read-only search role")
	srcEnv := c.fs.String("source-admin-dsn-env", "EMBEDSYNC_SOURCE_ADMIN_DSN", "env var with the source admin DSN")
	sinkEnv := c.fs.String("sink-admin-dsn-env", "EMBEDSYNC_SINK_ADMIN_DSN", "env var with the sink admin DSN")
	_ = c.fs.Parse(args)
	cfg, _, err := c.load()
	if err != nil {
		return err
	}
	roles := dbinit.Roles{Replicator: *repl, Sink: *sink, Reader: *reader}
	for _, side := range []struct{ name, env string }{{"source", *srcEnv}, {"sink", *sinkEnv}} {
		dsn, err := config.Env(side.env)
		if err != nil {
			return err
		}
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("%s admin connect: %w", side.name, err)
		}
		applied, err := dbinit.Apply(ctx, conn, side.name, cfg, roles)
		conn.Close(context.Background())
		if err != nil {
			return err
		}
		if len(applied) == 0 {
			fmt.Printf("%s: up to date\n", side.name)
		}
		for _, f := range applied {
			fmt.Printf("%s: applied %s\n", side.name, f)
		}
	}
	return nil
}

func cmdRun(ctx context.Context, args []string) error {
	c := newFlags("run")
	accept := c.fs.Bool("accept-schema-change", false, "accept a reviewed change to the source table's key/field columns")
	_ = c.fs.Parse(args)
	e, err := open(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	return app.Run(ctx, app.RunOptions{AcceptSchemaChange: *accept}, e)
}

func cmdBackfill(ctx context.Context, args []string) error {
	c := newFlags("backfill")
	gen := c.fs.String("generation", "", "generation id (default: serving)")
	wait := c.fs.Bool("wait", true, "wait for completion")
	_ = c.fs.Parse(args)
	e, err := open(ctx, c, false)
	if err != nil {
		return err
	}
	defer e.Close()
	return app.Backfill(ctx, e, *gen, *wait, os.Stdout)
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
	e, err := open(ctx, c, true)
	if err != nil {
		return err
	}
	defer e.Close()
	m := &migrate.Migrator{Cfg: e.Cfg, Store: e.Store, Source: e.Source, Tr: e.Tr, Log: e.Log, Embed: e.Embed, Out: os.Stdout,
		Searcher: &search.Searcher{Pool: e.Store.Pool, Schema: e.Cfg.Sink.Schema, Client: e.Embed}}
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
	_ = c.fs.Parse(args)
	if c.fs.NArg() == 0 {
		return errors.New("usage: embedsync search [flags] <query>")
	}
	e, err := open(ctx, c, false)
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
	s := &search.Searcher{Pool: pool, Schema: e.Cfg.Sink.Schema, Client: e.Embed}
	q := c.fs.Arg(0)
	for _, a := range c.fs.Args()[1:] {
		q += " " + a
	}
	res, err := s.Search(ctx, q, search.Options{K: *k, Generation: *gen, Exact: *exact})
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
		fmt.Printf("%2d. [%s] %.3f  %s\n", i+1, h.Key, h.Score, snippet)
	}
	return nil
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
		for _, g := range rep.Generations {
			fmt.Printf("%s: chunks expected=%d actual=%d missing=%d extra=%d stale=%d quarantined_keys=%d\n",
				g.Generation, g.ExpectedChunks, g.ActualChunks, g.Missing, g.Extra, g.Stale, g.Quarantined)
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
	e, err := open(ctx, c, false)
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
			fmt.Printf("#%d %-8s gen=%s key=%s lsn=%s reason=%s attempts=%d\n", d.ID, d.State, d.GenerationID, d.SourceKey, d.SourceLSN, d.Reason, d.Attempts)
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
