package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/local"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/setup"
)

func cmdSetup(ctx context.Context, args []string) error {
	c := newFlags("setup")
	admin := c.fs.String("admin-url", "", "admin connection URL, e.g. postgres://postgres:secret@localhost:5432/app (or $EMBEDSYNC_ADMIN_URL)")
	table := c.fs.String("table", "", "table to sync, schema.table")
	columns := c.fs.String("columns", "", "comma-separated columns to embed (default: the text columns)")
	model := c.fs.String("model", "", "bundled model key, or a Hugging Face id such as BAAI/bge-small-en-v1.5[@revision]")
	prefix := c.fs.String("query-prefix", "", "query prefix for a Hugging Face model that needs one")
	yes := c.fs.Bool("yes", false, "non-interactive: use flags and defaults, never prompt")
	reset := c.fs.Bool("reset-role-passwords", false, "give existing embedsync roles new passwords")
	force := c.fs.Bool("force", false, "replace an existing config without asking")
	start := c.fs.Bool("start", false, "start `embedsync run` when setup finishes (asked interactively)")
	_ = c.fs.Parse(args)
	interactive := !*yes && term.IsTerminal(int(os.Stdin.Fd()))
	p := setup.NewPrompter(os.Stdin, os.Stdout, interactive)
	var cols []string
	for _, s := range strings.Split(*columns, ",") {
		if s = strings.TrimSpace(s); s != "" {
			cols = append(cols, s)
		}
	}
	res, err := setup.Run(ctx, p, setup.Options{ConfigPath: *c.config, AdminURL: *admin, Table: *table, Columns: cols,
		Model: *model, QueryPrefix: *prefix, ResetPasswords: *reset, Force: *force})
	if setup.IsStop(err) {
		fmt.Fprintln(os.Stderr, "\n"+err.Error())
		return exitCode(1)
	}
	if errors.Is(err, setup.ErrNeedInput) {
		return fmt.Errorf("%w (or run `embedsync setup` in a terminal to be asked)", err)
	}
	if err != nil {
		return err
	}
	runNow := *start
	if !runNow && interactive {
		if runNow, err = p.Confirm("\nStart syncing now (embedsync run)?", true); err != nil {
			return err
		}
	}
	cfgFlag := []string{"--config", res.ConfigPath}
	if !runNow {
		fmt.Printf("\nNext:\n  embedsync run                 start syncing (keep it running; Ctrl-C stops)\n" +
			"  embedsync search \"your query\" search from another terminal\n  embedsync status              progress and health\n")
		if res.ConfigPath != defaultConfig() {
			fmt.Printf("  (add --config %s to each command)\n", res.ConfigPath)
		}
		return nil
	}
	fmt.Println("\nStarting embedsync run (Ctrl-C to stop). In another terminal try: embedsync search \"your query\"")
	return cmdRun(ctx, cfgFlag)
}

func cmdDoctor(ctx context.Context, args []string) error {
	c := newFlags("doctor")
	skip := c.fs.Bool("skip-embedder", false, "do not start the embedder")
	_ = c.fs.Parse(args)
	if n := setup.Doctor(ctx, os.Stdout, *c.config, *skip); n > 0 {
		fmt.Printf("\n%d check(s) failed\n", n)
		return exitCode(1)
	}
	fmt.Println("\nall checks passed")
	return nil
}

func cmdModel(ctx context.Context, args []string) error {
	sub := "list"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		sub, args = args[0], args[1:]
	}
	c := newFlags("model")
	key := c.fs.String("key", "", "config key for the new model (default: derived from its name)")
	prefix := c.fs.String("query-prefix", "", "query prefix, if the model card asks for one")
	_ = c.fs.Parse(args)
	switch sub {
	case "list":
		return setup.ListModels(os.Stdout, *c.config)
	case "add":
		if c.fs.NArg() != 1 {
			return errors.New("usage: embedsync model add <bundled key | owner/name[@revision]> [--key k] [--query-prefix p]")
		}
		if err := c.loadEnv(); err != nil {
			return err
		}
		p := setup.NewPrompter(os.Stdin, os.Stdout, term.IsTerminal(int(os.Stdin.Fd())))
		k, err := setup.AddModel(ctx, p, *c.config, c.fs.Arg(0), *key, *prefix)
		if err != nil {
			return err
		}
		fmt.Printf("added model %q to %s\n", k, *c.config)
		if local.RunPublished() {
			fmt.Println("restart `embedsync run` so its embedder loads the new model, then:")
		} else {
			fmt.Println("next:")
		}
		fmt.Printf("  embedsync migrate --to %s     build, check and switch to it (rollback: embedsync migrate rollback)\n", k)
		return nil
	}
	return fmt.Errorf("unknown model subcommand %q (list|add)", sub)
}
