// Package app wires configuration, connections and pipeline stages into the
// CLI commands.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/dbinit"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// Env bundles shared handles for commands.
type Env struct {
	Cfg    *config.Config
	Log    *slog.Logger
	Store  *store.Store
	Source *pgxpool.Pool
	Embed  *embedclient.Client // the built-in embedder ("local")
	// Providers resolves the provider each generation was built with.
	Providers *embedclient.Set
	// Pipes holds every configured pipeline's transformer. Key codecs are
	// filled in by `run` from the source catalog.
	Pipes pipeline.Specs
}

// Trs returns each pipeline's transformer.
func (e *Env) Trs() map[string]*transform.Transformer { return e.Pipes.Transformers() }

// ErrSinkSchemaOutdated means the sink was created by an older embedsync and
// `embedsync init` has not upgraded it yet.
type ErrSinkSchemaOutdated struct{ Have, Want int }

func (e *ErrSinkSchemaOutdated) Error() string {
	return fmt.Sprintf("the sink schema is at version %d but this embedsync needs version %d: stop `embedsync run`, then run `embedsync init` to upgrade it: "+
		"`embedsync init --admin-url <postgres admin URL>` (or set EMBEDSYNC_SOURCE_ADMIN_DSN / EMBEDSYNC_SINK_ADMIN_DSN); see docs/operations.md", e.Have, e.Want)
}

// Open connects to sink and source and builds the transformer and embed client.
func Open(ctx context.Context, cfg *config.Config, log *slog.Logger, needSource bool) (*Env, error) {
	sinkDSN, err := config.Env(cfg.Sink.DSNEnv)
	if err != nil {
		return nil, err
	}
	st, err := store.New(ctx, sinkDSN, cfg.Sink.Schema, 8)
	if err != nil {
		return nil, err
	}
	e := &Env{Cfg: cfg, Log: log, Store: st}
	want, err := dbinit.SchemaVersion()
	if err != nil {
		st.Close()
		return nil, err
	}
	if have, err := st.SchemaVersion(ctx); err != nil {
		st.Close()
		return nil, fmt.Errorf("read sink schema version (has `embedsync init` been run?): %w", err)
	} else if have < want {
		st.Close()
		return nil, &ErrSinkSchemaOutdated{Have: have, Want: want}
	} else if have > want {
		st.Close()
		return nil, fmt.Errorf("the sink schema is at version %d, newer than this embedsync supports (%d): use the embedsync version that upgraded it", have, want)
	}
	if needSource {
		srcDSN, err := config.Env(cfg.Source.DSNEnv)
		if err != nil {
			st.Close()
			return nil, err
		}
		pc, err := pgxpool.ParseConfig(srcDSN)
		if err != nil {
			st.Close()
			return nil, fmt.Errorf("parse source dsn: %w", err)
		}
		pc.MaxConns = 4
		pc.ConnConfig.RuntimeParams["application_name"] = "embedsync"
		e.Source, err = pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			st.Close()
			return nil, err
		}
		if err := e.Source.Ping(ctx); err != nil {
			e.Close()
			return nil, fmt.Errorf("connect source: %w", err)
		}
	}
	e.Pipes, err = pipeline.NewSpecs(cfg)
	if err != nil {
		e.Close()
		return nil, err
	}
	tel := &telemetry{}
	e.Embed = embedclient.New(cfg.EmbedderURL(), cfg.Embedder.Timeout.Duration, cfg.Embedder.RequestsPerSecond, cfg.Embedder.Burst,
		cfg.Embedder.Retry.InitialBackoff.Duration, cfg.Embedder.Retry.MaxBackoff.Duration, embedclient.Hooks{
			OnRetry: func(reason string) { tel.Retry(config.LocalProvider, "unavailable", 0) },
			OnRequest: func(outcome string, d time.Duration, n int) {
				tel.Request(config.LocalProvider, outcome, d, n)
				if outcome == "ok" {
					tel.Embedded(config.LocalProvider, n, embedclient.Usage{})
				}
			},
		})
	if e.Providers, err = buildProviders(cfg, e.Embed); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

// Close releases connections.
func (e *Env) Close() {
	if e.Source != nil {
		e.Source.Close()
	}
	if e.Store != nil {
		e.Store.Close()
	}
}

// ModelIdentity returns the encoder identity used in fingerprints.
func ModelIdentity(g *store.Generation) transform.ModelIdentity {
	return g.Encoder()
}
