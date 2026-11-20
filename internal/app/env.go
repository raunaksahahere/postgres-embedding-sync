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
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// Env bundles shared handles for commands.
type Env struct {
	Cfg    *config.Config
	Log    *slog.Logger
	Store  *store.Store
	Source *pgxpool.Pool
	Embed  *embedclient.Client
	Tr     *transform.Transformer
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
	names := make([]string, 0, len(cfg.Source.Fields))
	for _, f := range cfg.Source.Fields {
		names = append(names, f.Name)
	}
	e.Tr, err = transform.New(cfg.Source.Table, cfg.Transform.Template, names, cfg.Transform.ChunkWords, cfg.Transform.ChunkOverlap, cfg.TransformFingerprint())
	if err != nil {
		e.Close()
		return nil, err
	}
	e.Embed = embedclient.New(cfg.EmbedderURL(), cfg.Embedder.Timeout.Duration, cfg.Embedder.RequestsPerSecond, cfg.Embedder.Burst,
		cfg.Embedder.Retry.InitialBackoff.Duration, cfg.Embedder.Retry.MaxBackoff.Duration, embedclient.Hooks{
			OnRetry: func(string) { metrics.Retries.WithLabelValues("embedder").Inc() },
			OnRequest: func(outcome string, d time.Duration, n int) {
				metrics.EmbedRequests.WithLabelValues(outcome).Inc()
				metrics.EmbedRequestSeconds.Observe(d.Seconds())
			},
		})
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
	return transform.ModelIdentity{Name: g.ModelName, Revision: g.ModelRevision, DocumentPrefix: g.DocumentPrefix}
}
