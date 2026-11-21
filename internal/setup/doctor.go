package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/local"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/version"
)

type doctor struct {
	out   io.Writer
	fails int
}

func (d *doctor) ok(f string, a ...any)   { fmt.Fprintf(d.out, "[ok]   "+f+"\n", a...) }
func (d *doctor) warn(f string, a ...any) { fmt.Fprintf(d.out, "[warn] "+f+"\n", a...) }
func (d *doctor) fail(f string, a ...any) {
	d.fails++
	fmt.Fprintf(d.out, "[FAIL] "+f+"\n", a...)
}

// Doctor checks the installation, config, database and embedder, printing one
// line per check. It returns the number of failed checks.
func Doctor(ctx context.Context, out io.Writer, configPath string, skipEmbedder bool) int {
	d := &doctor{out: out}
	dir, _ := local.InstallDir()
	d.ok("embedsync %s (%s)", version.Version, dir)

	bin, binErr := local.EmbedderBinary()
	if binErr != nil {
		d.warn("bundled embedder not found (fine if embedder.url points at your own embedder)")
	} else {
		d.ok("bundled embedder: %s", bin)
	}
	man, err := local.LoadManifest()
	switch {
	case err != nil:
		d.fail("bundled models: %v", err)
	case len(man.Models) == 0:
		d.ok("no bundled models (models are downloaded from Hugging Face when first used)")
	default:
		for _, m := range man.Models {
			p := local.BundledModelDir(m.Name, m.Revision)
			if _, err := os.Stat(filepath.Join(p, "modules.json")); err != nil {
				d.fail("bundled model %s missing at %s", m.Name, p)
			} else {
				d.ok("bundled model %s (%d dimensions)", m.Name, m.Dimension)
			}
		}
	}

	var cfg *config.Config
	if _, err := os.Stat(configPath); err != nil {
		d.warn("no config at %s: run `embedsync setup`", configPath)
	} else if err := config.LoadEnvFile(config.EnvFilePath(configPath)); err != nil {
		d.fail("credentials file: %v", err)
	} else if cfg, err = config.Load(configPath); err != nil {
		d.fail("config %s: %v", configPath, err)
		cfg = nil
	} else {
		tables := make([]string, 0, len(cfg.Pipelines))
		for _, p := range cfg.Pipelines {
			tables = append(tables, p.Table)
		}
		d.ok("config %s (table %s, %d model(s))", configPath, strings.Join(tables, ", "), len(cfg.Models))
	}

	if cfg != nil {
		d.database(ctx, cfg)
	}

	if skipEmbedder {
		return d.fails
	}
	var models []local.ModelRef
	var probe local.ModelRef
	switch {
	case cfg != nil && !cfg.Embedder.Managed:
		d.embedURL(ctx, cfg)
		return d.fails
	case cfg != nil:
		keys := make([]string, 0, len(cfg.Models))
		for k := range cfg.Models {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			models = append(models, local.ModelRef{Name: cfg.Models[k].Name, Revision: cfg.Models[k].Revision})
		}
		m := cfg.Models[cfg.InitialGeneration]
		probe = local.ModelRef{Name: m.Name, Revision: m.Revision}
	case man != nil && len(man.Models) > 0:
		probe = local.ModelRef{Name: man.Models[0].Name, Revision: man.Models[0].Revision}
		models = []local.ModelRef{probe}
	default:
		d.ok("embedder check skipped: no model configured yet")
		return d.fails
	}
	if binErr != nil {
		d.fail("embedder: %v", binErr)
		return d.fails
	}
	t0 := time.Now()
	emb, err := local.Ensure(ctx, local.Options{Models: models})
	if err != nil {
		d.fail("embedder: %v", err)
		return d.fails
	}
	defer emb.Stop()
	dim, err := embedOnce(ctx, emb.URL, probe)
	if err != nil {
		d.fail("embedder: %v", err)
		return d.fails
	}
	d.ok("embedder serves %d model(s); embedded a test sentence with %s (%d dimensions) in %s",
		len(models), probe.Name, dim, time.Since(t0).Round(100*time.Millisecond))
	return d.fails
}

func (d *doctor) database(ctx context.Context, cfg *config.Config) {
	for _, side := range []struct{ name, env string }{{"source", cfg.Source.DSNEnv}, {"sink", cfg.Sink.DSNEnv}} {
		dsn := os.Getenv(side.env)
		if dsn == "" {
			d.fail("%s: environment variable %s is not set (run `embedsync setup`)", side.name, side.env)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := pgx.Connect(cctx, dsn)
		cancel()
		if err != nil {
			d.fail("%s: cannot connect: %v", side.name, err)
			continue
		}
		if side.name == "source" {
			var ver, wal string
			var pub, slot, active bool
			err = conn.QueryRow(ctx, `SELECT current_setting('server_version'), current_setting('wal_level'),
				EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1),
				EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $2),
				COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $2), false)`,
				cfg.Source.Publication, cfg.Source.Slot).Scan(&ver, &wal, &pub, &slot, &active)
			switch {
			case err != nil:
				d.fail("source: %v", err)
			case wal != "logical":
				d.fail("source: wal_level is %s, must be logical (ALTER SYSTEM SET wal_level = 'logical'; then restart PostgreSQL)", wal)
			case !pub:
				d.fail("source: publication %s missing: run `embedsync setup` (or `embedsync init`)", cfg.Source.Publication)
			default:
				d.ok("source: PostgreSQL %s, wal_level=logical, publication %s", ver, cfg.Source.Publication)
				switch {
				case !slot:
					d.ok("source: replication slot not created yet (`embedsync run` creates it)")
				case active:
					d.ok("source: replication slot %s is in use (embedsync run is streaming)", cfg.Source.Slot)
				default:
					d.warn("source: replication slot %s exists but nothing is streaming: start `embedsync run` "+
						"(an idle slot makes PostgreSQL keep WAL)", cfg.Source.Slot)
				}
			}
		} else {
			var vec string
			err = conn.QueryRow(ctx, `SELECT COALESCE((SELECT extversion FROM pg_extension WHERE extname = 'vector'), '')`).Scan(&vec)
			switch {
			case err != nil:
				d.fail("sink: %v", err)
			case vec == "":
				d.fail("sink: pgvector extension not created: run `embedsync setup`")
			default:
				d.ok("sink: pgvector %s", vec)
			}
		}
		conn.Close(context.Background())
	}
}

func (d *doctor) embedURL(ctx context.Context, cfg *config.Config) {
	m := cfg.Models[cfg.InitialGeneration]
	dim, err := embedOnce(ctx, cfg.EmbedderURL(), local.ModelRef{Name: m.Name, Revision: m.Revision})
	if err != nil {
		d.fail("embedder at %s: %v", cfg.EmbedderURL(), err)
		return
	}
	d.ok("embedder at %s embedded a test sentence (%d dimensions)", cfg.EmbedderURL(), dim)
}

func embedOnce(ctx context.Context, url string, m local.ModelRef) (int, error) {
	body, _ := json.Marshal(map[string]any{"model": m.Name, "revision": m.Revision, "kind": "query",
		"inputs": []string{"embedsync doctor test sentence"}})
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url+"/v1/embed", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("embed: %s: %s", resp.Status, bytes.TrimSpace(b))
	}
	var r struct {
		Dimension  int         `json:"dimension"`
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	if len(r.Embeddings) != 1 || len(r.Embeddings[0]) != r.Dimension {
		return 0, errors.New("embed: malformed response")
	}
	return r.Dimension, nil
}
