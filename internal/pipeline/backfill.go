package pipeline

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

// SnapRow is one row read by a backfill window.
type SnapRow struct {
	Key    string
	Fields map[string]*string
}

// WindowRows is a snapshot chunk handed from the executor to the planner. It
// is registered before the high watermark is written, so the planner always
// finds it when it reaches the high marker in the stream.
type WindowRows struct {
	WindowID string
	Gen      string // empty for a state window
	Pipeline string
	JobID    int64
	// Kind is the job kind: embed (snapshot rows embedded into Gen), state
	// (snapshot rows of a view component, written to state only) or render
	// (no rows: the planner renders up to Limit view documents after Lo
	// from the state at the end of the unit).
	Kind  string
	Limit int
	Lo    *string
	Rows  []SnapRow
	Final bool

	once    sync.Once
	done    chan struct{}
	outcome string
}

// Finish records the outcome and wakes the executor.
func (w *WindowRows) Finish(outcome string) {
	w.once.Do(func() {
		w.outcome = outcome
		close(w.done)
	})
}

// Registry holds at most a handful of in-flight windows.
type Registry struct {
	mu sync.Mutex
	m  map[string]*WindowRows
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{m: map[string]*WindowRows{}} }

// Put registers a window.
func (r *Registry) Put(w *WindowRows) {
	w.done = make(chan struct{})
	r.mu.Lock()
	r.m[w.WindowID] = w
	r.mu.Unlock()
}

// Take removes and returns a window (nil if unknown, e.g. from a previous process).
func (r *Registry) Take(id string) *WindowRows {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.m[id]
	delete(r.m, id)
	return w
}

// Backfill runs watermark windows for pending jobs, one job at a time. Each
// job scans one pipeline's table into one generation.
type Backfill struct {
	Cfg      *config.Config
	Specs    Specs
	Source   *pgxpool.Pool
	Store    *store.Store
	Registry *Registry
	Log      *slog.Logger
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (b *Backfill) marker(ctx context.Context, kind, gen string, job int64, window string) error {
	var w any
	if window != "" {
		w = window
	}
	_, err := b.Source.Exec(ctx, `INSERT INTO `+pgx.Identifier{b.Cfg.Source.ControlSchema, "markers"}.Sanitize()+
		` (kind, generation_id, job_id, window_id) VALUES ($1, $2, $3, $4)`, kind, gen, job, w)
	return err
}

// Run polls for jobs until ctx ends.
func (b *Backfill) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		job, err := b.Store.NextBackfillJob(ctx)
		if err != nil {
			b.Log.Warn("backfill: load job", "err", err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}
		if job == nil {
			sleepCtx(ctx, time.Second)
			continue
		}
		if err := b.runJob(ctx, job); err != nil && ctx.Err() == nil {
			b.Log.Warn("backfill job interrupted; will resume", "job", job.ID, "err", err)
			sleepCtx(ctx, 2*time.Second)
		}
	}
	return nil
}

func (b *Backfill) runJob(ctx context.Context, job *store.BackfillJob) error {
	spec := b.Specs[job.Pipeline]
	if spec == nil {
		return fmt.Errorf("backfill job %d is for pipeline %q, which is not configured", job.ID, job.Pipeline)
	}
	kind := job.Kind
	if kind == "" {
		kind = store.JobEmbed
	}
	gen := job.GenerationID
	if kind != store.JobState {
		// embed and render jobs fill one generation.
		g, err := b.Store.Generation(ctx, gen)
		if err != nil {
			return err
		}
		if g == nil {
			return fmt.Errorf("generation %s missing", gen)
		}
		if g.StreamFromLSN == nil {
			// The start marker orders "this generation consumes changes from here"
			// before every window of the job.
			if err := b.marker(ctx, "start", g.ID, job.ID, ""); err != nil {
				return fmt.Errorf("write start marker: %w", err)
			}
			for {
				g, err = b.Store.Generation(ctx, gen)
				if err != nil {
					return err
				}
				if g.StreamFromLSN != nil {
					break
				}
				if !sleepCtx(ctx, 200*time.Millisecond) {
					return ctx.Err()
				}
			}
		}
	}
	if err := b.Store.MarkJobRunning(ctx, job.ID); err != nil {
		return err
	}
	b.Log.Info("backfill started", "job", job.ID, "kind", kind, "generation", gen, "pipeline", spec.Name)
	limit := b.Cfg.Backfill.BatchSize
	discards := 0
	for ctx.Err() == nil {
		cur, err := b.Store.BackfillJobByID(ctx, job.ID)
		if err != nil {
			return err
		}
		if cur == nil {
			return fmt.Errorf("backfill job %d disappeared", job.ID)
		}
		if cur.State == "done" || cur.State == "cancelled" {
			b.Log.Info("backfill finished", "job", job.ID, "kind", kind, "generation", gen, "pipeline", spec.Name, "rows", cur.RowsScanned, "windows", cur.Windows)
			b.cleanupMarkers(ctx)
			return nil
		}
		// A generation that stopped being maintained (retired, rejected, stale)
		// can never complete a window; cancel instead of retrying forever and
		// blocking every later job.
		if kind != store.JobState {
			if gs, err := b.Store.Generation(ctx, gen); err != nil {
				return err
			} else if gs == nil || !(store.IsLive(gs.State) || gs.State == store.StatePending) {
				if _, err := b.Store.Pool.Exec(ctx, `UPDATE `+b.Store.T("backfill_jobs")+` SET state = 'cancelled', updated_at = now() WHERE id = $1 AND state IN ('pending','running')`, job.ID); err != nil {
					return err
				}
				b.Log.Warn("backfill cancelled: generation is no longer maintained", "job", job.ID, "generation", gen)
				return nil
			}
		}
		var outcome string
		if kind == store.JobRender {
			outcome, err = b.renderWindow(ctx, spec, gen, job.ID, cur.LastKey, limit)
		} else {
			outcome, err = b.window(ctx, spec, kind, gen, job.ID, cur.LastKey, limit)
		}
		if err != nil {
			return err
		}
		if outcome == "discarded" {
			discards++
			if limit > 1 && kind != store.JobRender {
				limit = max(1, limit/2)
			}
			sleepCtx(ctx, time.Duration(min(discards, 10))*50*time.Millisecond)
			continue
		}
		discards = 0
		limit = b.Cfg.Backfill.BatchSize
		if b.Cfg.Backfill.WindowPause.Duration > 0 {
			sleepCtx(ctx, b.Cfg.Backfill.WindowPause.Duration)
		}
	}
	return ctx.Err()
}

// window brackets one bounded primary-key scan with committed low/high
// markers. kind is embed (rows embedded into gen) or state (a view
// component's rows, written to state only; gen is empty).
func (b *Backfill) window(ctx context.Context, spec *Spec, kind, gen string, job int64, lo *string, limit int) (string, error) {
	id := newUUID()
	if err := b.marker(ctx, "low", gen, job, id); err != nil {
		return "", fmt.Errorf("low marker: %w", err)
	}
	if err := b.waitInFlight(ctx); err != nil {
		return "", err
	}
	rows, err := b.scan(ctx, spec, lo, limit)
	if err != nil {
		return "", err
	}
	w := &WindowRows{WindowID: id, Gen: gen, Pipeline: spec.Name, JobID: job, Kind: kind, Lo: lo, Rows: rows, Final: len(rows) < limit}
	b.Registry.Put(w)
	if err := b.marker(ctx, "high", gen, job, id); err != nil {
		b.Registry.Take(id)
		return "", fmt.Errorf("high marker: %w", err)
	}
	select {
	case <-w.done:
		return w.outcome, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// renderWindow asks the planner to render the next limit documents of a
// view after lo into gen. It scans nothing: the planner reads the view's
// state at the window's position in the stream.
func (b *Backfill) renderWindow(ctx context.Context, spec *Spec, gen string, job int64, lo *string, limit int) (string, error) {
	id := newUUID()
	if err := b.marker(ctx, "low", gen, job, id); err != nil {
		return "", fmt.Errorf("low marker: %w", err)
	}
	w := &WindowRows{WindowID: id, Gen: gen, Pipeline: spec.Name, JobID: job, Kind: store.JobRender, Lo: lo, Limit: limit}
	b.Registry.Put(w)
	if err := b.marker(ctx, "high", gen, job, id); err != nil {
		b.Registry.Take(id)
		return "", fmt.Errorf("high marker: %w", err)
	}
	select {
	case <-w.done:
		return w.outcome, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// waitInFlight waits until every transaction that was in progress right after
// the low marker committed has finished. WAL commit order is not visibility
// order: a transaction whose commit record precedes the low marker in the
// stream can still be invisible to a snapshot taken just after it (it has not
// left the proc array yet). Its change would then be neither in the window's
// "changed" set nor in the scan, and the window would resurrect a deleted row
// or range-delete an inserted one. Once these transactions have ended, any
// transaction a fresh snapshot cannot see commits after the low marker and is
// therefore seen by the planner between low and high.
func (b *Backfill) waitInFlight(ctx context.Context) error {
	var snap string
	if err := b.Source.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snap); err != nil {
		return fmt.Errorf("backfill snapshot: %w", err)
	}
	start := time.Now()
	warned := false
	for {
		var n int
		if err := b.Source.QueryRow(ctx, `SELECT count(*) FROM pg_snapshot_xip($1::pg_snapshot) AS x WHERE pg_xact_status(x) = 'in progress'`,
			snap).Scan(&n); err != nil {
			return fmt.Errorf("backfill wait for in-flight transactions: %w", err)
		}
		if n == 0 {
			return nil
		}
		if !warned && time.Since(start) > 30*time.Second {
			b.Log.Warn("backfill window waiting for long-running source transactions to finish", "transactions", n)
			warned = true
		}
		if !sleepCtx(ctx, 10*time.Millisecond) {
			return ctx.Err()
		}
	}
}

func (b *Backfill) scan(ctx context.Context, spec *Spec, lo *string, limit int) ([]SnapRow, error) {
	q, args, err := ScanQuery(spec.Def, spec.Codec, lo, limit)
	if err != nil {
		return nil, err
	}
	nk := len(spec.Def.Key)
	fields := spec.Def.AllFields()
	cols := nk + len(fields)
	rs, err := b.Source.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("backfill scan %s: %w", spec.Name, err)
	}
	defer rs.Close()
	var out []SnapRow
	for rs.Next() {
		vals := make([]*string, cols)
		ptrs := make([]any, cols)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		kv := make([]string, nk)
		for i := range kv {
			if vals[i] == nil {
				return nil, fmt.Errorf("backfill scan %s: NULL primary key value", spec.Name)
			}
			kv[i] = *vals[i]
		}
		key, err := spec.Codec.Encode(kv)
		if err != nil {
			return nil, fmt.Errorf("backfill scan %s: %w", spec.Name, err)
		}
		r := SnapRow{Key: key, Fields: map[string]*string{}}
		for i, f := range fields {
			r.Fields[f.Name] = vals[nk+i]
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

func (b *Backfill) cleanupMarkers(ctx context.Context) {
	_, err := b.Source.Exec(ctx, `DELETE FROM `+pgx.Identifier{b.Cfg.Source.ControlSchema, "markers"}.Sanitize()+` WHERE created_at < now() - interval '1 hour'`)
	if err != nil {
		b.Log.Warn("marker cleanup failed", "err", err)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ScanQuery builds one bounded primary-key scan of a pipeline's table.
// Columns are table-qualified so ORDER BY and WHERE use the typed key
// columns, never the ::text output aliases (an unqualified ORDER BY "id"
// would bind to the text output and sort "10" before "9"). A composite key is
// compared as a row, which PostgreSQL orders lexicographically, column by
// column, the same way keys.Codec.Compare does.
func ScanQuery(def *config.PipelineDef, codec keys.Codec, lo *string, limit int) (string, []any, error) {
	schema, table := def.SchemaTable()
	keyCols := make([]string, len(def.Key))
	fields := def.AllFields()
	cols := make([]string, 0, len(def.Key)+len(fields))
	for i, k := range def.Key {
		keyCols[i] = "t." + pgx.Identifier{k}.Sanitize()
		cols = append(cols, keyCols[i]+"::text")
	}
	for _, f := range fields {
		cols = append(cols, "t."+pgx.Identifier{f.Column}.Sanitize()+"::text")
	}
	q := `SELECT ` + strings.Join(cols, ", ") + ` FROM ` + pgx.Identifier{schema, table}.Sanitize() + ` AS t`
	var args []any
	if lo != nil {
		pred, err := codec.SourceCompare(keyCols, ">", *lo, &args)
		if err != nil {
			return "", nil, err
		}
		q += ` WHERE ` + pred
	}
	q += ` ORDER BY ` + codec.SourceOrderBy(keyCols) + fmt.Sprintf(` LIMIT %d`, limit)
	return q, args, nil
}
