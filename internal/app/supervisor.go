package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/migrate"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

type runState struct {
	progress atomic.Pointer[pipeline.Progress]
	mu       sync.Mutex
	paused   *pipeline.PauseError
	lastErr  string
	consumer atomic.Value // string
}

func (s *runState) setProgress(p *pipeline.Progress) { s.progress.Store(p) }

func (s *runState) setPaused(p *pipeline.PauseError) {
	s.mu.Lock()
	s.paused = p
	s.mu.Unlock()
}

func (s *runState) pausedErr() *pipeline.PauseError {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused
}

// ConsumerState returns the last computed consumer state.
func (s *runState) ConsumerState() string {
	if v, ok := s.consumer.Load().(string); ok {
		return v
	}
	return "starting"
}

// Run is `embedsync run`: it supervises the pipeline, restarting it with
// backoff after transient failures and holding still (without acknowledging
// anything) when paused.
func Run(ctx context.Context, opts RunOptions, e *Env) error {
	cfg := e.Cfg
	state := &runState{}
	searcher := NewSearcher(e, e.Store.Pool)
	if dsnEnv := cfg.Sink.ReaderDSNEnv; dsnEnv != "" {
		if dsn, err := config.Env(dsnEnv); err == nil {
			if rs, err := store.New(ctx, dsn, cfg.Sink.Schema, 8); err == nil {
				searcher.Pool = rs.Pool
				defer rs.Close()
			} else {
				e.Log.Warn("reader DSN unusable; searching with the sink role", "err", err)
			}
		}
	}
	srv := &http.Server{Addr: cfg.Metrics.Listen, Handler: Handler(e, state, searcher), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.Log.Error("http server failed", "err", err)
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	setThresholds(cfg)
	go monitor(ctx, e, state)

	backoff := time.Second
	for {
		err := pipelineRun(ctx, e, opts, state)
		if ctx.Err() != nil {
			_ = e.Store.SetPipelineStatus(context.Background(), "stopped", "", "graceful shutdown")
			return nil
		}
		if pe, ok := pipeline.IsPause(err); ok {
			state.setPaused(pe)
			metrics.SetConsumerState("paused")
			state.consumer.Store("paused")
			if pe.InStream && pe.Reason == "schema_incompatible" {
				_ = e.Store.MarkSchemaPause(context.Background(), pe.Pipeline)
			}
			_ = e.Store.SetPipelineStatus(context.Background(), "paused", pe.Reason, pe.Detail)
			e.Log.Error("pipeline paused; nothing past the last checkpoint was acknowledged", "reason", pe.Reason, "detail", pe.Detail)
			<-ctx.Done()
			return nil
		}
		state.mu.Lock()
		if err != nil {
			state.lastErr = err.Error()
		}
		state.mu.Unlock()
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		_ = e.Store.SetPipelineStatus(context.Background(), "restarting", "error", msg)
		e.Log.Warn("pipeline stopped; restarting", "err", err, "backoff", backoff)
		metrics.Retries.WithLabelValues("pipeline").Inc()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func setThresholds(cfg *config.Config) {
	metrics.AlertThreshold.WithLabelValues("freshness_seconds").Set(cfg.Alerts.FreshnessSeconds)
	metrics.AlertThreshold.WithLabelValues("retained_wal_warning_bytes").Set(float64(cfg.Alerts.RetainedWALWarningBytes))
	metrics.AlertThreshold.WithLabelValues("retained_wal_critical_bytes").Set(float64(cfg.Alerts.RetainedWALCriticalBytes))
	metrics.AlertThreshold.WithLabelValues("stall_after_seconds").Set(cfg.Pipeline.StallAfter.Seconds())
}

// monitor refreshes gauges and distinguishes idle from stalled.
func monitor(ctx context.Context, e *Env, state *runState) {
	fast := time.NewTicker(time.Second)
	defer fast.Stop()
	var unresolvedAge time.Duration
	slow := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-fast.C:
		}
		var oldestAge time.Duration
		cs := "starting"
		if p := state.progress.Load(); p != nil {
			n, oldest := p.Pending()
			metrics.PendingTransactions.Set(float64(n))
			if n > 0 {
				oldestAge = time.Since(oldest)
			}
			metrics.OldestPendingAge.Set(oldestAge.Seconds())
			msgAge := time.Since(p.LastMessage())
			metrics.LastMessageAge.Set(msgAge.Seconds())
			stall := e.Cfg.Pipeline.StallAfter.Duration
			sinceAdvance := time.Since(p.LastAdvance())
			metrics.LastProgressAge.Set(sinceAdvance.Seconds())
			// Stalled means no durable progress despite pending work, or a
			// silent server connection (periodic status updates request a
			// reply, so a live connection is never silent for long). A
			// consumer that is behind but advancing is active; its lag shows
			// in the freshness metric instead.
			switch {
			case n > 0 && sinceAdvance > stall, msgAge > stall:
				cs = "stalled"
			case n > 0:
				cs = "active"
			default:
				cs = "idle"
			}
		}
		if state.pausedErr() != nil {
			cs = "paused"
		}
		state.consumer.Store(cs)
		metrics.SetConsumerState(cs)
		metrics.FreshnessLag.Set(max(oldestAge, unresolvedAge).Seconds())

		if time.Since(slow) < 5*time.Second {
			continue
		}
		slow = time.Now()
		qctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		if slot, err := LoadSlot(qctx, e); err == nil && slot.Exists {
			metrics.RetainedWAL.Set(float64(slot.RetainedB))
			metrics.ConfirmedLag.Set(float64(slot.ConfirmedLag))
			if slot.SafeWALSize != nil {
				metrics.SlotSafeWAL.Set(float64(*slot.SafeWALSize))
			} else {
				metrics.SlotSafeWAL.Set(-1)
			}
			if slot.Active {
				metrics.SlotActive.Set(1)
			} else {
				metrics.SlotActive.Set(0)
			}
			for _, s := range []string{"reserved", "extended", "unreserved", "lost"} {
				v := 0.0
				if s == slot.WALStatus {
					v = 1
				}
				metrics.SlotWALStatus.WithLabelValues(s).Set(v)
			}
		}
		if counts, ages, err := e.Store.UnresolvedCounts(qctx); err == nil {
			unresolvedAge = 0
			gens, _ := e.Store.Generations(qctx)
			metrics.GenerationInfo.Reset()
			metrics.Unresolved.Reset()
			for _, g := range gens {
				metrics.GenerationInfo.WithLabelValues(g.ID, g.State, g.ModelName).Set(1)
				if store.IsLive(g.State) {
					metrics.Unresolved.WithLabelValues(g.ID).Set(float64(counts[g.ID]))
					unresolvedAge = max(unresolvedAge, ages[g.ID])
				}
			}
		}
		if sv, err := e.Store.Serving(qctx); err == nil && sv != nil {
			metrics.Serving.Reset()
			metrics.Serving.WithLabelValues(sv.GenerationID).Set(1)
		}
		if err := (&migrate.Migrator{Cfg: e.Cfg, Store: e.Store, Source: e.Source, Trs: e.Trs(), Log: e.Log}).FinalizeExpired(qctx); err != nil {
			e.Log.Warn("rollback window finalization failed", "err", err)
		}
		cancel()
	}
}

// Handler serves /metrics, /healthz, /readyz, /status and /search.
func Handler(e *Env, state *runState, searcher *search.Searcher) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		cs := state.ConsumerState()
		if cs == "idle" || cs == "active" {
			_, _ = w.Write([]byte(cs + "\n"))
			return
		}
		http.Error(w, cs, http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		rep, err := BuildStatus(r.Context(), e, state.ConsumerState())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rep)
	})
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		serveSearch(w, r, searcher, e.Cfg.Search.TenantFilter)
	})
	return mux
}
