package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/migrate"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

// GenStatus summarises one generation.
type GenStatus struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Model      string `json:"model"`
	Revision   string `json:"revision"`
	Dimension  int    `json:"dimension"`
	Distance   string `json:"distance"`
	Index      string `json:"index"`
	Documents  int64  `json:"documents"`
	Chunks     int64  `json:"chunks"`
	Unresolved int64  `json:"unresolved"`
	StreamFrom string `json:"stream_from_lsn,omitempty"`
	Serving    bool   `json:"serving"`
}

// PipelineStatus summarises one configured pipeline.
type PipelineStatus struct {
	Name            string   `json:"name"`
	Table           string   `json:"table"`
	Key             []string `json:"key"`
	KeyKinds        string   `json:"key_kinds,omitempty"`
	Signature       string   `json:"relation_signature,omitempty"`
	Registered      bool     `json:"registered"`
	SourceStateRows int64    `json:"source_state_rows"`
	Unresolved      int64    `json:"unresolved_rows"`
}

// StatusReport is printed by `embedsync status` and served at /status.
type StatusReport struct {
	Pipeline struct {
		State     string    `json:"state"`
		Reason    string    `json:"reason,omitempty"`
		Detail    string    `json:"detail,omitempty"`
		UpdatedAt time.Time `json:"updated_at"`
		Consumer  string    `json:"consumer,omitempty"`
	} `json:"pipeline"`
	Checkpoint struct {
		LSN       string    `json:"lsn"`
		UpdatedAt time.Time `json:"updated_at"`
		SourceLSN string    `json:"source_wal_lsn"`
		LagBytes  int64     `json:"lag_bytes"`
		Slot      string    `json:"slot"`
		SystemID  string    `json:"system_identifier"`
		// KeyKind and Signature describe the table when exactly one
		// pipeline is configured (the single-table fields); see Pipelines.
		KeyKind    string  `json:"key_kind"`
		Signature  string  `json:"relation_signature"`
		Exists     bool    `json:"exists"`
		AgeSeconds float64 `json:"age_seconds"`
	} `json:"checkpoint"`
	Slot struct {
		Exists            bool   `json:"exists"`
		Active            bool   `json:"active"`
		WALStatus         string `json:"wal_status"`
		RetainedBytes     int64  `json:"retained_wal_bytes"`
		ConfirmedLagBytes int64  `json:"confirmed_lag_bytes"`
		SafeWALBytes      *int64 `json:"safe_wal_bytes"`
	} `json:"slot"`
	Serving struct {
		Generation    string     `json:"generation"`
		Previous      *string    `json:"previous,omitempty"`
		RollbackUntil *time.Time `json:"rollback_until,omitempty"`
		SwitchedAt    time.Time  `json:"switched_at"`
	} `json:"serving"`
	Pipelines     []PipelineStatus    `json:"pipelines"`
	Generations   []GenStatus         `json:"generations"`
	Backfill      []store.BackfillJob `json:"backfill_jobs"`
	Unresolved    int64               `json:"unresolved_rows"`
	SourceStateN  int64               `json:"source_state_rows"`
	Migrations    []migrate.Migration `json:"migrations"`
	Alerts        []string            `json:"alerts"`
	FreshnessNote string              `json:"freshness_note"`
}

// BuildStatus gathers a status report. consumer is the in-process consumer
// state when called from `run` ("" from the CLI).
func BuildStatus(ctx context.Context, e *Env, consumer string) (*StatusReport, error) {
	r := &StatusReport{}
	var err error
	r.Pipeline.State, r.Pipeline.Reason, r.Pipeline.Detail, r.Pipeline.UpdatedAt, err = e.Store.PipelineStatus(ctx)
	if err != nil {
		return nil, err
	}
	r.Pipeline.Consumer = consumer
	cp, err := e.Store.LoadCheckpoint(ctx)
	if err != nil {
		return nil, err
	}
	if cp != nil {
		r.Checkpoint.Exists = true
		r.Checkpoint.LSN = cp.LSN.String()
		r.Checkpoint.UpdatedAt = cp.UpdatedAt
		r.Checkpoint.AgeSeconds = time.Since(cp.UpdatedAt).Seconds()
		r.Checkpoint.Slot = cp.SlotName
		r.Checkpoint.SystemID = cp.SystemIdentifier
	}
	rels, err := e.Store.Relations(ctx)
	if err != nil {
		return nil, err
	}
	stateRows := map[string]int64{}
	unresolvedBy := map[string]int64{}
	if rows, err := e.Store.Pool.Query(ctx, `SELECT pipeline, count(*) FROM `+e.Store.T("source_state")+` GROUP BY 1`); err == nil {
		for rows.Next() {
			var p string
			var n int64
			if rows.Scan(&p, &n) == nil {
				stateRows[p] = n
			}
		}
		rows.Close()
	}
	if rows, err := e.Store.Pool.Query(ctx, `SELECT d.pipeline, count(*) FROM `+e.Store.T("dlq")+` d JOIN `+e.Store.T("generations")+
		` g ON g.id = d.generation_id WHERE d.state = 'open' AND g.state = ANY($1) GROUP BY 1`, store.LiveStates); err == nil {
		for rows.Next() {
			var p string
			var n int64
			if rows.Scan(&p, &n) == nil {
				unresolvedBy[p] = n
			}
		}
		rows.Close()
	}
	for _, p := range e.Cfg.Pipelines {
		ps := PipelineStatus{Name: p.Name, Table: p.Table, Key: p.Key, SourceStateRows: stateRows[p.Name], Unresolved: unresolvedBy[p.Name]}
		if rel := rels[p.Name]; rel != nil {
			ps.Registered, ps.KeyKinds, ps.Signature = true, rel.KeyKinds, rel.RelationSignature
		}
		r.Pipelines = append(r.Pipelines, ps)
	}
	if len(r.Pipelines) == 1 {
		r.Checkpoint.KeyKind, r.Checkpoint.Signature = r.Pipelines[0].KeyKinds, r.Pipelines[0].Signature
	}
	if e.Source != nil {
		var cur string
		if err := e.Source.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&cur); err == nil {
			r.Checkpoint.SourceLSN = cur
			if cp != nil {
				c, _ := pglogrepl.ParseLSN(cur)
				r.Checkpoint.LagBytes = int64(c) - int64(cp.LSN)
			}
		}
		if s, err := LoadSlot(ctx, e); err == nil {
			r.Slot.Exists, r.Slot.Active, r.Slot.WALStatus = s.Exists, s.Active, s.WALStatus
			r.Slot.RetainedBytes, r.Slot.ConfirmedLagBytes, r.Slot.SafeWALBytes = s.RetainedB, s.ConfirmedLag, s.SafeWALSize
		}
	}
	sv, err := e.Store.Serving(ctx)
	if err != nil {
		return nil, err
	}
	if sv != nil {
		r.Serving.Generation, r.Serving.Previous, r.Serving.RollbackUntil, r.Serving.SwitchedAt = sv.GenerationID, sv.PreviousGenerationID, sv.RollbackUntil, sv.SwitchedAt
	}
	counts, _, err := e.Store.UnresolvedCounts(ctx)
	if err != nil {
		return nil, err
	}
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range gens {
		gs := GenStatus{ID: g.ID, State: g.State, Model: g.ModelName, Revision: g.ModelRevision, Dimension: g.Dimension, Distance: g.Distance,
			Index:      fmt.Sprintf("hnsw(m=%d,ef_construction=%d,ef_search=%d)", g.IndexParams.M, g.IndexParams.EFConstruction, g.IndexParams.EFSearch),
			Unresolved: counts[g.ID], Serving: sv != nil && sv.GenerationID == g.ID}
		if g.StreamFromLSN != nil {
			gs.StreamFrom = g.StreamFromLSN.String()
		}
		_ = e.Store.Pool.QueryRow(ctx, `SELECT count(DISTINCT (pipeline, source_key)), count(*) FROM `+pgx.Identifier{e.Store.Schema, g.ChunkTable}.Sanitize()).Scan(&gs.Documents, &gs.Chunks)
		if store.IsLive(g.State) {
			r.Unresolved += counts[g.ID]
		}
		r.Generations = append(r.Generations, gs)
	}
	_ = e.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM `+e.Store.T("source_state")).Scan(&r.SourceStateN)
	if r.Backfill, err = e.Store.BackfillJobs(ctx); err != nil {
		return nil, err
	}
	mig := &migrate.Migrator{Cfg: e.Cfg, Store: e.Store}
	if r.Migrations, err = mig.List(ctx, 5); err != nil {
		return nil, err
	}
	if r.Pipeline.State == "paused" {
		r.Alerts = append(r.Alerts, "pipeline paused: "+r.Pipeline.Reason)
	}
	if r.Slot.Exists && r.Slot.RetainedBytes >= int64(e.Cfg.Alerts.RetainedWALCriticalBytes) {
		r.Alerts = append(r.Alerts, fmt.Sprintf("retained WAL %d bytes >= critical %d", r.Slot.RetainedBytes, e.Cfg.Alerts.RetainedWALCriticalBytes))
	} else if r.Slot.Exists && r.Slot.RetainedBytes >= int64(e.Cfg.Alerts.RetainedWALWarningBytes) {
		r.Alerts = append(r.Alerts, fmt.Sprintf("retained WAL %d bytes >= warning %d", r.Slot.RetainedBytes, e.Cfg.Alerts.RetainedWALWarningBytes))
	}
	if r.Slot.WALStatus == "lost" || (cp != nil && e.Source != nil && !r.Slot.Exists) {
		r.Alerts = append(r.Alerts, "replication slot lost: run `embedsync rebuild --confirm`")
	}
	if r.Unresolved > 0 {
		r.Alerts = append(r.Alerts, fmt.Sprintf("%d unresolved (quarantined) rows count as stale for freshness and coverage", r.Unresolved))
	}
	r.FreshnessNote = "commit-to-searchable latency is exported as embedsync_commit_to_searchable_seconds; idle vs stalled as embedsync_consumer_state"
	return r, nil
}

// PrintStatus renders a human-readable report.
func PrintStatus(w io.Writer, r *StatusReport) {
	fmt.Fprintf(w, "pipeline:    %s", r.Pipeline.State)
	if r.Pipeline.Reason != "" {
		fmt.Fprintf(w, " (%s)", r.Pipeline.Reason)
	}
	fmt.Fprintln(w)
	if r.Pipeline.Detail != "" {
		fmt.Fprintf(w, "  detail:    %s\n", r.Pipeline.Detail)
	}
	if r.Checkpoint.Exists {
		fmt.Fprintf(w, "checkpoint:  %s (source WAL %s, lag %d bytes, updated %s ago)\n", r.Checkpoint.LSN, r.Checkpoint.SourceLSN, r.Checkpoint.LagBytes,
			time.Duration(r.Checkpoint.AgeSeconds*float64(time.Second)).Round(time.Second))
	} else {
		fmt.Fprintln(w, "checkpoint:  none (run `embedsync run` to create the slot)")
	}
	safe := "unlimited"
	if r.Slot.SafeWALBytes != nil {
		safe = fmt.Sprintf("%d bytes", *r.Slot.SafeWALBytes)
	}
	fmt.Fprintf(w, "slot:        exists=%v active=%v wal_status=%s retained=%d bytes safe_wal=%s\n", r.Slot.Exists, r.Slot.Active, r.Slot.WALStatus, r.Slot.RetainedBytes, safe)
	fmt.Fprintf(w, "serving:     %s", r.Serving.Generation)
	if r.Serving.Previous != nil {
		fmt.Fprintf(w, " (previous %s kept current until %s)", *r.Serving.Previous, r.Serving.RollbackUntil.Format(time.RFC3339))
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "source_state rows: %d   unresolved rows: %d\n", r.SourceStateN, r.Unresolved)
	if len(r.Pipelines) > 1 || (len(r.Pipelines) == 1 && r.Pipelines[0].Name != "default") {
		fmt.Fprintln(w, "pipelines:")
		for _, p := range r.Pipelines {
			fmt.Fprintf(w, "   %-12s %-28s key=%s (%s) state_rows=%d unresolved=%d\n", p.Name, p.Table, strings.Join(p.Key, ","), p.KeyKinds, p.SourceStateRows, p.Unresolved)
		}
	}
	fmt.Fprintln(w, "generations:")
	for _, g := range r.Generations {
		mark := " "
		if g.Serving {
			mark = "*"
		}
		fmt.Fprintf(w, " %s %-10s %-9s docs=%-6d chunks=%-7d unresolved=%-3d %s@%s dim=%d %s %s\n", mark, g.ID, g.State, g.Documents, g.Chunks,
			g.Unresolved, g.Model, shortRev(g.Revision), g.Dimension, g.Distance, g.Index)
	}
	if len(r.Backfill) > 0 {
		fmt.Fprintln(w, "backfill jobs:")
		for _, j := range r.Backfill {
			lk := "-"
			if j.LastKey != nil {
				lk = *j.LastKey
			}
			fmt.Fprintf(w, "   #%d %-10s %-12s %-9s rows=%d windows=%d last_key=%s\n", j.ID, j.GenerationID, j.Pipeline, j.State, j.RowsScanned, j.Windows, lk)
		}
	}
	if len(r.Migrations) > 0 {
		fmt.Fprintln(w, "migrations:")
		for _, m := range r.Migrations {
			from := "-"
			if m.From != nil {
				from = *m.From
			}
			fmt.Fprintf(w, "   #%d %s -> %s step=%s state=%s %s\n", m.ID, from, m.Target, m.Step, m.State, m.Error)
		}
	}
	if len(r.Alerts) > 0 {
		fmt.Fprintln(w, "alerts:")
		for _, a := range r.Alerts {
			fmt.Fprintf(w, "   ! %s\n", a)
		}
	}
}

func shortRev(r string) string {
	if len(r) > 10 {
		return r[:10]
	}
	return r
}

// PrintJSON writes v as indented JSON.
func PrintJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
