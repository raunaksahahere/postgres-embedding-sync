package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/migrate"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/verify"
)

// Backfill queues a watermark backfill of one pipeline (every pipeline when
// pipe is empty) for gen (serving generation when empty) and optionally waits
// for the running pipeline to complete it.
func Backfill(ctx context.Context, e *Env, gen, pipe string, wait bool, out io.Writer) error {
	if gen == "" {
		sv, err := e.Store.Serving(ctx)
		if err != nil {
			return err
		}
		if sv == nil {
			return errors.New("no serving generation; start `embedsync run` first")
		}
		gen = sv.GenerationID
	}
	g, err := e.Store.Generation(ctx, gen)
	if err != nil {
		return err
	}
	if g == nil || !(store.IsLive(g.State) || g.State == store.StatePending) {
		return fmt.Errorf("generation %q is not live", gen)
	}
	pipes := e.Cfg.PipelineNames()
	if pipe != "" {
		if e.Cfg.Pipe(pipe) == nil {
			return fmt.Errorf("unknown pipeline %q (configured: %s)", pipe, strings.Join(pipes, ", "))
		}
		pipes = []string{pipe}
	}
	var ids []int64
	for _, p := range pipes {
		if _, ok := g.Pipelines[p]; !ok {
			return fmt.Errorf("generation %s does not cover pipeline %q yet; `embedsync run` extends it at startup", gen, p)
		}
		id, err := e.Store.RequestBackfill(ctx, gen, p)
		if err != nil {
			return err
		}
		ids = append(ids, id)
		fmt.Fprintf(out, "backfill job %d queued for %s (pipeline %s); the running `embedsync run` process executes it\n", id, gen, p)
	}
	if !wait {
		return nil
	}
	last := map[int64]string{}
	for {
		done := 0
		for _, id := range ids {
			j, err := e.Store.BackfillJobByID(ctx, id)
			if err != nil {
				return err
			}
			if j == nil {
				return fmt.Errorf("backfill job %d disappeared", id)
			}
			msg := fmt.Sprintf("job %d (%s) %s: %d rows, %d windows", j.ID, j.Pipeline, j.State, j.RowsScanned, j.Windows)
			if msg != last[id] {
				fmt.Fprintln(out, msg)
				last[id] = msg
			}
			if j.State == "done" || j.State == "cancelled" {
				done++
			}
		}
		if done == len(ids) {
			return nil
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// VerifyReport is the output of `embedsync verify`. Totals cover every
// pipeline; Pipelines and each generation's pipelines break them down.
type VerifyReport struct {
	BoundaryLSN   string                         `json:"boundary_lsn"`
	CheckpointLSN string                         `json:"checkpoint_lsn"`
	SourceRows    int                            `json:"source_rows"`
	SourceState   verify.StateReport             `json:"source_state"`
	Pipelines     map[string]*PipelineVerifyRows `json:"pipelines"`
	Generations   []*verify.GenReport            `json:"generations"`
	OK            bool                           `json:"ok"`
}

// PipelineVerifyRows is one pipeline's source and source_state comparison.
type PipelineVerifyRows struct {
	SourceRows  int                `json:"source_rows"`
	SourceState verify.StateReport `json:"source_state"`
}

// Verify compares the entire sink with the source at a common boundary. It
// first waits for queued or running backfills (a generation is not expected
// to be complete before its backfill finishes, and source_state is only
// complete after the first one), then writes a barrier, waits until the
// checkpoint passes it, and reads the source and sink. Writers must be
// stopped for an exact comparison.
func Verify(ctx context.Context, e *Env, timeout time.Duration) (*VerifyReport, error) {
	deadline := time.Now().Add(timeout)
	for {
		j, err := e.Store.NextBackfillJob(ctx)
		if err != nil {
			return nil, err
		}
		if j == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("backfill job %d for generation %s is still %s (%d rows scanned); the sink is incomplete until it finishes, so verify later or raise --timeout", j.ID, j.GenerationID, j.State, j.RowsScanned)
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m := &migrate.Migrator{Cfg: e.Cfg, Store: e.Store, Source: e.Source}
	b, err := m.Barrier(ctx, "verify")
	if err != nil {
		return nil, err
	}
	cp, err := m.WaitCheckpoint(ctx, b, max(time.Until(deadline), 5*time.Second))
	if err != nil {
		return nil, err
	}
	rep := &VerifyReport{BoundaryLSN: b.String(), CheckpointLSN: cp.String(), OK: true, Pipelines: map[string]*PipelineVerifyRows{}}
	var rows verify.Rows
	// Every pipeline's table is read in one source snapshot.
	err = pgx.BeginTxFunc(ctx, e.Source, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		rows, err = verify.AllSourceRows(ctx, tx, e.Cfg)
		return err
	})
	if err != nil {
		return nil, err
	}
	rep.SourceRows = rows.Count()
	err = pgx.BeginTxFunc(ctx, e.Store.Pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, state, err := verify.StateRows(ctx, tx, e.Store.Schema)
		if err != nil {
			return err
		}
		rep.SourceState = verify.State(rows, state)
		rep.OK = rep.SourceState.OK()
		for _, p := range e.Cfg.PipelineNames() {
			pr := &PipelineVerifyRows{SourceRows: len(rows[p]), SourceState: verify.State(verify.Rows{p: rows[p]}, map[string]map[string]map[string]*string{p: state[p]})}
			rep.Pipelines[p] = pr
		}
		for p := range state {
			if e.Cfg.Pipe(p) == nil {
				// State of a pipeline that is no longer configured.
				rep.SourceState.Extra += len(state[p])
				rep.OK = false
			}
		}
		gens, err := e.Store.Generations(ctx)
		if err != nil {
			return err
		}
		for _, g := range gens {
			if !store.IsLive(g.State) {
				continue
			}
			gr, err := verify.Generation(ctx, tx, e.Store.Schema, g, e.Trs(), rows, e.Cfg.Pipeline.MaxDocumentBytes)
			if err != nil {
				return err
			}
			rep.Generations = append(rep.Generations, gr)
			rep.OK = rep.OK && gr.OK()
		}
		return nil
	})
	return rep, err
}

// ReplayResult summarises a DLQ replay.
type ReplayResult struct {
	Resolved   int `json:"resolved"`
	Superseded int `json:"skipped_superseded"`
	StillBad   int `json:"still_failing"`
}

// ReplayDLQ retries open quarantine records. A record is applied only if it is
// still open and source_state still holds the version that was rendered; a
// newer edit or delete (which supersedes the record in the same transaction
// that applies it) always wins. Locks are taken in the committer's order
// (source_state, then dlq, then chunks).
func ReplayDLQ(ctx context.Context, e *Env, id int64, out io.Writer) (*ReplayResult, error) {
	entries, err := e.Store.DLQ(ctx, "open", 100000)
	if err != nil {
		return nil, err
	}
	res := &ReplayResult{}
	for _, d := range entries {
		if id != 0 && d.ID != id {
			continue
		}
		g, err := e.Store.Generation(ctx, d.GenerationID)
		if err != nil {
			return nil, err
		}
		if g == nil || !store.IsLive(g.State) {
			continue
		}
		spec := e.Pipes[d.Pipeline]
		if spec == nil {
			fmt.Fprintf(out, "skipped dlq #%d: pipeline %q is not configured\n", d.ID, d.Pipeline)
			res.StillBad++
			continue
		}
		st, err := e.Store.LoadSourceState(ctx, e.Store.Pool, d.Pipeline, []string{d.SourceKey})
		if err != nil {
			return nil, err
		}
		s, exists := st[d.SourceKey]
		var vecs map[string]string
		var chunks []struct {
			no           int
			id, fp, text string
		}
		if exists {
			vals := map[string]string{}
			for k, v := range s.Fields {
				if v != nil {
					vals[k] = *v
				}
			}
			text, err := spec.Tr.Render(vals)
			if err != nil || len(text) > e.Cfg.Pipeline.MaxDocumentBytes {
				res.StillBad++
				_, _ = e.Store.Pool.Exec(ctx, `UPDATE `+e.Store.T("dlq")+` SET attempts = attempts + 1, updated_at = now() WHERE id = $1`, d.ID)
				continue
			}
			cs := spec.Tr.Chunks(d.SourceKey, text, ModelIdentity(g))
			inputs := make([]string, len(cs))
			for i, c := range cs {
				inputs[i] = g.DocumentPrefix + c.Text
				chunks = append(chunks, struct {
					no           int
					id, fp, text string
				}{c.No, c.ID, c.Fingerprint, c.Text})
			}
			vecs = map[string]string{}
			if len(inputs) > 0 {
				v, rej, err := e.Embed.Embed(ctx, embedclient.Request{Model: g.ModelName, Revision: g.ModelRevision, Kind: embedclient.Document, Inputs: inputs})
				if err != nil {
					return res, err
				}
				if len(rej) > 0 {
					res.StillBad++
					_, _ = e.Store.Pool.Exec(ctx, `UPDATE `+e.Store.T("dlq")+` SET attempts = attempts + 1, updated_at = now() WHERE id = $1`, d.ID)
					continue
				}
				for i, c := range cs {
					vecs[c.Fingerprint] = embedclient.FormatVector(v[i])
				}
			}
		}
		applied := false
		err = pgx.BeginFunc(ctx, e.Store.Pool, func(tx pgx.Tx) error {
			var lsn *string
			err := tx.QueryRow(ctx, `SELECT source_lsn::text FROM `+e.Store.T("source_state")+` WHERE pipeline = $1 AND source_key = $2 FOR UPDATE`, d.Pipeline, d.SourceKey).Scan(&lsn)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			nowExists := err == nil
			if nowExists != exists || (exists && (lsn == nil || *lsn != s.LSN.String())) {
				return nil // a newer change arrived; it owns the outcome
			}
			var state string
			if err := tx.QueryRow(ctx, `SELECT state FROM `+e.Store.T("dlq")+` WHERE id = $1 FOR UPDATE`, d.ID).Scan(&state); err != nil {
				return err
			}
			if state != "open" {
				return nil
			}
			tbl := pgx.Identifier{e.Store.Schema, g.ChunkTable}.Sanitize()
			if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` WHERE pipeline = $1 AND source_key = $2`, d.Pipeline, d.SourceKey); err != nil {
				return err
			}
			for _, c := range chunks {
				if _, err := tx.Exec(ctx, `INSERT INTO `+tbl+` (pipeline, source_key, chunk_no, chunk_id, fingerprint, content, embedding, source_lsn)
					VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::pg_lsn)`, d.Pipeline, d.SourceKey, c.no, c.id, c.fp, c.text, vecs[c.fp], s.LSN.String()); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE `+e.Store.T("dlq")+` SET state = 'resolved', resolved_at = now(), updated_at = now(), attempts = attempts + 1 WHERE id = $1`, d.ID); err != nil {
				return err
			}
			applied = true
			return nil
		})
		if err != nil {
			return res, err
		}
		if applied {
			res.Resolved++
			fmt.Fprintf(out, "resolved dlq #%d (generation %s, pipeline %s)\n", d.ID, d.GenerationID, d.Pipeline)
		} else {
			res.Superseded++
			fmt.Fprintf(out, "skipped dlq #%d: superseded by a newer change\n", d.ID)
		}
	}
	return res, nil
}

// Rebuild recovers from a lost slot: it drops the old slot and checkpoint,
// marks existing generations stale (they keep serving, unchanged), clears
// source_state and registers a fresh generation that the next `run` backfills.
// Promote it with `embedsync migrate --to <id>` once it is ready.
func Rebuild(ctx context.Context, e *Env, out io.Writer) (string, error) {
	sv, err := e.Store.Serving(ctx)
	if err != nil {
		return "", err
	}
	if sv == nil {
		return "", errors.New("nothing to rebuild: no serving generation")
	}
	serving, err := e.Store.Generation(ctx, sv.GenerationID)
	if err != nil {
		return "", err
	}
	replRaw, err := config.Env(e.Cfg.Source.ReplDSNEnv)
	if err != nil {
		return "", err
	}
	replDSN, err := replicationDSN(replRaw)
	if err != nil {
		return "", err
	}
	conn, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	slot, err := LoadSlot(ctx, e)
	if err != nil {
		return "", err
	}
	if slot.Exists {
		if slot.Active {
			return "", errors.New("slot is active: stop `embedsync run` first")
		}
		if err := pglogrepl.DropReplicationSlot(ctx, conn, e.Cfg.Source.Slot, pglogrepl.DropReplicationSlotOptions{}); err != nil {
			return "", fmt.Errorf("drop slot: %w", err)
		}
		fmt.Fprintf(out, "dropped slot %s\n", e.Cfg.Source.Slot)
	}
	newID := ""
	for i := 2; ; i++ {
		id := fmt.Sprintf("%s_r%d", serving.ModelKey, i)
		g, err := e.Store.Generation(ctx, id)
		if err != nil {
			return "", err
		}
		if g == nil {
			newID = id
			break
		}
	}
	err = pgx.BeginFunc(ctx, e.Store.Pool, func(tx pgx.Tx) error {
		stmts := []string{
			`DELETE FROM ` + e.Store.T("checkpoints"),
			`DELETE FROM ` + e.Store.T("pipeline_relations"),
			`DELETE FROM ` + e.Store.T("source_state"),
			`UPDATE ` + e.Store.T("backfill_jobs") + ` SET state = 'cancelled', updated_at = now() WHERE state IN ('pending','running')`,
			`UPDATE ` + e.Store.T("generations") + ` SET state = 'stale' WHERE state IN ('pending','building','ready','active','previous')`,
			`UPDATE ` + e.Store.T("serving") + ` SET previous_generation_id = NULL, rollback_until = NULL`,
			`UPDATE ` + e.Store.T("migrations") + ` SET state = 'failed', error = 'slot rebuild', updated_at = now() WHERE state = 'running'`,
		}
		for _, q := range stmts {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	m := e.Cfg.Models[serving.ModelKey]
	g, err := store.NewGeneration(newID, serving.ModelKey, m, e.Cfg)
	if err != nil {
		return "", err
	}
	if err := e.Store.CreateGeneration(ctx, g, e.Cfg.Sink.ReaderRole); err != nil {
		return "", err
	}
	fmt.Fprintf(out, "generation %s registered; %s keeps serving (stale) until you run `embedsync migrate --to %s` after the next `embedsync run` backfills it\n", newID, sv.GenerationID, newID)
	return newID, nil
}
