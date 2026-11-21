// Package migrate switches the serving generation between embedding models.
//
// A migration is a resumable state machine persisted in the sink:
//
//	backfill -> catchup -> gate -> promote      (gate failure: gate_failed)
//
// The candidate generation consumes the same change stream as the serving
// generation, in the same sink transactions, so "catch-up to a common durable
// boundary" means: the shared checkpoint has passed a barrier written after the
// candidate's backfill completed. Promotion is one UPDATE of the serving alias.
package migrate

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/verify"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// ErrGateFailed is returned when a candidate fails a promotion gate.
var ErrGateFailed = errors.New("promotion gate failed")

// Migrator runs model migrations.
type Migrator struct {
	Cfg      *config.Config
	Store    *store.Store
	Source   *pgxpool.Pool
	Trs      map[string]*transform.Transformer // per pipeline
	Log      *slog.Logger
	Searcher *search.Searcher
	Embed    *embedclient.Set
	Out      io.Writer
}

// Migration is a row of the migrations table.
type Migration struct {
	ID          int64           `json:"id"`
	Target      string          `json:"target_generation"`
	From        *string         `json:"from_generation"`
	Step        string          `json:"step"`
	State       string          `json:"state"`
	BoundaryLSN *string         `json:"boundary_lsn"`
	Report      json.RawMessage `json:"gate_report,omitempty"`
	Error       string          `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// GateResult is one declared gate and its observed value.
type GateResult struct {
	Name      string  `json:"name"`
	Observed  float64 `json:"observed"`
	Threshold float64 `json:"threshold"`
	Op        string  `json:"op"`
	Pass      bool    `json:"pass"`
	Note      string  `json:"note,omitempty"`
}

// ModelMetrics summarises one generation on the evaluation set.
type ModelMetrics struct {
	Generation    string  `json:"generation"`
	Provider      string  `json:"provider"`
	Model         string  `json:"model"`
	Revision      string  `json:"revision"`
	RecallAt10    float64 `json:"recall_at_10"`
	NDCGAt10      float64 `json:"ndcg_at_10"`
	ANNRecallAt10 float64 `json:"ann_vs_exact_recall_at_10"`
	// Exact (sequential scan) quality, to show what the ANN index costs.
	ExactRecallAt10 float64 `json:"exact_recall_at_10"`
	ExactNDCGAt10   float64 `json:"exact_ndcg_at_10"`
}

// GateReport is persisted with the migration and printed by `migrate status`.
type GateReport struct {
	From            string            `json:"from"`
	To              string            `json:"to"`
	BoundaryLSN     string            `json:"boundary_lsn"`
	CheckpointLSN   string            `json:"checkpoint_lsn"`
	Declared        config.Gates      `json:"declared_gates"`
	Coverage        *verify.GenReport `json:"coverage"`
	StateRows       int               `json:"source_state_rows"`
	Unresolved      int64             `json:"unresolved_rows"`
	EvalQueries     int               `json:"eval_queries"`
	Active          *ModelMetrics     `json:"active,omitempty"`
	Candidate       *ModelMetrics     `json:"candidate,omitempty"`
	DeltaNDCG       *CI               `json:"delta_ndcg_at_10,omitempty"`
	DeltaRecall     *CI               `json:"delta_recall_at_10,omitempty"`
	Gates           []GateResult      `json:"gates"`
	Pass            bool              `json:"pass"`
	EvaluatedAt     time.Time         `json:"evaluated_at"`
	DurationSeconds float64           `json:"duration_seconds"`
}

// Options for Migrate.
type Options struct {
	Restart     bool // start over after a rejected candidate
	GateOnly    bool // evaluate but do not promote
	PollEvery   time.Duration
	WaitTimeout time.Duration
}

func (m *Migrator) printf(format string, a ...any) {
	if m.Out != nil {
		fmt.Fprintf(m.Out, format+"\n", a...)
	}
}

func (m *Migrator) marker(ctx context.Context, kind, gen string) error {
	_, err := m.Source.Exec(ctx, `INSERT INTO `+pgx.Identifier{m.Cfg.Source.ControlSchema, "markers"}.Sanitize()+` (kind, generation_id) VALUES ($1, $2)`, kind, gen)
	return err
}

// Barrier writes a barrier marker and returns a source LSN after its commit.
func (m *Migrator) Barrier(ctx context.Context, gen string) (pglogrepl.LSN, error) {
	if err := m.marker(ctx, "barrier", gen); err != nil {
		return 0, fmt.Errorf("barrier marker: %w", err)
	}
	var s string
	if err := m.Source.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&s); err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(s)
}

// WaitCheckpoint blocks until the durable checkpoint reaches lsn.
func (m *Migrator) WaitCheckpoint(ctx context.Context, lsn pglogrepl.LSN, timeout time.Duration) (pglogrepl.LSN, error) {
	deadline := time.Now().Add(timeout)
	for {
		cp, err := m.Store.LoadCheckpoint(ctx)
		if err != nil {
			return 0, err
		}
		if cp != nil && cp.LSN >= lsn {
			return cp.LSN, nil
		}
		if time.Now().After(deadline) {
			cur := "none"
			if cp != nil {
				cur = cp.LSN.String()
			}
			return 0, fmt.Errorf("checkpoint %s did not reach %s within %s (is `embedsync run` running?)", cur, lsn, timeout)
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func scanMigration(row pgx.Row) (*Migration, error) {
	var mg Migration
	err := row.Scan(&mg.ID, &mg.Target, &mg.From, &mg.Step, &mg.State, &mg.BoundaryLSN, &mg.Report, &mg.Error, &mg.CreatedAt, &mg.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &mg, err
}

const migCols = `id, target_generation, from_generation, step, state, boundary_lsn::text, gate_report, error, created_at, updated_at`

// List returns recent migrations, newest first.
func (m *Migrator) List(ctx context.Context, limit int) ([]Migration, error) {
	rows, err := m.Store.Pool.Query(ctx, `SELECT `+migCols+` FROM `+m.Store.T("migrations")+` ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Migration
	for rows.Next() {
		mg, err := scanMigration(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *mg)
	}
	return out, rows.Err()
}

func (m *Migrator) setStep(ctx context.Context, id int64, step, state string, report any, errMsg string) error {
	var rb []byte
	if report != nil {
		rb, _ = json.Marshal(report)
	}
	_, err := m.Store.Pool.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET step = $2, state = $3, gate_report = coalesce($4, gate_report),
		error = $5, updated_at = now() WHERE id = $1`, id, step, state, rb, errMsg)
	return err
}

// resolveTarget maps a model key or generation id to a generation, creating it if needed.
func (m *Migrator) resolveTarget(ctx context.Context, target string, restart bool) (*store.Generation, string, error) {
	g, err := m.Store.Generation(ctx, target)
	if err != nil {
		return nil, "", err
	}
	modelKey := target
	if g != nil {
		modelKey = g.ModelKey
	}
	model, ok := m.Cfg.Models[modelKey]
	if !ok {
		return nil, "", fmt.Errorf("unknown model or generation %q", target)
	}
	if g != nil && (g.State == store.StateRejected || g.State == store.StateRetired || g.State == store.StateStale) {
		if !restart {
			return g, modelKey, nil
		}
		for i := 2; ; i++ {
			id := fmt.Sprintf("%s_%d", modelKey, i)
			other, err := m.Store.Generation(ctx, id)
			if err != nil {
				return nil, "", err
			}
			if other == nil {
				target = id
				g = nil
				break
			}
			if store.IsLive(other.State) || other.State == store.StatePending {
				return other, modelKey, nil
			}
		}
	}
	if g == nil {
		g, err = store.NewGeneration(target, modelKey, model, m.Cfg)
		if err != nil {
			return nil, "", err
		}
		if err := m.Store.CreateGeneration(ctx, g, m.Cfg.Sink.ReaderRole); err != nil {
			return nil, "", err
		}
		m.printf("registered generation %s (%s@%s, dim %d); backfill queued", g.ID, model.Name, model.Revision, model.Dimension)
	} else if err := g.MatchesModel(model, m.Cfg.PipelineFingerprints()); err != nil {
		return nil, "", err
	} else if miss := g.MissingPipelines(m.Cfg.PipelineFingerprints()); len(miss) > 0 {
		// `embedsync run` extends live generations to new pipelines at
		// startup; a candidate that does not cover them cannot pass coverage.
		return nil, "", fmt.Errorf("generation %s does not cover pipeline(s) %s yet; restart `embedsync run` so it extends the generation and backfills them", g.ID, strings.Join(miss, ", "))
	}
	return g, modelKey, nil
}

// Migrate runs (or resumes) a migration to target.
func (m *Migrator) Migrate(ctx context.Context, target string, o Options) (*GateReport, error) {
	if o.PollEvery == 0 {
		o.PollEvery = 2 * time.Second
	}
	sv, err := m.Store.Serving(ctx)
	if err != nil {
		return nil, err
	}
	if sv == nil {
		return nil, errors.New("no serving generation yet; start `embedsync run` first")
	}
	g, _, err := m.resolveTarget(ctx, target, o.Restart)
	if err != nil {
		return nil, err
	}
	if g.ID == sv.GenerationID {
		return nil, fmt.Errorf("generation %s is already serving", g.ID)
	}
	if g.State == store.StateRejected {
		mg, _ := scanMigration(m.Store.Pool.QueryRow(ctx, `SELECT `+migCols+` FROM `+m.Store.T("migrations")+` WHERE target_generation = $1 ORDER BY id DESC LIMIT 1`, g.ID))
		if mg != nil && len(mg.Report) > 0 {
			var rep GateReport
			_ = json.Unmarshal(mg.Report, &rep)
			return &rep, fmt.Errorf("%w: generation %s was rejected; inspect with `embedsync migrate status`, or pass --restart to build a fresh candidate", ErrGateFailed, g.ID)
		}
		return nil, fmt.Errorf("%w: generation %s was rejected; pass --restart", ErrGateFailed, g.ID)
	}
	mg, err := scanMigration(m.Store.Pool.QueryRow(ctx, `SELECT `+migCols+` FROM `+m.Store.T("migrations")+
		` WHERE target_generation = $1 AND state = 'running' ORDER BY id DESC LIMIT 1`, g.ID))
	if err != nil {
		return nil, err
	}
	if mg == nil {
		mg = &Migration{Target: g.ID, From: &sv.GenerationID, Step: "backfill", State: "running"}
		if err := m.Store.Pool.QueryRow(ctx, `INSERT INTO `+m.Store.T("migrations")+` (target_generation, from_generation, step, state)
			VALUES ($1, $2, 'backfill', 'running') RETURNING id`, g.ID, sv.GenerationID).Scan(&mg.ID); err != nil {
			return nil, err
		}
	} else {
		m.printf("resuming migration %d to %s at step %s", mg.ID, g.ID, mg.Step)
	}
	from := sv.GenerationID
	if mg.From != nil {
		from = *mg.From
	}
	if from != sv.GenerationID {
		return nil, fmt.Errorf("serving generation changed from %s to %s since the migration started; finalize or roll back first", from, sv.GenerationID)
	}

	var boundary pglogrepl.LSN
	for {
		// A candidate can go from ready back to building after the backfill
		// step: `embedsync run` extends live generations to newly configured
		// pipelines (store.AddPipelines) and backfills them. Gating it then
		// finds the new pipeline's chunks missing and rejects a candidate that
		// is merely incomplete, so before catch-up and before the gate (also on
		// resume) re-check readiness and go back to the backfill step. Catch-up
		// then takes a fresh barrier, so an older boundary never reaches the gate.
		if err := m.recheckReady(ctx, mg, g.ID); err != nil {
			return nil, err
		}
		if mg.Step == "backfill" {
			if err := m.waitReady(ctx, g.ID, o.PollEvery); err != nil {
				return nil, err
			}
			mg.Step = "catchup"
			if err := m.setStep(ctx, mg.ID, mg.Step, "running", nil, ""); err != nil {
				return nil, err
			}
		}
		if mg.Step != "catchup" {
			break
		}
		boundary, err = m.Barrier(ctx, g.ID)
		if err != nil {
			return nil, err
		}
		if _, err := m.Store.Pool.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET boundary_lsn = $2::pg_lsn WHERE id = $1`, mg.ID, boundary.String()); err != nil {
			return nil, err
		}
		m.printf("catch-up: waiting for the shared checkpoint to pass %s", boundary)
		timeout := m.Cfg.Migration.CatchupTimeout.Duration
		if o.WaitTimeout > 0 {
			timeout = o.WaitTimeout
		}
		if _, err := m.WaitCheckpoint(ctx, boundary, timeout); err != nil {
			return nil, err
		}
		mg.Step = "gate"
		if err := m.setStep(ctx, mg.ID, mg.Step, "running", nil, ""); err != nil {
			return nil, err
		}
	}
	if boundary == 0 && mg.BoundaryLSN != nil {
		// Resumed at gate/promote with a ready candidate: use the persisted boundary.
		boundary, _ = pglogrepl.ParseLSN(*mg.BoundaryLSN)
	}
	var rep *GateReport
	if mg.Step == "gate" {
		rep, err = m.Evaluate(ctx, from, g.ID, boundary)
		if err != nil {
			_ = m.setStep(ctx, mg.ID, "gate", "running", nil, err.Error())
			return nil, err
		}
		m.printReport(rep)
		if !rep.Pass && o.GateOnly {
			// A dry run must not destroy a built candidate: keep the report,
			// leave the generation as it is.
			_ = m.setStep(ctx, mg.ID, "gate", "running", rep, "gate-only evaluation failed")
			return rep, fmt.Errorf("%w for %s (gate-only: candidate kept)", ErrGateFailed, g.ID)
		}
		if !rep.Pass {
			if err := m.reject(ctx, mg.ID, g.ID, rep); err != nil {
				return rep, err
			}
			return rep, fmt.Errorf("%w for %s; previous generation %s keeps serving", ErrGateFailed, g.ID, from)
		}
		if o.GateOnly {
			_ = m.setStep(ctx, mg.ID, "gate", "running", rep, "")
			return rep, nil
		}
		mg.Step = "promote"
		if err := m.setStep(ctx, mg.ID, mg.Step, "running", rep, ""); err != nil {
			return nil, err
		}
	}
	if mg.Step == "promote" {
		if err := m.promote(ctx, mg.ID, from, g.ID); err != nil {
			return rep, err
		}
		m.printf("promoted %s; %s stays current until the rollback window ends (%s)", g.ID, from, m.Cfg.Migration.RollbackWindow.Duration)
	}
	return rep, nil
}

// resumeStep returns the step to continue a migration at, given its candidate
// generation's state: catch-up and the gate need a ready candidate, so they go
// back to backfill otherwise. Other steps are unchanged.
func resumeStep(step, genState string) string {
	if (step == "catchup" || step == "gate") && genState != store.StateReady {
		return "backfill"
	}
	return step
}

// recheckReady reloads the candidate and, if a migration at catchup or gate
// no longer has a ready candidate, persists the step back to backfill.
func (m *Migrator) recheckReady(ctx context.Context, mg *Migration, gen string) error {
	if mg.Step != "catchup" && mg.Step != "gate" {
		return nil
	}
	g, err := m.Store.Generation(ctx, gen)
	if err != nil {
		return err
	}
	if g == nil {
		return fmt.Errorf("generation %s not found", gen)
	}
	next := resumeStep(mg.Step, g.State)
	if next == mg.Step {
		return nil
	}
	m.printf("candidate %s is %s, not ready (e.g. a pipeline was added); back to the backfill step before catch-up", gen, g.State)
	mg.Step, mg.BoundaryLSN = next, nil
	return m.setStep(ctx, mg.ID, mg.Step, "running", nil, "")
}

func (m *Migrator) waitReady(ctx context.Context, gen string, poll time.Duration) error {
	last := ""
	for {
		g, err := m.Store.Generation(ctx, gen)
		if err != nil {
			return err
		}
		if g.State == store.StateReady {
			return nil
		}
		if !store.IsLive(g.State) && g.State != store.StatePending {
			return fmt.Errorf("generation %s is %s", gen, g.State)
		}
		jobs, _ := m.Store.BackfillJobs(ctx)
		msg := fmt.Sprintf("backfill %s: state=%s", gen, g.State)
		for _, j := range jobs {
			if j.GenerationID == gen {
				msg = fmt.Sprintf("backfill %s: job %d %s, %d rows in %d windows", gen, j.ID, j.State, j.RowsScanned, j.Windows)
				break
			}
		}
		if msg != last {
			m.printf("%s", msg)
			last = msg
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Migrator) reject(ctx context.Context, migID int64, gen string, rep *GateReport) error {
	err := pgx.BeginFunc(ctx, m.Store.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("generations")+` SET state = 'rejected', retired_at = now() WHERE id = $1`, gen); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("backfill_jobs")+` SET state = 'cancelled', updated_at = now() WHERE generation_id = $1 AND state IN ('pending','running')`, gen); err != nil {
			return err
		}
		rb, _ := json.Marshal(rep)
		_, err := tx.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET step = 'gate', state = 'gate_failed', gate_report = $2, updated_at = now() WHERE id = $1`, migID, rb)
		return err
	})
	if err != nil {
		return err
	}
	// Stop maintaining the rejected generation at a well-defined stream position.
	return m.marker(ctx, "retire", gen)
}

func (m *Migrator) promote(ctx context.Context, migID int64, from, to string) error {
	var orphan string
	err := pgx.BeginFunc(ctx, m.Store.Pool, func(tx pgx.Tx) error {
		var cur string
		var oldPrev *string
		if err := tx.QueryRow(ctx, `SELECT generation_id, previous_generation_id FROM `+m.Store.T("serving")+` WHERE name = $1 FOR UPDATE`,
			store.ServingName).Scan(&cur, &oldPrev); err != nil {
			return err
		}
		// Promoting again inside a rollback window: the rollback target becomes
		// the generation serving right now, so the older one is retired (and
		// stops being maintained) instead of staying live forever.
		if oldPrev != nil && *oldPrev != to && *oldPrev != from {
			if err := retireTx(ctx, tx, m.Store, *oldPrev); err != nil {
				return err
			}
			orphan = *oldPrev
		}
		if cur != from {
			return fmt.Errorf("serving generation is %s, expected %s", cur, from)
		}
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM `+m.Store.T("generations")+` WHERE id = $1 FOR UPDATE`, to).Scan(&state); err != nil {
			return err
		}
		if state != store.StateReady {
			return fmt.Errorf("candidate %s is %s, not ready", to, state)
		}
		var fromState string
		if err := tx.QueryRow(ctx, `SELECT state FROM `+m.Store.T("generations")+` WHERE id = $1 FOR UPDATE`, from).Scan(&fromState); err != nil {
			return err
		}
		// A stale generation (after a slot rebuild) cannot be a rollback target.
		var prev any = from
		var window any = m.Cfg.Migration.RollbackWindow.Seconds()
		nextState := store.StatePrevious
		if fromState == store.StateStale {
			prev, window, nextState = nil, nil, store.StateRetired
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("serving")+` SET generation_id = $2, previous_generation_id = $3,
			rollback_until = now() + make_interval(secs => $4::float8), switched_at = now() WHERE name = $1`,
			store.ServingName, to, prev, window); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("generations")+` SET state = 'active', promoted_at = now() WHERE id = $1`, to); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("generations")+` SET state = $2 WHERE id = $1`, from, nextState); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET step = 'done', state = 'promoted', updated_at = now() WHERE id = $1`, migID)
		return err
	})
	if err != nil || orphan == "" {
		return err
	}
	return m.marker(ctx, "retire", orphan)
}

// retireTx marks a generation retired and cancels its backfill jobs, so the
// backfill executor never keeps windowing a generation nobody maintains.
func retireTx(ctx context.Context, tx pgx.Tx, s *store.Store, gen string) error {
	if _, err := tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET state = 'retired', retired_at = now() WHERE id = $1`, gen); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE `+s.T("backfill_jobs")+` SET state = 'cancelled', updated_at = now() WHERE generation_id = $1 AND state IN ('pending','running')`, gen)
	return err
}

// Rollback swaps the serving alias back to the previous generation while the
// rollback window is open. The previous generation has stayed current because
// it kept consuming the change stream.
func (m *Migrator) Rollback(ctx context.Context) (string, error) {
	var restored string
	err := pgx.BeginFunc(ctx, m.Store.Pool, func(tx pgx.Tx) error {
		var cur string
		var prev *string
		var until *time.Time
		if err := tx.QueryRow(ctx, `SELECT generation_id, previous_generation_id, rollback_until FROM `+m.Store.T("serving")+` WHERE name = $1 FOR UPDATE`,
			store.ServingName).Scan(&cur, &prev, &until); err != nil {
			return err
		}
		if prev == nil {
			return errors.New("no previous generation to roll back to")
		}
		if until != nil && time.Now().After(*until) {
			return errors.New("rollback window has closed")
		}
		var st string
		if err := tx.QueryRow(ctx, `SELECT state FROM `+m.Store.T("generations")+` WHERE id = $1`, *prev).Scan(&st); err != nil {
			return err
		}
		if st != store.StatePrevious {
			return fmt.Errorf("previous generation %s is %s and may be stale", *prev, st)
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("serving")+` SET generation_id = $2, previous_generation_id = $3, switched_at = now() WHERE name = $1`,
			store.ServingName, *prev, cur); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("generations")+` SET state = 'active' WHERE id = $1`, *prev); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("generations")+` SET state = 'previous' WHERE id = $1`, cur); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET state = 'rolled_back', updated_at = now()
			WHERE id = (SELECT max(id) FROM `+m.Store.T("migrations")+` WHERE target_generation = $1 AND state = 'promoted')`, cur); err != nil {
			return err
		}
		restored = *prev
		return nil
	})
	return restored, err
}

// Finalize retires the previous generation (after the window unless force).
func (m *Migrator) Finalize(ctx context.Context, force bool) (string, error) {
	var retired string
	err := pgx.BeginFunc(ctx, m.Store.Pool, func(tx pgx.Tx) error {
		var prev *string
		var until *time.Time
		if err := tx.QueryRow(ctx, `SELECT previous_generation_id, rollback_until FROM `+m.Store.T("serving")+` WHERE name = $1 FOR UPDATE`,
			store.ServingName).Scan(&prev, &until); err != nil {
			return err
		}
		if prev == nil {
			return nil
		}
		if !force && until != nil && time.Now().Before(*until) {
			return fmt.Errorf("rollback window open until %s; pass --force to retire now", until.Format(time.RFC3339))
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("serving")+` SET previous_generation_id = NULL, rollback_until = NULL WHERE name = $1`, store.ServingName); err != nil {
			return err
		}
		if err := retireTx(ctx, tx, m.Store, *prev); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+m.Store.T("migrations")+` SET state = 'finalized', updated_at = now() WHERE state = 'promoted'`); err != nil {
			return err
		}
		retired = *prev
		return nil
	})
	if err != nil || retired == "" {
		return retired, err
	}
	return retired, m.marker(ctx, "retire", retired)
}

// FinalizeExpired retires the previous generation once its window has passed.
func (m *Migrator) FinalizeExpired(ctx context.Context) error {
	sv, err := m.Store.Serving(ctx)
	if err != nil || sv == nil || sv.PreviousGenerationID == nil || sv.RollbackUntil == nil || time.Now().Before(*sv.RollbackUntil) {
		return err
	}
	g, err := m.Finalize(ctx, true)
	if err == nil && g != "" && m.Log != nil {
		m.Log.Info("rollback window ended; retired previous generation", "generation", g)
	}
	return err
}

// Evaluate checks coverage, unresolved rows and declared quality gates.
func (m *Migrator) Evaluate(ctx context.Context, from, to string, boundary pglogrepl.LSN) (*GateReport, error) {
	start := time.Now()
	gates := m.Cfg.Migration.Gates
	rep := &GateReport{From: from, To: to, BoundaryLSN: boundary.String(), Declared: gates, EvaluatedAt: start}
	cand, err := m.Store.Generation(ctx, to)
	if err != nil {
		return nil, err
	}
	// Coverage at one sink snapshot: source_state and the candidate are written
	// by the same transactions, so this compares them at a common boundary.
	err = pgx.BeginTxFunc(ctx, m.Store.Pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var cp string
		if err := tx.QueryRow(ctx, `SELECT lsn::text FROM `+m.Store.T("checkpoints")+` WHERE consumer = $1`, store.Consumer).Scan(&cp); err != nil {
			return err
		}
		rep.CheckpointLSN = cp
		rows, _, err := verify.StateRows(ctx, tx, m.Store.Schema)
		if err != nil {
			return err
		}
		rep.StateRows = rows.Count()
		var views map[string]verify.ViewDocs
		if len(m.Cfg.Views) > 0 {
			rels, err := m.Store.Relations(ctx)
			if err != nil {
				return err
			}
			vr := map[string]viewdoc.Relation{}
			for n, r := range rels {
				vr[n] = viewdoc.Relation{KeyKinds: r.KeyKinds, Signature: r.RelationSignature}
			}
			if views, err = verify.StateViewDocs(ctx, tx, m.Store.Schema, m.Cfg, vr, rows); err != nil {
				return err
			}
		}
		rep.Coverage, err = verify.Generation(ctx, tx, m.Store.Schema, cand, m.Trs, verify.OnlyPipelines(rows, m.Cfg), views, m.Cfg.Pipeline.MaxDocumentBytes)
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM `+m.Store.T("dlq")+` WHERE generation_id = $1 AND state = 'open'`, to).Scan(&rep.Unresolved)
	})
	if err != nil {
		return nil, fmt.Errorf("coverage: %w", err)
	}
	cpLSN, _ := pglogrepl.ParseLSN(rep.CheckpointLSN)
	add := func(g GateResult) { rep.Gates = append(rep.Gates, g) }
	add(GateResult{Name: "catchup_checkpoint_past_boundary", Observed: float64(cpLSN), Threshold: float64(boundary), Op: ">=", Pass: cpLSN >= boundary})
	cov := rep.Coverage
	add(GateResult{Name: "coverage_missing_chunks", Observed: float64(cov.Missing), Threshold: 0, Op: "==", Pass: cov.Missing == 0})
	add(GateResult{Name: "coverage_extra_chunks", Observed: float64(cov.Extra), Threshold: 0, Op: "==", Pass: cov.Extra == 0})
	add(GateResult{Name: "coverage_stale_chunks", Observed: float64(cov.Stale), Threshold: 0, Op: "==", Pass: cov.Stale == 0})
	if gates.RequireZeroUnresolved {
		add(GateResult{Name: "unresolved_rows", Observed: float64(rep.Unresolved), Threshold: 0, Op: "==", Pass: rep.Unresolved == 0})
	}
	if m.Cfg.Migration.EvalQueries != "" && m.Cfg.Migration.EvalQrels != "" {
		if err := m.quality(ctx, rep, from, to); err != nil {
			return nil, fmt.Errorf("quality evaluation: %w", err)
		}
	} else {
		add(GateResult{Name: "quality", Pass: true, Note: "no evaluation set configured; quality gates not evaluated"})
	}
	rep.Pass = true
	for _, g := range rep.Gates {
		rep.Pass = rep.Pass && g.Pass
	}
	rep.DurationSeconds = time.Since(start).Seconds()
	return rep, nil
}

// Query is one evaluation query.
type Query struct {
	ID   string `json:"_id"`
	Text string `json:"text"`
}

// LoadQueries reads BEIR-style JSONL ({"_id","text"}).
func LoadQueries(path string) ([]Query, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Query
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var q Query
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, sc.Err()
}

// LoadQrels reads BEIR-style TSV (query-id, corpus-id, score) with a header.
func LoadQrels(path string) (map[string]map[string]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]map[string]float64{}
	sc := bufio.NewScanner(f)
	first := true
	for sc.Scan() {
		parts := strings.Split(strings.TrimSpace(sc.Text()), "\t")
		if len(parts) < 3 {
			continue
		}
		if first {
			first = false
			if _, err := strconv.ParseFloat(parts[2], 64); err != nil {
				continue
			}
		}
		s, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return nil, err
		}
		if out[parts[0]] == nil {
			out[parts[0]] = map[string]float64{}
		}
		out[parts[0]][parts[1]] = s
	}
	return out, sc.Err()
}

func (m *Migrator) embedQueries(ctx context.Context, gen string, qs []Query) ([]string, error) {
	enc, err := m.Searcher.Encoder(ctx, gen)
	if err != nil {
		return nil, err
	}
	dim := enc.Dim
	out := make([]string, len(qs))
	for i := 0; i < len(qs); i += 64 {
		end := min(i+64, len(qs))
		texts := make([]string, 0, end-i)
		for _, q := range qs[i:end] {
			texts = append(texts, q.Text)
		}
		inputs := texts
		vecs, rej, err := enc.Provider.Embed(ctx, enc.Request(texts...))
		if err != nil {
			return nil, err
		}
		if len(rej) > 0 {
			return nil, fmt.Errorf("embedding service rejected %d evaluation queries", len(rej))
		}
		for j := range inputs {
			if len(vecs[j]) != dim {
				return nil, fmt.Errorf("query vector dimension mismatch for %s", gen)
			}
			out[i+j] = embedclient.FormatVector(vecs[j])
		}
	}
	return out, nil
}

func scoresOf(h []search.Hit) []float64 {
	out := make([]float64, len(h))
	for i, x := range h {
		out[i] = x.Score
	}
	return out
}

// keysOf returns the evaluation document ids of hits. With one pipeline the
// id is the source key (the qrels format single-table installations use);
// with several it is "<pipeline>/<key>", which cannot collide across pipelines.
func (m *Migrator) keysOf(h []search.Hit) []string {
	out := make([]string, len(h))
	for i, x := range h {
		out[i] = EvalDocID(len(m.Cfg.Pipelines), x.Pipeline, x.Key)
	}
	return out
}

// EvalDocID is the corpus id a hit is judged under in eval_qrels.
func EvalDocID(pipelines int, pipeline, key string) string {
	if pipelines <= 1 {
		return key
	}
	return pipeline + "/" + key
}

func (m *Migrator) quality(ctx context.Context, rep *GateReport, from, to string) error {
	gates := m.Cfg.Migration.Gates
	qs, err := LoadQueries(m.Cfg.Migration.EvalQueries)
	if err != nil {
		return err
	}
	qrels, err := LoadQrels(m.Cfg.Migration.EvalQrels)
	if err != nil {
		return err
	}
	kept := qs[:0:0]
	for _, q := range qs {
		if len(qrels[q.ID]) > 0 {
			kept = append(kept, q)
		}
	}
	qs = kept
	rep.EvalQueries = len(qs)
	if len(qs) == 0 {
		return errors.New("no evaluation query has relevance judgments")
	}
	type perGen struct {
		recall, ndcg, ann      []float64
		exactRecall, exactNDCG []float64
		mm                     *ModelMetrics
		vecs                   []string
	}
	prep := func(gen string) (*perGen, error) {
		vecs, err := m.embedQueries(ctx, gen, qs)
		if err != nil {
			return nil, err
		}
		enc, err := m.Searcher.Encoder(ctx, gen)
		if err != nil {
			return nil, err
		}
		return &perGen{vecs: vecs, mm: &ModelMetrics{Generation: gen, Model: enc.Model, Revision: enc.Revision, Provider: enc.Provider.Name()}}, nil
	}
	a, err := prep(from)
	if err != nil {
		return err
	}
	c, err := prep(to)
	if err != nil {
		return err
	}
	// Both generations are searched at ONE sink snapshot: the committer writes
	// every live generation in the same transactions, so the paired comparison
	// sees the same corpus even while writes continue.
	err = pgx.BeginTxFunc(ctx, m.Store.Pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		for i, q := range qs {
			for _, pg := range []*perGen{a, c} {
				approx, err := m.Searcher.SearchVectorIn(ctx, tx, pg.mm.Generation, pg.vecs[i], 10, false)
				if err != nil {
					return err
				}
				exact, err := m.Searcher.SearchVectorIn(ctx, tx, pg.mm.Generation, pg.vecs[i], 10, true)
				if err != nil {
					return err
				}
				ak, ek := m.keysOf(approx), m.keysOf(exact)
				pg.recall = append(pg.recall, RecallAt(ak, qrels[q.ID], 10))
				pg.ndcg = append(pg.ndcg, NDCGAt(ak, qrels[q.ID], 10))
				pg.ann = append(pg.ann, TieAwareOverlap(ak, scoresOf(approx), ek, scoresOf(exact), 1e-6))
				pg.exactRecall = append(pg.exactRecall, RecallAt(ek, qrels[q.ID], 10))
				pg.exactNDCG = append(pg.exactNDCG, NDCGAt(ek, qrels[q.ID], 10))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, pg := range []*perGen{a, c} {
		pg.mm.RecallAt10, pg.mm.NDCGAt10, pg.mm.ANNRecallAt10 = Mean(pg.recall), Mean(pg.ndcg), Mean(pg.ann)
		pg.mm.ExactRecallAt10, pg.mm.ExactNDCGAt10 = Mean(pg.exactRecall), Mean(pg.exactNDCG)
	}
	rep.Active, rep.Candidate = a.mm, c.mm
	dn := PairedBootstrap(a.ndcg, c.ndcg, gates.BootstrapSamples, gates.ConfidenceLevel, gates.Seed)
	dr := PairedBootstrap(a.recall, c.recall, gates.BootstrapSamples, gates.ConfidenceLevel, gates.Seed+1)
	rep.DeltaNDCG, rep.DeltaRecall = &dn, &dr
	rep.Gates = append(rep.Gates,
		GateResult{Name: "candidate_ann_vs_exact_recall_at_10", Observed: c.mm.ANNRecallAt10, Threshold: gates.MinANNRecallAt10, Op: ">=", Pass: c.mm.ANNRecallAt10 >= gates.MinANNRecallAt10},
		GateResult{Name: "delta_ndcg_at_10_ci_lower", Observed: dn.Lower, Threshold: -gates.NonInferiorityMarginNDCG, Op: ">=", Pass: dn.Lower >= -gates.NonInferiorityMarginNDCG},
		GateResult{Name: "delta_recall_at_10_ci_lower", Observed: dr.Lower, Threshold: -gates.NonInferiorityMarginRecall, Op: ">=", Pass: dr.Lower >= -gates.NonInferiorityMarginRecall},
	)
	return nil
}

func (m *Migrator) printReport(r *GateReport) {
	m.printf("gate report %s -> %s (boundary %s, checkpoint %s)", r.From, r.To, r.BoundaryLSN, r.CheckpointLSN)
	if r.Active != nil && r.Candidate != nil {
		m.printf("  %-10s recall@10=%.4f nDCG@10=%.4f ann/exact=%.4f", r.Active.Generation, r.Active.RecallAt10, r.Active.NDCGAt10, r.Active.ANNRecallAt10)
		m.printf("  %-10s recall@10=%.4f nDCG@10=%.4f ann/exact=%.4f", r.Candidate.Generation, r.Candidate.RecallAt10, r.Candidate.NDCGAt10, r.Candidate.ANNRecallAt10)
		m.printf("  Δ nDCG@10 = %+.4f [%+.4f, %+.4f]  Δ recall@10 = %+.4f [%+.4f, %+.4f]  (%d queries, %.0f%% paired bootstrap)",
			r.DeltaNDCG.Mean, r.DeltaNDCG.Lower, r.DeltaNDCG.Upper, r.DeltaRecall.Mean, r.DeltaRecall.Lower, r.DeltaRecall.Upper, r.EvalQueries, r.Declared.ConfidenceLevel*100)
	}
	for _, g := range r.Gates {
		mark := "PASS"
		if !g.Pass {
			mark = "FAIL"
		}
		m.printf("  [%s] %-38s observed=%.4f %s %.4f %s", mark, g.Name, g.Observed, g.Op, g.Threshold, g.Note)
	}
	verdict := "PASS"
	if !r.Pass {
		verdict = "REJECTED"
	}
	m.printf("verdict: %s", verdict)
}
