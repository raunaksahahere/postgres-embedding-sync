package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/failpoint"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/filter"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

// Committer applies units in order. Each unit's row effects, quarantine
// records, backfill progress and checkpoint commit in one sink transaction;
// only then does durable progress (and therefore standby feedback) advance.
type Committer struct {
	Cfg      *config.Config
	Store    *store.Store
	Specs    Specs
	Progress *Progress
	Planner  *Planner
	Log      *slog.Logger
}

// Run applies units until in closes. It returns a *PauseError when the
// pipeline must stop before acknowledging work.
func (c *Committer) Run(ctx context.Context, in <-chan *Unit) error {
	for u := range in {
		select {
		case <-u.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		if u.Pause != nil {
			return u.Pause
		}
		if u.err != nil {
			if errors.Is(u.err, context.Canceled) {
				return u.err
			}
			// Not a pause: configuration or service errors are retried by the
			// supervisor with backoff; nothing in this unit was acknowledged.
			return fmt.Errorf("embedding stage: %w", u.err)
		}
		if c.Cfg.Pipeline.OnPoison == "pause" {
			if p := c.poisonPause(u); p != nil {
				return p
			}
		}
		if err := c.applyWithRetry(ctx, u); err != nil {
			return err
		}
	}
	return nil
}

func (c *Committer) poisonPause(u *Unit) *PauseError {
	for gid := range u.TargetGens() {
		for id, d := range u.Effective(gid) {
			if d.Poison != "" {
				return &PauseError{Reason: "poison_row", Detail: fmt.Sprintf("generation %s, pipeline %s: row version at %s cannot be embedded (%s); on_poison=pause", gid, id.Pipeline, d.LSN, d.Poison)}
			}
			for _, ch := range u.Chunks[gid][id] {
				if _, bad := u.Rejected[gid][ch.Fingerprint]; bad {
					return &PauseError{Reason: "poison_row", Detail: fmt.Sprintf("generation %s, pipeline %s: embedding service rejected a chunk at %s; on_poison=pause", gid, id.Pipeline, d.LSN)}
				}
			}
		}
	}
	return nil
}

func (c *Committer) applyWithRetry(ctx context.Context, u *Unit) error {
	for attempt := 0; ; attempt++ {
		start := time.Now()
		err := c.apply(ctx, u)
		if err == nil {
			metrics.SinkCommitSeconds.Observe(time.Since(start).Seconds())
			c.afterCommit(u)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var pe *PauseError
		if errors.As(err, &pe) {
			return pe
		}
		metrics.Retries.WithLabelValues("sink").Inc()
		backoff := time.Duration(rand.Int64N(int64(min(time.Duration(100<<min(attempt, 8))*time.Millisecond, 15*time.Second))))
		c.Log.Warn("sink apply failed; retrying", "attempt", attempt+1, "backoff", backoff, "err", err)
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
	}
}

func (c *Committer) afterCommit(u *Unit) {
	failpoint.Hit("after_commit_before_feedback")
	c.Progress.SetDurable(u.EndLSN)
	metrics.CheckpointLSN.Set(float64(u.EndLSN))
	now := time.Now()
	for _, t := range u.Txns {
		metrics.CommitToSearchable.Observe(now.Sub(t.CommitTime).Seconds())
	}
	metrics.SourceTransactions.Add(float64(len(u.Txns)))
	for _, p := range u.Progress {
		p.Entry.Finish("applied")
	}
	c.Planner.Release(u.Seq)
}

func (c *Committer) apply(ctx context.Context, u *Unit) error {
	tx, err := c.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var cur string
	if err := tx.QueryRow(ctx, `SELECT lsn::text FROM `+c.Store.T("checkpoints")+` WHERE consumer = $1 FOR UPDATE`, store.Consumer).Scan(&cur); err != nil {
		return fmt.Errorf("lock checkpoint: %w", err)
	}
	curLSN, _ := pglogrepl.ParseLSN(cur)
	if u.EndLSN <= curLSN {
		// Already durable (e.g. idle position behind a replayed checkpoint).
		return nil
	}
	if err := c.applyState(ctx, tx, u); err != nil {
		return err
	}
	for gid, gi := range u.TargetGens() {
		if err := c.applyGen(ctx, tx, u, gi); err != nil {
			return fmt.Errorf("apply generation %s: %w", gid, err)
		}
	}
	for _, gs := range u.GenStarts {
		if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("generations")+` SET state = 'building', stream_from_lsn = $2::pg_lsn
			WHERE id = $1 AND stream_from_lsn IS NULL`, gs.Gen, gs.LSN.String()); err != nil {
			return err
		}
	}
	for _, g := range u.Retires {
		if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("generations")+` SET retired_at = coalesce(retired_at, now()) WHERE id = $1`, g); err != nil {
			return err
		}
	}
	for _, p := range u.Progress {
		state := "running"
		if p.Final {
			state = "done"
		}
		if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("backfill_jobs")+` SET last_key = $2, rows_scanned = rows_scanned + $3,
			windows = windows + 1, state = CASE WHEN state = 'cancelled' THEN state ELSE $4 END,
			completed_at = CASE WHEN $4 = 'done' THEN now() ELSE completed_at END, updated_at = now() WHERE id = $1`,
			p.JobID, p.LastKey, p.Rows, state); err != nil {
			return err
		}
		if p.Final && p.Gen != "" {
			// A generation is ready once every pipeline's backfill into it is
			// complete, not when the first one finishes. (State jobs of a
			// view belong to no generation.)
			if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("generations")+` SET state = 'ready', ready_at = now() WHERE id = $1 AND state = 'building'
				AND NOT EXISTS (SELECT 1 FROM `+c.Store.T("backfill_jobs")+` WHERE generation_id = $1 AND state IN ('pending', 'running'))`, p.Gen); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("checkpoints")+` SET lsn = $2::pg_lsn, updated_at = now() WHERE consumer = $1`,
		store.Consumer, u.EndLSN.String()); err != nil {
		return err
	}
	failpoint.Hit("before_sink_commit")
	return tx.Commit(ctx)
}

// rangePredicate renders "rows of pipe inside (lo, hi]" over the encoded key
// column. The typed casts sit behind a CASE on the pipeline so they never run
// on another pipeline's keys (a composite key is not a bigint, a text key is
// not JSON), whatever order the planner evaluates the conjuncts in.
func (c *Committer) rangePredicate(pipe, col string, lo, hi *string, args *[]any) (string, error) {
	spec := c.Specs[pipe]
	if spec == nil {
		return "", fmt.Errorf("range delete for unknown pipeline %q", pipe)
	}
	*args = append(*args, pipe)
	pp := fmt.Sprintf("$%d", len(*args))
	conds := []string{}
	if lo != nil {
		s, err := spec.Codec.SinkCompare(col, ">", *lo, args)
		if err != nil {
			return "", err
		}
		conds = append(conds, s)
	}
	if hi != nil {
		s, err := spec.Codec.SinkCompare(col, "<=", *hi, args)
		if err != nil {
			return "", err
		}
		conds = append(conds, s)
	}
	if len(conds) == 0 {
		return "pipeline = " + pp, nil
	}
	return "pipeline = " + pp + " AND CASE WHEN pipeline = " + pp + " THEN " + strings.Join(conds, " AND ") + " ELSE false END", nil
}

func sortedIDs[T any](m map[DocID]T) []DocID {
	out := make([]DocID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out
}

func sortedPipes(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// applyState maintains source_state: the allowlisted projection of every
// pipeline's source table at the checkpoint.
// isView reports whether pipe is a view: its documents are assembled from
// its components' state, so it has no state rows of its own.
func (c *Committer) isView(pipe string) bool {
	s := c.Specs[pipe]
	return s != nil && s.IsView()
}

func (c *Committer) applyState(ctx context.Context, tx pgx.Tx, u *Unit) error {
	st := c.Store.T("source_state")
	refs := c.Store.T("source_refs")
	for _, pipe := range sortedPipes(u.Truncates) {
		if _, err := tx.Exec(ctx, `DELETE FROM `+st+` WHERE pipeline = $1`, pipe); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM `+refs+` WHERE pipeline = $1`, pipe); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+c.Store.T("dlq")+` SET state = 'superseded', updated_at = now() WHERE state = 'open' AND pipeline = $1`, pipe); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, rd := range u.RangeDeletes {
		if c.isView(rd.Pipeline) {
			continue
		}
		sig := rd.Pipeline + "\x00" + strOr(rd.Lo, "\x01") + "\x00" + strOr(rd.Hi, "\x01")
		if seen[sig] {
			continue
		}
		seen[sig] = true
		for _, t := range []string{st, refs} {
			args := []any{rd.Keep}
			pred, err := c.rangePredicate(rd.Pipeline, "source_key", rd.Lo, rd.Hi, &args)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM `+t+` WHERE `+pred+` AND source_key <> ALL($1::text[])`, args...); err != nil {
				return fmt.Errorf("range delete state: %w", err)
			}
		}
	}
	final := map[DocID]*DocEffect{}
	for id, d := range u.Stream {
		final[id] = d
	}
	for _, m := range u.Snapshot {
		for id, d := range m {
			final[id] = d
		}
	}
	// View component rows (state only); a component's rows never appear in
	// Stream or Snapshot.
	for id, d := range u.State {
		final[id] = d
	}
	var delPipes, delKeys, upPipes, upKeys, upLSN []string
	var upFields [][]byte
	var refPipes, refKeys, refNames, refTargets []string
	var touchedPipes, touchedKeys []string
	for _, id := range sortedIDs(final) {
		d := final[id]
		if c.isView(id.Pipeline) {
			continue
		}
		if spec := c.Specs[id.Pipeline]; spec != nil && spec.IsComponent() {
			touchedPipes, touchedKeys = append(touchedPipes, id.Pipeline), append(touchedKeys, id.Key)
			if !d.Delete {
				names := make([]string, 0, len(d.Refs))
				for n := range d.Refs {
					names = append(names, n)
				}
				sort.Strings(names)
				for _, n := range names {
					refPipes, refKeys = append(refPipes, id.Pipeline), append(refKeys, id.Key)
					refNames, refTargets = append(refNames, n), append(refTargets, d.Refs[n])
				}
			}
		}
		if d.Delete {
			delPipes, delKeys = append(delPipes, id.Pipeline), append(delKeys, id.Key)
			continue
		}
		b, err := json.Marshal(d.Fields)
		if err != nil {
			return err
		}
		upPipes, upKeys = append(upPipes, id.Pipeline), append(upKeys, id.Key)
		upFields = append(upFields, b)
		upLSN = append(upLSN, d.LSN.String())
	}
	if len(delKeys) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM `+st+` WHERE (pipeline, source_key) IN (SELECT * FROM unnest($1::text[], $2::text[]))`, delPipes, delKeys); err != nil {
			return err
		}
	}
	if len(upKeys) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO `+st+` (pipeline, source_key, fields, source_lsn, updated_at)
			SELECT p, k, f, l::pg_lsn, now() FROM unnest($1::text[], $2::text[], $3::jsonb[], $4::text[]) AS u(p, k, f, l)
			ON CONFLICT (pipeline, source_key) DO UPDATE SET fields = EXCLUDED.fields, source_lsn = EXCLUDED.source_lsn, updated_at = now()`,
			upPipes, upKeys, upFields, upLSN); err != nil {
			return fmt.Errorf("upsert state: %w", err)
		}
	}
	// The reference index follows each component row's latest version.
	if len(touchedKeys) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM `+refs+` WHERE (pipeline, source_key) IN (SELECT * FROM unnest($1::text[], $2::text[]))`, touchedPipes, touchedKeys); err != nil {
			return err
		}
	}
	if len(refKeys) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO `+refs+` (pipeline, source_key, ref, ref_key)
			SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])`, refPipes, refKeys, refNames, refTargets); err != nil {
			return fmt.Errorf("upsert refs: %w", err)
		}
	}
	return nil
}

func (c *Committer) applyGen(ctx context.Context, tx pgx.Tx, u *Unit, gi *GenInfo) error {
	gid := gi.Gen.ID
	tbl := pgx.Identifier{c.Store.Schema, gi.Gen.ChunkTable}.Sanitize()
	dlq := c.Store.T("dlq")
	for _, pipe := range sortedPipes(u.Truncates) {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` WHERE pipeline = $1`, pipe); err != nil {
			return err
		}
	}
	for _, rd := range u.RangeDeletes {
		if rd.Gen != gid {
			continue
		}
		args := []any{rd.Keep}
		pred, err := c.rangePredicate(rd.Pipeline, "source_key", rd.Lo, rd.Hi, &args)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` WHERE `+pred+` AND source_key <> ALL($1::text[])`, args...); err != nil {
			return fmt.Errorf("range delete: %w", err)
		}
		args2 := []any{rd.Keep, gid}
		pred2, err := c.rangePredicate(rd.Pipeline, "source_key", rd.Lo, rd.Hi, &args2)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE `+dlq+` SET state = 'superseded', updated_at = now() WHERE generation_id = $2 AND state = 'open' AND `+
			pred2+` AND source_key <> ALL($1::text[])`, args2...); err != nil {
			return err
		}
	}
	eff := u.Effective(gid)
	var (
		delP, delK, supP, supK        []string
		trimP, trimK                  []string
		trimN                         []int32
		cP, cKey, cID, cFP, cText, cV []string
		cNo                           []int32
		cLSN, cMeta                   []string
		cTime                         []*time.Time
		qP, qKey, qLSN, qReason, qErr []string
		upserts, deletes, poisoned    int
	)
	for _, id := range sortedIDs(eff) { // deterministic lock order
		d := eff[id]
		if d.Delete {
			delP, delK = append(delP, id.Pipeline), append(delK, id.Key)
			supP, supK = append(supP, id.Pipeline), append(supK, id.Key)
			deletes++
			continue
		}
		reason, errMsg := d.Poison, d.PoisonErr
		chunks := u.Chunks[gid][id]
		if reason == "" {
			for _, ch := range chunks {
				if msg, bad := u.Rejected[gid][ch.Fingerprint]; bad {
					reason, errMsg = "embedding_rejected", msg
					break
				}
				if _, ok := u.Vectors[gid][ch.Fingerprint]; !ok {
					return fmt.Errorf("missing vector for a chunk of generation %s", gid)
				}
			}
		}
		if reason != "" {
			// Quarantine atomically with the rest of the unit: remove stale
			// chunks so search never serves an outdated version, and record
			// the versioned failure for inspection and replay.
			delP, delK = append(delP, id.Pipeline), append(delK, id.Key)
			qP, qKey, qLSN = append(qP, id.Pipeline), append(qKey, id.Key), append(qLSN, d.LSN.String())
			qReason, qErr = append(qReason, reason), append(qErr, truncate(errMsg, 500))
			metrics.Quarantined.WithLabelValues(reason).Inc()
			poisoned++
			continue
		}
		supP, supK = append(supP, id.Pipeline), append(supK, id.Key)
		trimP, trimK, trimN = append(trimP, id.Pipeline), append(trimK, id.Key), append(trimN, int32(len(chunks)))
		var ct *time.Time
		if !d.CommitTime.IsZero() {
			t := d.CommitTime
			ct = &t
		}
		meta := c.metaJSON(id.Pipeline, d.Fields)
		for _, ch := range chunks {
			cMeta = append(cMeta, meta)
			cP = append(cP, id.Pipeline)
			cKey = append(cKey, id.Key)
			cNo = append(cNo, int32(ch.No))
			cID = append(cID, ch.ID)
			cFP = append(cFP, ch.Fingerprint)
			cText = append(cText, ch.Text)
			cV = append(cV, u.Vectors[gid][ch.Fingerprint])
			cLSN = append(cLSN, d.LSN.String())
			cTime = append(cTime, ct)
		}
		upserts++
	}
	if len(delK) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` WHERE (pipeline, source_key) IN (SELECT * FROM unnest($1::text[], $2::text[]))`, delP, delK); err != nil {
			return err
		}
	}
	if len(trimK) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` t USING unnest($1::text[], $2::text[], $3::int[]) AS d(p, k, n)
			WHERE t.pipeline = d.p AND t.source_key = d.k AND t.chunk_no >= d.n`, trimP, trimK, trimN); err != nil {
			return fmt.Errorf("trim chunks: %w", err)
		}
	}
	if len(cKey) > 0 {
		// A chunk is rewritten when its text or its document's metadata
		// changed: a metadata-only update changes filter results without
		// re-embedding (the vector came from the sink).
		if _, err := tx.Exec(ctx, `INSERT INTO `+tbl+` AS t (pipeline, source_key, chunk_no, chunk_id, fingerprint, content, embedding, source_lsn, source_commit_time, meta)
			SELECT p, k, n, id, fp, txt, v::vector, l::pg_lsn, ct, m::jsonb
			FROM unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::timestamptz[], $10::text[]) AS u(p, k, n, id, fp, txt, v, l, ct, m)
			ON CONFLICT (pipeline, source_key, chunk_no) DO UPDATE SET chunk_id = EXCLUDED.chunk_id, fingerprint = EXCLUDED.fingerprint,
				content = EXCLUDED.content, embedding = EXCLUDED.embedding, source_lsn = EXCLUDED.source_lsn,
				source_commit_time = EXCLUDED.source_commit_time, meta = EXCLUDED.meta, updated_at = now()
			WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint OR t.meta IS DISTINCT FROM EXCLUDED.meta`,
			cP, cKey, cNo, cID, cFP, cText, cV, cLSN, cTime, cMeta); err != nil {
			return fmt.Errorf("upsert chunks: %w", err)
		}
	}
	if len(supK) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE `+dlq+` SET state = 'superseded', updated_at = now()
			WHERE generation_id = $1 AND state = 'open' AND (pipeline, source_key) IN (SELECT * FROM unnest($2::text[], $3::text[]))`, gid, supP, supK); err != nil {
			return err
		}
	}
	if len(qKey) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO `+dlq+` (generation_id, pipeline, source_key, source_lsn, reason, error, state)
			SELECT $1, p, k, l::pg_lsn, r, e, 'open' FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::text[]) AS u(p, k, l, r, e)
			ON CONFLICT (generation_id, pipeline, source_key) WHERE state = 'open' DO UPDATE SET source_lsn = EXCLUDED.source_lsn,
				reason = EXCLUDED.reason, error = EXCLUDED.error, attempts = `+dlq+`.attempts + 1, updated_at = now()`,
			gid, qP, qKey, qLSN, qReason, qErr); err != nil {
			return fmt.Errorf("quarantine: %w", err)
		}
	}
	origin := "stream"
	if len(u.Snapshot[gid]) > 0 {
		origin = "mixed"
	}
	metrics.DocsApplied.WithLabelValues(gid, origin, "upsert").Add(float64(upserts))
	metrics.DocsApplied.WithLabelValues(gid, origin, "delete").Add(float64(deletes))
	metrics.DocsApplied.WithLabelValues(gid, origin, "quarantine").Add(float64(poisoned))
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var _ = embedclient.FormatVector

// strOr returns *s, or def for nil (keys never contain NUL or \x01).
func strOr(s *string, def string) string {
	if s == nil {
		return def
	}
	return *s
}

// metaJSON renders a document's declared metadata from its fields (the
// hidden __meta_ fields carried with the row) as the jsonb stored on each
// of its chunks. A value that does not parse as its declared type is left
// out (the catalog check rejects incompatible column types at startup).
func (c *Committer) metaJSON(pipe string, fields map[string]*string) string {
	return MetaJSON(c.Cfg.MetaFor(pipe), fields)
}

// MetaJSON is the metadata jsonb of a document with the given fields.
func MetaJSON(decls []config.MetaField, fields map[string]*string) string {
	if len(decls) == 0 {
		return "{}"
	}
	m := map[string]any{}
	for _, d := range decls {
		v, err := filter.Type(d.Type).Normalize(fields[config.MetaFieldPrefix+d.Name])
		if err != nil || v == nil {
			continue
		}
		m[d.Name] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}
