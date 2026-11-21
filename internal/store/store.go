// Package store owns every SQL statement against the sink (pgvector) database.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
)

// Consumer is the checkpoint row name for the single replication consumer.
const Consumer = "main"

// ServingName is the serving alias row name.
const ServingName = "default"

// Generation states.
const (
	StatePending  = "pending"
	StateBuilding = "building"
	StateReady    = "ready"
	StateActive   = "active"
	StatePrevious = "previous"
	StateRejected = "rejected"
	StateRetired  = "retired"
	StateStale    = "stale"
)

// LiveStates are generations that consume the change stream.
var LiveStates = []string{StateBuilding, StateReady, StateActive, StatePrevious}

// IsLive reports whether a state consumes changes.
func IsLive(s string) bool {
	for _, l := range LiveStates {
		if l == s {
			return true
		}
	}
	return false
}

var genIDRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// Generation is one index built by one encoder with one transform.
type Generation struct {
	ID                   string                 `json:"id"`
	ModelKey             string                 `json:"model_key"`
	ModelName            string                 `json:"model_name"`
	ModelRevision        string                 `json:"model_revision"`
	Dimension            int                    `json:"dimension"`
	Distance             string                 `json:"distance"`
	QueryPrefix          string                 `json:"query_prefix"`
	DocumentPrefix       string                 `json:"document_prefix"`
	IndexType            string                 `json:"index_type"`
	IndexParams          config.HNSW            `json:"index_params"`
	TransformFingerprint string                 `json:"transform_fingerprint"`
	Pipelines            map[string]GenPipeline `json:"pipelines"`
	ChunkTable           string                 `json:"chunk_table"`
	State                string                 `json:"state"`
	StreamFromLSN        *pglogrepl.LSN         `json:"stream_from_lsn,omitempty"`
	CreatedAt            time.Time              `json:"created_at"`
	PromotedAt           *time.Time             `json:"promoted_at,omitempty"`
	Extra                json.RawMessage        `json:"-"`
}

// GenPipeline is what a generation records about one pipeline it covers.
type GenPipeline struct {
	TransformFingerprint string `json:"transform_fingerprint"`
}

// Checkpoint is the durable replication progress and source identity.
type Checkpoint struct {
	SystemIdentifier string
	Timeline         int32
	Database         string
	SlotName         string
	Publication      string
	SlotCreatedLSN   pglogrepl.LSN
	SchemaPauseLSN   *pglogrepl.LSN // stream paused on a schema change at this checkpoint
	// SchemaPausePipeline names the pipeline whose relation changed ("" for a
	// pause recorded before pipelines existed: the single table, "default").
	SchemaPausePipeline string
	LSN                 pglogrepl.LSN
	UpdatedAt           time.Time
}

// Relation is the accepted shape of one pipeline's source table.
type Relation struct {
	Pipeline          string
	SourceTable       string
	KeyColumns        []string
	KeyKinds          string // keys.Codec string form, e.g. "int,text"
	RelationSignature string
}

// Serving is the alias row.
type Serving struct {
	GenerationID         string
	PreviousGenerationID *string
	RollbackUntil        *time.Time
	SwitchedAt           time.Time
}

// Store wraps a pool with the configured schema.
type Store struct {
	Pool   *pgxpool.Pool
	Schema string
}

// New connects to the sink.
func New(ctx context.Context, dsn, schema string, maxConns int32) (*Store, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse sink dsn: %w", err)
	}
	if maxConns > 0 {
		pc.MaxConns = maxConns
	}
	pc.ConnConfig.RuntimeParams["application_name"] = "embedsync"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect sink: %w", err)
	}
	return &Store{Pool: pool, Schema: schema}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// T qualifies a table name with the sink schema.
func (s *Store) T(name string) string { return pgx.Identifier{s.Schema, name}.Sanitize() }

// LoadCheckpoint returns nil when no checkpoint exists.
func (s *Store) LoadCheckpoint(ctx context.Context) (*Checkpoint, error) {
	var c Checkpoint
	var lsn, created string
	var pause, pausePipe *string
	err := s.Pool.QueryRow(ctx, `SELECT system_identifier, timeline, database, slot_name, publication,
		slot_created_lsn::text, schema_pause_lsn::text, schema_pause_pipeline, lsn::text, updated_at FROM `+s.T("checkpoints")+` WHERE consumer = $1`, Consumer).
		Scan(&c.SystemIdentifier, &c.Timeline, &c.Database, &c.SlotName, &c.Publication, &created, &pause, &pausePipe, &lsn, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.LSN, _ = pglogrepl.ParseLSN(lsn)
	c.SlotCreatedLSN, _ = pglogrepl.ParseLSN(created)
	if pause != nil {
		if p, err := pglogrepl.ParseLSN(*pause); err == nil {
			c.SchemaPauseLSN = &p
		}
	}
	if pausePipe != nil {
		c.SchemaPausePipeline = *pausePipe
	}
	return &c, nil
}

// InsertCheckpoint creates the checkpoint row for a newly created slot.
func (s *Store) InsertCheckpoint(ctx context.Context, c Checkpoint) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.T("checkpoints")+` (consumer, system_identifier, timeline, database, slot_name,
		publication, slot_created_lsn, lsn)
		VALUES ($1,$2,$3,$4,$5,$6,$7::pg_lsn,$8::pg_lsn)`,
		Consumer, c.SystemIdentifier, c.Timeline, c.Database, c.SlotName, c.Publication, c.SlotCreatedLSN.String(), c.LSN.String())
	return err
}

// Relations loads the accepted shape of every registered pipeline.
func (s *Store) Relations(ctx context.Context) (map[string]*Relation, error) {
	rows, err := s.Pool.Query(ctx, `SELECT pipeline, source_table, key_columns, key_kinds, relation_signature FROM `+s.T("pipeline_relations"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*Relation{}
	for rows.Next() {
		var r Relation
		if err := rows.Scan(&r.Pipeline, &r.SourceTable, &r.KeyColumns, &r.KeyKinds, &r.RelationSignature); err != nil {
			return nil, err
		}
		out[r.Pipeline] = &r
	}
	return out, rows.Err()
}

// InsertRelation registers a pipeline's source table shape.
func (s *Store) InsertRelation(ctx context.Context, r Relation) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.T("pipeline_relations")+` (pipeline, source_table, key_columns, key_kinds, relation_signature)
		VALUES ($1, $2, $3, $4, $5)`, r.Pipeline, r.SourceTable, r.KeyColumns, r.KeyKinds, r.RelationSignature)
	return err
}

// UpdateRelationSignature records an operator-accepted schema for one
// pipeline and clears the recorded schema pause.
func (s *Store) UpdateRelationSignature(ctx context.Context, pipeline, sig string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+s.T("pipeline_relations")+` SET relation_signature = $2, updated_at = now() WHERE pipeline = $1`, pipeline, sig); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE `+s.T("checkpoints")+` SET schema_pause_lsn = NULL, schema_pause_pipeline = NULL WHERE consumer = $1`, Consumer)
		return err
	})
}

// MarkSchemaPause records that the stream stopped on an incompatible relation
// of pipeline at the current checkpoint.
func (s *Store) MarkSchemaPause(ctx context.Context, pipeline string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE `+s.T("checkpoints")+` SET schema_pause_lsn = lsn, schema_pause_pipeline = $2 WHERE consumer = $1`, Consumer, pipeline)
	return err
}

// SchemaVersion returns the newest applied sink migration (0 if none).
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var n *int
	err := s.Pool.QueryRow(ctx, `SELECT max(version) FROM `+s.T("schema_migrations")).Scan(&n)
	if err != nil || n == nil {
		return 0, err
	}
	return *n, nil
}

const genCols = `id, model_key, model_name, model_revision, dimension, distance, query_prefix, document_prefix, index_type,
	index_params, transform_fingerprint, pipelines, chunk_table, state, stream_from_lsn::text, created_at, promoted_at`

func scanGen(row pgx.Row) (*Generation, error) {
	var g Generation
	var params, pipes []byte
	var lsn *string
	if err := row.Scan(&g.ID, &g.ModelKey, &g.ModelName, &g.ModelRevision, &g.Dimension, &g.Distance, &g.QueryPrefix, &g.DocumentPrefix,
		&g.IndexType, &params, &g.TransformFingerprint, &pipes, &g.ChunkTable, &g.State, &lsn, &g.CreatedAt, &g.PromotedAt); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(params, &g.IndexParams)
	g.Pipelines = map[string]GenPipeline{}
	if err := json.Unmarshal(pipes, &g.Pipelines); err != nil {
		return nil, fmt.Errorf("generation %s pipelines: %w", g.ID, err)
	}
	if lsn != nil {
		l, err := pglogrepl.ParseLSN(*lsn)
		if err == nil {
			g.StreamFromLSN = &l
		}
	}
	return &g, nil
}

// Generations lists all generations ordered by creation.
func (s *Store) Generations(ctx context.Context) ([]*Generation, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+genCols+` FROM `+s.T("generations")+` ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Generation
	for rows.Next() {
		g, err := scanGen(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Generation loads one generation or returns nil.
func (s *Store) Generation(ctx context.Context, id string) (*Generation, error) {
	g, err := scanGen(s.Pool.QueryRow(ctx, `SELECT `+genCols+` FROM `+s.T("generations")+` WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return g, err
}

// NewGeneration describes a generation of a configured model over every
// configured pipeline.
func NewGeneration(id, modelKey string, m config.Model, cfg *config.Config) (*Generation, error) {
	if !genIDRe.MatchString(id) {
		return nil, fmt.Errorf("invalid generation id %q", id)
	}
	pipes := map[string]GenPipeline{}
	for name, fp := range cfg.PipelineFingerprints() {
		pipes[name] = GenPipeline{TransformFingerprint: fp}
	}
	return &Generation{
		ID: id, ModelKey: modelKey, ModelName: m.Name, ModelRevision: m.Revision, Dimension: m.Dimension,
		Distance: m.Distance, QueryPrefix: m.QueryPrefix, DocumentPrefix: m.DocumentPrefix, IndexType: "hnsw",
		IndexParams: m.HNSW, TransformFingerprint: cfg.CombinedFingerprint(), Pipelines: pipes, ChunkTable: "chunks_" + id, State: StatePending,
	}, nil
}

// PipelineNames returns the generation's pipelines, sorted.
func (g *Generation) PipelineNames() []string {
	out := make([]string, 0, len(g.Pipelines))
	for n := range g.Pipelines {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// OpsClass returns the pgvector operator class for the distance metric.
func (g *Generation) OpsClass() string {
	switch g.Distance {
	case "l2":
		return "vector_l2_ops"
	case "ip":
		return "vector_ip_ops"
	}
	return "vector_cosine_ops"
}

// DistanceOp returns the pgvector distance operator.
func (g *Generation) DistanceOp() string {
	switch g.Distance {
	case "l2":
		return "<->"
	case "ip":
		return "<#>"
	}
	return "<=>"
}

// MatchesModel reports whether a configured model and the transforms of the
// pipelines the generation covers equal what it was built with. Configured
// pipelines the generation does not cover yet are not an error (see
// MissingPipelines); covered pipelines that are no longer configured are.
func (g *Generation) MatchesModel(m config.Model, fps map[string]string) error {
	for _, name := range g.PipelineNames() {
		fp, ok := fps[name]
		if !ok {
			return fmt.Errorf("generation %s covers pipeline %q, which is no longer configured; restore it (removing a pipeline needs a new sink schema)", g.ID, name)
		}
		if got := g.Pipelines[name].TransformFingerprint; got != fp {
			return fmt.Errorf("generation %s was built with transform %s for pipeline %q but config now hashes to %s; create a new generation and migrate", g.ID, got, name, fp)
		}
	}
	switch {
	case g.ModelName != m.Name || g.ModelRevision != m.Revision:
		return fmt.Errorf("generation %s was built with %s@%s but config now says %s@%s", g.ID, g.ModelName, g.ModelRevision, m.Name, m.Revision)
	case g.Dimension != m.Dimension || g.Distance != m.Distance:
		return fmt.Errorf("generation %s dimension/distance differ from config", g.ID)
	case g.DocumentPrefix != m.DocumentPrefix || g.QueryPrefix != m.QueryPrefix:
		return fmt.Errorf("generation %s prompts differ from config", g.ID)
	}
	return nil
}

// MissingPipelines lists configured pipelines the generation does not cover yet.
func (g *Generation) MissingPipelines(fps map[string]string) []string {
	var out []string
	for name := range fps {
		if _, ok := g.Pipelines[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// CreateGeneration inserts the generation row, its chunk table, indexes and
// reader grant, plus a pending backfill job, in one transaction.
func (s *Store) CreateGeneration(ctx context.Context, g *Generation, readerRole string) error {
	params, _ := json.Marshal(g.IndexParams)
	pipes, _ := json.Marshal(g.Pipelines)
	tbl := s.T(g.ChunkTable)
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("generations")+` (id, model_key, model_name, model_revision, dimension, distance,
			query_prefix, document_prefix, index_type, index_params, transform_fingerprint, pipelines, chunk_table, state)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			g.ID, g.ModelKey, g.ModelName, g.ModelRevision, g.Dimension, g.Distance, g.QueryPrefix, g.DocumentPrefix,
			g.IndexType, params, g.TransformFingerprint, pipes, g.ChunkTable, StatePending); err != nil {
			return fmt.Errorf("insert generation: %w", err)
		}
		ddl := []string{
			fmt.Sprintf(`CREATE TABLE %s (
				pipeline           text NOT NULL,
				source_key         text NOT NULL,
				chunk_no           integer NOT NULL,
				chunk_id           text NOT NULL,
				fingerprint        text NOT NULL,
				content            text NOT NULL,
				embedding          vector(%d) NOT NULL,
				source_lsn         pg_lsn NOT NULL,
				source_commit_time timestamptz,
				updated_at         timestamptz NOT NULL DEFAULT now(),
				PRIMARY KEY (pipeline, source_key, chunk_no))`, tbl, g.Dimension),
			fmt.Sprintf(`CREATE INDEX %s ON %s (fingerprint)`, pgx.Identifier{g.ChunkTable + "_fp"}.Sanitize(), tbl),
			fmt.Sprintf(`CREATE INDEX %s ON %s USING hnsw (embedding %s) WITH (m = %d, ef_construction = %d)`,
				pgx.Identifier{g.ChunkTable + "_hnsw"}.Sanitize(), tbl, g.OpsClass(), g.IndexParams.M, g.IndexParams.EFConstruction),
		}
		if readerRole != "" {
			ddl = append(ddl, fmt.Sprintf(`GRANT SELECT ON %s TO %s`, tbl, pgx.Identifier{readerRole}.Sanitize()))
		}
		for _, q := range ddl {
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("create chunk table: %w", err)
			}
		}
		for _, p := range g.PipelineNames() {
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, state) VALUES ($1, $2, 'pending')`, g.ID, p); err != nil {
				return err
			}
		}
		return nil
	})
}

// AddPipelines extends a live or pending generation to pipelines added to the
// configuration: it records their transform fingerprints and queues one
// backfill job per new pipeline, in one transaction.
func (s *Store) AddPipelines(ctx context.Context, genID string, add map[string]string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		names := make([]string, 0, len(add))
		for n := range add {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			b, _ := json.Marshal(GenPipeline{TransformFingerprint: add[n]})
			if _, err := tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET pipelines = pipelines || jsonb_build_object($2::text, $3::jsonb) WHERE id = $1`,
				genID, n, b); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, state) VALUES ($1, $2, 'pending')
				ON CONFLICT (generation_id, pipeline) WHERE state IN ('pending','running') DO NOTHING`, genID, n); err != nil {
				return err
			}
		}
		return nil
	})
}

// Serving returns the serving alias or nil.
func (s *Store) Serving(ctx context.Context) (*Serving, error) {
	var sv Serving
	err := s.Pool.QueryRow(ctx, `SELECT generation_id, previous_generation_id, rollback_until, switched_at FROM `+s.T("serving")+` WHERE name = $1`, ServingName).
		Scan(&sv.GenerationID, &sv.PreviousGenerationID, &sv.RollbackUntil, &sv.SwitchedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &sv, err
}

// InitServing points the alias at the first generation if no alias exists.
func (s *Store) InitServing(ctx context.Context, genID string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO `+s.T("serving")+` (name, generation_id) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`, ServingName, genID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			_, err = tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET promoted_at = now() WHERE id = $1`, genID)
		}
		return err
	})
}

// SetPipelineStatus records running/paused state outside any checkpoint transaction.
func (s *Store) SetPipelineStatus(ctx context.Context, state, reason, detail string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.T("pipeline_status")+` (consumer, state, reason, detail, updated_at) VALUES ($1,$2,$3,$4, now())
		ON CONFLICT (consumer) DO UPDATE SET state = EXCLUDED.state, reason = EXCLUDED.reason, detail = EXCLUDED.detail, updated_at = now()`,
		Consumer, state, reason, detail)
	return err
}

// PipelineStatus reads the status row.
func (s *Store) PipelineStatus(ctx context.Context) (state, reason, detail string, at time.Time, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT state, reason, detail, updated_at FROM `+s.T("pipeline_status")+` WHERE consumer = $1`, Consumer).Scan(&state, &reason, &detail, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return "unknown", "", "", time.Time{}, nil
	}
	return
}

// BackfillJob is a resumable watermark backfill of one pipeline into one generation.
type BackfillJob struct {
	ID           int64
	GenerationID string
	Pipeline     string
	State        string
	LastKey      *string // encoded key (keys.Codec) of the last applied window
	RowsScanned  int64
	Windows      int64
}

const jobCols = `id, generation_id, pipeline, state, last_key, rows_scanned, windows`

func scanJob(row pgx.Row) (*BackfillJob, error) {
	var j BackfillJob
	if err := row.Scan(&j.ID, &j.GenerationID, &j.Pipeline, &j.State, &j.LastKey, &j.RowsScanned, &j.Windows); err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) jobs(ctx context.Context, where string, args ...any) ([]BackfillJob, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+jobCols+` FROM `+s.T("backfill_jobs")+` `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BackfillJob
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// NextBackfillJob returns the oldest pending/running job.
func (s *Store) NextBackfillJob(ctx context.Context) (*BackfillJob, error) {
	j, err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols+` FROM `+s.T("backfill_jobs")+`
		WHERE state IN ('pending','running') ORDER BY id LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// BackfillJobByID loads one job (nil if it does not exist).
func (s *Store) BackfillJobByID(ctx context.Context, id int64) (*BackfillJob, error) {
	j, err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols+` FROM `+s.T("backfill_jobs")+` WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// BackfillJobs lists recent jobs.
func (s *Store) BackfillJobs(ctx context.Context) ([]BackfillJob, error) {
	return s.jobs(ctx, `ORDER BY id DESC LIMIT 20`)
}

// ActiveBackfillJobs lists every pending or running job, optionally of one generation.
func (s *Store) ActiveBackfillJobs(ctx context.Context, genID string) ([]BackfillJob, error) {
	return s.jobs(ctx, `WHERE state IN ('pending','running') AND ($1 = '' OR generation_id = $1) ORDER BY id`, genID)
}

// RequestBackfill queues a re-scan of one pipeline for a live generation
// (no-op if one is queued).
func (s *Store) RequestBackfill(ctx context.Context, genID, pipeline string) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, state) VALUES ($1, $2, 'pending')
		ON CONFLICT (generation_id, pipeline) WHERE state IN ('pending','running') DO UPDATE SET updated_at = now() RETURNING id`, genID, pipeline).Scan(&id)
	return id, err
}

// MarkJobRunning flips a pending job to running.
func (s *Store) MarkJobRunning(ctx context.Context, id int64) error {
	_, err := s.Pool.Exec(ctx, `UPDATE `+s.T("backfill_jobs")+` SET state = 'running', started_at = coalesce(started_at, now()), updated_at = now() WHERE id = $1 AND state = 'pending'`, id)
	return err
}

// DLQEntry is one quarantined row version.
type DLQEntry struct {
	ID           int64     `json:"id"`
	GenerationID string    `json:"generation_id"`
	Pipeline     string    `json:"pipeline"`
	SourceKey    string    `json:"source_key"`
	SourceLSN    string    `json:"source_lsn"`
	Reason       string    `json:"reason"`
	Error        string    `json:"error"`
	Attempts     int       `json:"attempts"`
	State        string    `json:"state"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// DLQ lists entries filtered by state ("" for all).
func (s *Store) DLQ(ctx context.Context, state string, limit int) ([]DLQEntry, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, generation_id, pipeline, source_key, source_lsn::text, reason, error, attempts, state, created_at, updated_at
		FROM `+s.T("dlq")+` WHERE ($1 = '' OR state = $1) ORDER BY id LIMIT $2`, state, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DLQEntry
	for rows.Next() {
		var e DLQEntry
		if err := rows.Scan(&e.ID, &e.GenerationID, &e.Pipeline, &e.SourceKey, &e.SourceLSN, &e.Reason, &e.Error, &e.Attempts, &e.State, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UnresolvedCounts returns open DLQ counts and the oldest open age per generation.
func (s *Store) UnresolvedCounts(ctx context.Context) (map[string]int64, map[string]time.Duration, error) {
	rows, err := s.Pool.Query(ctx, `SELECT generation_id, count(*), extract(epoch FROM now() - min(created_at)) FROM `+s.T("dlq")+` WHERE state = 'open' GROUP BY 1`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	ages := map[string]time.Duration{}
	for rows.Next() {
		var g string
		var n int64
		var age float64
		if err := rows.Scan(&g, &n, &age); err != nil {
			return nil, nil, err
		}
		counts[g] = n
		ages[g] = time.Duration(age * float64(time.Second))
	}
	return counts, ages, rows.Err()
}

// SourceState is the allowlisted projection of one source row at the checkpoint.
type SourceState struct {
	Key    string
	Fields map[string]*string
	LSN    pglogrepl.LSN
}

// LoadSourceState reads one pipeline's state rows for the given keys.
func (s *Store) LoadSourceState(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, pipeline string, keys []string) (map[string]SourceState, error) {
	rows, err := q.Query(ctx, `SELECT source_key, fields, source_lsn::text FROM `+s.T("source_state")+` WHERE pipeline = $1 AND source_key = ANY($2)`, pipeline, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]SourceState{}
	for rows.Next() {
		var st SourceState
		var raw []byte
		var lsn string
		if err := rows.Scan(&st.Key, &raw, &lsn); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &st.Fields); err != nil {
			return nil, err
		}
		st.LSN, _ = pglogrepl.ParseLSN(lsn)
		out[st.Key] = st
	}
	return out, rows.Err()
}

// TryAdvisoryLock takes a session-level lock that prevents two `run` processes
// from consuming the same checkpoint. The returned conn must stay open.
func (s *Store) TryAdvisoryLock(ctx context.Context) (*pgxpool.Conn, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, "embedsync:"+s.Schema).Scan(&ok); err != nil {
		conn.Release()
		return nil, err
	}
	if !ok {
		conn.Release()
		return nil, errors.New("another embedsync run process holds the consumer lock for this sink schema")
	}
	return conn, nil
}
