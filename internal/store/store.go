// Package store owns every SQL statement against the sink (pgvector) database.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
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
	Provider             ProviderIdentity       `json:"provider"`
	// SearchFingerprint is what the generation's search columns (metadata
	// and full-text) are complete for; SearchTarget what they are being
	// built for. Filtered and hybrid search need SearchFingerprint to equal
	// the configuration's.
	SearchFingerprint string          `json:"search_fingerprint,omitempty"`
	SearchTarget      string          `json:"search_target,omitempty"`
	ChunkTable        string          `json:"chunk_table"`
	State             string          `json:"state"`
	StreamFromLSN     *pglogrepl.LSN  `json:"stream_from_lsn,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	PromotedAt        *time.Time      `json:"promoted_at,omitempty"`
	Extra             json.RawMessage `json:"-"`
}

// ProviderIdentity is how a generation's vectors were computed: which
// configured provider, of which kind, at which endpoint (recorded without
// credentials, for audit), and with which request settings. Queries against
// the generation use the same provider and settings.
type ProviderIdentity struct {
	Name              string `json:"name"`
	Kind              string `json:"kind"`
	Endpoint          string `json:"endpoint,omitempty"`
	RequestDimensions int    `json:"request_dimensions,omitempty"`
	QueryInputType    string `json:"query_input_type,omitempty"`
	DocumentInputType string `json:"document_input_type,omitempty"`
}

// Encoder is the identity chunk fingerprints use: the model, its document
// prompt and, for a non-local provider, the provider and request settings.
func (g *Generation) Encoder() transform.ModelIdentity {
	m := transform.ModelIdentity{Name: g.ModelName, Revision: g.ModelRevision, DocumentPrefix: g.DocumentPrefix}
	if p := g.Provider; p.Name != "" && p.Name != config.LocalProvider {
		m.Provider = fmt.Sprintf("%s|%s|%d|%s", p.Name, p.Kind, p.RequestDimensions, p.DocumentInputType)
	}
	return m
}

// NewProviderIdentity describes how a configured model is embedded.
func NewProviderIdentity(cfg *config.Config, m config.Model) ProviderIdentity {
	id := ProviderIdentity{Name: m.ProviderName(), Kind: config.LocalProvider, RequestDimensions: m.RequestDimensions,
		QueryInputType: m.QueryInputType, DocumentInputType: m.DocumentInputType}
	if p, ok := cfg.Providers[id.Name]; ok {
		id.Kind = p.Kind
		id.Endpoint = redactURL(p.ProviderURL())
	}
	return id
}

// redactURL drops credentials and query parameters from an endpoint URL.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

// GenPipeline is what a generation records about one pipeline it covers.
type GenPipeline struct {
	TransformFingerprint string `json:"transform_fingerprint"`
	// View marks a document view: its backfill renders from state ("render"
	// jobs) instead of scanning a table ("embed" jobs).
	View bool `json:"view,omitempty"`
}

// Job kinds.
const (
	JobEmbed  = "embed"
	JobState  = "state"
	JobRender = "render"
)

func (p GenPipeline) jobKind() string {
	if p.View {
		return JobRender
	}
	return JobEmbed
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
	index_params, transform_fingerprint, pipelines, chunk_table, state, stream_from_lsn::text, created_at, promoted_at, provider, coalesce(search_fingerprint, ''), coalesce(search_target, '')`

func scanGen(row pgx.Row) (*Generation, error) {
	var g Generation
	var params, pipes, prov []byte
	var lsn *string
	if err := row.Scan(&g.ID, &g.ModelKey, &g.ModelName, &g.ModelRevision, &g.Dimension, &g.Distance, &g.QueryPrefix, &g.DocumentPrefix,
		&g.IndexType, &params, &g.TransformFingerprint, &pipes, &g.ChunkTable, &g.State, &lsn, &g.CreatedAt, &g.PromotedAt, &prov, &g.SearchFingerprint, &g.SearchTarget); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(prov, &g.Provider); err != nil {
		return nil, fmt.Errorf("generation %s provider: %w", g.ID, err)
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
		pipes[name] = GenPipeline{TransformFingerprint: fp, View: cfg.View(name) != nil}
	}
	return &Generation{
		ID: id, ModelKey: modelKey, ModelName: m.Name, ModelRevision: m.Revision, Dimension: m.Dimension,
		Distance: m.Distance, QueryPrefix: m.QueryPrefix, DocumentPrefix: m.DocumentPrefix, IndexType: "hnsw",
		IndexParams: m.HNSW, TransformFingerprint: cfg.CombinedFingerprint(), Pipelines: pipes, ChunkTable: "chunks_" + id, State: StatePending,
		Provider: NewProviderIdentity(cfg, m), SearchTarget: cfg.SearchFingerprint(),
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
	case g.Provider.Name != m.ProviderName() || g.Provider.RequestDimensions != m.RequestDimensions ||
		g.Provider.QueryInputType != m.QueryInputType || g.Provider.DocumentInputType != m.DocumentInputType:
		return fmt.Errorf("generation %s was embedded by provider %q (request_dimensions %d, input types %q/%q) but its model now says provider %q (%d, %q/%q); add a new model key and migrate",
			g.ID, g.Provider.Name, g.Provider.RequestDimensions, g.Provider.DocumentInputType, g.Provider.QueryInputType,
			m.ProviderName(), m.RequestDimensions, m.DocumentInputType, m.QueryInputType)
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
	prov, _ := json.Marshal(g.Provider)
	tbl := s.T(g.ChunkTable)
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("generations")+` (id, model_key, model_name, model_revision, dimension, distance,
			query_prefix, document_prefix, index_type, index_params, transform_fingerprint, pipelines, chunk_table, state, provider, search_target)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
			g.ID, g.ModelKey, g.ModelName, g.ModelRevision, g.Dimension, g.Distance, g.QueryPrefix, g.DocumentPrefix,
			g.IndexType, params, g.TransformFingerprint, pipes, g.ChunkTable, StatePending, prov, g.SearchTarget); err != nil {
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
				meta               jsonb NOT NULL DEFAULT '{}',
				tsv                tsvector,
				source_lsn         pg_lsn NOT NULL,
				source_commit_time timestamptz,
				updated_at         timestamptz NOT NULL DEFAULT now(),
				PRIMARY KEY (pipeline, source_key, chunk_no))`, tbl, g.Dimension),
			fmt.Sprintf(`CREATE INDEX %s ON %s (fingerprint)`, pgx.Identifier{g.ChunkTable + "_fp"}.Sanitize(), tbl),
			fmt.Sprintf(`CREATE INDEX %s ON %s USING gin (meta)`, pgx.Identifier{g.ChunkTable + "_meta"}.Sanitize(), tbl),
			fmt.Sprintf(`CREATE INDEX %s ON %s USING gin (tsv)`, pgx.Identifier{g.ChunkTable + "_tsv"}.Sanitize(), tbl),
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
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES ($1, $2, $3, 'pending')`,
				g.ID, p, g.Pipelines[p].jobKind()); err != nil {
				return err
			}
		}
		return nil
	})
}

// AddPipelines extends a live or pending generation to pipelines added to the
// configuration: it records their transform fingerprints and queues one
// backfill job per new pipeline, in one transaction.
func (s *Store) AddPipelines(ctx context.Context, genID string, add map[string]GenPipeline) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		names := make([]string, 0, len(add))
		for n := range add {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			b, _ := json.Marshal(add[n])
			if _, err := tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET pipelines = pipelines || jsonb_build_object($2::text, $3::jsonb) WHERE id = $1`,
				genID, n, b); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES ($1, $2, $3, 'pending')
				ON CONFLICT (generation_id, pipeline) WHERE state IN ('pending','running') DO NOTHING`, genID, n, add[n].jobKind()); err != nil {
				return err
			}
		}
		// A candidate that was ready is not complete any more: it becomes
		// ready again when the new pipelines' backfills finish. Serving and
		// rollback generations keep their state and fill in the new tables.
		_, err := tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET state = 'building', ready_at = NULL WHERE id = $1 AND state = 'ready'
			AND NOT EXISTS (SELECT 1 FROM `+s.T("serving")+` WHERE generation_id = $1 OR previous_generation_id = $1)`, genID)
		return err
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
	GenerationID string // empty for state jobs
	Pipeline     string // pipeline, view (render) or view component (state)
	Kind         string // embed, state or render
	State        string
	LastKey      *string // encoded key (keys.Codec) of the last applied window
	RowsScanned  int64
	Windows      int64
}

const jobCols = `id, coalesce(generation_id, ''), pipeline, kind, state, last_key, rows_scanned, windows`

func scanJob(row pgx.Row) (*BackfillJob, error) {
	var j BackfillJob
	if err := row.Scan(&j.ID, &j.GenerationID, &j.Pipeline, &j.Kind, &j.State, &j.LastKey, &j.RowsScanned, &j.Windows); err != nil {
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

// NextBackfillJob returns the next pending/running job: state jobs first (a
// view renders only once its state is complete), then the oldest.
func (s *Store) NextBackfillJob(ctx context.Context) (*BackfillJob, error) {
	j, err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols+` FROM `+s.T("backfill_jobs")+`
		WHERE state IN ('pending','running') ORDER BY (kind = 'state') DESC, id LIMIT 1`))
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

// RequestStateBackfill queues a state scan of a view component (no-op if one is queued).
func (s *Store) RequestStateBackfill(ctx context.Context, component string) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES (NULL, $1, 'state', 'pending')
		ON CONFLICT (pipeline) WHERE kind = 'state' AND state IN ('pending','running') DO UPDATE SET updated_at = now() RETURNING id`, component).Scan(&id)
	return id, err
}

// Children returns, per parent key, the keys of a component's rows whose
// reference ref points at it (from source_refs).
func (s *Store) Children(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, pipeline, ref string, parents []string) (map[string][]string, error) {
	rows, err := q.Query(ctx, `SELECT ref_key, source_key FROM `+s.T("source_refs")+` WHERE pipeline = $1 AND ref = $2 AND ref_key = ANY($3) ORDER BY 1, 2`,
		pipeline, ref, parents)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var p, k string
		if err := rows.Scan(&p, &k); err != nil {
			return nil, err
		}
		out[p] = append(out[p], k)
	}
	return out, rows.Err()
}

// KeysAfter lists up to limit state keys of a pipeline after lo, in the
// key order of codec (used to page through a view's root rows).
func (s *Store) KeysAfter(ctx context.Context, pipeline string, codec keys.Codec, lo *string, limit int) ([]string, error) {
	args := []any{pipeline}
	pred := "pipeline = $1"
	if lo != nil {
		cmp, err := codec.SinkCompare("source_key", ">", *lo, &args)
		if err != nil {
			return nil, err
		}
		pred += " AND CASE WHEN pipeline = $1 THEN " + cmp + " ELSE false END"
	}
	args = append(args, limit)
	order := strings.Join(codec.SinkExprs("source_key"), ", ")
	// The sort runs on rows already restricted to the pipeline, so the
	// typed casts only see this codec's keys.
	rows, err := s.Pool.Query(ctx, `SELECT source_key FROM `+s.T("source_state")+` WHERE `+pred+
		` ORDER BY `+order+fmt.Sprintf(` LIMIT $%d`, len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RefKeys returns the distinct keys that a component's reference ref points
// at (e.g. every parent with a section), for truncate fan-out.
func (s *Store) RefKeys(ctx context.Context, pipeline, ref string) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT ref_key FROM `+s.T("source_refs")+` WHERE pipeline = $1 AND ref = $2`, pipeline, ref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RequestBackfill queues a re-scan of one pipeline for a live generation
// (no-op if one is queued).
func (s *Store) RequestBackfill(ctx context.Context, genID, pipeline, kind string) (int64, error) {
	var id int64
	err := s.Pool.QueryRow(ctx, `INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES ($1, $2, $3, 'pending')
		ON CONFLICT (generation_id, pipeline) WHERE state IN ('pending','running') DO UPDATE SET updated_at = now() RETURNING id`, genID, pipeline, kind).Scan(&id)
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

// SearchReady reports whether filtered and hybrid search may use the
// generation for the configured search fingerprint.
func (g *Generation) SearchReady(fp string) bool { return g.SearchFingerprint == fp }

// QueueSearchRescan starts (re)building a live generation's search columns
// for fp: it records the target and queues a source re-scan of the document
// pipelines and views in rescan, the ones declaring metadata (embed jobs,
// which reuse stored vectors; for a view, state jobs for its tables listed
// in views, then a render job). The full-text column needs no re-scan: the
// search builder fills it from the stored content.
//
// A job already pending or running for the same pipeline would otherwise
// absorb the re-scan and resume from its cursor, leaving the rows behind it
// with old metadata. With restart (only while no backfill runs, at startup)
// such a job starts over from the beginning; without it nothing is queued
// and (false, nil) is returned so the caller retries once the job is done.
// Returns false as well if fp is already built or being built.
func (s *Store) QueueSearchRescan(ctx context.Context, g *Generation, fp string, rescan map[string]bool, views map[string][]string, restart bool) (bool, error) {
	if g.SearchFingerprint == fp || g.SearchTarget == fp {
		return false, nil
	}
	errBusy := errors.New("busy")
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+s.T("generations")+` SET search_target = $2 WHERE id = $1`, g.ID, fp); err != nil {
			return err
		}
		onConflict := `DO NOTHING`
		if restart {
			onConflict = `DO UPDATE SET last_key = NULL, updated_at = now()`
		}
		queue := func(q string, args ...any) error {
			tag, err := tx.Exec(ctx, q+` `+onConflict, args...)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return errBusy
			}
			return nil
		}
		for _, p := range g.PipelineNames() {
			if !rescan[p] {
				continue
			}
			kind := g.Pipelines[p].jobKind()
			for _, c := range views[p] {
				if err := queue(`INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES (NULL, $1, 'state', 'pending')
					ON CONFLICT (pipeline) WHERE kind = 'state' AND state IN ('pending','running')`, c); err != nil {
					return err
				}
			}
			if err := queue(`INSERT INTO `+s.T("backfill_jobs")+` (generation_id, pipeline, kind, state) VALUES ($1, $2, $3, 'pending')
				ON CONFLICT (generation_id, pipeline) WHERE state IN ('pending','running')`, g.ID, p, kind); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, errBusy) {
		return false, nil
	}
	return err == nil, err
}

// FillSearchText brings a generation's full-text column (and the metadata of
// pipelines that declare none, which must be empty) up to date for the next
// limit rows after the cursor (pipeline, source_key, chunk_no), in primary
// key order. It computes from each row's own stored content and changes
// nothing else. Rows locked by a concurrent writer are skipped, never waited
// for (so it cannot deadlock with the committer, which writes those rows'
// columns itself); skipped reports how many, and a pass that skipped rows
// must be repeated before the column can be called complete. last is nil
// when the table is exhausted.
func (s *Store) FillSearchText(ctx context.Context, g *Generation, textConfig string, noMeta []string, after []string, limit int) (last []string, changed, skipped int64, err error) {
	tbl := pgx.Identifier{s.Schema, g.ChunkTable}.Sanitize()
	if after == nil {
		after = []string{"", "", "-1"}
	}
	var p, k string
	var n, total int32
	err = s.Pool.QueryRow(ctx, `SELECT pipeline, source_key, chunk_no, count(*) OVER () FROM (SELECT pipeline, source_key, chunk_no FROM `+tbl+`
		WHERE (pipeline, source_key, chunk_no) > ($1, $2, $3::int) ORDER BY pipeline, source_key, chunk_no LIMIT $4) b
		ORDER BY pipeline DESC, source_key DESC, chunk_no DESC LIMIT 1`, after[0], after[1], after[2], limit).Scan(&p, &k, &n, &total)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, 0, nil
	}
	if err != nil {
		return nil, 0, 0, err
	}
	last = []string{p, k, strconv.Itoa(int(n))}
	var locked int64
	err = s.Pool.QueryRow(ctx, `WITH l AS (
			SELECT pipeline, source_key, chunk_no FROM `+tbl+`
			WHERE (pipeline, source_key, chunk_no) > ($1, $2, $3::int) AND (pipeline, source_key, chunk_no) <= ($4, $5, $6::int)
			ORDER BY pipeline, source_key, chunk_no FOR UPDATE SKIP LOCKED),
		u AS (
			UPDATE `+tbl+` t SET tsv = to_tsvector($7::regconfig, t.content),
				meta = CASE WHEN t.pipeline = ANY($8::text[]) THEN '{}'::jsonb ELSE t.meta END
			FROM l WHERE t.pipeline = l.pipeline AND t.source_key = l.source_key AND t.chunk_no = l.chunk_no
			  AND (t.tsv IS DISTINCT FROM to_tsvector($7::regconfig, t.content) OR (t.pipeline = ANY($8::text[]) AND t.meta <> '{}'::jsonb))
			RETURNING 1)
		SELECT (SELECT count(*) FROM l), (SELECT count(*) FROM u)`,
		after[0], after[1], after[2], last[0], last[1], last[2], textConfig, noMeta).Scan(&locked, &changed)
	if err != nil {
		return nil, 0, 0, err
	}
	return last, changed, int64(total) - locked, nil
}

// CompleteSearch marks a generation's search columns complete for fp once
// its full-text pass is done (the caller's part) and no backfill job that
// could still write metadata remains. It reports whether it did.
func (s *Store) CompleteSearch(ctx context.Context, genID, fp string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE `+s.T("generations")+` g SET search_fingerprint = $2
		WHERE id = $1 AND search_target = $2 AND search_fingerprint IS DISTINCT FROM $2
		AND state IN ('building', 'ready', 'active', 'previous')
		AND NOT EXISTS (SELECT 1 FROM `+s.T("backfill_jobs")+` j WHERE j.state IN ('pending', 'running') AND (j.generation_id = $1 OR j.generation_id IS NULL))`, genID, fp)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
