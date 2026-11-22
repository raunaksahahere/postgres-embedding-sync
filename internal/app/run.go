package app

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/filter"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// ErrSlotLost means the source no longer retains the WAL this sink needs.
var ErrSlotLost = errors.New("replication slot lost or invalidated")

// Catalog is what startup learns about one pipeline's source table.
type Catalog struct {
	Codec     keys.Codec
	Signature string
}

// Catalogs holds each pipeline's catalog; SchemaErrs holds pipelines whose
// table no longer matches the configured mapping.
type Catalogs struct {
	Pipes      map[string]*Catalog
	SchemaErrs map[string]*SchemaError
}

// loadPipeCatalog validates one pipeline's table against its configuration.
func loadPipeCatalog(ctx context.Context, e *Env, p *config.PipelineDef) (*Catalog, error) {
	schema, table := p.SchemaTable()
	reg := pgx.Identifier{schema, table}.Sanitize()
	types := map[string]uint32{}
	rows, err := e.Source.Query(ctx, `SELECT attname, atttypid FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, reg)
	if err != nil {
		return nil, fmt.Errorf("pipeline %s: read source columns of %s: %w", p.Name, p.Table, err)
	}
	for rows.Next() {
		var n string
		var t uint32
		if err := rows.Scan(&n, &t); err != nil {
			rows.Close()
			return nil, err
		}
		types[n] = t
	}
	rows.Close()
	var pk []string
	var ident string
	if err := e.Source.QueryRow(ctx, `SELECT coalesce(array_agg(a.attname ORDER BY a.attnum) FILTER (WHERE a.attname IS NOT NULL), '{}'), c.relreplident::text
		FROM pg_class c LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
		LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
		WHERE c.oid = $1::regclass GROUP BY c.relreplident`, reg).Scan(&pk, &ident); err != nil {
		return nil, fmt.Errorf("pipeline %s: read primary key: %w", p.Name, err)
	}
	if !sameSet(pk, p.Key) {
		return nil, schemaErr(p.Name, "pipeline %s: source table %s must have the primary key %v (found %v)", p.Name, p.Table, []string(p.Key), pk)
	}
	if ident != "d" && ident != "f" {
		return nil, fmt.Errorf("pipeline %s: source table %s replica identity must be DEFAULT (primary key) or FULL", p.Name, p.Table)
	}
	var kinds []keys.Kind
	for _, k := range p.Key {
		kk, err := keys.FromTypeOID(types[k])
		if err != nil {
			return nil, fmt.Errorf("pipeline %s: key column %s: %w", p.Name, k, err)
		}
		kinds = append(kinds, kk)
	}
	for _, c := range p.FieldColumns() {
		if _, ok := types[c]; !ok {
			return nil, schemaErr(p.Name, "pipeline %s: allowlisted column %q does not exist on %s", p.Name, c, p.Table)
		}
	}
	for _, m := range p.Metadata {
		oid, ok := types[m.Column]
		if !ok {
			return nil, schemaErr(p.Name, "pipeline %s: metadata column %q does not exist on %s", p.Name, m.Column, p.Table)
		}
		if err := metaCompatible(filter.Type(m.Type), oid); err != nil {
			return nil, fmt.Errorf("pipeline %s: metadata field %s (column %s): %w", p.Name, m.Name, m.Column, err)
		}
	}
	sig := pipeline.RelationSignature(p, func(col string) (uint32, bool) { t, ok := types[col]; return t, ok })
	return &Catalog{Codec: keys.NewCodec(kinds...), Signature: sig}, nil
}

// metaCompatible reports whether a column of type oid can carry a metadata
// field of type t: text takes any column (its text form is stored as is),
// float also takes integer columns, other types must match.
func metaCompatible(t filter.Type, oid uint32) error {
	if t == filter.Text {
		return nil
	}
	have, err := filter.TypeForOID(oid)
	if err != nil {
		return err
	}
	if have == t || (t == filter.Float && have == filter.Int) {
		return nil
	}
	return fmt.Errorf("declared %s but the column is %s", t, have)
}

func sameSet(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	have := map[string]bool{}
	for _, x := range a {
		have[x] = true
	}
	for _, x := range b {
		if !have[x] {
			return false
		}
	}
	return true
}

// LoadCatalog validates every pipeline's table and the shared publication
// against the config. Per-pipeline schema mismatches are returned in
// SchemaErrs (the stream may not have reached them yet); anything else is an
// error.
func LoadCatalog(ctx context.Context, e *Env) (*Catalogs, error) {
	cfg := e.Cfg
	out := &Catalogs{Pipes: map[string]*Catalog{}, SchemaErrs: map[string]*SchemaError{}}
	for _, p := range cfg.TableDefs() {
		c, err := loadPipeCatalog(ctx, e, p)
		var se *SchemaError
		switch {
		case errors.As(err, &se):
			out.SchemaErrs[p.Name] = se
		case err != nil:
			return nil, err
		default:
			out.Pipes[p.Name] = c
		}
	}

	var ins, upd, del, trunc bool
	if err := e.Source.QueryRow(ctx, `SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1`, cfg.Source.Publication).
		Scan(&ins, &upd, &del, &trunc); err != nil {
		return nil, fmt.Errorf("publication %q not found (run `embedsync init`): %w", cfg.Source.Publication, err)
	}
	if !ins || !upd || !del || !trunc {
		return nil, fmt.Errorf("publication %q must publish insert, update, delete and truncate", cfg.Source.Publication)
	}
	prow, err := e.Source.Query(ctx, `SELECT schemaname, tablename, attnames FROM pg_publication_tables WHERE pubname = $1`, cfg.Source.Publication)
	if err != nil {
		return nil, err
	}
	defer prow.Close()
	byTable := map[string]*config.PipelineDef{}
	for _, d := range cfg.TableDefs() {
		byTable[d.Table] = d
	}
	seen := map[string]bool{}
	sawMarkers := false
	for prow.Next() {
		var s, t string
		var cols []string
		if err := prow.Scan(&s, &t, &cols); err != nil {
			return nil, err
		}
		if s == cfg.Source.ControlSchema && t == "markers" {
			sawMarkers = true
			continue
		}
		p := byTable[s+"."+t]
		if p == nil {
			return nil, fmt.Errorf("publication %q includes %s.%s, which no pipeline names; it must contain only the pipelines' tables and the marker table: if that table was ever synced, restore its pipeline in the config; otherwise remove it with ALTER PUBLICATION %s DROP TABLE %s", cfg.Source.Publication, s, t, pgx.Identifier{cfg.Source.Publication}.Sanitize(), pgx.Identifier{s, t}.Sanitize())
		}
		seen[p.Name] = true
		have := map[string]bool{}
		for _, c := range cols {
			have[c] = true
		}
		for _, c := range p.PublishedColumns() {
			if !have[c] {
				if _, schemaPending := out.SchemaErrs[p.Name]; schemaPending {
					continue // reported by the schema check of this pipeline
				}
				return nil, schemaErr(p.Name, "publication %q does not publish column %q of %s (run `embedsync init` after changing fields)", cfg.Source.Publication, c, p.Table)
			}
		}
		if len(cols) > len(p.PublishedColumns()) {
			e.Log.Warn("publication publishes columns outside the allowlist; consider a column list", "publication", cfg.Source.Publication, "table", p.Table)
		}
	}
	if err := prow.Err(); err != nil {
		return nil, err
	}
	if !sawMarkers {
		return nil, fmt.Errorf("publication %q must include %s.markers", cfg.Source.Publication, cfg.Source.ControlSchema)
	}
	for _, p := range cfg.TableDefs() {
		if !seen[p.Name] {
			return nil, fmt.Errorf("publication %q does not include %s (pipeline %s); run `embedsync init` to add it", cfg.Source.Publication, p.Table, p.Name)
		}
	}
	return out, nil
}

// SlotInfo is a row of pg_replication_slots.
type SlotInfo struct {
	Exists       bool
	Plugin       string
	Database     string
	Active       bool
	WALStatus    string
	ConfirmedLSN *pglogrepl.LSN
	RetainedB    int64
	ConfirmedLag int64
	SafeWALSize  *int64
}

// LoadSlot reads the slot's state.
func LoadSlot(ctx context.Context, e *Env) (*SlotInfo, error) {
	var s SlotInfo
	var confirmed *string
	var walStatus, plugin, db *string
	var retained, lag *int64
	err := e.Source.QueryRow(ctx, `SELECT plugin, database, active, wal_status, confirmed_flush_lsn::text,
		pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, safe_wal_size
		FROM pg_replication_slots WHERE slot_name = $1`, e.Cfg.Source.Slot).Scan(&plugin, &db, &s.Active, &walStatus, &confirmed, &retained, &lag, &s.SafeWALSize)
	if errors.Is(err, pgx.ErrNoRows) {
		return &s, nil
	}
	if err != nil {
		return nil, err
	}
	s.Exists = true
	if plugin != nil {
		s.Plugin = *plugin
	}
	if db != nil {
		s.Database = *db
	}
	if walStatus != nil {
		s.WALStatus = *walStatus
	}
	if confirmed != nil {
		l, _ := pglogrepl.ParseLSN(*confirmed)
		s.ConfirmedLSN = &l
	}
	if retained != nil {
		s.RetainedB = *retained
	}
	if lag != nil {
		s.ConfirmedLag = *lag
	}
	return &s, nil
}

func replicationDSN(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		q := u.Query()
		q.Set("replication", "database")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return dsn + " replication=database", nil
}

// RunOptions are flags for `embedsync run`.
type RunOptions struct {
	AcceptSchemaChange bool
}

// atSchemaPause reports whether the stream paused on an incompatible relation
// of pipeline at the current checkpoint (nothing after it has been
// acknowledged since). A pause recorded before pipelines existed belongs to
// the single table, i.e. the "default" pipeline.
func atSchemaPause(cp *store.Checkpoint, pipe string) bool {
	if cp.SchemaPauseLSN == nil || *cp.SchemaPauseLSN != cp.LSN {
		return false
	}
	paused := cp.SchemaPausePipeline
	if paused == "" {
		paused = config.DefaultPipeline
	}
	return paused == pipe
}

// configMatchesSignature reports whether an accepted relation signature
// ("col:typeoid,...") covers exactly the pipeline's key and field columns.
func configMatchesSignature(p *config.PipelineDef, sig string) bool {
	want := append(append([]string{}, p.Key...), p.FieldColumns()...)
	parts := strings.Split(sig, ",")
	if len(parts) != len(want) {
		return false
	}
	have := map[string]bool{}
	for _, part := range parts {
		name, typ, ok := strings.Cut(part, ":")
		if !ok || typ == "missing" {
			return false
		}
		have[name] = true
	}
	for _, c := range want {
		if !have[c] {
			return false
		}
	}
	return true
}

// SchemaError is a source schema that no longer matches the configured
// mapping (a renamed or dropped allowlisted column, a changed key). It pauses
// with the same reason as an incompatible change seen in the stream.
type SchemaError struct {
	Pipeline string
	msg      string
}

func (e *SchemaError) Error() string { return e.msg }

func schemaErr(pipe, format string, a ...any) error {
	return &SchemaError{Pipeline: pipe, msg: fmt.Sprintf(format, a...)}
}

// resolveRelations decides, per pipeline, which relation shape and key codec
// the stream is consumed with: the accepted one, an operator-accepted new
// one (only at a schema pause of that pipeline), or the catalog's for a
// pipeline seen for the first time. New pipelines are returned in fresh and
// operator-accepted shape changes in accepts; the caller persists both only
// after every startup check has passed.
func resolveRelations(e *Env, cp *store.Checkpoint, rels map[string]*store.Relation, cats *Catalogs,
	opts RunOptions) (accepted map[string]*Catalog, fresh, accepts []store.Relation, err error) {
	cfg := e.Cfg
	accepted = map[string]*Catalog{}
	for _, p := range cfg.TableDefs() {
		rel := rels[p.Name]
		cat, se := cats.Pipes[p.Name], cats.SchemaErrs[p.Name]
		if rel == nil {
			if se != nil {
				return nil, nil, nil, &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: p.Name, Detail: se.Error()}
			}
			accepted[p.Name] = cat
			fresh = append(fresh, store.Relation{Pipeline: p.Name, SourceTable: p.Table, KeyColumns: p.Key, KeyKinds: cat.Codec.String(), RelationSignature: cat.Signature})
			continue
		}
		if rel.SourceTable != p.Table || strings.Join(rel.KeyColumns, ",") != strings.Join(p.Key, ",") {
			return nil, nil, nil, &pipeline.PauseError{Reason: "identity_mismatch", Pipeline: p.Name, Detail: fmt.Sprintf(
				"pipeline %s was registered for %s key %v but the config now says %s key %v; a pipeline's table and key cannot change (use a new pipeline name, or rebuild)",
				p.Name, rel.SourceTable, rel.KeyColumns, p.Table, []string(p.Key))}
		}
		codec, perr := keys.ParseCodec(rel.KeyKinds)
		if perr != nil {
			return nil, nil, nil, perr
		}
		paused := cp != nil && atSchemaPause(cp, p.Name)
		if se != nil {
			// The source changed but the stream may not have reached the
			// change yet. If the configuration still matches the accepted
			// shape, keep streaming with it: the reader pauses exactly at the
			// change.
			if !configMatchesSignature(p, rel.RelationSignature) || paused {
				return nil, nil, nil, &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: p.Name, Detail: se.Error() +
					"; update the field mapping if a column was renamed, then restart with --accept-schema-change"}
			}
			e.Log.Warn("source table no longer matches the field mapping; streaming with the accepted mapping until the change is reached", "pipeline", p.Name, "detail", se.Error())
			accepted[p.Name] = &Catalog{Codec: codec, Signature: rel.RelationSignature}
			continue
		}
		if cat.Codec.String() != rel.KeyKinds {
			return nil, nil, nil, &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: p.Name, Detail: fmt.Sprintf(
				"pipeline %s: primary key type changed from %s to %s; rebuild required", p.Name, rel.KeyKinds, cat.Codec.String())}
		}
		if rel.RelationSignature == cat.Signature {
			accepted[p.Name] = cat
			continue
		}
		switch {
		case paused && opts.AcceptSchemaChange:
			// The stream stopped at the change and nothing after it was
			// acknowledged: accept the new shape from exactly here.
			accepts = append(accepts, store.Relation{Pipeline: p.Name, RelationSignature: cat.Signature})
			e.Log.Warn("accepting source schema change", "pipeline", p.Name, "from", rel.RelationSignature, "to", cat.Signature)
			accepted[p.Name] = cat
		case paused:
			return nil, nil, nil, &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: p.Name, Detail: fmt.Sprintf(
				"pipeline %s: source table shape changed from %q to %q; review it, update field mappings if needed, then restart with --accept-schema-change", p.Name, rel.RelationSignature, cat.Signature)}
		case configMatchesSignature(p, rel.RelationSignature):
			// The catalog changed ahead of the stream: keep the accepted
			// shape; the reader pauses when it reaches the change.
			if opts.AcceptSchemaChange {
				e.Log.Warn("--accept-schema-change ignored: the stream has not reached the schema change yet", "pipeline", p.Name)
			}
			e.Log.Warn("source table shape changed ahead of the stream; streaming with the accepted shape until the change is reached",
				"pipeline", p.Name, "accepted", rel.RelationSignature, "catalog", cat.Signature)
			accepted[p.Name] = &Catalog{Codec: codec, Signature: rel.RelationSignature}
		default:
			return nil, nil, nil, &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: p.Name, Detail: fmt.Sprintf(
				"pipeline %s: the field mapping no longer matches the accepted shape %q, but the stream has not reached the schema change yet; restore the previous mapping, let the pipeline pause at the change, then switch the mapping and restart with --accept-schema-change", p.Name, rel.RelationSignature)}
		}
	}
	for name := range rels {
		if !tableConfigured(cfg, name) {
			return nil, nil, nil, &pipeline.PauseError{Reason: "startup_check", Detail: fmt.Sprintf(
				"pipeline %q is registered in this sink but no longer configured; restore it (removing a pipeline is not supported in place)", name)}
		}
	}
	return accepted, fresh, accepts, nil
}

// pipelineRun is one attempt of the streaming pipeline with fresh connections.
func pipelineRun(ctx context.Context, e *Env, opts RunOptions, state *runState) error {
	cfg := e.Cfg
	lockConn, err := e.Store.TryAdvisoryLock(ctx)
	if err != nil {
		return err
	}
	defer func() {
		// The lock is session-level: release it before the connection goes
		// back to the pool, or a restart could fail to take it again.
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock_all()`)
		lockConn.Release()
	}()

	cats, err := LoadCatalog(ctx, e)
	if err != nil {
		var se *SchemaError
		if errors.As(err, &se) {
			return &pipeline.PauseError{Reason: "schema_incompatible", Pipeline: se.Pipeline, Detail: se.Error()}
		}
		return &pipeline.PauseError{Reason: "startup_check", Detail: err.Error()}
	}
	replDSNRaw, err := config.Env(cfg.Source.ReplDSNEnv)
	if err != nil {
		return err
	}
	replDSN, err := replicationDSN(replDSNRaw)
	if err != nil {
		return err
	}
	conn, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer conn.Close(context.Background())
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}

	cp, err := e.Store.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}
	rels, err := e.Store.Relations(ctx)
	if err != nil {
		return err
	}
	if cp == nil {
		// A fresh slot starts every pipeline from the catalog's shape.
		rels = map[string]*store.Relation{}
	}
	slot, err := LoadSlot(ctx, e)
	if err != nil {
		return err
	}
	// Identity first: an accepted schema change is only recorded against the
	// slot and source this checkpoint belongs to.
	if cp != nil {
		if err := checkIdentity(cfg, cp, sys, slot); err != nil {
			return err
		}
	}
	accepted, fresh, accepts, err := resolveRelations(e, cp, rels, cats, opts)
	if err != nil {
		return err
	}
	// Every check that can pause runs before anything is persisted for new
	// pipelines (relations, generation coverage, backfill jobs): a paused
	// start must leave the sink as it was, so reverting the config works.
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	if err := checkGenerations(cfg, gens); err != nil {
		return err
	}
	if cp == nil {
		if slot.Exists {
			return &pipeline.PauseError{Reason: "startup_check", Detail: fmt.Sprintf(
				"slot %q exists but this sink has no checkpoint for it; drop the slot or run `embedsync rebuild --confirm`", cfg.Source.Slot)}
		}
		res, err := pglogrepl.CreateReplicationSlot(ctx, conn, cfg.Source.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
			Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
		if err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
		if err != nil {
			return err
		}
		cp = &store.Checkpoint{SystemIdentifier: sys.SystemID, Timeline: sys.Timeline, Database: sys.DBName, SlotName: cfg.Source.Slot,
			Publication: cfg.Source.Publication, SlotCreatedLSN: lsn, LSN: lsn}
		// Relations belong to the slot they were accepted on.
		if _, err := e.Store.Pool.Exec(ctx, `DELETE FROM `+e.Store.T("pipeline_relations")); err != nil {
			return err
		}
		if err := e.Store.InsertCheckpoint(ctx, *cp); err != nil {
			return fmt.Errorf("insert checkpoint: %w", err)
		}
		e.Log.Info("created replication slot", "slot", cfg.Source.Slot, "consistent_point", lsn.String())
	}
	for _, r := range accepts {
		// The stream stopped at the change and nothing after it was
		// acknowledged: the new shape is accepted from exactly here.
		if err := e.Store.UpdateRelationSignature(ctx, r.Pipeline, r.RelationSignature); err != nil {
			return err
		}
	}
	for _, r := range fresh {
		if err := e.Store.InsertRelation(ctx, r); err != nil {
			return fmt.Errorf("register pipeline %s: %w", r.Pipeline, err)
		}
		e.Log.Info("registered pipeline", "pipeline", r.Pipeline, "table", r.SourceTable, "key", r.KeyColumns, "key_kinds", r.KeyKinds)
		if spec := e.Pipes[r.Pipeline]; spec != nil && spec.IsComponent() {
			// A view's table enters state through a state backfill; the
			// view renders once every one of its tables is complete.
			id, err := e.Store.RequestStateBackfill(ctx, r.Pipeline)
			if err != nil {
				return fmt.Errorf("queue state backfill of %s: %w", r.Pipeline, err)
			}
			e.Log.Info("state backfill queued", "component", r.Pipeline, "job", id)
		}
	}
	acceptedSig := map[string]string{}
	viewRels := map[string]viewdoc.Relation{}
	for name, c := range accepted {
		e.Pipes[name].Codec = c.Codec
		acceptedSig[name] = c.Signature
		viewRels[name] = viewdoc.Relation{KeyKinds: c.Codec.String(), Signature: c.Signature}
	}
	for _, v := range e.Pipes.Views() {
		vs, err := viewdoc.Build(v.ViewDef, viewRels)
		if err != nil {
			return &pipeline.PauseError{Reason: "startup_check", Detail: err.Error()}
		}
		v.View, v.Codec = vs, vs.Root.Codec
	}
	if err := ensureGenerations(ctx, e); err != nil {
		return err
	}
	warnMovedEndpoints(e, gens)
	if err := queueSearchRescans(ctx, e); err != nil {
		return err
	}
	if gens, err = e.Store.Generations(ctx); err != nil {
		return err
	}
	live := map[string]*pipeline.GenInfo{}
	for _, g := range gens {
		if store.IsLive(g.State) && g.StreamFromLSN != nil {
			live[g.ID] = &pipeline.GenInfo{Gen: g, Model: ModelIdentity(g)}
		}
	}
	activeJobs := map[int64]*pipeline.JobCursor{}
	jobs, err := e.Store.ActiveBackfillJobs(ctx, "")
	if err != nil {
		return err
	}
	for _, j := range jobs {
		activeJobs[j.ID] = &pipeline.JobCursor{Pipeline: j.Pipeline, Last: j.LastKey, Kind: j.Kind}
	}

	if err := e.Store.SetPipelineStatus(ctx, "running", "", ""); err != nil {
		return err
	}
	metrics.SetConsumerState("starting")
	progress := &pipeline.Progress{}
	progress.Init(cp.LSN)
	metrics.CheckpointLSN.Set(float64(cp.LSN))
	state.setProgress(progress)

	registry := pipeline.NewRegistry()
	items := make(chan pipeline.Item, cfg.Pipeline.TxQueue)
	units := make(chan *pipeline.Unit, cfg.Pipeline.MaxInflightUnits)
	commits := make(chan *pipeline.Unit, cfg.Pipeline.MaxInflightUnits)

	planner := &pipeline.Planner{Cfg: cfg, Specs: e.Pipes, Registry: registry, Log: e.Log, Live: live, ActiveJob: activeJobs, Jobs: e.Store,
		Lookup: func(ctx context.Context, pipe string, ks []string) (map[string]store.SourceState, error) {
			return e.Store.LoadSourceState(ctx, e.Store.Pool, pipe, ks)
		},
		RefLookup: func(ctx context.Context, pipe, ref string, parents []string) (map[string][]string, error) {
			return e.Store.Children(ctx, e.Store.Pool, pipe, ref, parents)
		},
		RefKeys:  e.Store.RefKeys,
		RootKeys: e.Store.KeysAfter,
		LoadGen:  e.Store.Generation, ModelOf: ModelIdentity}
	embed := pipeline.NewEmbedStage(e.Providers, e.Store, e.Pipes, cfg.Embedder.BatchSize, e.Log)
	committer := &pipeline.Committer{Cfg: cfg, Store: e.Store, Specs: e.Pipes, Progress: progress, Planner: planner, Log: e.Log}
	reader := &pipeline.Reader{Conn: conn, Cfg: cfg, Specs: e.Pipes, Progress: progress, Out: items, StartLSN: cp.LSN, AcceptedSig: acceptedSig, Log: e.Log}
	bf := &pipeline.Backfill{Cfg: cfg, Specs: e.Pipes, Source: e.Source, Store: e.Store, Registry: registry, Log: e.Log}
	// readerCtx stops intake on shutdown; workCtx lets in-flight units finish.
	readerCtx, stopReader := context.WithCancel(context.Background())
	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopReader()
	defer stopWork()

	errs := make(chan error, 5)
	var wg sync.WaitGroup
	start := func(name string, f func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := f()
			if err != nil && !errors.Is(err, context.Canceled) {
				e.Log.Debug("stage exited", "stage", name, "err", err)
			}
			errs <- tagErr(name, err)
		}()
	}
	start("reader", func() error { defer close(items); return reader.Run(readerCtx) })
	start("planner", func() error { return planner.Run(workCtx, items, units) })
	start("embed", func() error { return embed.Run(workCtx, units, commits) })
	start("committer", func() error {
		err := committer.Run(workCtx, commits)
		// Nothing can be committed after this point: stop intake and release
		// upstream stages immediately. Uncommitted work is replayed on restart.
		stopReader()
		stopWork()
		return err
	})
	bfCtx, stopBF := context.WithCancel(workCtx)
	defer stopBF()
	go func() { _ = bf.Run(bfCtx) }()
	sb := &searchBuilder{e: e}
	go sb.run(bfCtx)

	var first error
	shutdown := func() {
		stopReader()
		stopBF()
		t := time.AfterFunc(cfg.Pipeline.ShutdownTimeout.Duration, stopWork)
		defer t.Stop()
		wg.Wait()
	}
	select {
	case <-ctx.Done():
		e.Log.Info("shutting down: draining in-flight units", "timeout", cfg.Pipeline.ShutdownTimeout.Duration)
		shutdown()
		return nil
	case first = <-errs:
	}
	// A stage stopped: drain the rest, then prefer the most specific error.
	shutdown()
	close(errs)
	all := []error{first}
	for err := range errs {
		all = append(all, err)
	}
	return pickRunError(all)
}

// pickRunError chooses what one pipeline attempt reports: a pause if any
// stage paused, otherwise the first real error. The planner and the
// committer return the same pause; only the committer's return proves that
// every unit before the paused one is durable, i.e. that the checkpoint is
// exactly at the change. If the committer did not get there (for example it
// was cancelled by the shutdown timeout), the pipeline still pauses, but the
// pause is not recorded as a stream position: the next start replays up to
// the change and records it there.
func pickRunError(all []error) error {
	var pause *pipeline.PauseError
	committerPaused := false
	for _, err := range all {
		if pe, ok := pipeline.IsPause(err); ok {
			pause = pe
			var se *stageErr
			if errors.As(err, &se) && se.stage == "committer" {
				committerPaused = true
			}
		}
	}
	if pause != nil {
		if pause.InStream && !committerPaused {
			c := *pause
			c.InStream = false
			return &c
		}
		return pause
	}
	var first error
	for _, err := range all {
		if first == nil || errors.Is(first, context.Canceled) {
			first = err
		}
	}
	return first
}

type stageErr struct {
	stage string
	err   error
}

func (s *stageErr) Error() string { return s.stage + ": " + s.err.Error() }
func (s *stageErr) Unwrap() error { return s.err }

func tagErr(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &stageErr{stage, err}
}

func checkIdentity(cfg *config.Config, cp *store.Checkpoint, sys pglogrepl.IdentifySystemResult, slot *SlotInfo) error {
	switch {
	case cp.SystemIdentifier != sys.SystemID:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: fmt.Sprintf("checkpoint belongs to source system %s but connected to %s", cp.SystemIdentifier, sys.SystemID)}
	case cp.Database != sys.DBName:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: fmt.Sprintf("checkpoint database %s != %s", cp.Database, sys.DBName)}
	case cp.SlotName != cfg.Source.Slot || cp.Publication != cfg.Source.Publication:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: "slot or publication differ from the checkpoint; use a new sink schema or rebuild"}
	case !slot.Exists:
		return &pipeline.PauseError{Reason: "slot_lost", Detail: fmt.Sprintf("%v: slot %q no longer exists; run `embedsync rebuild --confirm` (see docs/operations.md)", ErrSlotLost, cfg.Source.Slot)}
	case slot.WALStatus == "lost":
		return &pipeline.PauseError{Reason: "slot_lost", Detail: fmt.Sprintf("%v: slot %q was invalidated (wal_status=lost); run `embedsync rebuild --confirm`", ErrSlotLost, cfg.Source.Slot)}
	case slot.Plugin != "pgoutput" || slot.Database != sys.DBName:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: "slot plugin or database differ"}
	case slot.ConfirmedLSN != nil && *slot.ConfirmedLSN > cp.LSN:
		return &pipeline.PauseError{Reason: "slot_ahead", Detail: fmt.Sprintf(
			"slot confirmed_flush_lsn %s is ahead of checkpoint %s: changes in between were acknowledged without being applied here (slot recreated or shared?); rebuild required", slot.ConfirmedLSN, cp.LSN)}
	case slot.ConfirmedLSN != nil && *slot.ConfirmedLSN < cp.SlotCreatedLSN:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: "slot is older than the checkpoint's slot; refusing to adopt it"}
	}
	return nil
}

// tableConfigured reports whether a registered relation (pipeline or view
// component) is still configured.
func tableConfigured(cfg *config.Config, name string) bool {
	for _, d := range cfg.TableDefs() {
		if d.Name == name {
			return true
		}
	}
	return false
}

// checkGenerations verifies that every live or pending generation was built
// with a configured model and with the configured transform of every
// pipeline it covers (pipelines it does not cover yet are added later).
func checkGenerations(cfg *config.Config, gens []*store.Generation) error {
	fps := cfg.PipelineFingerprints()
	for _, g := range gens {
		if !store.IsLive(g.State) && g.State != store.StatePending {
			continue
		}
		m, ok := cfg.Models[g.ModelKey]
		if !ok {
			return &pipeline.PauseError{Reason: "startup_check", Detail: fmt.Sprintf("generation %s uses model key %q which is not in the config", g.ID, g.ModelKey)}
		}
		if err := g.MatchesModel(m, fps); err != nil {
			return &pipeline.PauseError{Reason: "startup_check", Detail: err.Error()}
		}
	}
	return nil
}

// ensureGenerations creates the initial generation and serving alias on first
// run, and extends every live or pending generation to pipelines added to the
// configuration (queueing one backfill job per new pipeline and generation).
func ensureGenerations(ctx context.Context, e *Env) error {
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	if len(gens) == 0 {
		m := e.Cfg.Models[e.Cfg.InitialGeneration]
		g, err := store.NewGeneration(e.Cfg.InitialGeneration, e.Cfg.InitialGeneration, m, e.Cfg)
		if err != nil {
			return err
		}
		if err := e.Store.CreateGeneration(ctx, g, e.Cfg.Sink.ReaderRole); err != nil {
			return err
		}
		e.Log.Info("created initial generation", "generation", g.ID, "model", m.Name, "revision", m.Revision, "pipelines", g.PipelineNames())
		gens = append(gens, g)
	}
	fps := e.Cfg.PipelineFingerprints()
	for _, g := range gens {
		if !store.IsLive(g.State) && g.State != store.StatePending {
			continue
		}
		miss := g.MissingPipelines(fps)
		if len(miss) == 0 {
			continue
		}
		add := map[string]store.GenPipeline{}
		for _, n := range miss {
			add[n] = store.GenPipeline{TransformFingerprint: fps[n], View: e.Cfg.View(n) != nil}
		}
		if err := e.Store.AddPipelines(ctx, g.ID, add); err != nil {
			return fmt.Errorf("extend generation %s: %w", g.ID, err)
		}
		e.Log.Info("extended generation to new pipelines; backfill queued", "generation", g.ID, "pipelines", miss)
	}
	sv, err := e.Store.Serving(ctx)
	if err != nil {
		return err
	}
	if sv == nil {
		return e.Store.InitServing(ctx, gens[0].ID)
	}
	return nil
}

// warnMovedEndpoints logs when a provider that built a live generation now
// points at another endpoint. Moving a server is legitimate, but vectors
// from two different deployments must be the same model for the generation
// to stay coherent; the operator is told rather than silently trusted.
func warnMovedEndpoints(e *Env, gens []*store.Generation) {
	for _, g := range gens {
		if !store.IsLive(g.State) || g.Provider.Endpoint == "" {
			continue
		}
		p, ok := e.Cfg.Providers[g.Provider.Name]
		if !ok {
			continue
		}
		now := store.NewProviderIdentity(e.Cfg, e.Cfg.Models[g.ModelKey]).Endpoint
		if now != "" && now != g.Provider.Endpoint {
			e.Log.Warn("embedding provider endpoint changed since the generation was built; make sure it serves the same model",
				"generation", g.ID, "provider", g.Provider.Name, "kind", p.Kind, "built_with", g.Provider.Endpoint, "now", now)
		}
	}
}

// searchRescanPlan is what a search column rebuild re-scans from the
// source: the document pipelines and views that declare metadata, and each
// view's component tables.
func searchRescanPlan(cfg *config.Config) (map[string]bool, map[string][]string) {
	rescan := map[string]bool{}
	for _, n := range cfg.DocNames() {
		rescan[n] = len(cfg.MetaFor(n)) > 0
	}
	views := map[string][]string{}
	for _, v := range cfg.Views {
		for _, c := range v.Components() {
			views[v.Name] = append(views[v.Name], c.Def.Name)
		}
	}
	return rescan, views
}

// queueSearchRescans starts building the search columns (metadata and
// full-text) of every live generation that does not have them for the
// current configuration: after the upgrade that adds them, and after a
// metadata declaration or the text search configuration changes. Only the
// pipelines and views that declare metadata are re-scanned from the source
// (reusing stored vectors, so nothing is re-embedded); the full-text index
// is built by the search builder, which also turns filtered and hybrid
// search on when everything is complete. It runs before
// the backfill runner starts, so an unfinished job of a re-scanned pipeline
// can safely start over.
func queueSearchRescans(ctx context.Context, e *Env) error {
	if _, err := e.Store.Pool.Exec(ctx, `SELECT $1::text::regconfig`, e.Cfg.Search.TextConfig); err != nil {
		return fmt.Errorf("search.text_config %q is not a text search configuration in the sink database: %w", e.Cfg.Search.TextConfig, err)
	}
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	fp := e.Cfg.SearchFingerprint()
	rescan, views := searchRescanPlan(e.Cfg)
	for _, g := range gens {
		if !store.IsLive(g.State) && g.State != store.StatePending {
			continue
		}
		queued, err := e.Store.QueueSearchRescan(ctx, g, fp, rescan, views, true)
		if err != nil {
			return fmt.Errorf("queue search re-scan of %s: %w", g.ID, err)
		}
		if queued {
			e.Log.Info("building search columns (metadata, full text) in the background; vector search is unaffected", "generation", g.ID)
		}
	}
	return nil
}

// searchBuilder completes the search columns of generations being built
// for the current configuration: it builds the full-text expression index
// concurrently (no row is rewritten, so the vector index is untouched),
// clears metadata left from removed declarations, and marks the generation
// complete once no backfill that could still write metadata remains. It
// also starts the build of a live generation built for another
// configuration (e.g. created by `migrate` with a different config file)
// once its own jobs are done.
type searchBuilder struct {
	e *Env
}

func (b *searchBuilder) run(ctx context.Context) {
	for {
		if err := b.step(ctx); err != nil && ctx.Err() == nil {
			b.e.Log.Warn("search builder: will retry", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (b *searchBuilder) step(ctx context.Context) error {
	gens, err := b.e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	fp := b.e.Cfg.SearchFingerprint()
	var noMeta []string
	for _, n := range b.e.Cfg.DocNames() {
		if len(b.e.Cfg.MetaFor(n)) == 0 {
			noMeta = append(noMeta, n)
		}
	}
	for _, g := range gens {
		if !store.IsLive(g.State) || g.SearchFingerprint == fp {
			continue
		}
		if g.SearchTarget != fp {
			rescan, views := searchRescanPlan(b.e.Cfg)
			queued, err := b.e.Store.QueueSearchRescan(ctx, g, fp, rescan, views, false)
			if err != nil {
				return fmt.Errorf("queue search re-scan of %s: %w", g.ID, err)
			}
			if queued {
				b.e.Log.Info("building search columns (metadata, full text) in the background; vector search is unaffected", "generation", g.ID)
			}
			continue // next step sees the new target
		}
		if err := b.e.Store.EnsureFTSIndex(ctx, g, b.e.Cfg.Search.TextConfig, b.e.Cfg.TextSearchExpr("content")); err != nil {
			return fmt.Errorf("generation %s: %w", g.ID, err)
		}
		for ctx.Err() == nil {
			n, more, err := b.e.Store.ClearStaleMeta(ctx, g, noMeta, 500)
			if err != nil {
				return fmt.Errorf("clear stale metadata of %s: %w", g.ID, err)
			}
			if !more {
				break
			}
			if n == 0 {
				return nil // rows held by a writer: retry next step
			}
		}
		done, err := b.e.Store.CompleteSearch(ctx, g.ID, fp)
		if err != nil {
			return err
		}
		if done {
			b.e.Log.Info("search columns ready: filtered, text and hybrid search enabled", "generation", g.ID)
		}
	}
	return nil
}
