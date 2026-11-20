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
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

// ErrSlotLost means the source no longer retains the WAL this sink needs.
var ErrSlotLost = errors.New("replication slot lost or invalidated")

// Catalog is what startup learns about the source table.
type Catalog struct {
	KeyKind   keys.Kind
	Signature string
}

// LoadCatalog validates the source table and publication against the config.
func LoadCatalog(ctx context.Context, e *Env) (*Catalog, error) {
	cfg := e.Cfg
	schema, table := cfg.SourceSchemaTable()
	reg := pgx.Identifier{schema, table}.Sanitize()
	types := map[string]uint32{}
	rows, err := e.Source.Query(ctx, `SELECT attname, atttypid FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, reg)
	if err != nil {
		return nil, fmt.Errorf("read source columns: %w", err)
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
		return nil, fmt.Errorf("read primary key: %w", err)
	}
	if len(pk) != 1 || pk[0] != cfg.Source.Key {
		return nil, schemaErr("source table %s must have a single-column primary key %q (found %v)", cfg.Source.Table, cfg.Source.Key, pk)
	}
	if ident != "d" && ident != "f" {
		return nil, fmt.Errorf("source table %s replica identity must be DEFAULT (primary key) or FULL", cfg.Source.Table)
	}
	kk, err := keys.FromTypeOID(types[cfg.Source.Key])
	if err != nil {
		return nil, err
	}
	for _, c := range cfg.FieldColumns() {
		if _, ok := types[c]; !ok {
			return nil, schemaErr("allowlisted column %q does not exist on %s", c, cfg.Source.Table)
		}
	}
	sig := pipeline.RelationSignature(cfg, func(col string) (uint32, bool) { t, ok := types[col]; return t, ok })

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
	sawSource, sawMarkers := false, false
	for prow.Next() {
		var s, t string
		var cols []string
		if err := prow.Scan(&s, &t, &cols); err != nil {
			return nil, err
		}
		switch {
		case s == schema && t == table:
			sawSource = true
			have := map[string]bool{}
			for _, c := range cols {
				have[c] = true
			}
			for _, c := range cfg.PublishedColumns() {
				if !have[c] {
					return nil, schemaErr("publication %q does not publish column %q", cfg.Source.Publication, c)
				}
			}
			if len(cols) > len(cfg.PublishedColumns()) {
				e.Log.Warn("publication publishes columns outside the allowlist; consider a column list", "publication", cfg.Source.Publication)
			}
		case s == cfg.Source.ControlSchema && t == "markers":
			sawMarkers = true
		default:
			return nil, fmt.Errorf("publication %q includes %s.%s; it must contain only the source table and the marker table", cfg.Source.Publication, s, t)
		}
	}
	if !sawSource || !sawMarkers {
		return nil, fmt.Errorf("publication %q must include %s and %s.markers", cfg.Source.Publication, cfg.Source.Table, cfg.Source.ControlSchema)
	}
	return &Catalog{KeyKind: kk, Signature: sig}, prow.Err()
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

// pipelineRun is one attempt of the streaming pipeline with fresh connections.
// atSchemaPause reports whether the stream paused on an incompatible relation
// at the current checkpoint (nothing after it has been acknowledged since).
func atSchemaPause(cp *store.Checkpoint) bool {
	return cp.SchemaPauseLSN != nil && *cp.SchemaPauseLSN == cp.LSN
}

// configMatchesSignature reports whether an accepted relation signature
// ("col:typeoid,...") covers exactly the configured key and field columns.
func configMatchesSignature(cfg *config.Config, sig string) bool {
	want := cfg.PublishedColumns()
	parts := strings.Split(sig, ",")
	if len(parts) != len(want) {
		return false
	}
	have := map[string]bool{}
	for _, p := range parts {
		name, typ, ok := strings.Cut(p, ":")
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
type SchemaError struct{ msg string }

func (e *SchemaError) Error() string { return e.msg }

func schemaErr(format string, a ...any) error { return &SchemaError{msg: fmt.Sprintf(format, a...)} }

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

	cp0, err := e.Store.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}
	cat, err := LoadCatalog(ctx, e)
	if err != nil {
		var se *SchemaError
		if !errors.As(err, &se) {
			return &pipeline.PauseError{Reason: "startup_check", Detail: err.Error()}
		}
		// The source changed but the stream may not have reached the change
		// yet. If the configuration still matches the accepted shape, keep
		// streaming with it: the reader pauses exactly at the change.
		if cp0 == nil || !configMatchesSignature(cfg, cp0.RelationSignature) || atSchemaPause(cp0) {
			return &pipeline.PauseError{Reason: "schema_incompatible", Detail: se.Error() +
				"; update the field mapping if a column was renamed, then restart with --accept-schema-change"}
		}
		kk, _ := keys.Parse(cp0.KeyKind)
		cat = &Catalog{KeyKind: kk, Signature: cp0.RelationSignature}
		e.Log.Warn("source table no longer matches the field mapping; streaming with the accepted mapping until the change is reached", "detail", se.Error())
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
	slot, err := LoadSlot(ctx, e)
	if err != nil {
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
			Publication: cfg.Source.Publication, SourceTable: cfg.Source.Table, SlotCreatedLSN: lsn, KeyKind: string(cat.KeyKind),
			RelationSignature: cat.Signature, LSN: lsn}
		if err := e.Store.InsertCheckpoint(ctx, *cp); err != nil {
			return fmt.Errorf("insert checkpoint: %w", err)
		}
		e.Log.Info("created replication slot", "slot", cfg.Source.Slot, "consistent_point", lsn.String())
	} else {
		if err := checkIdentity(cfg, cp, sys, slot, cat); err != nil {
			return err
		}
		if cp.RelationSignature != cat.Signature {
			switch {
			case atSchemaPause(cp) && opts.AcceptSchemaChange:
				// The stream stopped at the change and nothing after it was
				// acknowledged: accept the new shape from exactly here.
				if err := e.Store.UpdateRelationSignature(ctx, cat.Signature); err != nil {
					return err
				}
				e.Log.Warn("accepted source schema change", "from", cp.RelationSignature, "to", cat.Signature)
				cp.RelationSignature = cat.Signature
			case atSchemaPause(cp):
				return &pipeline.PauseError{Reason: "schema_incompatible", Detail: fmt.Sprintf(
					"source table shape changed from %q to %q; review it, update field mappings if needed, then restart with --accept-schema-change", cp.RelationSignature, cat.Signature)}
			case configMatchesSignature(cfg, cp.RelationSignature):
				// The catalog changed ahead of the stream: keep the accepted
				// shape; the reader pauses when it reaches the change.
				if opts.AcceptSchemaChange {
					e.Log.Warn("--accept-schema-change ignored: the stream has not reached the schema change yet")
				}
				e.Log.Warn("source table shape changed ahead of the stream; streaming with the accepted shape until the change is reached",
					"accepted", cp.RelationSignature, "catalog", cat.Signature)
				cat.Signature = cp.RelationSignature
			default:
				return &pipeline.PauseError{Reason: "schema_incompatible", Detail: fmt.Sprintf(
					"the field mapping no longer matches the accepted shape %q, but the stream has not reached the schema change yet; restore the previous mapping, let the pipeline pause at the change, then switch the mapping and restart with --accept-schema-change", cp.RelationSignature)}
			}
		}
	}
	if err := ensureGenerations(ctx, e); err != nil {
		return err
	}
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	live := map[string]*pipeline.GenInfo{}
	for _, g := range gens {
		if store.IsLive(g.State) || g.State == store.StatePending {
			m, ok := cfg.Models[g.ModelKey]
			if !ok {
				return &pipeline.PauseError{Reason: "startup_check", Detail: fmt.Sprintf("generation %s uses model key %q which is not in the config", g.ID, g.ModelKey)}
			}
			if err := g.MatchesModel(m, cfg.TransformFingerprint()); err != nil {
				return &pipeline.PauseError{Reason: "startup_check", Detail: err.Error()}
			}
		}
		if store.IsLive(g.State) && g.StreamFromLSN != nil {
			live[g.ID] = &pipeline.GenInfo{Gen: g, Model: ModelIdentity(g)}
		}
	}
	activeJobs := map[int64]*string{}
	jobs, err := e.Store.BackfillJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.State == "pending" || j.State == "running" {
			activeJobs[j.ID] = j.LastKey
		}
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

	planner := &pipeline.Planner{Cfg: cfg, KeyKind: cat.KeyKind, Tr: e.Tr, Registry: registry, Log: e.Log, Live: live, ActiveJob: activeJobs,
		Lookup: func(ctx context.Context, ks []string) (map[string]store.SourceState, error) {
			return e.Store.LoadSourceState(ctx, e.Store.Pool, ks)
		},
		LoadGen: e.Store.Generation, ModelOf: ModelIdentity}
	embed := pipeline.NewEmbedStage(e.Embed, e.Store, e.Tr, cfg.Embedder.BatchSize, cfg.Embedder.Concurrency, e.Log)
	committer := &pipeline.Committer{Cfg: cfg, Store: e.Store, KeyKind: cat.KeyKind, Progress: progress, Planner: planner, Log: e.Log}
	reader := &pipeline.Reader{Conn: conn, Cfg: cfg, Progress: progress, Out: items, StartLSN: cp.LSN, AcceptedSig: cp.RelationSignature, Log: e.Log}
	bf := &pipeline.Backfill{Cfg: cfg, KeyKind: cat.KeyKind, Source: e.Source, Store: e.Store, Registry: registry, Log: e.Log}

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
	for err := range errs {
		if _, ok := pipeline.IsPause(err); ok {
			first = err
		}
		if first == nil || errors.Is(first, context.Canceled) {
			first = err
		}
	}
	if _, ok := pipeline.IsPause(first); !ok {
		if pe, ok2 := firstPause(first); ok2 {
			first = pe
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

func firstPause(err error) (*pipeline.PauseError, bool) { return pipeline.IsPause(err) }

func checkIdentity(cfg *config.Config, cp *store.Checkpoint, sys pglogrepl.IdentifySystemResult, slot *SlotInfo, cat *Catalog) error {
	switch {
	case cp.SystemIdentifier != sys.SystemID:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: fmt.Sprintf("checkpoint belongs to source system %s but connected to %s", cp.SystemIdentifier, sys.SystemID)}
	case cp.Database != sys.DBName:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: fmt.Sprintf("checkpoint database %s != %s", cp.Database, sys.DBName)}
	case cp.SlotName != cfg.Source.Slot || cp.Publication != cfg.Source.Publication || cp.SourceTable != cfg.Source.Table:
		return &pipeline.PauseError{Reason: "identity_mismatch", Detail: "slot, publication or source table differ from the checkpoint; use a new sink schema or rebuild"}
	case cp.KeyKind != string(cat.KeyKind):
		return &pipeline.PauseError{Reason: "schema_incompatible", Detail: fmt.Sprintf("primary key type changed from %s to %s; rebuild required", cp.KeyKind, cat.KeyKind)}
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

// ensureGenerations creates the initial generation and serving alias on first run.
func ensureGenerations(ctx context.Context, e *Env) error {
	gens, err := e.Store.Generations(ctx)
	if err != nil {
		return err
	}
	if len(gens) == 0 {
		m := e.Cfg.Models[e.Cfg.InitialGeneration]
		g, err := store.NewGeneration(e.Cfg.InitialGeneration, e.Cfg.InitialGeneration, m, e.Cfg.TransformFingerprint())
		if err != nil {
			return err
		}
		if err := e.Store.CreateGeneration(ctx, g, e.Cfg.Sink.ReaderRole); err != nil {
			return err
		}
		e.Log.Info("created initial generation", "generation", g.ID, "model", m.Name, "revision", m.Revision)
		gens = append(gens, g)
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
