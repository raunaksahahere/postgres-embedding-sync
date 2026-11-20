package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
)

type relKind int

const (
	relOther relKind = iota
	relSource
	relMarkers
)

type relation struct {
	kind    relKind
	cols    []string
	keyIdx  int
	fieldAt map[string]int // logical field -> column index
	err     error
}

// Reader consumes pgoutput from a dedicated slot and assembles committed transactions.
type Reader struct {
	Conn        *pgconn.PgConn
	Cfg         *config.Config
	Progress    *Progress
	Out         chan<- Item
	StartLSN    LSN // checkpoint: transactions ending at or before it are already applied
	AcceptedSig string
	Log         *slog.Logger

	rels           map[uint32]*relation
	cur            *Txn
	lastIdleSent   time.Time
	nextStatus     time.Time
	srcNS, srcName string
}

// Run streams until ctx is cancelled or the connection fails.
func (r *Reader) Run(ctx context.Context) error {
	r.rels = map[uint32]*relation{}
	r.srcNS, r.srcName = r.Cfg.SourceSchemaTable()
	err := pglogrepl.StartReplication(ctx, r.Conn, r.Cfg.Source.Slot, r.StartLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + r.Cfg.Source.Publication + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	r.Log.Info("replication started", "slot", r.Cfg.Source.Slot, "from", r.StartLSN.String())
	r.Progress.Touch(0)
	for {
		if ctx.Err() != nil {
			r.sendStatus(false)
			return ctx.Err()
		}
		if time.Now().After(r.nextStatus) {
			if err := r.sendStatus(true); err != nil {
				return err
			}
		}
		rctx, cancel := context.WithDeadline(ctx, r.nextStatus)
		msg, err := r.Conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if err := r.handleCopyData(ctx, m.Data); err != nil {
				return err
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replication error: %s (%s)", m.Message, m.Code)
		}
	}
}

// sendStatus reports durable progress only. Write, flush and apply all carry
// the durable LSN so the server never treats unfinished work as received.
func (r *Reader) sendStatus(replyRequested bool) error {
	r.nextStatus = time.Now().Add(r.Cfg.Pipeline.StandbyInterval.Duration)
	d := r.Progress.Durable()
	if d == 0 {
		return nil
	}
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), r.Conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: d, WALFlushPosition: d, WALApplyPosition: d, ReplyRequested: replyRequested,
	})
	if err != nil {
		return fmt.Errorf("standby status: %w", err)
	}
	return nil
}

func (r *Reader) handleCopyData(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return err
		}
		r.Progress.Touch(ka.ServerWALEnd)
		metrics.ServerWALEnd.Set(float64(ka.ServerWALEnd))
		r.maybeIdle(ctx, ka.ServerWALEnd)
		if ka.ReplyRequested {
			return r.sendStatus(false)
		}
	case pglogrepl.XLogDataByteID:
		x, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return err
		}
		r.Progress.Touch(x.ServerWALEnd)
		metrics.ServerWALEnd.Set(float64(x.ServerWALEnd))
		return r.handleMessage(ctx, x.WALData)
	}
	return nil
}

// maybeIdle hands the planner an idle position so the checkpoint (and hence
// the slot's confirmed_flush_lsn) can advance across WAL that contains no
// published changes. It is only emitted when every received transaction is
// durable, and it travels through the same ordered queue as transactions.
func (r *Reader) maybeIdle(ctx context.Context, walEnd LSN) {
	if r.cur != nil || !r.Progress.Idle() || walEnd <= r.Progress.Durable() {
		return
	}
	if time.Since(r.lastIdleSent) < r.Cfg.Pipeline.IdleCheckpointInterval.Duration {
		return
	}
	select {
	case r.Out <- Item{IdleLSN: walEnd}:
		r.Progress.MarkIdleReceived(walEnd)
		r.lastIdleSent = time.Now()
	default:
	}
}

func (r *Reader) handleMessage(ctx context.Context, wal []byte) error {
	msg, err := pglogrepl.Parse(wal)
	if err != nil {
		return fmt.Errorf("parse pgoutput: %w", err)
	}
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		r.rels[m.RelationID] = r.buildRelation(m)
		if rel := r.rels[m.RelationID]; rel.err != nil && r.cur != nil && r.cur.Err == nil {
			r.cur.Err = rel.err
		}
	case *pglogrepl.BeginMessage:
		r.cur = &Txn{Xid: m.Xid, CommitTime: m.CommitTime}
		r.Progress.partial.Store(true)
	case *pglogrepl.InsertMessage:
		return r.row(KInsert, m.RelationID, nil, 0, m.Tuple)
	case *pglogrepl.UpdateMessage:
		return r.row(KUpdate, m.RelationID, m.OldTuple, m.OldTupleType, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return r.row(KDelete, m.RelationID, m.OldTuple, m.OldTupleType, nil)
	case *pglogrepl.TruncateMessage:
		if r.cur == nil {
			return fmt.Errorf("truncate outside transaction")
		}
		for _, id := range m.RelationIDs {
			if rel := r.rels[id]; rel != nil && rel.kind == relSource {
				r.append(Change{Kind: KTruncate})
				metrics.Changes.WithLabelValues("truncate").Inc()
			}
		}
	case *pglogrepl.CommitMessage:
		if r.cur == nil {
			return fmt.Errorf("commit without begin")
		}
		txn := r.cur
		r.cur = nil
		txn.EndLSN = m.TransactionEndLSN
		if txn.EndLSN <= r.StartLSN {
			// Already applied before the restart: replay is skipped, not re-applied.
			r.Progress.partial.Store(false)
			return nil
		}
		return r.emit(ctx, txn)
	}
	return nil
}

// emit blocks on the bounded queue (backpressure) while continuing to send
// standby status so the server does not time the connection out.
func (r *Reader) emit(ctx context.Context, txn *Txn) error {
	meta := TxnMeta{EndLSN: txn.EndLSN, CommitTime: txn.CommitTime}
	r.Progress.MarkReceived(meta)
	r.Progress.partial.Store(false)
	for {
		wait := time.Until(r.nextStatus)
		if wait <= 0 {
			if err := r.sendStatus(true); err != nil {
				return err
			}
			continue
		}
		t := time.NewTimer(wait)
		select {
		case r.Out <- Item{Txn: txn}:
			t.Stop()
			return nil
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

func (r *Reader) append(c Change) {
	if r.cur.Err != nil {
		return
	}
	if len(r.cur.Changes) >= r.Cfg.Pipeline.MaxTransactionChanges {
		r.cur.Err = &PauseError{Reason: "transaction_too_large", Detail: fmt.Sprintf(
			"source transaction xid=%d exceeds pipeline.max_transaction_changes=%d; raise the limit and restart", r.cur.Xid, r.Cfg.Pipeline.MaxTransactionChanges)}
		r.cur.Changes = nil
		return
	}
	r.cur.Changes = append(r.cur.Changes, c)
}

func (r *Reader) buildRelation(m *pglogrepl.RelationMessage) *relation {
	rel := &relation{kind: relOther, keyIdx: -1, fieldAt: map[string]int{}}
	for _, c := range m.Columns {
		rel.cols = append(rel.cols, c.Name)
	}
	switch {
	case m.Namespace == r.srcNS && m.RelationName == r.srcName:
		rel.kind = relSource
		idx := map[string]int{}
		types := map[string]uint32{}
		for i, c := range m.Columns {
			idx[c.Name] = i
			types[c.Name] = c.DataType
		}
		sig := relationSignature(r.Cfg, func(col string) (uint32, bool) { t, ok := types[col]; return t, ok })
		if sig != r.AcceptedSig {
			rel.err = &PauseError{Reason: "schema_incompatible", InStream: true, Detail: fmt.Sprintf(
				"source relation %s.%s changed: accepted signature %q, stream now has %q. Update the field mapping if a column was renamed, then restart with --accept-schema-change", r.srcNS, r.srcName, r.AcceptedSig, sig)}
		}
		if i, ok := idx[r.Cfg.Source.Key]; ok {
			rel.keyIdx = i
		} else if rel.err == nil {
			rel.err = &PauseError{Reason: "schema_incompatible", InStream: true, Detail: "primary key column missing from stream"}
		}
		for _, f := range r.Cfg.Source.Fields {
			if i, ok := idx[f.Column]; ok {
				rel.fieldAt[f.Name] = i
			}
		}
	case m.Namespace == r.Cfg.Source.ControlSchema && m.RelationName == "markers":
		rel.kind = relMarkers
	}
	return rel
}

// RelationSignature renders the accepted shape of the key and allowlisted
// columns. lookup returns a column's type OID.
func relationSignature(cfg *config.Config, lookup func(col string) (uint32, bool)) string {
	parts := []string{}
	cols := append([]string{cfg.Source.Key}, cfg.FieldColumns()...)
	for _, c := range cols {
		t, ok := lookup(c)
		if !ok {
			parts = append(parts, c+":missing")
			continue
		}
		parts = append(parts, c+":"+strconv.FormatUint(uint64(t), 10))
	}
	return strings.Join(parts, ",")
}

// RelationSignature is exported for startup checks.
func RelationSignature(cfg *config.Config, lookup func(col string) (uint32, bool)) string {
	return relationSignature(cfg, lookup)
}

func tupleValue(t *pglogrepl.TupleData, i int) (val *string, unchanged bool, err error) {
	if t == nil || i < 0 || i >= len(t.Columns) {
		return nil, false, fmt.Errorf("tuple column %d missing", i)
	}
	c := t.Columns[i]
	switch c.DataType {
	case 'n':
		return nil, false, nil
	case 'u':
		return nil, true, nil
	case 't':
		s := string(c.Data)
		return &s, false, nil
	}
	return nil, false, fmt.Errorf("unsupported tuple data type %q", c.DataType)
}

func (r *Reader) row(kind ChangeKind, relID uint32, oldT *pglogrepl.TupleData, oldType uint8, newT *pglogrepl.TupleData) error {
	if r.cur == nil {
		return fmt.Errorf("row change outside transaction")
	}
	rel := r.rels[relID]
	if rel == nil {
		return fmt.Errorf("change for unknown relation %d", relID)
	}
	switch rel.kind {
	case relMarkers:
		if kind == KInsert {
			mk, err := decodeMarker(rel, newT)
			if err != nil {
				return err
			}
			r.append(Change{Kind: KMarker, Marker: mk})
		}
		return nil
	case relOther:
		return nil
	}
	if rel.err != nil {
		if r.cur.Err == nil {
			r.cur.Err = rel.err
		}
		return nil
	}
	ch := Change{Kind: kind}
	keyOf := func(t *pglogrepl.TupleData) (string, error) {
		v, unchanged, err := tupleValue(t, rel.keyIdx)
		if err != nil {
			return "", err
		}
		if v == nil || unchanged {
			return "", fmt.Errorf("primary key value missing in change")
		}
		return *v, nil
	}
	var err error
	switch kind {
	case KDelete:
		ch.Key, err = keyOf(oldT)
		metrics.Changes.WithLabelValues("delete").Inc()
	case KInsert, KUpdate:
		ch.Key, err = keyOf(newT)
		if err != nil {
			break
		}
		if kind == KUpdate && (oldType == 'K' || oldType == 'O') && oldT != nil {
			oldKey, kerr := keyOf(oldT)
			if kerr != nil {
				err = kerr
				break
			}
			if oldKey != ch.Key {
				ch.OldKey = oldKey
			}
		}
		ch.Fields = make(map[string]*string, len(rel.fieldAt))
		for name, i := range rel.fieldAt {
			v, unchanged, verr := tupleValue(newT, i)
			if verr != nil {
				err = verr
				break
			}
			if unchanged {
				ch.Unchanged = append(ch.Unchanged, name)
				continue
			}
			ch.Fields[name] = v
		}
		if kind == KInsert {
			metrics.Changes.WithLabelValues("insert").Inc()
		} else {
			metrics.Changes.WithLabelValues("update").Inc()
		}
	}
	if err != nil {
		if r.cur.Err == nil {
			r.cur.Err = &PauseError{Reason: "unsupported_input", Detail: err.Error()}
		}
		return nil
	}
	r.append(ch)
	return nil
}

func decodeMarker(rel *relation, t *pglogrepl.TupleData) (Marker, error) {
	var mk Marker
	for i, name := range rel.cols {
		v, _, err := tupleValue(t, i)
		if err != nil {
			return mk, err
		}
		if v == nil {
			continue
		}
		switch name {
		case "kind":
			mk.Kind = *v
		case "generation_id":
			mk.Gen = *v
		case "job_id":
			mk.JobID, _ = strconv.ParseInt(*v, 10, 64)
		case "window_id":
			mk.WindowID = *v
		}
	}
	if mk.Kind == "" {
		return mk, fmt.Errorf("marker without kind")
	}
	return mk, nil
}
