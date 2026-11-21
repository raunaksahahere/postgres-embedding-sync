// Package pipeline implements the replication reader, ordered planner,
// embedding stage, transactional committer and watermark backfill executor.
//
// Data flow (all channels bounded):
//
//	reader --Item--> planner --Unit--> embed stage --Unit--> committer
//	   ^                ^                                        |
//	   |                +---- backfill registry <--- executor    |
//	   +------------- durable LSN (standby feedback) <-----------+
//
// The committer applies units strictly in commit-LSN order, writing all row
// effects and the checkpoint in one sink transaction, so durable progress is
// always contiguous: no worker, retry or slow response can move feedback past
// unfinished work.
//
// Several pipelines (source tables) share the one stream. A unit holds whole
// source transactions, whatever tables they touch, so a transaction that
// changes two pipelines' tables commits in one sink transaction even when the
// embeddings of its documents finish out of order. Documents are identified by
// DocID{pipeline, key}.
package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// Spec is the runtime view of one configured pipeline.
type Spec struct {
	Def   *config.PipelineDef
	Name  string
	Codec keys.Codec // filled from the source catalog (or the accepted relation)
	Tr    *transform.Transformer
}

// NewSpec builds a pipeline's transformer; the codec is set once key types are known.
func NewSpec(p *config.PipelineDef) (*Spec, error) {
	tr, err := transform.New(p.Table, p.Transform.Template, p.FieldNames(), p.Transform.ChunkWords, p.Transform.ChunkOverlap, p.TransformFingerprint())
	if err != nil {
		return nil, fmt.Errorf("pipeline %s: %w", p.Name, err)
	}
	return &Spec{Def: p, Name: p.Name, Tr: tr}, nil
}

// Specs indexes pipelines by name.
type Specs map[string]*Spec

// NewSpecs builds a Spec for every configured pipeline.
func NewSpecs(cfg *config.Config) (Specs, error) {
	out := Specs{}
	for i := range cfg.Pipelines {
		s, err := NewSpec(&cfg.Pipelines[i])
		if err != nil {
			return nil, err
		}
		out[s.Name] = s
	}
	return out, nil
}

// Transformers returns each pipeline's transformer.
func (s Specs) Transformers() map[string]*transform.Transformer {
	out := make(map[string]*transform.Transformer, len(s))
	for n, p := range s {
		out[n] = p.Tr
	}
	return out
}

// DocID identifies a document: a pipeline and its encoded source key.
type DocID struct {
	Pipeline string
	Key      string
}

func (d DocID) String() string { return d.Pipeline + "/" + d.Key }

// Less orders DocIDs (deterministic lock order in the committer).
func (d DocID) Less(o DocID) bool {
	if d.Pipeline != o.Pipeline {
		return d.Pipeline < o.Pipeline
	}
	return d.Key < o.Key
}

// LSN aliases pglogrepl.LSN.
type LSN = pglogrepl.LSN

// ChangeKind classifies decoded changes.
type ChangeKind uint8

// Change kinds.
const (
	KInsert ChangeKind = iota + 1
	KUpdate
	KDelete
	KTruncate
	KMarker
)

// Marker is a row inserted into the control marker table.
type Marker struct {
	Kind     string
	Gen      string
	JobID    int64
	WindowID string
}

// Change is one decoded row change of a pipeline's source table or a marker.
type Change struct {
	Kind      ChangeKind
	Pipeline  string             // empty for markers
	Key       string             // encoded key (keys.Codec)
	OldKey    string             // set when an UPDATE changed the primary key
	Fields    map[string]*string // logical field -> value (nil = SQL NULL); excludes unchanged TOAST
	Unchanged []string           // logical fields whose TOAST value was not sent
	Marker    Marker
}

// Txn is one committed source transaction.
type Txn struct {
	Xid        uint32
	EndLSN     LSN
	CommitTime time.Time
	Changes    []Change
	Err        error // schema incompatibility or size limit: pause before applying
}

// Item flows from the reader to the planner.
type Item struct {
	Txn     *Txn
	IdleLSN LSN // when Txn is nil: everything up to IdleLSN has been received and nothing is pending
}

// PauseError stops the pipeline before the offending work is acknowledged.
type PauseError struct {
	Reason string
	Detail string
	// InStream is set when the stream itself reached the condition (as
	// opposed to a startup check), e.g. the first incompatible relation.
	InStream bool
	// Pipeline names the pipeline whose relation caused a schema pause.
	Pipeline string
}

func (e *PauseError) Error() string { return fmt.Sprintf("paused (%s): %s", e.Reason, e.Detail) }

// IsPause reports whether err is a PauseError.
func IsPause(err error) (*PauseError, bool) {
	var p *PauseError
	ok := errors.As(err, &p)
	return p, ok
}

// DocEffect is the final state of one document within a unit.
type DocEffect struct {
	ID         DocID
	Delete     bool
	Fields     map[string]*string
	Text       string
	LSN        LSN
	CommitTime time.Time
	Poison     string // planner-detected poison reason ("" when fine)
	PoisonErr  string
}

// RangeDelete removes one pipeline's sink rows inside a backfill window's key
// range that were neither in the snapshot nor changed during the window.
type RangeDelete struct {
	Gen      string
	Pipeline string
	Lo       *string // exclusive; nil = unbounded
	Hi       *string // inclusive; nil = unbounded
	Keep     []string
}

// GenStart records the stream position from which a generation consumes changes.
type GenStart struct {
	Gen string
	LSN LSN
}

// WindowProgress is backfill progress persisted with the window's effects.
type WindowProgress struct {
	JobID    int64
	Gen      string
	Pipeline string
	LastKey  *string
	Rows     int
	Final    bool
	Entry    *WindowRows
}

// TxnMeta is per-transaction metadata for latency metrics.
type TxnMeta struct {
	EndLSN     LSN
	CommitTime time.Time
}

// GenInfo is a live generation plus its encoder identity.
type GenInfo struct {
	Gen   *store.Generation
	Model transform.ModelIdentity
}

// Unit is a group of consecutive source transactions applied in one sink transaction.
type Unit struct {
	Seq          uint64
	EndLSN       LSN
	Txns         []TxnMeta
	Truncates    map[string]bool // pipelines truncated in this unit (applied before every other effect)
	Stream       map[DocID]*DocEffect
	Snapshot     map[string]map[DocID]*DocEffect // gen -> doc -> effect
	RangeDeletes []RangeDelete
	GenStarts    []GenStart
	Retires      []string
	Progress     []WindowProgress
	Gens         map[string]*GenInfo // generations that receive stream effects
	SnapGens     map[string]*GenInfo // generations that receive snapshot effects
	Pause        *PauseError

	// Filled by the embed stage.
	Chunks   map[string]map[DocID][]transform.Chunk // gen -> doc -> chunks
	Vectors  map[string]map[string]string           // gen -> fingerprint -> pgvector text
	Rejected map[string]map[string]string           // gen -> fingerprint -> error
	ready    chan struct{}
	err      error
	docs     int
}

func newUnit(seq uint64) *Unit {
	return &Unit{
		Seq: seq, Truncates: map[string]bool{}, Stream: map[DocID]*DocEffect{}, Snapshot: map[string]map[DocID]*DocEffect{},
		Gens: map[string]*GenInfo{}, SnapGens: map[string]*GenInfo{},
		Chunks: map[string]map[DocID][]transform.Chunk{}, Vectors: map[string]map[string]string{},
		Rejected: map[string]map[string]string{}, ready: make(chan struct{}),
	}
}

// Effective returns the effects for one generation: snapshot effects override
// stream effects for the same key (the planner removes snapshot entries that a
// later stream change supersedes).
func (u *Unit) Effective(gen string) map[DocID]*DocEffect {
	out := map[DocID]*DocEffect{}
	if _, ok := u.Gens[gen]; ok {
		for k, e := range u.Stream {
			out[k] = e
		}
	}
	for k, e := range u.Snapshot[gen] {
		out[k] = e
	}
	return out
}

// TargetGens returns every generation touched by the unit.
func (u *Unit) TargetGens() map[string]*GenInfo {
	out := map[string]*GenInfo{}
	for k, g := range u.Gens {
		out[k] = g
	}
	for k, g := range u.SnapGens {
		out[k] = g
	}
	return out
}

// Progress tracks received and durable LSNs for standby feedback and metrics.
type Progress struct {
	durable  atomic.Uint64
	received atomic.Uint64
	walEnd   atomic.Uint64
	lastMsg  atomic.Int64
	advanced atomic.Int64
	partial  atomic.Bool

	mu      sync.Mutex
	pending []TxnMeta
}

// Durable is the highest LSN whose effects are committed in the sink.
func (p *Progress) Durable() LSN { return LSN(p.durable.Load()) }

// Received is the end LSN of the last item handed to the planner.
func (p *Progress) Received() LSN { return LSN(p.received.Load()) }

// Init sets both positions to the checkpoint at startup.
func (p *Progress) Init(lsn LSN) {
	p.durable.Store(uint64(lsn))
	p.received.Store(uint64(lsn))
	p.advanced.Store(time.Now().UnixNano())
}

// LastAdvance is when durable progress last moved (or startup). A consumer
// with pending work whose durable position stops moving is stalled; one that
// is merely behind keeps advancing.
func (p *Progress) LastAdvance() time.Time { return time.Unix(0, p.advanced.Load()) }

// MarkReceived records a transaction handed to the planner.
func (p *Progress) MarkReceived(m TxnMeta) {
	p.mu.Lock()
	p.pending = append(p.pending, m)
	p.mu.Unlock()
	p.received.Store(uint64(m.EndLSN))
}

// MarkIdleReceived records an idle position handed to the planner.
func (p *Progress) MarkIdleReceived(lsn LSN) { p.received.Store(uint64(lsn)) }

// SetDurable advances durable progress after a sink commit.
func (p *Progress) SetDurable(lsn LSN) {
	if uint64(lsn) <= p.durable.Load() {
		return
	}
	p.durable.Store(uint64(lsn))
	p.advanced.Store(time.Now().UnixNano())
	p.mu.Lock()
	i := 0
	for i < len(p.pending) && p.pending[i].EndLSN <= lsn {
		i++
	}
	p.pending = append(p.pending[:0], p.pending[i:]...)
	p.mu.Unlock()
}

// Pending returns the number of non-durable transactions and the oldest commit time.
func (p *Progress) Pending() (int, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) == 0 {
		return 0, time.Time{}
	}
	return len(p.pending), p.pending[0].CommitTime
}

// Idle reports whether everything received is durable and no transaction is being assembled.
func (p *Progress) Idle() bool {
	return !p.partial.Load() && p.received.Load() <= p.durable.Load()
}

// Touch records a message from the server.
func (p *Progress) Touch(walEnd LSN) {
	p.lastMsg.Store(time.Now().UnixNano())
	if uint64(walEnd) > p.walEnd.Load() {
		p.walEnd.Store(uint64(walEnd))
	}
}

// LastMessage returns the time of the last server message.
func (p *Progress) LastMessage() time.Time { return time.Unix(0, p.lastMsg.Load()) }

// WALEnd returns the last server WAL end seen.
func (p *Progress) WALEnd() LSN { return LSN(p.walEnd.Load()) }
