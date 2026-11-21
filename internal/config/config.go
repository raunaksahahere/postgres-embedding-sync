// Package config loads and validates the embedsync YAML configuration.
//
// Credentials never live in the YAML file: every DSN is read from an
// environment variable named by the *_env fields.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from strings like "250ms".
type Duration struct{ time.Duration }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

// MarshalJSON renders a Duration as its string form.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// ByteSize is an integer byte count that unmarshals from "512MiB", "2GiB" or plain integers.
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	v, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}

// ParseByteSize parses sizes such as "1024", "64KiB", "512MiB", "4GiB".
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			var n int64
			if _, err := fmt.Sscanf(strings.TrimSuffix(s, u.suffix), "%d", &n); err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return n * u.mult, nil
		}
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n, nil
}

// Field maps a logical template field to a physical source column. Renaming a
// physical column only requires updating Column; the transform fingerprint is
// computed from logical names so existing embeddings stay valid.
type Field struct {
	Name   string `yaml:"name" json:"name"`
	Column string `yaml:"column" json:"column"`
}

// KeyColumns is a primary key: one column ("id") or several ([tenant_id, id]).
type KeyColumns []string

// UnmarshalYAML accepts a scalar or a sequence.
func (k *KeyColumns) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		var s string
		if err := n.Decode(&s); err != nil {
			return err
		}
		*k = KeyColumns{s}
		return nil
	}
	var l []string
	if err := n.Decode(&l); err != nil {
		return err
	}
	*k = l
	return nil
}

// Source describes the replication connection shared by every pipeline. The
// table, key and fields here are the single-table form, kept valid: they
// become one pipeline named "default".
type Source struct {
	DSNEnv        string     `yaml:"dsn_env"`
	ReplDSNEnv    string     `yaml:"replication_dsn_env"`
	Table         string     `yaml:"table"`
	Key           KeyColumns `yaml:"key"`
	Fields        []Field    `yaml:"fields"`
	Publication   string     `yaml:"publication"`
	Slot          string     `yaml:"slot"`
	ControlSchema string     `yaml:"control_schema"`
}

// DefaultPipeline is the name the single-table form (source.table) gets.
const DefaultPipeline = "default"

// PipelineDef is one replicated table (a "pipeline"): its key columns,
// allowlisted fields and transform. Every pipeline shares the slot,
// publication, embedder and generations; documents are identified by
// (pipeline, key).
type PipelineDef struct {
	Name      string     `yaml:"name" json:"name"`
	Table     string     `yaml:"table" json:"table"`
	Key       KeyColumns `yaml:"key" json:"key"`
	Fields    []Field    `yaml:"fields" json:"fields"`
	Transform *Transform `yaml:"transform" json:"transform"` // nil: inherit the top-level transform
}

// Sink describes the pgvector database.
type Sink struct {
	DSNEnv       string `yaml:"dsn_env"`
	ReaderDSNEnv string `yaml:"reader_dsn_env"`
	Schema       string `yaml:"schema"`
	ReaderRole   string `yaml:"reader_role"`
}

// Transform controls rendering and chunking. Everything here is part of the
// transform fingerprint.
type Transform struct {
	Template     string `yaml:"template" json:"template"`
	ChunkWords   int    `yaml:"chunk_words" json:"chunk_words"`
	ChunkOverlap int    `yaml:"chunk_overlap" json:"chunk_overlap"`
}

// Retry controls exponential backoff with full jitter.
type Retry struct {
	InitialBackoff Duration `yaml:"initial_backoff"`
	MaxBackoff     Duration `yaml:"max_backoff"`
}

// Embedder configures the HTTP embedding service client. With Managed set,
// embedsync starts the embedder shipped with the installer on a private
// localhost port and stops it on exit; URL and URLEnv are then ignored.
type Embedder struct {
	Managed           bool     `yaml:"managed"`
	URL               string   `yaml:"url"`
	URLEnv            string   `yaml:"url_env"`
	Timeout           Duration `yaml:"timeout"`
	BatchSize         int      `yaml:"batch_size"`
	Concurrency       int      `yaml:"concurrency"`
	RequestsPerSecond float64  `yaml:"requests_per_second"`
	Burst             int      `yaml:"burst"`
	Retry             Retry    `yaml:"retry"`
}

// LocalProvider is the name of the built-in embedder (the embedder block).
const LocalProvider = "local"

// Pricing labels an estimated cost: a price per million tokens and where
// that price was taken from (a URL and date, or "self-hosted").
type Pricing struct {
	USDPerMillionTokens float64 `yaml:"usd_per_million_tokens" json:"usd_per_million_tokens"`
	Source              string  `yaml:"source" json:"source"`
}

// ProviderDef is an embedding endpoint other than the built-in embedder.
// kind "openai" speaks the OpenAI embeddings API (OpenAI, Gemini's
// OpenAI-compatible endpoint, NVIDIA API catalog, Ollama, vLLM, TEI);
// kind "gemini" speaks Google's batchEmbedContents, which embeds documents
// and queries with different task types (url defaults to the public API).
// The API key is only ever read from the environment variable named here.
type ProviderDef struct {
	Kind              string   `yaml:"kind" json:"kind"`
	URL               string   `yaml:"url" json:"url"`
	URLEnv            string   `yaml:"url_env" json:"url_env,omitempty"`
	APIKeyEnv         string   `yaml:"api_key_env" json:"api_key_env,omitempty"`
	Timeout           Duration `yaml:"timeout" json:"timeout"`
	BatchSize         int      `yaml:"batch_size" json:"batch_size"`
	MaxBatchTokens    int      `yaml:"max_batch_tokens" json:"max_batch_tokens"`
	Concurrency       int      `yaml:"concurrency" json:"concurrency"`
	RequestsPerSecond float64  `yaml:"requests_per_second" json:"requests_per_second"`
	Burst             int      `yaml:"burst" json:"burst"`
	TokensPerMinute   int      `yaml:"tokens_per_minute" json:"tokens_per_minute"`
	Retry             Retry    `yaml:"retry" json:"-"`
	MaxRetryAfter     Duration `yaml:"max_retry_after" json:"max_retry_after"`
	Pricing           Pricing  `yaml:"pricing" json:"pricing"`
}

// HNSW holds pgvector HNSW index settings.
type HNSW struct {
	M              int `yaml:"m" json:"m"`
	EFConstruction int `yaml:"ef_construction" json:"ef_construction"`
	EFSearch       int `yaml:"ef_search" json:"ef_search"`
}

// Model is one embedding model that a generation can be built from.
type Model struct {
	Name           string `yaml:"name" json:"name"`
	Revision       string `yaml:"revision" json:"revision"`
	Dimension      int    `yaml:"dimension" json:"dimension"`
	Distance       string `yaml:"distance" json:"distance"`
	QueryPrefix    string `yaml:"query_prefix" json:"query_prefix"`
	DocumentPrefix string `yaml:"document_prefix" json:"document_prefix"`
	HNSW           HNSW   `yaml:"hnsw" json:"hnsw"`
	// Provider names the endpoint that computes this model's vectors
	// ("local" or empty: the built-in embedder; otherwise a providers key).
	Provider string `yaml:"provider" json:"provider,omitempty"`
	// RequestDimensions asks the provider for shorter vectors (OpenAI
	// "dimensions"); 0 uses the model's native size, which must equal
	// Dimension.
	RequestDimensions int `yaml:"request_dimensions" json:"request_dimensions,omitempty"`
	// QueryInputType and DocumentInputType are sent as the provider's
	// input_type for queries and documents (e.g. "query" / "passage").
	QueryInputType    string `yaml:"query_input_type" json:"query_input_type,omitempty"`
	DocumentInputType string `yaml:"document_input_type" json:"document_input_type,omitempty"`
}

// ProviderName returns the model's provider ("local" when unset).
func (m Model) ProviderName() string {
	if m.Provider == "" {
		return LocalProvider
	}
	return m.Provider
}

// Pipeline bounds memory, batching and failure policy.
type Pipeline struct {
	MaxInflightUnits       int      `yaml:"max_inflight_units"`
	TxQueue                int      `yaml:"tx_queue"`
	MaxBatchTxns           int      `yaml:"max_batch_txns"`
	MaxBatchDocs           int      `yaml:"max_batch_docs"`
	MaxTransactionChanges  int      `yaml:"max_transaction_changes"`
	MaxDocumentBytes       int      `yaml:"max_document_bytes"`
	OnPoison               string   `yaml:"on_poison"`
	IdleCheckpointInterval Duration `yaml:"idle_checkpoint_interval"`
	StandbyInterval        Duration `yaml:"standby_interval"`
	ShutdownTimeout        Duration `yaml:"shutdown_timeout"`
	StallAfter             Duration `yaml:"stall_after"`
}

// Backfill controls watermark backfill windows.
type Backfill struct {
	BatchSize   int      `yaml:"batch_size"`
	WindowPause Duration `yaml:"window_pause"`
}

// Gates are declared before running an evaluation; promotion requires all to pass.
type Gates struct {
	MinANNRecallAt10           float64 `yaml:"min_ann_recall_at_10" json:"min_ann_recall_at_10"`
	NonInferiorityMarginNDCG   float64 `yaml:"noninferiority_margin_ndcg" json:"noninferiority_margin_ndcg"`
	NonInferiorityMarginRecall float64 `yaml:"noninferiority_margin_recall" json:"noninferiority_margin_recall"`
	BootstrapSamples           int     `yaml:"bootstrap_samples" json:"bootstrap_samples"`
	ConfidenceLevel            float64 `yaml:"confidence_level" json:"confidence_level"`
	Seed                       int64   `yaml:"seed" json:"seed"`
	RequireZeroUnresolved      bool    `yaml:"require_zero_unresolved" json:"require_zero_unresolved"`
}

// Migration configures blue/green model switches.
type Migration struct {
	RollbackWindow Duration `yaml:"rollback_window"`
	CatchupTimeout Duration `yaml:"catchup_timeout"`
	Gates          Gates    `yaml:"gates"`
	EvalQueries    string   `yaml:"eval_queries"`
	EvalQrels      string   `yaml:"eval_qrels"`
}

// Metrics configures the HTTP listener for /metrics, /healthz, /status and /search.
type Metrics struct {
	Listen string `yaml:"listen"`
}

// Alerts holds thresholds exported as gauges so Prometheus rules and the
// status command agree on what "bad" means.
type Alerts struct {
	FreshnessSeconds         float64  `yaml:"freshness_seconds"`
	RetainedWALWarningBytes  ByteSize `yaml:"retained_wal_warning_bytes"`
	RetainedWALCriticalBytes ByteSize `yaml:"retained_wal_critical_bytes"`
}

// Config is the whole embedsync.yaml.
type Config struct {
	Source            Source                 `yaml:"source"`
	Pipelines         []PipelineDef          `yaml:"pipelines"`
	Views             []ViewDef              `yaml:"views"`
	Sink              Sink                   `yaml:"sink"`
	Transform         Transform              `yaml:"transform"`
	Embedder          Embedder               `yaml:"embedder"`
	Models            map[string]Model       `yaml:"models"`
	Providers         map[string]ProviderDef `yaml:"providers"`
	InitialGeneration string                 `yaml:"initial_generation"`
	Pipeline          Pipeline               `yaml:"pipeline"`
	Backfill          Backfill               `yaml:"backfill"`
	Migration         Migration              `yaml:"migration"`
	Metrics           Metrics                `yaml:"metrics"`
	Alerts            Alerts                 `yaml:"alerts"`
}

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
var genRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)
var pipeRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// Load reads, defaults and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse parses YAML bytes.
func Parse(b []byte) (*Config, error) {
	c := &Config{}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.normalize(); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	def := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	defI := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	defD := func(p *Duration, v time.Duration) {
		if p.Duration == 0 {
			p.Duration = v
		}
	}
	def(&c.Source.DSNEnv, "EMBEDSYNC_SOURCE_DSN")
	def(&c.Source.ReplDSNEnv, c.Source.DSNEnv)
	def(&c.Source.Publication, "embedsync_pub")
	def(&c.Source.Slot, "embedsync_slot")
	def(&c.Source.ControlSchema, "embedsync_control")
	def(&c.Sink.DSNEnv, "EMBEDSYNC_SINK_DSN")
	def(&c.Sink.Schema, "embedsync")
	defI(&c.Transform.ChunkWords, 120)
	for i := range c.Pipelines {
		if t := c.Pipelines[i].Transform; t != nil {
			defI(&t.ChunkWords, 120)
		}
	}
	for i := range c.Views {
		v := &c.Views[i]
		if t := v.Transform; t != nil {
			defI(&t.ChunkWords, 120)
		}
		defI(&v.MaxChildren, 1000)
		defI(&v.MaxFanoutPerTxn, 10000)
	}
	defI(&c.Embedder.BatchSize, 32)
	defI(&c.Embedder.Concurrency, 2)
	defI(&c.Embedder.Burst, 4)
	defD(&c.Embedder.Timeout, 30*time.Second)
	defD(&c.Embedder.Retry.InitialBackoff, 200*time.Millisecond)
	defD(&c.Embedder.Retry.MaxBackoff, 15*time.Second)
	if c.Embedder.RequestsPerSecond == 0 {
		c.Embedder.RequestsPerSecond = 50
	}
	for k, p := range c.Providers {
		def(&p.Kind, "openai")
		defI(&p.BatchSize, 64)
		defI(&p.Concurrency, 2)
		defI(&p.Burst, 4)
		defD(&p.Timeout, 60*time.Second)
		defD(&p.Retry.InitialBackoff, 500*time.Millisecond)
		defD(&p.Retry.MaxBackoff, 30*time.Second)
		defD(&p.MaxRetryAfter, 60*time.Second)
		c.Providers[k] = p
	}
	for k, m := range c.Models {
		def(&m.Distance, "cosine")
		defI(&m.HNSW.M, 16)
		defI(&m.HNSW.EFConstruction, 64)
		defI(&m.HNSW.EFSearch, 100)
		c.Models[k] = m
	}
	defI(&c.Pipeline.MaxInflightUnits, 8)
	defI(&c.Pipeline.TxQueue, 64)
	defI(&c.Pipeline.MaxBatchTxns, 256)
	defI(&c.Pipeline.MaxBatchDocs, 256)
	defI(&c.Pipeline.MaxTransactionChanges, 100000)
	defI(&c.Pipeline.MaxDocumentBytes, 256*1024)
	def(&c.Pipeline.OnPoison, "quarantine")
	defD(&c.Pipeline.IdleCheckpointInterval, 10*time.Second)
	defD(&c.Pipeline.StandbyInterval, 5*time.Second)
	defD(&c.Pipeline.ShutdownTimeout, 30*time.Second)
	defD(&c.Pipeline.StallAfter, 60*time.Second)
	defI(&c.Backfill.BatchSize, 500)
	defD(&c.Migration.RollbackWindow, 30*time.Minute)
	defD(&c.Migration.CatchupTimeout, 10*time.Minute)
	g := &c.Migration.Gates
	if g.MinANNRecallAt10 == 0 {
		g.MinANNRecallAt10 = 0.95
	}
	defI(&g.BootstrapSamples, 2000)
	if g.ConfidenceLevel == 0 {
		g.ConfidenceLevel = 0.95
	}
	if g.Seed == 0 {
		g.Seed = 20261120
	}
	def(&c.Metrics.Listen, ":9187")
	if c.Alerts.FreshnessSeconds == 0 {
		c.Alerts.FreshnessSeconds = 10
	}
	if c.Alerts.RetainedWALWarningBytes == 0 {
		c.Alerts.RetainedWALWarningBytes = 1 << 30
	}
	if c.Alerts.RetainedWALCriticalBytes == 0 {
		c.Alerts.RetainedWALCriticalBytes = 4 << 30
	}
}

// Validate checks invariants that would otherwise surface as runtime surprises.
func (c *Config) Validate() error {
	var errs []string
	add := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if len(c.Pipelines) == 0 && len(c.Views) == 0 {
		add("configure source.table (one table), pipelines (one or more tables) or views")
	}
	names, tables := map[string]bool{}, map[string]bool{}
	for i := range c.Pipelines {
		p := &c.Pipelines[i]
		where := fmt.Sprintf("pipeline %q", p.Name)
		if !pipeRe.MatchString(p.Name) {
			add("pipeline name %q must match %s", p.Name, pipeRe)
		}
		if names[p.Name] {
			add("duplicate pipeline name %q", p.Name)
		}
		names[p.Name] = true
		schema, table, ok := strings.Cut(p.Table, ".")
		if !ok || !identRe.MatchString(schema) || !identRe.MatchString(table) {
			add("%s: table must be schema-qualified lowercase identifiers, got %q", where, p.Table)
		}
		if tables[p.Table] {
			add("%s: table %s is already used by another pipeline", where, p.Table)
		}
		tables[p.Table] = true
		if schema == c.Source.ControlSchema {
			add("%s: table must not live in the control schema %q", where, c.Source.ControlSchema)
		}
		if len(p.Key) == 0 {
			add("%s: key must name at least one primary-key column", where)
		}
		if len(p.Key) > 8 {
			add("%s: key has more than 8 columns", where)
		}
		keySeen := map[string]bool{}
		for _, k := range p.Key {
			if !identRe.MatchString(k) {
				add("%s: key column %q must be a lowercase identifier", where, k)
			}
			if keySeen[k] {
				add("%s: duplicate key column %q", where, k)
			}
			keySeen[k] = true
		}
		if len(p.Fields) == 0 {
			add("%s: fields must list at least one allowlisted column", where)
		}
		seen := map[string]bool{}
		for _, f := range p.Fields {
			if !identRe.MatchString(f.Name) || !identRe.MatchString(f.Column) {
				add("%s: fields entries need lowercase identifier name/column, got %+v", where, f)
			}
			if seen[f.Name] {
				add("%s: duplicate field %q", where, f.Name)
			}
			seen[f.Name] = true
		}
		t := p.Transform
		if t == nil {
			continue
		}
		if t.Template == "" {
			add("%s: transform.template is required", where)
		}
		if t.ChunkWords < 8 {
			add("%s: transform.chunk_words must be >= 8", where)
		}
		if t.ChunkOverlap < 0 || t.ChunkOverlap >= t.ChunkWords {
			add("%s: transform.chunk_overlap must be in [0, chunk_words)", where)
		}
	}
	for _, s := range []string{c.Source.Publication, c.Source.Slot, c.Source.ControlSchema, c.Sink.Schema} {
		if !identRe.MatchString(s) {
			add("invalid identifier %q", s)
		}
	}
	c.validateViews(add, tables, names)
	if c.Sink.ReaderRole != "" && !identRe.MatchString(c.Sink.ReaderRole) {
		add("invalid sink.reader_role")
	}
	if !c.Embedder.Managed && c.Embedder.URL == "" && c.Embedder.URLEnv == "" && c.usesLocal() {
		add("embedder.url or embedder.url_env is required (or embedder.managed: true)")
	}
	if len(c.Models) == 0 {
		add("at least one model is required")
	}
	for k, m := range c.Models {
		if !genRe.MatchString(k) {
			add("model key %q must match %s", k, genRe)
		}
		if m.Name == "" || m.Revision == "" {
			add("model %s needs name and pinned revision", k)
		}
		if m.Dimension <= 0 || m.Dimension > 16000 {
			add("model %s has invalid dimension", k)
		}
		if m.Distance != "cosine" && m.Distance != "l2" && m.Distance != "ip" {
			add("model %s distance must be cosine, l2 or ip", k)
		}
	}
	for name, p := range c.Providers {
		if name == LocalProvider || !genRe.MatchString(name) {
			add("provider name %q must match %s and not be %q (the built-in embedder)", name, genRe, LocalProvider)
		}
		if p.Kind != "openai" && p.Kind != "gemini" {
			add("provider %s: kind must be openai (OpenAI-compatible embeddings API) or gemini (Gemini batchEmbedContents with task types)", name)
		}
		if p.URL == "" && p.URLEnv == "" && p.Kind != "gemini" {
			add("provider %s: url or url_env is required", name)
		}
		if p.BatchSize < 1 || p.Concurrency < 1 || p.RequestsPerSecond < 0 || p.TokensPerMinute < 0 || p.MaxBatchTokens < 0 {
			add("provider %s: batch_size and concurrency must be positive; rate limits must not be negative", name)
		}
		if p.Pricing.USDPerMillionTokens < 0 || (p.Pricing.USDPerMillionTokens > 0 && p.Pricing.Source == "") {
			add("provider %s: pricing.source must say where usd_per_million_tokens comes from (URL and date)", name)
		}
	}
	for k, m := range c.Models {
		if pn := m.ProviderName(); pn != LocalProvider {
			if _, ok := c.Providers[pn]; !ok {
				add("model %s uses provider %q, which is not configured", k, pn)
			}
		}
		if m.RequestDimensions < 0 || (m.RequestDimensions > 0 && m.RequestDimensions != m.Dimension) {
			add("model %s: request_dimensions must equal dimension when set", k)
		}
	}
	if _, ok := c.Models[c.InitialGeneration]; !ok {
		add("initial_generation %q must name a model", c.InitialGeneration)
	}
	if c.Pipeline.OnPoison != "quarantine" && c.Pipeline.OnPoison != "pause" {
		add("pipeline.on_poison must be quarantine or pause")
	}
	if c.Backfill.BatchSize < 1 || c.Backfill.BatchSize > 100000 {
		add("backfill.batch_size out of range")
	}
	if c.Embedder.BatchSize < 1 || c.Embedder.Concurrency < 1 {
		add("embedder batch_size and concurrency must be positive")
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("invalid config:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// normalize turns the single-table form into the "default" pipeline and
// gives every pipeline without its own transform the top-level one.
func (c *Config) normalize() error {
	legacy := c.Source.Table != "" || len(c.Source.Key) > 0 || len(c.Source.Fields) > 0
	if legacy && len(c.Pipelines) > 0 {
		return fmt.Errorf("invalid config:\n  - use either source.table/key/fields (one table) or pipelines, not both")
	}
	if legacy {
		c.Pipelines = []PipelineDef{{Name: DefaultPipeline, Table: c.Source.Table, Key: c.Source.Key, Fields: c.Source.Fields}}
	}
	for i := range c.Pipelines {
		if c.Pipelines[i].Transform == nil {
			t := c.Transform
			c.Pipelines[i].Transform = &t
		}
	}
	for i := range c.Views {
		if c.Views[i].Transform == nil {
			t := c.Transform
			c.Views[i].Transform = &t
		}
	}
	return nil
}

// Pipe returns the named pipeline or nil.
func (c *Config) Pipe(name string) *PipelineDef {
	for i := range c.Pipelines {
		if c.Pipelines[i].Name == name {
			return &c.Pipelines[i]
		}
	}
	return nil
}

// PipelineNames lists pipeline names in config order.
func (c *Config) PipelineNames() []string {
	out := make([]string, len(c.Pipelines))
	for i, p := range c.Pipelines {
		out[i] = p.Name
	}
	return out
}

// SchemaTable splits the pipeline's table.
func (p *PipelineDef) SchemaTable() (string, string) {
	s, t, _ := strings.Cut(p.Table, ".")
	return s, t
}

// FieldColumns returns the physical allowlisted columns in config order.
func (p *PipelineDef) FieldColumns() []string {
	out := make([]string, 0, len(p.Fields))
	for _, f := range p.Fields {
		out = append(out, f.Column)
	}
	return out
}

// FieldNames returns the logical field names in config order.
func (p *PipelineDef) FieldNames() []string {
	out := make([]string, 0, len(p.Fields))
	for _, f := range p.Fields {
		out = append(out, f.Name)
	}
	return out
}

// PublishedColumns is the key plus every allowlisted column, deduplicated.
func (p *PipelineDef) PublishedColumns() []string {
	out := []string{}
	have := map[string]bool{}
	for _, col := range append(append([]string{}, p.Key...), p.FieldColumns()...) {
		if !have[col] {
			have[col] = true
			out = append(out, col)
		}
	}
	return out
}

// TransformFingerprint hashes everything that affects the pipeline's chunk
// text. Logical field names (not physical columns) are included so a column
// rename is not a re-embedding event. The formula is the one single-table
// installations have always used, so their chunks keep their fingerprints.
func (p *PipelineDef) TransformFingerprint() string {
	payload, _ := json.Marshal(struct {
		Fields    []string  `json:"fields"`
		Transform Transform `json:"transform"`
		Version   int       `json:"version"`
	}{p.FieldNames(), *p.Transform, 1})
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:16])
}

// PipelineFingerprints maps every document pipeline (standalone pipelines
// and views) to the fingerprint of everything that shapes its text.
func (c *Config) PipelineFingerprints() map[string]string {
	out := make(map[string]string, len(c.Pipelines)+len(c.Views))
	for i := range c.Pipelines {
		out[c.Pipelines[i].Name] = c.Pipelines[i].TransformFingerprint()
	}
	for i := range c.Views {
		out[c.Views[i].Name] = c.Views[i].Fingerprint()
	}
	return out
}

// CombinedFingerprint summarises every pipeline's transform for the
// generation row. A single "default" pipeline keeps the single-table value.
func (c *Config) CombinedFingerprint() string {
	if len(c.Pipelines) == 1 && c.Pipelines[0].Name == DefaultPipeline {
		return c.Pipelines[0].TransformFingerprint()
	}
	fps := c.PipelineFingerprints()
	names := make([]string, 0, len(fps))
	for n := range fps {
		names = append(names, n)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		fmt.Fprintf(h, "%s=%s\n", n, fps[n])
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// Env returns the value of an environment variable or an error naming it.
func Env(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return v, nil
}

// EmbedderURL resolves the embedder URL, preferring the env override.
func (c *Config) EmbedderURL() string {
	if c.Embedder.URLEnv != "" {
		if v := os.Getenv(c.Embedder.URLEnv); v != "" {
			return v
		}
	}
	return c.Embedder.URL
}

// usesLocal reports whether any model uses the built-in embedder.
func (c *Config) usesLocal() bool {
	if len(c.Models) == 0 {
		return true
	}
	for _, m := range c.Models {
		if m.ProviderName() == LocalProvider {
			return true
		}
	}
	return false
}

// ProviderURL resolves a provider's base URL, preferring the env override.
func (p ProviderDef) ProviderURL() string {
	if p.URLEnv != "" {
		if v := os.Getenv(p.URLEnv); v != "" {
			return v
		}
	}
	return p.URL
}
