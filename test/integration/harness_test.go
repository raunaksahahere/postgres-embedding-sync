//go:build integration

// Package integration runs embedsync against a real PostgreSQL + pgvector
// cluster. The binary under test runs as a subprocess so failpoints and
// SIGKILL behave like real crashes.
//
//	EMBEDSYNC_IT_ADMIN_DSN=postgres://postgres:pw@127.0.0.1:55433/postgres?sslmode=disable \
//	  go test -tags integration ./test/integration/ -v
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	binary   string
	adminDSN string
)

func TestMain(m *testing.M) {
	adminDSN = os.Getenv("EMBEDSYNC_IT_ADMIN_DSN")
	if adminDSN == "" {
		fmt.Println("EMBEDSYNC_IT_ADMIN_DSN not set; skipping integration tests")
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "embedsync-it")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "embedsync")
	args := []string{"build", "-o", binary}
	if os.Getenv("EMBEDSYNC_IT_RACE") == "1" {
		args = append(args, "-race")
	}
	args = append(args, "../../cmd/embedsync")
	cmd := exec.Command("go", args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
	if err := ensureRoles(); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func ensureRoles() error {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	for _, q := range []string{
		`CREATE ROLE embedsync_replicator LOGIN REPLICATION PASSWORD 'it-repl'`,
		`CREATE ROLE embedsync_sink LOGIN PASSWORD 'it-sink'`,
		`CREATE ROLE embedsync_reader LOGIN PASSWORD 'it-reader'`,
	} {
		if _, err := c.Exec(ctx, q); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------- fake embedder

// fakeEmbedder serves deterministic encoders:
//
//	bow     bag-of-words hashing (lexical retrieval works)
//	bowalt  the same bag-of-words in a permuted space of the SAME dimension,
//	        trained (like e5/bge) with "query: " / "passage: " prompts that it
//	        strips: equal quality, but vectors are incompatible with bow, so
//	        a query encoded by the wrong model finds the wrong documents
//	noise   a vector derived from the whole text (no retrieval signal)
type fakeEmbedder struct {
	srv    *httptest.Server
	outage atomic.Bool
	reject atomic.Value // string
	mu     sync.Mutex
	log    []string // "model kind n"
	calls  atomic.Int64
	embeds atomic.Int64
	// prefixMiss counts bowalt inputs sent without their prompt.
	prefixMiss atomic.Int64
	delayMS    atomic.Int64
	// slow delays requests with an input containing this token by slowMS,
	// so embeddings of one unit (or one transaction) finish out of order.
	slow   atomic.Value // string
	slowMS atomic.Int64
	// done records "<first input>" in completion order (when recordDone).
	recordDone atomic.Bool
	doneLog    []string

	// OpenAI-compatible endpoint (/v1/embeddings) faults: the next
	// throttleN requests get 429 with Retry-After: retryAfter seconds; the
	// next malformedN get a 200 with one embedding missing. outage applies.
	throttleN    atomic.Int64
	retryAfter   atomic.Int64
	malformedN   atomic.Int64
	openaiCalls  atomic.Int64
	openaiInputs atomic.Int64
}

const mockAPIKey = "it-secret-key"

func (f *fakeEmbedder) handleOpenAI(w http.ResponseWriter, r *http.Request) {
	f.openaiCalls.Add(1)
	if r.Header.Get("Authorization") != "Bearer "+mockAPIKey {
		http.Error(w, `{"error":{"message":"bad key"}}`, http.StatusUnauthorized)
		return
	}
	if f.outage.Load() {
		http.Error(w, `{"error":{"message":"down"}}`, http.StatusServiceUnavailable)
		return
	}
	if f.throttleN.Load() > 0 {
		f.throttleN.Add(-1)
		w.Header().Set("Retry-After", strconv.FormatInt(f.retryAfter.Load(), 10))
		http.Error(w, `{"error":{"message":"rate limited"}}`, http.StatusTooManyRequests)
		return
	}
	var req struct {
		Model      string
		Input      []string
		Dimensions int
		InputType  string `json:"input_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if !strings.HasPrefix(req.Model, "fake/") {
		http.Error(w, `{"error":{"message":"unknown model"}}`, 404)
		return
	}
	if bad := f.reject.Load().(string); bad != "" {
		for _, s := range req.Input {
			if strings.Contains(s, bad) {
				http.Error(w, `{"error":{"message":"input rejected by policy"}}`, 400)
				return
			}
		}
	}
	kind := "document"
	if strings.HasPrefix(strings.Join(req.Input, ""), "query: ") || req.InputType == "query" {
		kind = "query"
	}
	f.mu.Lock()
	f.log = append(f.log, fmt.Sprintf("openai %s %s %d", req.Model, kind, len(req.Input)))
	f.mu.Unlock()
	type item struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	var data []item
	tokens := 0
	for i, s := range req.Input {
		data = append(data, item{Index: i, Embedding: encode(req.Model, s)})
		tokens += len(strings.Fields(s))
	}
	if f.malformedN.Load() > 0 && len(data) > 1 {
		f.malformedN.Add(-1)
		data = data[:len(data)-1] // one embedding short: invalid
	} else {
		f.openaiInputs.Add(int64(len(req.Input)))
	}
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "model": req.Model, "data": data,
		"usage": map[string]int{"prompt_tokens": tokens, "total_tokens": tokens}})
}

const dim = 64

func newFakeEmbedder(t *testing.T) *fakeEmbedder {
	f := &fakeEmbedder{}
	f.reject.Store("")
	f.slow.Store("")
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

var wordRe = regexp.MustCompile(`[a-z0-9]+`)

func hash(seed, s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(seed))
	h.Write([]byte{0})
	h.Write([]byte(s))
	return h.Sum64()
}

var synonyms = map[string]string{"automobile": "car", "vehicle": "car", "physician": "doctor", "medic": "doctor", "feline": "cat"}

func encode(model, text string) []float32 {
	v := make([]float64, dim)
	switch model {
	case "fake/noise":
		x := hash("noise", text)
		for i := range v {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			v[i] = float64(int64(x%2001)-1000) / 1000
		}
	default:
		if model == "fake/bowalt" {
			text = strings.TrimPrefix(strings.TrimPrefix(text, "query: "), "passage: ")
		}
		for _, w := range wordRe.FindAllString(strings.ToLower(text), -1) {
			// A few synonyms make the fake model "semantic": a paraphrase
			// lands on the same vector while sharing no word with the text.
			if s, ok := synonyms[w]; ok {
				w = s
			}
			h := hash("a", w)
			sign := 1.0
			if h&1 == 1 {
				sign = -1
			}
			v[(h>>1)%dim] += sign
		}
		// A small constant component keeps documents from being exactly
		// orthogonal: purely sparse vectors leave HNSW on plateaus of equal
		// distance where greedy search cannot find the one matching row.
		for i := range v {
			v[i] += 0.05
		}
		if model == "fake/bowalt" {
			p := make([]float64, dim)
			for i := range v {
				p[(i*7+3)%dim] = v[i] // 7 is coprime with 64: a permutation
			}
			v = p
		}
	}
	n := 0.0
	for _, x := range v {
		n += x * x
	}
	if n == 0 {
		v[0], n = 1, 1
	}
	out := make([]float32, dim)
	for i, x := range v {
		out[i] = float32(x / math.Sqrt(n))
	}
	return out
}

func (f *fakeEmbedder) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/embeddings" {
		f.handleOpenAI(w, r)
		return
	}
	if r.URL.Path == "/healthz" {
		w.Write([]byte(`{"status":"ok"}`))
		return
	}
	f.calls.Add(1)
	if f.outage.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	if d := f.delayMS.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	var req struct {
		Model, Revision, Kind string
		Inputs                []string
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if !strings.HasPrefix(req.Model, "fake/") || req.Revision != "rev1" {
		http.Error(w, "unknown model", 404)
		return
	}
	if tok := f.slow.Load().(string); tok != "" {
		for _, s := range req.Inputs {
			if strings.Contains(s, tok) {
				time.Sleep(time.Duration(f.slowMS.Load()) * time.Millisecond)
				break
			}
		}
	}
	if f.recordDone.Load() && len(req.Inputs) > 0 {
		defer func() {
			f.mu.Lock()
			f.doneLog = append(f.doneLog, req.Inputs[0])
			f.mu.Unlock()
		}()
	}
	if bad := f.reject.Load().(string); bad != "" {
		var idx []int
		for i, s := range req.Inputs {
			if strings.Contains(s, bad) {
				idx = append(idx, i)
			}
		}
		if len(idx) > 0 {
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]any{"detail": map[string]any{"error": "rejected", "rejected": idx}})
			return
		}
	}
	if req.Model == "fake/bowalt" {
		want := "passage: "
		if req.Kind == "query" {
			want = "query: "
		}
		for _, s := range req.Inputs {
			if !strings.HasPrefix(s, want) {
				f.prefixMiss.Add(1)
			}
		}
	}
	f.mu.Lock()
	f.log = append(f.log, fmt.Sprintf("%s %s %d", req.Model, req.Kind, len(req.Inputs)))
	f.mu.Unlock()
	f.embeds.Add(int64(len(req.Inputs)))
	out := map[string]any{"model": req.Model, "revision": req.Revision, "dimension": dim, "normalized": true}
	vecs := make([][]float32, len(req.Inputs))
	for i, s := range req.Inputs {
		vecs[i] = encode(req.Model, s)
	}
	out["embeddings"] = vecs
	json.NewEncoder(w).Encode(out)
}

// ---------------------------------------------------------------- environment

type env struct {
	t        *testing.T
	bin      string // binary under test (default: the one built from this tree)
	db       string
	slot     string
	dir      string
	cfgPath  string
	port     int
	admin    *pgxpool.Pool // superuser on the test database
	emb      *fakeEmbedder
	opts     envOpts
	envVars  []string
	mu       sync.Mutex
	procLogs []*syncBuffer
}

type envOpts struct {
	chunkWords   int
	chunkOverlap int
	batch        int // backfill batch size
	column       string
	onPoison     string
	evalQueries  string
	evalQrels    string
	// pipelines, when set, replaces the single-table source mapping with a
	// `pipelines:` block (YAML, indented by two spaces).
	pipelines string
	// views, when set, adds a `views:` block (YAML, indented by two spaces).
	views string
	// setupSQL runs as the admin before `embedsync init` (extra tables).
	setupSQL []string
	// embedBatch overrides embedder.batch_size (default 16).
	embedBatch int
	// binary runs every command with this binary instead of the current one.
	binary string
	// initialGen overrides initial_generation (default v1).
	initialGen string
	// Real endpoints (gated tests): embedderURL replaces the fake built-in
	// embedder; extraModels and extraProviders are appended (YAML, indented
	// by two and four spaces as under models: / providers:).
	embedderURL    string
	extraModels    string
	extraProviders string
	// search, when set, adds a `search:` block (YAML, indented by two spaces).
	search string
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func dsnFor(base, user, pw, db string) string {
	u, _ := url.Parse(base)
	if user != "" {
		u.User = url.UserPassword(user, pw)
	}
	u.Path = "/" + db
	return u.String()
}

func newEnv(t *testing.T, o envOpts) *env {
	t.Helper()
	if o.chunkWords == 0 {
		o.chunkWords, o.chunkOverlap = 20, 4
	}
	if o.batch == 0 {
		o.batch = 50
	}
	if o.column == "" {
		o.column = "body"
	}
	if o.onPoison == "" {
		o.onPoison = "quarantine"
	}
	ctx := context.Background()
	e := &env{t: t, db: "it_" + randHex(5), slot: "it_" + randHex(5), dir: t.TempDir(), emb: newFakeEmbedder(t), opts: o, bin: o.binary}
	if e.bin == "" {
		e.bin = binary
	}
	e.port = freePort(t)
	c, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "CREATE DATABASE "+e.db); err != nil {
		t.Fatal(err)
	}
	c.Exec(ctx, "GRANT CONNECT ON DATABASE "+e.db+" TO embedsync_replicator, embedsync_sink, embedsync_reader")
	c.Close(ctx)
	e.admin, err = pgxpool.New(ctx, dsnFor(adminDSN, "", "", e.db))
	if err != nil {
		t.Fatal(err)
	}
	e.exec(`CREATE TABLE public.documents (id bigint PRIMARY KEY, title text, body text, secret text, updated_at timestamptz DEFAULT now())`)
	e.exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	for _, q := range o.setupSQL {
		e.exec(q)
	}
	e.envVars = append(os.Environ(),
		"EMBEDSYNC_SOURCE_DSN="+dsnFor(adminDSN, "embedsync_replicator", "it-repl", e.db),
		"EMBEDSYNC_SINK_DSN="+dsnFor(adminDSN, "embedsync_sink", "it-sink", e.db),
		"EMBEDSYNC_READER_DSN="+dsnFor(adminDSN, "embedsync_reader", "it-reader", e.db),
		"EMBEDSYNC_SOURCE_ADMIN_DSN="+dsnFor(adminDSN, "", "", e.db),
		"EMBEDSYNC_SINK_ADMIN_DSN="+dsnFor(adminDSN, "", "", e.db),
		"EMBEDSYNC_IT_MOCK_KEY="+mockAPIKey,
	)
	e.writeConfig()
	t.Cleanup(e.cleanup)
	out, err := e.cli("init")
	if err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	return e
}

func (e *env) writeConfig() {
	o := e.opts
	models := `
  v1: {name: fake/bow, revision: rev1, dimension: 64, distance: cosine, hnsw: {m: 16, ef_construction: 64, ef_search: 100}}
  v2: {name: fake/bowalt, revision: rev1, dimension: 64, distance: cosine, query_prefix: "query: ", document_prefix: "passage: ", hnsw: {m: 16, ef_construction: 64, ef_search: 100}}
  v2b: {name: fake/bowalt, revision: rev1, dimension: 64, distance: cosine, query_prefix: "query: ", document_prefix: "passage: ", hnsw: {m: 8, ef_construction: 64, ef_search: 100}}
  bad: {name: fake/noise, revision: rev1, dimension: 64, distance: cosine}`
	providers := ""
	if o.binary == "" { // older binaries in upgrade tests reject unknown config fields
		models += `
  pv1: {provider: mock, name: fake/bow, revision: rev1, dimension: 64, distance: cosine, hnsw: {m: 16, ef_construction: 64, ef_search: 100}}
  pv2: {provider: mock, name: fake/bowalt, revision: rev1, dimension: 64, distance: cosine, query_prefix: "query: ", document_prefix: "passage: ", hnsw: {m: 16, ef_construction: 64, ef_search: 100}}
  pbad: {provider: mock, name: fake/noise, revision: rev1, dimension: 64, distance: cosine}`
		providers = fmt.Sprintf(`
providers:%s
  mock:
    kind: openai
    url: %s/v1
    api_key_env: EMBEDSYNC_IT_MOCK_KEY
    timeout: 5s
    batch_size: 8
    concurrency: 2
    retry: {initial_backoff: 20ms, max_backoff: 300ms}
    max_retry_after: 3s
    pricing: {usd_per_million_tokens: 0.02, source: "integration test fixture (not a real price)"}`, o.extraProviders, e.emb.srv.URL)
		models += o.extraModels
	}
	embURL := e.emb.srv.URL
	if o.embedderURL != "" {
		embURL = o.embedderURL
	}
	eval := ""
	if o.evalQueries != "" {
		eval = fmt.Sprintf("\n  eval_queries: %s\n  eval_qrels: %s", o.evalQueries, o.evalQrels)
	}
	tables := fmt.Sprintf(`
  table: public.documents
  key: id
  fields: [{name: title, column: title}, {name: body, column: %s}]`, o.column)
	pipes := ""
	if o.pipelines != "" {
		tables, pipes = "", "\npipelines:\n"+o.pipelines
	}
	if o.views != "" {
		tables, pipes = "", pipes+"\nviews:\n"+o.views
	}
	if o.search != "" {
		pipes += "\nsearch:\n" + o.search
	}
	initialGen := o.initialGen
	if initialGen == "" {
		initialGen = "v1"
	}
	embedBatch := o.embedBatch
	if embedBatch == 0 {
		embedBatch = 16
	}
	cfg := fmt.Sprintf(`
source:%s
  publication: embedsync_pub
  slot: %s%s
sink: {schema: embedsync, reader_role: embedsync_reader, reader_dsn_env: EMBEDSYNC_READER_DSN}
transform: {template: "{{.title}}\n{{.body}}", chunk_words: %d, chunk_overlap: %d}
embedder:
  url: %s
  timeout: 5s
  batch_size: %d
  concurrency: 3
  requests_per_second: 0
  retry: {initial_backoff: 20ms, max_backoff: 300ms}
models:%s
%s
initial_generation: %s
pipeline:
  tx_queue: 16
  max_inflight_units: 4
  max_batch_txns: 64
  max_batch_docs: 64
  max_document_bytes: 200000
  on_poison: %s
  idle_checkpoint_interval: 1s
  standby_interval: 1s
  shutdown_timeout: 10s
  stall_after: 15s
backfill: {batch_size: %d}
migration:
  rollback_window: 10m
  catchup_timeout: 60s%s
  gates: {min_ann_recall_at_10: 0.9, noninferiority_margin_ndcg: 0.02, noninferiority_margin_recall: 0.02, bootstrap_samples: 1000, seed: 7, require_zero_unresolved: true}
metrics: {listen: "127.0.0.1:%d"}
`, tables, e.slot, pipes, o.chunkWords, o.chunkOverlap, embURL, embedBatch, models, providers, initialGen, o.onPoison, o.batch, eval, e.port)
	e.cfgPath = filepath.Join(e.dir, "embedsync.yaml")
	if err := os.WriteFile(e.cfgPath, []byte(cfg), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) cleanup() {
	ctx := context.Background()
	if e.t.Failed() && os.Getenv("EMBEDSYNC_IT_KEEP") == "1" {
		e.t.Logf("keeping database %s and slot %s for inspection", e.db, e.slot)
		e.dumpLogs()
		return
	}
	e.admin.Close()
	c, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return
	}
	defer c.Close(ctx)
	for i := 0; i < 50; i++ {
		var active bool
		err := c.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, e.slot).Scan(&active)
		if err != nil || !active {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Exec(ctx, `SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1`, e.slot)
	c.Exec(ctx, "DROP DATABASE IF EXISTS "+e.db+" WITH (FORCE)")
	if e.t.Failed() {
		e.dumpLogs()
	}
}

func (e *env) dumpLogs() {
	{
		e.mu.Lock()
		for i, b := range e.procLogs {
			s := b.String()
			if len(s) > 6000 {
				s = s[len(s)-6000:]
			}
			e.t.Logf("---- process %d log tail ----\n%s", i, s)
		}
		e.mu.Unlock()
	}
}

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.admin.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
}

func (e *env) scalar(q string, args ...any) string {
	e.t.Helper()
	var s *string
	if err := e.admin.QueryRow(context.Background(), q, args...).Scan(&s); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	if s == nil {
		return ""
	}
	return *s
}

func (e *env) cliCmd(args ...string) *exec.Cmd {
	cmd := exec.Command(e.bin, append(args[:1:1], append([]string{"--config", e.cfgPath}, args[1:]...)...)...)
	cmd.Env = e.envVars
	return cmd
}

func (e *env) cli(args ...string) (string, error) {
	cmd := exec.Command(e.bin, append(args[:1:1], append([]string{"--config", e.cfgPath}, args[1:]...)...)...)
	if args[0] == "dlq" || (args[0] == "migrate" && len(args) > 1 && !strings.HasPrefix(args[1], "-")) {
		// subcommand first, then flags
		cmd = exec.Command(e.bin, append(args[:2:2], append([]string{"--config", e.cfgPath}, args[2:]...)...)...)
	}
	cmd.Env = e.envVars
	out, err := cmd.CombinedOutput()
	return string(out), err
}

type proc struct {
	cmd  *exec.Cmd
	done chan struct{}
	code int
	log  *syncBuffer
}

func (e *env) start(extraEnv ...string) *proc {
	e.t.Helper()
	cmd := exec.Command(e.bin, "run", "--config", e.cfgPath)
	for _, a := range extraEnv {
		if strings.HasPrefix(a, "--") {
			cmd.Args = append(cmd.Args, a)
		}
	}
	cmd.Env = e.envVars
	for _, a := range extraEnv {
		if !strings.HasPrefix(a, "--") {
			cmd.Env = append(cmd.Env, a)
		}
	}
	buf := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	p := &proc{cmd: cmd, done: make(chan struct{}), log: buf}
	e.mu.Lock()
	e.procLogs = append(e.procLogs, buf)
	e.mu.Unlock()
	go func() {
		err := cmd.Wait()
		p.code = 0
		if ee, ok := err.(*exec.ExitError); ok {
			p.code = ee.ExitCode()
			if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				p.code = -int(ws.Signal())
			}
		}
		close(p.done)
	}()
	e.t.Cleanup(func() { p.kill() })
	return p
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitLog waits until this process has logged substr.
func (p *proc) waitLog(t *testing.T, d time.Duration, substr string) {
	t.Helper()
	waitFor(t, d, "log line "+strconv.Quote(substr), func() bool { return strings.Contains(p.log.String(), substr) })
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *proc) kill() {
	if !p.exited() {
		p.cmd.Process.Signal(syscall.SIGKILL)
		<-p.done
	}
}

func (p *proc) stop(t *testing.T) {
	if p.exited() {
		return
	}
	p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		t.Fatal("process did not stop within 30s")
	}
	if p.code != 0 {
		t.Fatalf("graceful stop exited %d", p.code)
	}
}

func (p *proc) wait(t *testing.T, d time.Duration) int {
	select {
	case <-p.done:
		return p.code
	case <-time.After(d):
		t.Fatalf("process still running after %s", d)
	}
	return 0
}

func waitFor(t *testing.T, d time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

func (e *env) httpGet(path string) (int, []byte) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", e.port, path))
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	b.ReadFrom(resp.Body)
	return resp.StatusCode, b.Bytes()
}

func (e *env) waitReady() {
	e.t.Helper()
	waitFor(e.t, 60*time.Second, "readyz", func() bool { c, _ := e.httpGet("/readyz"); return c == 200 })
}

// status returns the parsed `status --json` output.
func (e *env) status() map[string]any {
	e.t.Helper()
	out, err := e.cli("status", "--json")
	if err != nil {
		e.t.Fatalf("status: %v %s", err, out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		e.t.Fatalf("status json: %v %s", err, out)
	}
	return m
}

type pipeCounts struct {
	ExpectedChunks int `json:"expected_chunks"`
	ActualChunks   int `json:"actual_chunks"`
	Missing        int `json:"missing"`
	Extra          int `json:"extra"`
	Stale          int `json:"stale"`
	MetaStale      int `json:"meta_stale"`
}

type genReport struct {
	Generation      string                `json:"generation"`
	ExpectedChunks  int                   `json:"expected_chunks"`
	ActualChunks    int                   `json:"actual_chunks"`
	Missing         int                   `json:"missing"`
	Extra           int                   `json:"extra"`
	Stale           int                   `json:"stale"`
	Quarantined     int                   `json:"quarantined_keys"`
	MissingQ        int                   `json:"missing_due_to_quarantine"`
	Examples        []string              `json:"example_problems"`
	MetaStale       int                   `json:"meta_stale"`
	FTSIndexMissing bool                  `json:"fts_index_missing"`
	SearchBuilding  bool                  `json:"search_columns_building"`
	Pipelines       map[string]pipeCounts `json:"pipelines"`
}

type stateCounts struct {
	Missing, Extra, Stale int
}

type verifyReport struct {
	BoundaryLSN   string      `json:"boundary_lsn"`
	CheckpointLSN string      `json:"checkpoint_lsn"`
	SourceRows    int         `json:"source_rows"`
	SourceState   stateCounts `json:"source_state"`
	Pipelines     map[string]struct {
		SourceRows  int         `json:"source_rows"`
		SourceState stateCounts `json:"source_state"`
	} `json:"pipelines"`
	Generations []genReport `json:"generations"`
	ViewOrphans int         `json:"view_orphan_rows"`
	OK          bool        `json:"ok"`
}

func (r verifyReport) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// verify runs `embedsync verify` (barrier, wait for checkpoint, full compare).
func (e *env) verify() verifyReport {
	e.t.Helper()
	out, err := e.cli("verify", "--json", "--timeout", "90s")
	var r verifyReport
	i := strings.Index(out, "{")
	if i < 0 {
		e.t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if jerr := json.Unmarshal([]byte(out[i:]), &r); jerr != nil {
		e.t.Fatalf("verify output: %v (%v)\n%s", jerr, err, out)
	}
	return r
}

func (e *env) requireConsistent(label string) verifyReport {
	e.t.Helper()
	r := e.verify()
	if !r.OK {
		e.t.Fatalf("%s: sink differs from source: %s", label, r)
	}
	for _, g := range r.Generations {
		if g.MetaStale != 0 || g.FTSIndexMissing {
			e.t.Fatalf("%s: generation %s has stale search columns (meta_stale=%d fts_index_missing=%v): %s", label, g.Generation, g.MetaStale, g.FTSIndexMissing, r)
		}
		e.t.Logf("%s: %s expected=%d actual=%d missing=%d extra=%d stale=%d meta_stale=%d fts_index_missing=%v search_building=%v quarantined=%d (source rows %d)",
			label, g.Generation, g.ExpectedChunks, g.ActualChunks, g.Missing, g.Extra, g.Stale, g.MetaStale, g.FTSIndexMissing, g.SearchBuilding, g.Quarantined, r.SourceRows)
		if len(g.Pipelines) > 1 {
			for name, c := range g.Pipelines {
				e.t.Logf("%s:   %s pipeline %s: source rows %d, chunks expected=%d actual=%d missing=%d extra=%d stale=%d",
					label, g.Generation, name, r.Pipelines[name].SourceRows, c.ExpectedChunks, c.ActualChunks, c.Missing, c.Extra, c.Stale)
			}
		}
	}
	return r
}

// checkpointLSN and confirmedLSN expose the feedback invariant.
func (e *env) checkpointLSN() string {
	return e.scalar(`SELECT lsn::text FROM embedsync.checkpoints WHERE consumer = 'main'`)
}

func (e *env) assertFeedbackNotAhead() {
	e.t.Helper()
	var ok *bool
	err := e.admin.QueryRow(context.Background(), `SELECT s.confirmed_flush_lsn <= c.lsn FROM pg_replication_slots s, embedsync.checkpoints c
		WHERE s.slot_name = $1 AND c.consumer = 'main'`, e.slot).Scan(&ok)
	if err != nil {
		e.t.Fatal(err)
	}
	if ok != nil && !*ok {
		e.t.Fatal("slot confirmed_flush_lsn is ahead of the durable checkpoint: feedback acknowledged unfinished work")
	}
}

func (e *env) metric(name string) float64 {
	_, b := e.httpGet("/metrics")
	total := 0.0
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, name) {
			var v float64
			fields := strings.Fields(line)
			fmt.Sscanf(fields[len(fields)-1], "%g", &v)
			total += v
		}
	}
	return total
}

// writer issues random committed and aborted transactions until stopped.
type writer struct {
	stop    chan struct{}
	wg      sync.WaitGroup
	commits atomic.Int64
	aborts  atomic.Int64
	errs    atomic.Int64
}

var vocab = strings.Fields(`replication slot wal lsn vector index search fresh chunk embed model backfill window watermark
 sourdough tomato marathon telescope jupiter coffee garden river mountain harbor violin quartz meadow lantern`)

func sentence(r *mrand, n int) string {
	w := make([]string, n)
	for i := range w {
		w[i] = vocab[r.Intn(len(vocab))]
	}
	return strings.Join(w, " ")
}

type mrand struct{ x uint64 }

func (m *mrand) Intn(n int) int {
	m.x ^= m.x << 13
	m.x ^= m.x >> 7
	m.x ^= m.x << 17
	return int(m.x % uint64(n))
}

func (e *env) startWriters(n int, keySpace int, seed uint64) *writer {
	return e.startPacedWriters(n, keySpace, seed, 0, 0)
}

// startPacedWriters is startWriters with a pause of about pace between each
// writer's transactions, for workloads that must stay within the pipeline's
// capacity (otherwise backfill windows wait behind an ever-growing backlog).
// Keys are drawn from (base, base+keySpace]; key changes move rows into
// (base+keySpace, base+2*keySpace].
func (e *env) startPacedWriters(n int, keySpace int, seed uint64, pace time.Duration, base int) *writer {
	w := &writer{stop: make(chan struct{})}
	for i := 0; i < n; i++ {
		w.wg.Add(1)
		go func(i int) {
			defer w.wg.Done()
			r := &mrand{x: seed + uint64(i)*7919 + 1}
			ctx := context.Background()
			for {
				select {
				case <-w.stop:
					return
				default:
				}
				tx, err := e.admin.Begin(ctx)
				if err != nil {
					w.errs.Add(1)
					time.Sleep(50 * time.Millisecond)
					continue
				}
				ops := 1 + r.Intn(4)
				failed := false
				for j := 0; j < ops && !failed; j++ {
					k := base + 1 + r.Intn(keySpace)
					var err error
					switch op := r.Intn(10); {
					case op < 4:
						_, err = tx.Exec(ctx, `INSERT INTO documents (id, title, body, secret) VALUES ($1, $2, $3, 'pw') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body`,
							k, sentence(r, 3), sentence(r, 5+r.Intn(60)))
					case op < 7:
						_, err = tx.Exec(ctx, `UPDATE documents SET title = $2 WHERE id = $1`, k, sentence(r, 4))
					case op < 9:
						_, err = tx.Exec(ctx, `DELETE FROM documents WHERE id = $1`, k)
					default:
						nk := base + keySpace + 1 + r.Intn(keySpace)
						_, err = tx.Exec(ctx, `UPDATE documents SET id = $2 WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM documents WHERE id = $2)`, k, nk)
					}
					if err != nil {
						failed = true
					}
				}
				if failed || r.Intn(5) == 0 {
					tx.Rollback(ctx)
					w.aborts.Add(1)
				} else if err := tx.Commit(ctx); err != nil {
					w.errs.Add(1)
				} else {
					w.commits.Add(1)
				}
				time.Sleep(time.Duration(r.Intn(4))*time.Millisecond + pace/2 + time.Duration(r.Intn(int(pace/time.Millisecond)+1))*time.Millisecond)
			}
		}(i)
	}
	return w
}

func (w *writer) halt() {
	close(w.stop)
	w.wg.Wait()
}

func (e *env) seed(n int, words int) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO documents (id, title, body, secret)
		SELECT g, 'title ' || g, repeat('seed body word ' || g || ' ', %d), 'secret ' || g FROM generate_series(1, %d) g`, words, n))
}
