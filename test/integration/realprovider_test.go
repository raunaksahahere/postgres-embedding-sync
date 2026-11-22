//go:build integration

package integration

// Real embedding endpoints. These tests are gated: each endpoint runs only
// when its environment variable is set, and a skipped endpoint is reported
// as not verified. Nothing here uses the fake embedder.
//
//	EMBEDSYNC_IT_CPU_EMBEDDER_URL  the built-in CPU embedder (embedder/ image), e.g. http://127.0.0.1:58001
//	EMBEDSYNC_IT_GPU_OPENAI_URL    an OpenAI-compatible GPU server, e.g. Ollama ROCm/CUDA: http://127.0.0.1:11435/v1
//	EMBEDSYNC_IT_GPU_MODEL         its model (default all-minilm) and EMBEDSYNC_IT_GPU_REVISION (pin, e.g. the Ollama digest)
//	GEMINI_API_KEY                 Gemini API (hosted; batchEmbedContents with task types)
//	EMBEDSYNC_IT_SCIFACT           path to BEIR SciFact corpus.jsonl.gz (throughput workload)
//	EMBEDSYNC_IT_RESULTS           optional JSON output path for the measurements

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type realEndpoint struct {
	name, provider, model string
	extraModels           string
	extraProviders        string
	embedderURL           string
	rows                  int // throughput workload size
	note                  string
}

func realEndpoints(t *testing.T) []realEndpoint {
	var out []realEndpoint
	if u := os.Getenv("EMBEDSYNC_IT_CPU_EMBEDDER_URL"); u != "" {
		out = append(out, realEndpoint{name: "cpu", provider: "local", model: "cpu_minilm", embedderURL: u, rows: 600,
			note: "built-in embedder (sentence-transformers all-MiniLM-L6-v2) on the CPU",
			extraModels: `
  cpu_minilm: {name: sentence-transformers/all-MiniLM-L6-v2, revision: 1110a243fdf4706b3f48f1d95db1a4f5529b4d41, dimension: 384, distance: cosine}`})
	}
	if u := os.Getenv("EMBEDSYNC_IT_GPU_OPENAI_URL"); u != "" {
		m := os.Getenv("EMBEDSYNC_IT_GPU_MODEL")
		if m == "" {
			m = "all-minilm"
		}
		rev := os.Getenv("EMBEDSYNC_IT_GPU_REVISION")
		if rev == "" {
			rev = "unpinned"
		}
		out = append(out, realEndpoint{name: "gpu", provider: "gpu", model: "gpu_minilm", rows: 600,
			note: "OpenAI-compatible server on a GPU (" + m + ")",
			extraProviders: fmt.Sprintf(`
  gpu:
    kind: openai
    url: %s
    timeout: 60s
    batch_size: 64
    concurrency: 2
    pricing: {usd_per_million_tokens: 0, source: "self-hosted GPU"}`, u),
			extraModels: fmt.Sprintf(`
  gpu_minilm: {provider: gpu, name: %s, revision: %q, dimension: 384, distance: cosine}`, m, rev)})
	}
	if os.Getenv("GEMINI_API_KEY") != "" {
		out = append(out, realEndpoint{name: "gemini", provider: "gemini", model: "gemini768", rows: 60,
			note: "Gemini API free tier (gemini-embedding-001, 768 dimensions, RETRIEVAL_DOCUMENT/QUERY task types); rate-limited to stay inside the free quota",
			extraProviders: `
  gemini:
    kind: gemini
    api_key_env: GEMINI_API_KEY
    timeout: 60s
    batch_size: 100
    concurrency: 1
    requests_per_second: 0.5
    burst: 1
    tokens_per_minute: 20000
    max_retry_after: 65s
    retry: {initial_backoff: 2s, max_backoff: 60s}
    pricing: {usd_per_million_tokens: 0, source: "Gemini API free tier (no charge for this run)"}`,
			extraModels: `
  gemini768: {provider: gemini, name: gemini-embedding-001, revision: gemini-embedding-001, dimension: 768, request_dimensions: 768, distance: cosine}`})
	}
	if len(out) == 0 {
		t.Skip("no real endpoint configured (EMBEDSYNC_IT_CPU_EMBEDDER_URL, EMBEDSYNC_IT_GPU_OPENAI_URL, GEMINI_API_KEY): real-endpoint verification NOT run")
	}
	return out
}

func loadSciFact(t *testing.T, e *env, rows int) {
	path := os.Getenv("EMBEDSYNC_IT_SCIFACT")
	if path == "" {
		t.Skip("EMBEDSYNC_IT_SCIFACT not set")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	n := 0
	for sc.Scan() && n < rows {
		var d struct {
			ID    string `json:"_id"`
			Title string `json:"title"`
			Text  string `json:"text"`
		}
		if json.Unmarshal(sc.Bytes(), &d) != nil {
			continue
		}
		n++
		e.exec(`INSERT INTO documents (id, title, body) VALUES ($1, $2, $3)`, n, d.Title, d.Text)
	}
}

type throughput struct {
	Endpoint      string  `json:"endpoint"`
	Note          string  `json:"note"`
	Real          bool    `json:"real_endpoint"`
	Rows          int     `json:"rows"`
	Embeddings    float64 `json:"embeddings"`
	Seconds       float64 `json:"seconds"`
	RowsPerSec    float64 `json:"rows_per_second"`
	EmbedsPerSec  float64 `json:"embeddings_per_second"`
	Tokens        float64 `json:"tokens"`
	TokenSource   string  `json:"token_source"`
	Retries       float64 `json:"retries"`
	P50RequestSec float64 `json:"mean_request_seconds"`
}

// measure backfills rows into a fresh generation of the endpoint's model
// and reports rows/s (source rows made searchable) and embeddings/s
// (chunk vectors computed), from the start of `run` to backfill done.
func measure(t *testing.T, ep realEndpoint, rows int, real bool, opts envOpts) throughput {
	opts.initialGen, opts.embedderURL, opts.extraModels, opts.extraProviders = ep.model, ep.embedderURL, ep.extraModels, ep.extraProviders
	opts.chunkWords, opts.chunkOverlap = 120, 20
	e := newEnv(t, opts)
	if real {
		loadSciFact(t, e, rows)
	} else {
		e.seed(rows, 200)
	}
	start := time.Now()
	p := e.start()
	waitFor(t, 30*time.Minute, "backfill of "+ep.name, func() bool { return e.jobsDone() })
	secs := time.Since(start).Seconds()
	prov := ep.provider
	inputs := e.metric(fmt.Sprintf(`embedsync_provider_inputs_total{provider=%q}`, prov))
	tokens := e.metric(fmt.Sprintf(`embedsync_provider_tokens_total{provider=%q,source="reported"}`, prov))
	src := "reported"
	if tokens == 0 {
		tokens, src = e.metric(fmt.Sprintf(`embedsync_provider_tokens_total{provider=%q,source="estimated"}`, prov)), "estimated"
	}
	reqSum := e.metric(fmt.Sprintf(`embedsync_provider_request_seconds_sum{provider=%q}`, prov))
	reqN := e.metric(fmt.Sprintf(`embedsync_provider_request_seconds_count{provider=%q}`, prov))
	retries := e.metric(fmt.Sprintf(`embedsync_provider_retries_total{provider=%q`, prov))
	r := e.requireConsistent("real endpoint " + ep.name)
	out := throughput{Endpoint: ep.name, Note: ep.note, Real: real, Rows: r.SourceRows, Embeddings: inputs, Seconds: secs,
		RowsPerSec: float64(r.SourceRows) / secs, EmbedsPerSec: inputs / secs, Tokens: tokens, TokenSource: src, Retries: retries}
	if reqN > 0 {
		out.P50RequestSec = reqSum / reqN
	}
	p.stop(t)
	return out
}

// Throughput of the same workload through each real endpoint (and the mock
// for reference, labelled as such). Rows/s counts source rows made
// searchable; embeddings/s counts chunk vectors (a row has several chunks).
func TestRealProviderThroughput(t *testing.T) {
	eps := realEndpoints(t)
	var results []throughput
	for _, ep := range eps {
		res := measure(t, ep, ep.rows, true, envOpts{batch: 200})
		t.Logf("REAL %-6s rows=%d embeddings=%.0f time=%.1fs rows/s=%.1f embeddings/s=%.1f tokens=%.0f (%s) retries=%.0f mean request=%.3fs",
			res.Endpoint, res.Rows, res.Embeddings, res.Seconds, res.RowsPerSec, res.EmbedsPerSec, res.Tokens, res.TokenSource, res.Retries, res.P50RequestSec)
		results = append(results, res)
	}
	mock := measure(t, realEndpoint{name: "mock", provider: "mock", model: "pv1", note: "MOCK: in-process fake OpenAI-compatible server (no model; not a real endpoint)"},
		600, false, envOpts{batch: 200})
	t.Logf("MOCK   rows=%d embeddings=%.0f time=%.1fs rows/s=%.1f embeddings/s=%.1f (not a real endpoint)", mock.Rows, mock.Embeddings, mock.Seconds, mock.RowsPerSec, mock.EmbedsPerSec)
	results = append(results, mock)
	if path := os.Getenv("EMBEDSYNC_IT_RESULTS"); path != "" {
		b, _ := json.MarshalIndent(map[string]any{"measured_at": time.Now().UTC().Format(time.RFC3339), "results": results}, "", "  ")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A cross-provider migration on real endpoints through the existing quality
// gate (the CI retrieval fixture over the demo corpus): the built-in CPU
// embedder's all-MiniLM-L6-v2 to the same model served by the GPU server
// (expected to pass), rollback, then to the hosted model if configured
// (whatever the gate decides is reported).
func TestRealCrossProviderMigration(t *testing.T) {
	cpu := os.Getenv("EMBEDSYNC_IT_CPU_EMBEDDER_URL")
	if cpu == "" || os.Getenv("EMBEDSYNC_IT_GPU_OPENAI_URL") == "" {
		t.Skip("needs EMBEDSYNC_IT_CPU_EMBEDDER_URL and EMBEDSYNC_IT_GPU_OPENAI_URL: real cross-provider migration NOT run")
	}
	var models, providers string
	for _, ep := range realEndpoints(t) {
		models += ep.extraModels
		providers += ep.extraProviders
	}
	seed, err := os.ReadFile("../../examples/seed.sql")
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, envOpts{initialGen: "cpu_minilm", embedderURL: cpu, extraModels: models, extraProviders: providers,
		chunkWords: 120, chunkOverlap: 20,
		evalQueries: absPath(t, "../../eval/fixtures/ci/queries.jsonl"), evalQrels: absPath(t, "../../eval/fixtures/ci/qrels.tsv"),
		setupSQL: []string{`DROP TABLE public.documents`, string(seed)}})
	p := e.start()
	e.waitReady()
	e.requireConsistent("cpu generation")
	run := func(target string) (string, error) {
		out, err := e.cli("migrate", "--to", target)
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, "recall@10=") || strings.Contains(l, "Δ nDCG") || strings.HasPrefix(l, "verdict") {
				t.Logf("%s: %s", target, strings.TrimSpace(l))
			}
		}
		return out, err
	}
	if out, err := run("gpu_minilm"); err != nil {
		t.Fatalf("CPU -> GPU migration (same model) should pass the gate: %v\n%s", err, out)
	}
	if g, prov := e.searchProvider("replication slot disk usage"); g != "gpu_minilm" || prov != "gpu" {
		t.Fatalf("after promotion search must use the GPU provider: %s/%s", g, prov)
	}
	if out, err := e.cli("migrate", "rollback"); err != nil {
		t.Fatalf("rollback: %v %s", err, out)
	}
	if g, prov := e.searchProvider("replication slot disk usage"); g != "cpu_minilm" || prov != "local" {
		t.Fatalf("after rollback search must use the CPU embedder: %s/%s", g, prov)
	}
	if os.Getenv("GEMINI_API_KEY") != "" {
		out, err := run("gemini768")
		t.Logf("CPU -> Gemini migration: err=%v (gate verdict above)", err)
		if err != nil && !strings.Contains(out, "REJECTED") {
			t.Fatalf("CPU -> Gemini migration failed outside the gate: %v\n%s", err, out)
		}
		if err == nil {
			if g, prov := e.searchProvider("replication slot disk usage"); g != "gemini768" || prov != "gemini" {
				t.Fatalf("after promotion search must use Gemini: %s/%s", g, prov)
			}
			if out, err := e.cli("migrate", "rollback"); err != nil {
				t.Fatalf("rollback from Gemini: %v %s", err, out)
			}
		}
	}
	e.requireConsistent("after real cross-provider migrations")
	p.stop(t)
}

func absPath(t *testing.T, rel string) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return wd + "/" + rel
}
