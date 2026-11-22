package embedclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type retryEvent struct {
	reason string
	wait   time.Duration
}

type recTel struct {
	mu       sync.Mutex
	outcomes []string
	retries  []retryEvent
	embedded []Usage
}

func (r *recTel) Request(_, outcome string, _ time.Duration, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
}

func (r *recTel) Retry(_, reason string, wait time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retries = append(r.retries, retryEvent{reason, wait})
}

func (r *recTel) Embedded(_ string, _ int, u Usage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.embedded = append(r.embedded, u)
}

func (r *recTel) snapshot() ([]string, []retryEvent, []Usage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.outcomes), slices.Clone(r.retries), slices.Clone(r.embedded)
}

// vecFor is the fake model: dim values, the first encoding len(s)+1.
func vecFor(s string, dim int) []float32 {
	v := make([]float32, dim)
	v[0] = float32(len(s) + 1)
	for i := 1; i < dim; i++ {
		v[i] = float32(i)
	}
	return v
}

type oaiItem struct {
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

// writeOK answers an OpenAI request with indexed data in reverse order.
func writeOK(w http.ResponseWriter, inputs []string, dim int, usage map[string]int) {
	data := make([]oaiItem, 0, len(inputs))
	for i := len(inputs) - 1; i >= 0; i-- {
		data = append(data, oaiItem{Index: i, Embedding: vecFor(inputs[i], dim)})
	}
	body := map[string]any{"object": "list", "data": data, "model": "m"}
	if usage != nil {
		body["usage"] = usage
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func decodeOAI(t *testing.T, r *http.Request) openAIRequest {
	t.Helper()
	var req openAIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Errorf("decode request: %v", err)
	}
	return req
}

func testCfg(url string) HTTPConfig {
	return HTTPConfig{
		Name:           "test",
		BaseURL:        url,
		Timeout:        5 * time.Second,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	}
}

func checkVectors(t *testing.T, inputs []string, vectors map[int][]float32, skip ...int) {
	t.Helper()
	for i, s := range inputs {
		if slices.Contains(skip, i) {
			if _, ok := vectors[i]; ok {
				t.Errorf("input %d should have no vector", i)
			}
			continue
		}
		v, ok := vectors[i]
		if !ok || v[0] != float32(len(s)+1) {
			t.Errorf("input %d: vector %v, want first value %d", i, v, len(s)+1)
		}
	}
}

func TestOpenAIHappyPath(t *testing.T) {
	cases := []struct {
		name       string
		dims       int
		inputType  string
		wantFields map[string]any // nil value = field must be absent
	}{
		{"passthrough", 3, "query", map[string]any{"dimensions": float64(3), "input_type": "query"}},
		{"defaults", 0, "", map[string]any{"dimensions": nil, "input_type": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/embeddings" || r.Method != http.MethodPost {
					t.Errorf("got %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
					t.Errorf("Authorization = %q", got)
				}
				if got := r.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q", got)
				}
				raw, _ := io.ReadAll(r.Body)
				var m map[string]any
				if err := json.Unmarshal(raw, &m); err != nil {
					t.Fatal(err)
				}
				if m["model"] != "text-embedding-3-small" || m["encoding_format"] != "float" {
					t.Errorf("model/encoding_format: %v", m)
				}
				for k, want := range tc.wantFields {
					got, present := m[k]
					if want == nil && present {
						t.Errorf("%s should be absent, got %v", k, got)
					}
					if want != nil && got != want {
						t.Errorf("%s = %v, want %v", k, got, want)
					}
				}
				var req openAIRequest
				_ = json.Unmarshal(raw, &req)
				writeOK(w, req.Input, 3, map[string]int{"prompt_tokens": 7, "total_tokens": 7})
			}))
			defer srv.Close()
			tel := &recTel{}
			cfg := testCfg(srv.URL + "/v1/") // trailing slash tolerated
			cfg.APIKey = "sk-test"
			p := NewHTTP(cfg, tel)
			inputs := []string{"a", "bb", "ccc"}
			req := Request{Model: "text-embedding-3-small", Revision: "r1", Inputs: inputs, Dimensions: tc.dims, InputType: tc.inputType, Expect: 3}

			resp, err := p.EmbedOnce(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Model != req.Model || resp.Revision != "r1" || resp.Dimension != 3 {
				t.Errorf("response header %+v", resp)
			}
			for i, s := range inputs {
				if resp.Embeddings[i][0] != float32(len(s)+1) {
					t.Errorf("order not restored: %v", resp.Embeddings)
				}
			}
			vectors, rejected, err := p.Embed(context.Background(), req)
			if err != nil || len(rejected) != 0 {
				t.Fatalf("Embed: %v %v", err, rejected)
			}
			checkVectors(t, inputs, vectors)
			outcomes, _, embedded := tel.snapshot()
			if !slices.Equal(outcomes, []string{"ok", "ok"}) {
				t.Errorf("outcomes %v", outcomes)
			}
			if len(embedded) != 2 || embedded[0] != (Usage{Tokens: 7, Reported: true}) {
				t.Errorf("usage %v", embedded)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{"seconds", map[string]string{"Retry-After": "1"}, time.Second},
		{"fractional seconds", map[string]string{"Retry-After": "0.5"}, 500 * time.Millisecond},
		{"http date", map[string]string{"Retry-After": now.Add(3 * time.Second).Format(http.TimeFormat)}, 3 * time.Second},
		{"past date", map[string]string{"Retry-After": now.Add(-time.Minute).Format(http.TimeFormat)}, 0},
		{"ms", map[string]string{"retry-after-ms": "150"}, 150 * time.Millisecond},
		{"ms wins", map[string]string{"retry-after-ms": "150", "Retry-After": "9"}, 150 * time.Millisecond},
		{"bad ms falls back", map[string]string{"retry-after-ms": "soon", "Retry-After": "2"}, 2 * time.Second},
		{"garbage", map[string]string{"Retry-After": "later"}, 0},
		{"negative", map[string]string{"Retry-After": "-4"}, 0},
		{"none", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if got := parseRetryAfter(h, now); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOpenAIThrottled(t *testing.T) {
	cases := []struct {
		name          string
		header, value string
		maxRetryAfter time.Duration
		minWait       time.Duration
		maxWait       time.Duration
	}{
		{"retry-after-ms honoured", "retry-after-ms", "150", 0, 150 * time.Millisecond, 2 * time.Second},
		{"retry-after capped", "Retry-After", "3600", 60 * time.Millisecond, 60 * time.Millisecond, 2 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeOAI(t, r)
				if calls.Add(1) == 1 {
					w.Header().Set(tc.header, tc.value)
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = io.WriteString(w, `{"error":{"message":"rate limited"}}`)
					return
				}
				writeOK(w, req.Input, 2, nil)
			}))
			defer srv.Close()
			tel := &recTel{}
			cfg := testCfg(srv.URL)
			cfg.MaxRetryAfter = tc.maxRetryAfter
			p := NewHTTP(cfg, tel)
			inputs := []string{"a", "b"}
			start := time.Now()
			vectors, _, err := p.Embed(context.Background(), Request{Model: "m", Inputs: inputs})
			elapsed := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			checkVectors(t, inputs, vectors)
			if elapsed < tc.minWait || elapsed > tc.maxWait {
				t.Errorf("elapsed %v, want in [%v, %v]", elapsed, tc.minWait, tc.maxWait)
			}
			outcomes, retries, _ := tel.snapshot()
			if !slices.Equal(outcomes, []string{"throttled", "ok"}) {
				t.Errorf("outcomes %v", outcomes)
			}
			if len(retries) != 1 || retries[0].reason != "throttled" || retries[0].wait < tc.minWait {
				t.Errorf("retries %v", retries)
			}
		})
	}

	t.Run("EmbedOnce exposes the wait", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()
		_, err := NewHTTP(testCfg(srv.URL), nil).EmbedOnce(context.Background(), Request{Model: "m", Inputs: []string{"a"}})
		var tr *TransientError
		if !errors.As(err, &tr) {
			t.Fatalf("want TransientError, got %v", err)
		}
		if got := RetryAfterOf(err); got != 2*time.Second {
			t.Errorf("RetryAfterOf = %v", got)
		}
	})
}

func TestOpenAIOutage(t *testing.T) {
	t.Run("recovers", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req := decodeOAI(t, r)
			if calls.Add(1) <= 3 {
				http.Error(w, "overloaded", http.StatusServiceUnavailable)
				return
			}
			writeOK(w, req.Input, 2, nil)
		}))
		defer srv.Close()
		tel := &recTel{}
		inputs := []string{"a", "bb"}
		vectors, _, err := NewHTTP(testCfg(srv.URL), tel).Embed(context.Background(), Request{Model: "m", Inputs: inputs})
		if err != nil {
			t.Fatal(err)
		}
		checkVectors(t, inputs, vectors)
		outcomes, retries, _ := tel.snapshot()
		if !slices.Equal(outcomes, []string{"unavailable", "unavailable", "unavailable", "ok"}) {
			t.Errorf("outcomes %v", outcomes)
		}
		if len(retries) != 3 || retries[0].reason != "unavailable" {
			t.Errorf("retries %v", retries)
		}
	})

	t.Run("context cancel", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "down", http.StatusBadGateway)
		}))
		defer srv.Close()
		cfg := testCfg(srv.URL)
		cfg.InitialBackoff, cfg.MaxBackoff = 20*time.Millisecond, 50*time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, _, err := NewHTTP(cfg, nil).Embed(ctx, Request{Model: "m", Inputs: []string{"a"}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want deadline exceeded, got %v", err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("returned after %v", d)
		}
	})

	t.Run("connection refused is transient", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		url := srv.URL
		srv.Close()
		tel := &recTel{}
		_, err := NewHTTP(testCfg(url), tel).EmbedOnce(context.Background(), Request{Model: "m", Inputs: []string{"a"}})
		var tr *TransientError
		if !errors.As(err, &tr) {
			t.Fatalf("want TransientError, got %v", err)
		}
		if outcomes, _, _ := tel.snapshot(); !slices.Equal(outcomes, []string{"unavailable"}) {
			t.Errorf("outcomes %v", outcomes)
		}
	})
}

func TestOpenAIMalformedResponses(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"wrong count", `{"data":[{"index":0,"embedding":[1,2,3]}]}`},
		{"duplicate index", `{"data":[{"index":1,"embedding":[1,2,3]},{"index":1,"embedding":[1,2,3]}]}`},
		{"index out of range", `{"data":[{"index":0,"embedding":[1,2,3]},{"index":5,"embedding":[1,2,3]}]}`},
		{"dimension differs from Expect", `{"data":[{"index":0,"embedding":[1,2,3,4]},{"index":1,"embedding":[1,2,3,4]}]}`},
		{"lengths differ", `{"data":[{"index":0,"embedding":[1,2,3]},{"index":1,"embedding":[1,2]}]}`},
		{"empty embedding", `{"data":[{"index":0,"embedding":[]},{"index":1,"embedding":[1,2,3]}]}`},
		{"overflowing value", `{"data":[{"index":0,"embedding":[1e400,1,1]},{"index":1,"embedding":[1,2,3]}]}`},
		{"all zeros", `{"data":[{"index":0,"embedding":[0,0,0]},{"index":1,"embedding":[1,2,3]}]}`},
		{"base64 instead of floats", `{"data":[{"index":0,"embedding":"AAAA"},{"index":1,"embedding":"AAAA"}]}`},
		{"not json", `<html>gateway</html>`},
	}
	inputs := []string{"x", "yy"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeOAI(t, r)
				if n := calls.Add(1); n == 1 || n == 2 {
					_, _ = io.WriteString(w, tc.body)
					return
				}
				writeOK(w, req.Input, 3, nil)
			}))
			defer srv.Close()
			tel := &recTel{}
			p := NewHTTP(testCfg(srv.URL), tel)
			req := Request{Model: "m", Inputs: inputs, Expect: 3}

			_, err := p.EmbedOnce(context.Background(), req)
			var tr *TransientError
			if !errors.As(err, &tr) {
				t.Fatalf("EmbedOnce: want TransientError, got %v", err)
			}
			vectors, _, err := p.Embed(context.Background(), req)
			if err != nil {
				t.Fatalf("Embed: %v", err)
			}
			checkVectors(t, inputs, vectors)
			outcomes, retries, _ := tel.snapshot()
			if !slices.Equal(outcomes, []string{"invalid", "invalid", "ok"}) {
				t.Errorf("outcomes %v", outcomes)
			}
			if len(retries) != 1 || retries[0].reason != "invalid" {
				t.Errorf("retries %v", retries)
			}
		})
	}
}

func TestOpenAIBatching(t *testing.T) {
	cases := []struct {
		name      string
		inputs    []string
		maxInputs int
		maxTokens int
		want      []int // inputs per request, in order
	}{
		{"by count", slices.Repeat([]string{"abcd"}, 10), 4, 0, []int{4, 4, 2}},
		{"by tokens", slices.Repeat([]string{strings.Repeat("t", 40)}, 5), 0, 25, []int{2, 2, 1}},
		{"oversize input alone", []string{"12345678", strings.Repeat("z", 400), "abcdefgh", "ijklmnop"}, 0, 25, []int{1, 1, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var sizes []int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeOAI(t, r)
				mu.Lock()
				sizes = append(sizes, len(req.Input))
				mu.Unlock()
				writeOK(w, req.Input, 2, nil)
			}))
			defer srv.Close()
			cfg := testCfg(srv.URL)
			cfg.MaxBatchInputs, cfg.MaxBatchTokens = tc.maxInputs, tc.maxTokens
			inputs := slices.Clone(tc.inputs)
			for i := range inputs { // make every input distinct in length
				inputs[i] += strings.Repeat(".", i)
			}
			vectors, _, err := NewHTTP(cfg, nil).Embed(context.Background(), Request{Model: "m", Inputs: inputs})
			if err != nil {
				t.Fatal(err)
			}
			checkVectors(t, inputs, vectors)
			if !slices.Equal(sizes, tc.want) {
				t.Errorf("request sizes %v, want %v", sizes, tc.want)
			}
		})
	}

	t.Run("EmbedOnce refuses an oversize batch", func(t *testing.T) {
		cfg := testCfg("http://127.0.0.1:1")
		cfg.MaxBatchInputs = 2
		_, err := NewHTTP(cfg, nil).EmbedOnce(context.Background(), Request{Model: "m", Inputs: []string{"a", "b", "c"}})
		if err == nil || !strings.Contains(err.Error(), "batch limit") {
			t.Errorf("got %v", err)
		}
	})
}

func TestOpenAIBisectsPoisonInputs(t *testing.T) {
	inputs := []string{"a", "bb", "POISON one", "dddd", "eeeee", "ffffff", "ggggggg", "POISON two", "iiiiiiiii"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeOAI(t, r)
		for _, s := range req.Input {
			if strings.Contains(s, "POISON") {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"message":"input contains POISON","type":"invalid_request_error"}}`)
				return
			}
		}
		writeOK(w, req.Input, 2, nil)
	}))
	defer srv.Close()
	tel := &recTel{}
	vectors, rejected, err := NewHTTP(testCfg(srv.URL), tel).Embed(context.Background(), Request{Model: "m", Inputs: inputs})
	if err != nil {
		t.Fatal(err)
	}
	checkVectors(t, inputs, vectors, 2, 7)
	if len(vectors) != 7 || len(rejected) != 2 {
		t.Fatalf("vectors %d rejected %v", len(vectors), rejected)
	}
	for _, i := range []int{2, 7} {
		if !strings.Contains(rejected[i], "POISON") || !strings.Contains(rejected[i], "400") {
			t.Errorf("rejected[%d] = %q", i, rejected[i])
		}
	}
	if _, retries, _ := tel.snapshot(); len(retries) != 0 {
		t.Errorf("rejections must not be retried: %v", retries)
	}
}

func TestOpenAIRequestWide400IsPermanent(t *testing.T) {
	const msg = `Invalid JSON payload received. Unknown name "input_type": Cannot find field.`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": 400, "message": msg, "status": "INVALID_ARGUMENT"}})
	}))
	defer srv.Close()
	p := NewHTTP(testCfg(srv.URL), nil)
	// A single input refused by a provider that also refuses a known-good
	// probe is a configuration error, never a quarantine.
	t.Run("1 input: probe refused too, permanent", func(t *testing.T) {
		vectors, rejected, err := p.Embed(context.Background(), Request{Model: "m", Inputs: []string{"text"}})
		if err == nil || !strings.Contains(err.Error(), "probe") || len(rejected) != 0 || len(vectors) != 0 {
			t.Fatalf("single refused input with a broken provider: err %v vectors %v rejected %v", err, vectors, rejected)
		}
	})
	for _, n := range []int{2, 9} {
		t.Run(fmt.Sprintf("%d inputs", n), func(t *testing.T) {
			inputs := slices.Repeat([]string{"text"}, n)
			vectors, rejected, err := p.Embed(context.Background(), Request{Model: "m", Inputs: inputs, InputType: "query"})
			var tr *TransientError
			var rej *RejectedError
			if err == nil || errors.As(err, &tr) || errors.As(err, &rej) {
				t.Fatalf("want permanent error, got %v", err)
			}
			if !strings.Contains(err.Error(), `"test"`) || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "input_type") {
				t.Errorf("error lacks provider/status/message: %v", err)
			}
			if len(rejected) != 0 || len(vectors) != 0 {
				t.Errorf("vectors %v rejected %v", vectors, rejected)
			}
		})
	}
	t.Run("EmbedOnce single input is RejectedError", func(t *testing.T) {
		_, err := p.EmbedOnce(context.Background(), Request{Model: "m", Inputs: []string{"text"}})
		var rej *RejectedError
		if !errors.As(err, &rej) || !slices.Equal(rej.Indexes, []int{0}) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestOpenAIAuthErrorsArePermanent(t *testing.T) {
	const key = "sk-very-secret-123"
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":{"message":"Incorrect API key provided: `+key+`"}}`)
			}))
			defer srv.Close()
			tel := &recTel{}
			cfg := testCfg(srv.URL)
			cfg.Name, cfg.APIKey = "openai-prod", key
			_, _, err := NewHTTP(cfg, tel).Embed(context.Background(), Request{Model: "m", Inputs: []string{"a", "b"}})
			var tr *TransientError
			var rej *RejectedError
			if err == nil || errors.As(err, &tr) || errors.As(err, &rej) {
				t.Fatalf("want permanent error, got %v", err)
			}
			if !strings.Contains(err.Error(), "openai-prod") || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Errorf("error %q lacks provider or status", err)
			}
			if strings.Contains(err.Error(), key) {
				t.Errorf("error leaks the API key: %q", err)
			}
			if outcomes, retries, _ := tel.snapshot(); !slices.Equal(outcomes, []string{"error"}) || len(retries) != 0 {
				t.Errorf("outcomes %v retries %v", outcomes, retries)
			}
		})
	}
}

// TestOpenAIGeminiCompatShape mirrors Gemini's OpenAI-compatible endpoint:
// no index fields and no usage object.
func TestOpenAIGeminiCompatShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeOAI(t, r)
		if req.Dimensions != 768 || req.EncodingFormat != "float" {
			t.Errorf("request %+v", req)
		}
		type item struct {
			Object    string    `json:"object"`
			Embedding []float32 `json:"embedding"`
		}
		data := make([]item, len(req.Input))
		for i, s := range req.Input {
			data[i] = item{"embedding", vecFor(s, 768)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "model": "gemini-embedding-001"})
	}))
	defer srv.Close()
	tel := &recTel{}
	inputs := []string{"abcde", "abcdefghi", "z"} // estimates 2 + 3 + 1
	vectors, _, err := NewHTTP(testCfg(srv.URL), tel).Embed(context.Background(),
		Request{Model: "gemini-embedding-001", Inputs: inputs, Dimensions: 768, Expect: 768})
	if err != nil {
		t.Fatal(err)
	}
	checkVectors(t, inputs, vectors)
	if _, _, embedded := tel.snapshot(); len(embedded) != 1 || embedded[0] != (Usage{Tokens: 6, Reported: false}) {
		t.Errorf("usage %v", embedded)
	}
}

func TestOpenAITokensPerMinute(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeOAI(t, r)
		writeOK(w, req.Input, 2, nil)
	}))
	defer srv.Close()
	cfg := testCfg(srv.URL)
	cfg.TokensPerMinute = 60000 // 1000 tokens/s, burst 60000
	p := NewHTTP(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 75000 estimated tokens exceed the burst: must wait for a full bucket
	// (available now) rather than fail or block forever.
	start := time.Now()
	if _, _, err := p.Embed(ctx, Request{Model: "m", Inputs: []string{strings.Repeat("x", 300000)}}); err != nil {
		t.Fatal(err)
	}
	// The bucket is now empty: 100 more tokens take 100ms to refill,
	// counted from the first request's wait.
	if _, _, err := p.Embed(ctx, Request{Model: "m", Inputs: []string{strings.Repeat("y", 400)}}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 90*time.Millisecond {
		t.Errorf("token budget not enforced: both requests done after %v", d)
	}
}

func TestHTTPBackoff(t *testing.T) {
	p := NewHTTP(HTTPConfig{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, nil)
	for attempt := range 40 {
		ceil := min(10*time.Millisecond<<min(attempt, 10), 50*time.Millisecond)
		for range 20 {
			if d := p.Backoff(attempt); d < 0 || d > ceil {
				t.Fatalf("Backoff(%d) = %v, want within [0, %v]", attempt, d, ceil)
			}
		}
	}
	def := NewHTTP(HTTPConfig{}, nil)
	if d := def.Backoff(100); d < 0 || d > 15*time.Second {
		t.Errorf("default Backoff(100) = %v", d)
	}
	if def.MaxBatch() != 64 {
		t.Errorf("default MaxBatch = %d", def.MaxBatch())
	}
}

func TestHTTPUnknownKind(t *testing.T) {
	p := NewHTTP(HTTPConfig{Name: "x", Kind: "cohere", BaseURL: "http://127.0.0.1:1"}, nil)
	_, _, err := p.Embed(context.Background(), Request{Model: "m", Inputs: []string{"a"}})
	var tr *TransientError
	if err == nil || errors.As(err, &tr) || !strings.Contains(err.Error(), "cohere") {
		t.Errorf("got %v", err)
	}
}

func geminiOK(w http.ResponseWriter, body geminiBatchRequest, dim int) {
	type emb struct {
		Values []float32 `json:"values"`
	}
	out := make([]emb, len(body.Requests))
	for i, r := range body.Requests {
		out[i] = emb{vecFor(r.Content.Parts[0].Text, dim)}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": out})
}

func TestGeminiCodec(t *testing.T) {
	cases := []struct {
		name      string
		model     string
		kind      Kind
		inputType string
		dims      int
		wantTask  string
	}{
		{"document", "gemini-embedding-001", Document, "", 0, "RETRIEVAL_DOCUMENT"},
		{"query", "gemini-embedding-001", Query, "", 768, "RETRIEVAL_QUERY"},
		{"input type overrides kind", "models/gemini-embedding-001", Query, "SEMANTIC_SIMILARITY", 1536, "SEMANTIC_SIMILARITY"},
		{"empty kind is document", "gemini-embedding-001", "", "", 0, "RETRIEVAL_DOCUMENT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dim := max(tc.dims, 3)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1beta/models/gemini-embedding-001:batchEmbedContents" {
					t.Errorf("got %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("x-goog-api-key"); got != "g-key" {
					t.Errorf("x-goog-api-key = %q", got)
				}
				if got := r.Header.Get("Authorization"); got != "" {
					t.Errorf("unexpected Authorization header %q", got)
				}
				raw, _ := io.ReadAll(r.Body)
				var generic struct {
					Requests []map[string]any `json:"requests"`
				}
				var body geminiBatchRequest
				if json.Unmarshal(raw, &generic) != nil || json.Unmarshal(raw, &body) != nil {
					t.Fatalf("bad body %s", raw)
				}
				for i, rq := range body.Requests {
					if rq.Model != "models/gemini-embedding-001" || rq.TaskType != tc.wantTask || rq.OutputDimensionality != tc.dims {
						t.Errorf("request %d = %+v", i, rq)
					}
					if _, present := generic.Requests[i]["outputDimensionality"]; present != (tc.dims > 0) {
						t.Errorf("outputDimensionality presence = %v for dims %d", present, tc.dims)
					}
				}
				geminiOK(w, body, dim)
			}))
			defer srv.Close()
			tel := &recTel{}
			cfg := testCfg(srv.URL + "/v1beta")
			cfg.Kind, cfg.APIKey = KindGemini, "g-key"
			inputs := []string{"one", "three", "fifteen chars.."}
			vectors, _, err := NewHTTP(cfg, tel).Embed(context.Background(), Request{
				Model: tc.model, Kind: tc.kind, InputType: tc.inputType, Dimensions: tc.dims, Inputs: inputs, Expect: dim,
			})
			if err != nil {
				t.Fatal(err)
			}
			checkVectors(t, inputs, vectors)
			if _, _, embedded := tel.snapshot(); len(embedded) != 1 || embedded[0] != (Usage{Tokens: 1 + 2 + 4}) {
				t.Errorf("usage %v", embedded)
			}
		})
	}

	t.Run("429 then success", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body geminiBatchRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if calls.Add(1) == 1 {
				w.Header().Set("retry-after-ms", "100")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`)
				return
			}
			geminiOK(w, body, 4)
		}))
		defer srv.Close()
		tel := &recTel{}
		cfg := testCfg(srv.URL)
		cfg.Kind = KindGemini
		start := time.Now()
		inputs := []string{"a", "bb"}
		vectors, _, err := NewHTTP(cfg, tel).Embed(context.Background(), Request{Model: "gemini-embedding-001", Inputs: inputs})
		if err != nil {
			t.Fatal(err)
		}
		checkVectors(t, inputs, vectors)
		if d := time.Since(start); d < 100*time.Millisecond {
			t.Errorf("Retry-After not honoured: %v", d)
		}
		outcomes, retries, _ := tel.snapshot()
		if !slices.Equal(outcomes, []string{"throttled", "ok"}) || len(retries) != 1 || retries[0].reason != "throttled" {
			t.Errorf("outcomes %v retries %v", outcomes, retries)
		}
	})

	t.Run("permanent error carries gemini message", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":403,"message":"API key not valid. Please pass a valid API key.","status":"PERMISSION_DENIED"}}`)
		}))
		defer srv.Close()
		cfg := testCfg(srv.URL)
		cfg.Kind, cfg.APIKey = KindGemini, "AIza-secret"
		_, _, err := NewHTTP(cfg, nil).Embed(context.Background(), Request{Model: "gemini-embedding-001", Inputs: []string{"a"}})
		if err == nil || !strings.Contains(err.Error(), "API key not valid") || strings.Contains(err.Error(), "AIza-secret") {
			t.Errorf("got %v", err)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		p := NewHTTP(HTTPConfig{Kind: KindGemini, MaxBatchInputs: 500}, nil)
		if p.MaxBatch() != geminiMaxBatch {
			t.Errorf("MaxBatch = %d", p.MaxBatch())
		}
		if got := p.codec.url(p.base, Request{Model: "gemini-embedding-001"}); got != DefaultGeminiBaseURL+"/models/gemini-embedding-001:batchEmbedContents" {
			t.Errorf("url = %s", got)
		}
	})
}

// A single poison input is quarantined once a probe shows the provider works.
func TestSingleRefusedInputQuarantinedAfterProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeOAI(t, r)
		if strings.Contains(req.Input[0], "POISON") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"input contains POISON"}}`)
			return
		}
		writeOK(w, req.Input, 2, nil)
	}))
	defer srv.Close()
	p := NewHTTP(testCfg(srv.URL), nil)
	vectors, rejected, err := p.Embed(context.Background(), Request{Model: "m", Inputs: []string{"POISON row"}})
	if err != nil || len(vectors) != 0 || len(rejected) != 1 {
		t.Fatalf("err %v vectors %v rejected %v", err, vectors, rejected)
	}
}

// An input the provider keeps answering with an unusable vector is isolated
// after maxInvalid attempts; the rest of the batch is embedded. If every
// input is affected (e.g. a wrong configured dimension) it is a permanent
// configuration error.
func TestPersistentlyInvalidResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeOAI(t, r)
		data := make([]oaiItem, 0, len(req.Input))
		for i, s := range req.Input {
			v := vecFor(s, 2)
			if strings.Contains(s, "ZERO") {
				v = []float32{0, 0}
			}
			data = append(data, oaiItem{Index: i, Embedding: v})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()
	p := NewHTTP(testCfg(srv.URL), nil)
	inputs := []string{"a", "ZERO vector", "ccc", "dddd"}
	vectors, rejected, err := p.Embed(context.Background(), Request{Model: "m", Inputs: inputs})
	if err != nil || len(vectors) != 3 || len(rejected) != 1 || !strings.Contains(rejected[1], "invalid embedding") {
		t.Fatalf("err %v vectors %d rejected %v", err, len(vectors), rejected)
	}
	_, _, err = p.Embed(context.Background(), Request{Model: "m", Inputs: []string{"x", "y", "z"}, Expect: 7})
	if err == nil || !strings.Contains(err.Error(), "every input") {
		t.Fatalf("a dimension mismatch on every input must be permanent: %v", err)
	}
}

func TestParseRetryAfterClamps(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "1e30")
	if d := parseRetryAfter(h, time.Now()); d <= 0 || d > maxRetryAfterHeader {
		t.Fatalf("huge Retry-After must clamp, got %s", d)
	}
}
