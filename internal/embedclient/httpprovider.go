package embedclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/time/rate"
)

// Wire formats an HTTPProvider can speak (HTTPConfig.Kind).
const (
	// KindOpenAI is POST {BaseURL}/embeddings, the OpenAI embeddings API
	// also served by Gemini's OpenAI compatibility endpoint, the NVIDIA
	// API catalog, Ollama (/v1), vLLM and HF TEI.
	KindOpenAI = "openai"
	// KindGemini is Google's native Gemini API,
	// POST {BaseURL}/models/{model}:batchEmbedContents, which (unlike the
	// compatibility endpoint) distinguishes document and query embeddings
	// through taskType.
	KindGemini = "gemini"
)

// Default base URLs used when HTTPConfig.BaseURL is empty.
const (
	DefaultOpenAIBaseURL = "https://api.openai.com/v1"
	DefaultGeminiBaseURL = "https://generativelanguage.googleapis.com/v1beta"
)

// geminiMaxBatch is batchEmbedContents' limit on requests per call.
const geminiMaxBatch = 100

// HTTPConfig configures a remote embeddings endpoint.
type HTTPConfig struct {
	Name string
	// Kind selects the wire format: KindOpenAI (default when "") or
	// KindGemini. Any other value makes every request fail permanently.
	Kind string
	// BaseURL is the API root, e.g. "https://api.openai.com/v1" or
	// "http://127.0.0.1:11434/v1"; a trailing slash is tolerated. Empty
	// selects DefaultOpenAIBaseURL or DefaultGeminiBaseURL.
	BaseURL string
	// APIKey is sent as "Authorization: Bearer <APIKey>" (openai) or
	// "x-goog-api-key: <APIKey>" (gemini); "" sends no credential. It
	// never appears in errors.
	APIKey string
	// Timeout bounds one HTTP attempt (default 60s when 0).
	Timeout time.Duration
	// MaxBatchInputs is the number of inputs per request (default 64;
	// capped at 100 for gemini).
	MaxBatchInputs int
	// MaxBatchTokens caps the estimated tokens (ceil(bytes/4) per input)
	// of one request; 0 = unlimited. A single input over the limit is sent
	// alone.
	MaxBatchTokens int
	// RequestsPerSecond limits requests (0 = unlimited) with Burst.
	RequestsPerSecond float64
	Burst             int
	// TokensPerMinute limits estimated tokens sent (0 = unlimited). The
	// bucket holds one minute of tokens; a request larger than that waits
	// for a full bucket.
	TokensPerMinute int
	// InitialBackoff and MaxBackoff bound the full-jitter retry delay
	// (defaults 200ms and 15s).
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// MaxRetryAfter caps how long a Retry-After is honoured (default 60s).
	MaxRetryAfter time.Duration
}

const (
	defaultHTTPTimeout   = 60 * time.Second
	defaultHTTPBatch     = 64
	defaultHTTPInitial   = 200 * time.Millisecond
	defaultHTTPMaxBack   = 15 * time.Second
	defaultMaxRetryAfter = 60 * time.Second
	maxProviderMessage   = 300
)

// HTTPProvider is a Provider for remote embedding APIs (see HTTPConfig.Kind
// for the wire formats). It is safe for concurrent use.
type HTTPProvider struct {
	cfg    HTTPConfig
	base   string
	codec  codec // nil for an unknown Kind
	http   *http.Client
	reqLim *rate.Limiter
	tokLim *rate.Limiter // nil when TokensPerMinute is 0
	tel    Telemetry
}

var _ Provider = (*HTTPProvider)(nil)

// NewHTTP creates a remote provider. tel may be nil.
func NewHTTP(cfg HTTPConfig, tel Telemetry) *HTTPProvider {
	if cfg.Kind == "" {
		cfg.Kind = KindOpenAI
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultHTTPTimeout
	}
	if cfg.MaxBatchInputs <= 0 {
		cfg.MaxBatchInputs = defaultHTTPBatch
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultHTTPInitial
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultHTTPMaxBack
	}
	if cfg.MaxRetryAfter <= 0 {
		cfg.MaxRetryAfter = defaultMaxRetryAfter
	}
	if tel == nil {
		tel = nopTelemetry{}
	}
	p := &HTTPProvider{
		cfg:    cfg,
		http:   &http.Client{Timeout: cfg.Timeout},
		reqLim: rate.NewLimiter(rate.Inf, 0),
		tel:    tel,
	}
	switch cfg.Kind {
	case KindOpenAI:
		p.codec = openAICodec{}
		p.base = DefaultOpenAIBaseURL
	case KindGemini:
		p.codec = geminiCodec{}
		p.base = DefaultGeminiBaseURL
		p.cfg.MaxBatchInputs = min(p.cfg.MaxBatchInputs, geminiMaxBatch)
	}
	if cfg.BaseURL != "" {
		p.base = cfg.BaseURL
	}
	p.base = strings.TrimRight(p.base, "/")
	if cfg.RequestsPerSecond > 0 {
		p.reqLim = rate.NewLimiter(rate.Limit(cfg.RequestsPerSecond), max(cfg.Burst, 1))
	}
	if cfg.TokensPerMinute > 0 {
		p.tokLim = rate.NewLimiter(rate.Limit(float64(cfg.TokensPerMinute)/60), cfg.TokensPerMinute)
	}
	return p
}

// Name implements Provider.
func (o *HTTPProvider) Name() string { return o.cfg.Name }

// MaxBatch implements Provider.
func (o *HTTPProvider) MaxBatch() int { return o.cfg.MaxBatchInputs }

// Backoff implements Provider: full jitter in
// [0, min(MaxBackoff, InitialBackoff<<attempt)].
func (o *HTTPProvider) Backoff(attempt int) time.Duration {
	ceil := o.cfg.InitialBackoff
	for i := 0; i < attempt && ceil < o.cfg.MaxBackoff; i++ {
		ceil *= 2
	}
	if ceil > o.cfg.MaxBackoff || ceil <= 0 {
		ceil = o.cfg.MaxBackoff
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

// codec is one wire format: how a batch becomes an HTTP request and how a
// 200 body becomes vectors. Shared validation (finite, non-zero, equal
// length, Expect) happens after decode.
type codec interface {
	// url returns the endpoint for req under base (no trailing slash).
	url(base string, req Request) string
	// auth sets the credential header; key is non-empty.
	auth(h http.Header, key string)
	// encode builds the request body.
	encode(req Request) ([]byte, error)
	// decode parses a 200 body into n vectors in input order (unvalidated
	// beyond count and ordering) and the provider-reported usage, if any.
	decode(raw []byte, n int) ([][]float32, Usage, error)
}

// openAICodec speaks the OpenAI embeddings API.
type openAICodec struct{}

type openAIRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format"`
	Dimensions     int      `json:"dimensions,omitempty"`
	InputType      string   `json:"input_type,omitempty"`
}

type openAIResponse struct {
	Data []struct {
		Index     *int      `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Usage *struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

func (openAICodec) url(base string, _ Request) string { return base + "/embeddings" }

func (openAICodec) auth(h http.Header, key string) { h.Set("Authorization", "Bearer "+key) }

func (openAICodec) encode(req Request) ([]byte, error) {
	return json.Marshal(openAIRequest{
		Model:          req.Model,
		Input:          req.Inputs,
		EncodingFormat: "float",
		Dimensions:     max(req.Dimensions, 0),
		InputType:      req.InputType,
	})
}

// decode orders data by index.
//
// Ordering rule: every data[i].index must be in [0, n) and appear exactly
// once. If no item carries a non-zero index (all absent or 0, as Gemini's
// compatibility endpoint answers), the array order is used; the array
// length has already been checked to equal n.
func (openAICodec) decode(raw []byte, n int) ([][]float32, Usage, error) {
	var body openAIResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, Usage{}, fmt.Errorf("decode: %w", err)
	}
	if len(body.Data) != n {
		return nil, Usage{}, fmt.Errorf("got %d embeddings for %d inputs", len(body.Data), n)
	}
	positional := true
	for _, d := range body.Data {
		if d.Index != nil && *d.Index != 0 {
			positional = false
			break
		}
	}
	vecs := make([][]float32, n)
	seen := make([]bool, n)
	for i, d := range body.Data {
		at := i
		if !positional {
			if d.Index == nil {
				return nil, Usage{}, fmt.Errorf("data[%d] has no index", i)
			}
			at = *d.Index
			if at < 0 || at >= n {
				return nil, Usage{}, fmt.Errorf("index %d out of range for %d inputs", at, n)
			}
			if seen[at] {
				return nil, Usage{}, fmt.Errorf("duplicate index %d", at)
			}
		}
		seen[at] = true
		vecs[at] = d.Embedding
	}
	var usage Usage
	if body.Usage != nil {
		switch {
		case body.Usage.PromptTokens > 0:
			usage = Usage{Tokens: body.Usage.PromptTokens, Reported: true}
		case body.Usage.TotalTokens > 0:
			usage = Usage{Tokens: body.Usage.TotalTokens, Reported: true}
		}
	}
	return vecs, usage, nil
}

// geminiCodec speaks the native Gemini batchEmbedContents API.
type geminiCodec struct{}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiEmbedRequest struct {
	Model                string        `json:"model"`
	Content              geminiContent `json:"content"`
	TaskType             string        `json:"taskType"`
	OutputDimensionality int           `json:"outputDimensionality,omitempty"`
}

type geminiBatchRequest struct {
	Requests []geminiEmbedRequest `json:"requests"`
}

type geminiBatchResponse struct {
	Embeddings []struct {
		Values []float32 `json:"values"`
	} `json:"embeddings"`
}

func geminiModel(model string) string { return strings.TrimPrefix(model, "models/") }

func (geminiCodec) url(base string, req Request) string {
	return base + "/models/" + url.PathEscape(geminiModel(req.Model)) + ":batchEmbedContents"
}

func (geminiCodec) auth(h http.Header, key string) { h.Set("x-goog-api-key", key) }

// geminiTaskType is req.InputType when set, else RETRIEVAL_QUERY for Query
// and RETRIEVAL_DOCUMENT otherwise.
func geminiTaskType(req Request) string {
	switch {
	case req.InputType != "":
		return req.InputType
	case req.Kind == Query:
		return "RETRIEVAL_QUERY"
	default:
		return "RETRIEVAL_DOCUMENT"
	}
}

func (geminiCodec) encode(req Request) ([]byte, error) {
	model := "models/" + geminiModel(req.Model)
	task := geminiTaskType(req)
	body := geminiBatchRequest{Requests: make([]geminiEmbedRequest, len(req.Inputs))}
	for i, s := range req.Inputs {
		body.Requests[i] = geminiEmbedRequest{
			Model:                model,
			Content:              geminiContent{Parts: []geminiPart{{Text: s}}},
			TaskType:             task,
			OutputDimensionality: max(req.Dimensions, 0),
		}
	}
	return json.Marshal(body)
}

// decode takes embeddings in request order; Gemini reports no usage.
func (geminiCodec) decode(raw []byte, n int) ([][]float32, Usage, error) {
	var body geminiBatchResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, Usage{}, fmt.Errorf("decode: %w", err)
	}
	if len(body.Embeddings) != n {
		return nil, Usage{}, fmt.Errorf("got %d embeddings for %d inputs", len(body.Embeddings), n)
	}
	vecs := make([][]float32, n)
	for i, e := range body.Embeddings {
		vecs[i] = e.Values
	}
	return vecs, Usage{}, nil
}

// providerFailure is the cause inside a *TransientError returned by
// HTTPProvider: it records the telemetry outcome of the failed attempt
// and, for HTTP 429, the server-requested wait.
type providerFailure struct {
	outcome string
	wait    time.Duration
	err     error
}

func (f *providerFailure) Error() string { return f.err.Error() }
func (f *providerFailure) Unwrap() error { return f.err }

func transient(outcome string, wait time.Duration, err error) error {
	return &TransientError{Err: &providerFailure{outcome: outcome, wait: wait, err: err}}
}

// RetryAfterOf returns the wait a throttled (HTTP 429) response asked for
// via Retry-After or retry-after-ms, or 0 when err carries none.
func RetryAfterOf(err error) time.Duration {
	var f *providerFailure
	if errors.As(err, &f) {
		return f.wait
	}
	return 0
}

func failureOutcome(err error) string {
	var f *providerFailure
	if errors.As(err, &f) {
		return f.outcome
	}
	return "error"
}

// batchRejected means the provider refused a multi-input request with a
// 400/413/422; Embed bisects the batch to find the offending inputs.
type batchRejected struct {
	status  int
	message string
}

func (e *batchRejected) Error() string {
	return fmt.Sprintf("batch rejected (HTTP %d): %s", e.status, e.message)
}

// estimateTokens is the provider-neutral token estimate: ceil(bytes/4).
func estimateTokens(s string) int { return (len(s) + 3) / 4 }

func (o *HTTPProvider) wait(ctx context.Context, tokens int) error {
	err := o.reqLim.Wait(ctx)
	if err == nil && o.tokLim != nil && tokens > 0 {
		// WaitN fails for n > burst, so an oversize request waits for a
		// full bucket instead.
		err = o.tokLim.WaitN(ctx, min(tokens, o.tokLim.Burst()))
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The limiter refuses up front when the wait would pass ctx's
		// deadline.
		return fmt.Errorf("embedding provider %q: rate limit wait: %w", o.cfg.Name, context.DeadlineExceeded)
	}
	return nil
}

// EmbedOnce implements Provider: one rate-limited attempt for at most
// MaxBatch inputs, without retrying. A refused single input returns
// *RejectedError; a refused multi-input batch returns an internal error
// that Embed bisects.
func (o *HTTPProvider) EmbedOnce(ctx context.Context, req Request) (*Response, error) {
	n := len(req.Inputs)
	if o.codec == nil {
		return nil, fmt.Errorf("embedding provider %q: unknown kind %q (want %q or %q)", o.cfg.Name, o.cfg.Kind, KindOpenAI, KindGemini)
	}
	if n == 0 {
		return &Response{Model: req.Model, Revision: req.Revision}, nil
	}
	if n > o.cfg.MaxBatchInputs {
		return nil, fmt.Errorf("embedding provider %q: %d inputs exceed the batch limit %d", o.cfg.Name, n, o.cfg.MaxBatchInputs)
	}
	estimate := 0
	for _, s := range req.Inputs {
		estimate += estimateTokens(s)
	}
	if err := o.wait(ctx, estimate); err != nil {
		return nil, err
	}
	body, err := o.codec.encode(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.codec.url(o.base, req), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embedding provider %q: %w", o.cfg.Name, err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")
	if o.cfg.APIKey != "" {
		o.codec.auth(hreq.Header, o.cfg.APIKey)
	}

	start := time.Now()
	report := func(outcome string) { o.tel.Request(o.cfg.Name, outcome, time.Since(start), n) }
	resp, err := o.http.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			report("error")
			return nil, ctx.Err()
		}
		report("unavailable")
		return nil, transient("unavailable", 0, fmt.Errorf("embedding provider %q: %s", o.cfg.Name, o.redact(err.Error())))
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		if ctx.Err() != nil {
			report("error")
			return nil, ctx.Err()
		}
		report("unavailable")
		return nil, transient("unavailable", 0, fmt.Errorf("embedding provider %q: read response: %s", o.cfg.Name, o.redact(err.Error())))
	}

	status := resp.StatusCode
	switch {
	case status == http.StatusOK:
		vecs, usage, err := o.codec.decode(raw, n)
		var dim int
		if err == nil {
			dim, err = validate(vecs, req.Expect)
		}
		if err != nil {
			report("invalid")
			return nil, transient("invalid", 0, fmt.Errorf("embedding provider %q: invalid response: %w", o.cfg.Name, err))
		}
		if !usage.Reported {
			usage = Usage{Tokens: estimate}
		}
		report("ok")
		o.tel.Embedded(o.cfg.Name, n, usage)
		return &Response{Model: req.Model, Revision: req.Revision, Dimension: dim, Embeddings: vecs}, nil
	case status == http.StatusTooManyRequests:
		report("throttled")
		wait := parseRetryAfter(resp.Header, time.Now())
		return nil, transient("throttled", wait, fmt.Errorf("embedding provider %q: HTTP 429 (retry after %s): %s", o.cfg.Name, wait, o.message(raw)))
	case status >= 500 || status == http.StatusRequestTimeout:
		report("unavailable")
		return nil, transient("unavailable", 0, fmt.Errorf("embedding provider %q: HTTP %d: %s", o.cfg.Name, status, o.message(raw)))
	case status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge || status == http.StatusUnprocessableEntity:
		report("rejected")
		msg := o.message(raw)
		if n > 1 {
			return nil, &batchRejected{status: status, message: msg}
		}
		return nil, &RejectedError{Indexes: []int{0}, Message: fmt.Sprintf("HTTP %d: %s", status, msg)}
	default: // 401, 403, 404 and anything unexpected: permanent.
		report("error")
		return nil, fmt.Errorf("embedding provider %q: HTTP %d: %s", o.cfg.Name, status, o.message(raw))
	}
}

// validate checks decoded vectors: each non-empty, all the same length
// (== expect when expect > 0), finite and not all zeros. It returns the
// dimension.
func validate(vecs [][]float32, expect int) (int, error) {
	dim := -1
	for i, v := range vecs {
		if len(v) == 0 {
			return 0, fmt.Errorf("empty embedding at index %d", i)
		}
		if dim < 0 {
			dim = len(v)
		} else if len(v) != dim {
			return 0, fmt.Errorf("embedding lengths differ (%d and %d)", dim, len(v))
		}
		zero := true
		for _, x := range v {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				return 0, fmt.Errorf("non-finite value in embedding at index %d", i)
			}
			if x != 0 {
				zero = false
			}
		}
		if zero {
			return 0, fmt.Errorf("all-zero embedding at index %d", i)
		}
	}
	if expect > 0 && dim != expect {
		return 0, fmt.Errorf("got %d dimensions, expected %d", dim, expect)
	}
	return dim, nil
}

// message extracts a provider error message from an error body (OpenAI
// {"error":{"message"}}, TEI/Ollama {"error":"..."}, vLLM {"message"},
// FastAPI {"detail"}), falling back to the raw body. The API key is
// redacted and the result truncated to 300 characters.
func (o *HTTPProvider) message(raw []byte) string {
	var b struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
		Detail  json.RawMessage `json:"detail"`
	}
	msg := ""
	if json.Unmarshal(raw, &b) == nil {
		var s string
		var obj struct {
			Message string `json:"message"`
		}
		switch {
		case json.Unmarshal(b.Error, &obj) == nil && obj.Message != "":
			msg = obj.Message
		case json.Unmarshal(b.Error, &s) == nil && s != "":
			msg = s
		case b.Message != "":
			msg = b.Message
		case json.Unmarshal(b.Detail, &s) == nil && s != "":
			msg = s
		case len(b.Detail) > 0 && string(b.Detail) != "null":
			msg = string(b.Detail)
		}
	}
	if msg == "" {
		msg = strings.TrimSpace(string(raw))
	}
	if msg == "" {
		msg = "(empty body)"
	}
	return truncate(o.redact(msg), maxProviderMessage)
}

func (o *HTTPProvider) redact(s string) string {
	if o.cfg.APIKey == "" {
		return s
	}
	return strings.ReplaceAll(s, o.cfg.APIKey, "[redacted]")
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[:limit]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s + "..."
}

// parseRetryAfter reads retry-after-ms (milliseconds) or Retry-After
// (delay-seconds or an HTTP-date). It returns 0 when neither is usable.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	if v := strings.TrimSpace(h.Get("Retry-After-Ms")); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); err == nil && ms > 0 && !math.IsInf(ms, 0) {
			return time.Duration(ms * float64(time.Millisecond))
		}
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs <= 0 || math.IsInf(secs, 0) || math.IsNaN(secs) {
			return 0
		}
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// Embed implements Provider. Inputs are split into batches by
// MaxBatchInputs and MaxBatchTokens (order preserved) and sent one batch at
// a time; transient failures are retried with full-jitter backoff (at least
// the capped Retry-After when throttled). A batch the provider refuses is
// bisected to isolate the inputs it will never accept.
//
// A refusal that covers everything is treated as a request-wide
// configuration error (for example an unsupported "input_type" field or
// "dimensions" value), not as bad inputs: if every input of a multi-input
// batch, or every input of the whole request, ends up rejected, Embed
// returns a permanent error and quarantines nothing. A one-input request
// whose input is refused is a rejection (the input is quarantined): one
// bad chunk must not stop the pipeline, and a request-wide error shows up
// on the first multi-input batch anyway.
func (o *HTTPProvider) Embed(ctx context.Context, req Request) (vectors map[int][]float32, rejected map[int]string, err error) {
	vectors = make(map[int][]float32, len(req.Inputs))
	rejected = map[int]string{}
	for _, idx := range o.batches(req.Inputs) {
		if err := o.embedBatch(ctx, req, idx, vectors, rejected); err != nil {
			return nil, nil, err
		}
		if len(idx) > 1 && allRejected(idx, rejected) {
			return nil, nil, o.requestWide(rejected[idx[len(idx)-1]])
		}
	}
	if len(req.Inputs) > 1 && len(rejected) == len(req.Inputs) {
		return nil, nil, o.requestWide(rejected[len(req.Inputs)-1])
	}
	return vectors, rejected, nil
}

func allRejected(idx []int, rejected map[int]string) bool {
	for _, i := range idx {
		if _, ok := rejected[i]; !ok {
			return false
		}
	}
	return true
}

func (o *HTTPProvider) requestWide(msg string) error {
	return fmt.Errorf("embedding provider %q refused every input (request-wide error, check model/dimensions/input_type): %s", o.cfg.Name, msg)
}

// batches groups input indexes into requests by input count and estimated
// tokens. An input whose estimate alone exceeds MaxBatchTokens gets its own
// batch.
func (o *HTTPProvider) batches(inputs []string) [][]int {
	var out [][]int
	var cur []int
	tokens := 0
	for i, s := range inputs {
		t := estimateTokens(s)
		if len(cur) > 0 && (len(cur) >= o.cfg.MaxBatchInputs || (o.cfg.MaxBatchTokens > 0 && tokens+t > o.cfg.MaxBatchTokens)) {
			out = append(out, cur)
			cur, tokens = nil, 0
		}
		cur = append(cur, i)
		tokens += t
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// embedBatch embeds the inputs at idx (original request indexes), retrying
// transient failures and bisecting on batch rejection.
func (o *HTTPProvider) embedBatch(ctx context.Context, req Request, idx []int, vectors map[int][]float32, rejected map[int]string) error {
	sub := req
	sub.Inputs = make([]string, len(idx))
	for i, at := range idx {
		sub.Inputs[i] = req.Inputs[at]
	}
	for attempt := 0; ; attempt++ {
		resp, err := o.EmbedOnce(ctx, sub)
		var br *batchRejected
		var rej *RejectedError
		var tr *TransientError
		switch {
		case err == nil:
			for i, at := range idx {
				vectors[at] = resp.Embeddings[i]
			}
			return nil
		case errors.As(err, &br):
			half := len(idx) / 2
			if err := o.embedBatch(ctx, req, idx[:half], vectors, rejected); err != nil {
				return err
			}
			return o.embedBatch(ctx, req, idx[half:], vectors, rejected)
		case errors.As(err, &rej):
			for _, i := range rej.Indexes {
				if i < 0 || i >= len(idx) {
					return fmt.Errorf("embedding provider %q rejected out-of-range index %d", o.cfg.Name, i)
				}
				rejected[idx[i]] = rej.Message
			}
			return nil
		case errors.As(err, &tr):
			wait := o.Backoff(attempt)
			if ra := min(RetryAfterOf(err), o.cfg.MaxRetryAfter); ra > wait {
				wait = ra
			}
			o.tel.Retry(o.cfg.Name, failureOutcome(err), wait)
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		default:
			return err
		}
	}
}

type nopTelemetry struct{}

func (nopTelemetry) Request(string, string, time.Duration, int) {}
func (nopTelemetry) Retry(string, string, time.Duration)        {}
func (nopTelemetry) Embedded(string, int, Usage)                {}
