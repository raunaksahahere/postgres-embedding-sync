// Package embedclient talks to the local embedding service over HTTP.
//
// The client distinguishes transient failures (connection errors, timeouts,
// 429 and 5xx), which are retried with capped exponential backoff and full
// jitter, from per-input rejections (HTTP 422 naming input indexes), which are
// returned so the caller can quarantine exactly those rows.
package embedclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// Kind selects the query or document encoder prompt on the service side (the
// service itself is prompt-agnostic; prefixes are applied by the caller).
type Kind string

const (
	// Document embeds corpus chunks.
	Document Kind = "document"
	// Query embeds search queries.
	Query Kind = "query"
)

// Request is the wire request of the built-in embedder and the provider-
// neutral request every Provider takes.
type Request struct {
	Model    string   `json:"model"`
	Revision string   `json:"revision"`
	Kind     Kind     `json:"kind"`
	Inputs   []string `json:"inputs"`
	// Dimensions asks a provider that supports it for vectors of this
	// length (OpenAI "dimensions"); 0 leaves the model's default. The
	// built-in embedder ignores it.
	Dimensions int `json:"-"`
	// Expect is the vector length the caller requires; a provider rejects
	// a response with any other length as invalid. 0 skips the check.
	Expect int `json:"-"`
	// InputType is sent as the provider's input-type field when set (e.g.
	// "query" / "passage" for models trained with asymmetric inputs). The
	// caller picks the document or query value from the generation.
	InputType string `json:"-"`
}

// Response is the wire response.
type Response struct {
	Model      string      `json:"model"`
	Revision   string      `json:"revision"`
	Dimension  int         `json:"dimension"`
	Normalized bool        `json:"normalized"`
	Embeddings [][]float32 `json:"embeddings"`
}

type rejectBody struct {
	Detail struct {
		Error    string `json:"error"`
		Rejected []int  `json:"rejected"`
	} `json:"detail"`
}

// RejectedError reports inputs the service will never accept.
type RejectedError struct {
	Indexes []int
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("embedding service rejected %d input(s): %s", len(e.Indexes), e.Message)
}

// TransientError wraps a retryable failure.
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return "transient embedding failure: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// Hooks receive retry and request telemetry without importing metrics here.
type Hooks struct {
	OnRetry   func(reason string)
	OnRequest func(outcome string, d time.Duration, inputs int)
}

// Client is safe for concurrent use.
type Client struct {
	base       string
	http       *http.Client
	limiter    *rate.Limiter
	initial    time.Duration
	maxBackoff time.Duration
	hooks      Hooks
}

// New creates a client. rps <= 0 disables rate limiting.
func New(base string, timeout time.Duration, rps float64, burst int, initial, maxBackoff time.Duration, hooks Hooks) *Client {
	lim := rate.NewLimiter(rate.Inf, 0)
	if rps > 0 {
		lim = rate.NewLimiter(rate.Limit(rps), max(burst, 1))
	}
	return &Client{
		base:       strings.TrimRight(base, "/"),
		http:       &http.Client{Timeout: timeout},
		limiter:    lim,
		initial:    initial,
		maxBackoff: maxBackoff,
		hooks:      hooks,
	}
}

// EmbedOnce performs one rate-limited request without retries.
func (c *Client) EmbedOnce(ctx context.Context, req Request) (*Response, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	body, _ := json.Marshal(req)
	start := time.Now()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(hreq)
	if err != nil {
		c.report("error", start, len(req.Inputs))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransientError{err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		c.report("error", start, len(req.Inputs))
		return nil, &TransientError{err}
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		var out Response
		if err := json.Unmarshal(raw, &out); err != nil {
			c.report("error", start, len(req.Inputs))
			return nil, &TransientError{fmt.Errorf("decode response: %w", err)}
		}
		if out.Model != req.Model || out.Revision != req.Revision {
			c.report("mismatch", start, len(req.Inputs))
			return nil, fmt.Errorf("embedding service served %s@%s, requested %s@%s", out.Model, out.Revision, req.Model, req.Revision)
		}
		if len(out.Embeddings) != len(req.Inputs) {
			c.report("error", start, len(req.Inputs))
			return nil, &TransientError{fmt.Errorf("got %d embeddings for %d inputs", len(out.Embeddings), len(req.Inputs))}
		}
		c.report("ok", start, len(req.Inputs))
		return &out, nil
	case resp.StatusCode == http.StatusUnprocessableEntity:
		var rb rejectBody
		if json.Unmarshal(raw, &rb) == nil && len(rb.Detail.Rejected) > 0 {
			c.report("rejected", start, len(req.Inputs))
			return nil, &RejectedError{Indexes: rb.Detail.Rejected, Message: rb.Detail.Error}
		}
		c.report("error", start, len(req.Inputs))
		return nil, fmt.Errorf("embedding service returned 422 without rejected indexes")
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		c.report("error", start, len(req.Inputs))
		return nil, &TransientError{fmt.Errorf("HTTP %d", resp.StatusCode)}
	case resp.StatusCode == http.StatusNotFound:
		c.report("error", start, len(req.Inputs))
		return nil, fmt.Errorf("embedding service does not serve %s@%s (HTTP 404)", req.Model, req.Revision)
	default:
		c.report("error", start, len(req.Inputs))
		return nil, fmt.Errorf("embedding service HTTP %d", resp.StatusCode)
	}
}

func (c *Client) report(outcome string, start time.Time, n int) {
	if c.hooks.OnRequest != nil {
		c.hooks.OnRequest(outcome, time.Since(start), n)
	}
}

// Backoff returns the full-jitter delay for a 0-based attempt.
func (c *Client) Backoff(attempt int) time.Duration {
	ceil := c.initial << min(attempt, 20)
	if ceil > c.maxBackoff || ceil <= 0 {
		ceil = c.maxBackoff
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

// Embed retries transient failures until success, a permanent error or ctx
// cancellation. Rejected inputs are removed and the remainder retried, so the
// result maps every accepted input index to its vector.
func (c *Client) Embed(ctx context.Context, req Request) (vectors map[int][]float32, rejected map[int]string, err error) {
	vectors = make(map[int][]float32, len(req.Inputs))
	rejected = map[int]string{}
	pending := make([]int, len(req.Inputs))
	for i := range pending {
		pending[i] = i
	}
	attempt := 0
	for len(pending) > 0 {
		sub := req
		sub.Inputs = make([]string, len(pending))
		for i, idx := range pending {
			sub.Inputs[i] = req.Inputs[idx]
		}
		resp, err := c.EmbedOnce(ctx, sub)
		var rej *RejectedError
		var tr *TransientError
		switch {
		case err == nil:
			for i, idx := range pending {
				vectors[idx] = resp.Embeddings[i]
			}
			return vectors, rejected, nil
		case errors.As(err, &rej):
			drop := map[int]bool{}
			for _, ri := range rej.Indexes {
				if ri >= 0 && ri < len(pending) {
					drop[ri] = true
					rejected[pending[ri]] = rej.Message
				}
			}
			if len(drop) == 0 {
				return nil, nil, fmt.Errorf("embedding service rejected out-of-range indexes")
			}
			next := pending[:0:0]
			for i, idx := range pending {
				if !drop[i] {
					next = append(next, idx)
				}
			}
			pending = next
		case errors.As(err, &tr):
			if c.hooks.OnRetry != nil {
				c.hooks.OnRetry("transient")
			}
			select {
			case <-time.After(c.Backoff(attempt)):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			attempt++
		default:
			return nil, nil, err
		}
	}
	return vectors, rejected, nil
}

// FormatVector renders a vector in pgvector text form.
func FormatVector(v []float32) string {
	var b strings.Builder
	b.Grow(len(v) * 10)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', 8, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// Health checks the service.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("embedder health HTTP %d", resp.StatusCode)
	}
	return nil
}
