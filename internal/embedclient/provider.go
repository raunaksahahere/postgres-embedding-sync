package embedclient

import (
	"context"
	"fmt"
	"time"
)

// Provider computes embeddings for one configured endpoint. Ingestion,
// search and migration evaluation all go through it; which provider serves
// a generation is part of the generation's identity.
//
// Error contract (all providers):
//   - *TransientError: retryable (connection errors, timeouts, HTTP 429 and
//     5xx, malformed or invalid responses). Embed retries these itself with
//     jittered backoff, honouring Retry-After; EmbedOnce returns them.
//   - *RejectedError: specific inputs will never be accepted (they are
//     quarantined by the caller); Embed drops them and retries the rest.
//   - anything else is permanent for the request (configuration, auth,
//     unknown model) and stops the caller (the pipeline restarts with
//     backoff; nothing is acknowledged).
type Provider interface {
	// Name is the configured provider name ("local" for the built-in embedder).
	Name() string
	// Embed embeds every input, splitting into the provider's bounded
	// batches and retrying transient failures until success, a permanent
	// error or ctx cancellation. vectors maps input index to its vector;
	// rejected maps input index to the reason it can never be embedded.
	Embed(ctx context.Context, req Request) (vectors map[int][]float32, rejected map[int]string, err error)
	// EmbedOnce performs one rate-limited request for a small batch (at
	// most MaxBatch inputs) without retrying.
	EmbedOnce(ctx context.Context, req Request) (*Response, error)
	// Backoff is the jittered delay before retry number attempt (0-based).
	Backoff(attempt int) time.Duration
	// MaxBatch is the largest number of inputs one request may carry.
	MaxBatch() int
}

// Usage is what one successful request cost, as reported by the provider
// (Reported) or estimated from input length (characters / 4).
type Usage struct {
	Tokens   int
	Reported bool
}

// Telemetry receives per-provider request, retry and usage events. The
// pipeline wires it to Prometheus; tests may leave it nil.
type Telemetry interface {
	// Request is called once per HTTP attempt. outcome is ok, rejected,
	// throttled (429), unavailable (5xx/connection), invalid (malformed
	// response), or error.
	Request(provider, outcome string, d time.Duration, inputs int)
	// Retry is called before each retry with the reason (the failed
	// attempt's outcome) and the delay that will be waited.
	Retry(provider, reason string, wait time.Duration)
	// Embedded is called for every successful request.
	Embedded(provider string, inputs int, usage Usage)
}

// Ensure the built-in client satisfies Provider.
var _ Provider = (*Client)(nil)

// Name implements Provider.
func (c *Client) Name() string { return "local" }

// MaxBatch implements Provider. The built-in embedder accepts any batch the
// pipeline forms (embedder.batch_size bounds it).
func (c *Client) MaxBatch() int { return 1 << 20 }

// Set holds every configured provider. Callers resolve the provider that
// built a generation by the name and kind recorded with it, so a query is
// always embedded by the same endpoint and settings as the documents.
type Set struct {
	byName map[string]Provider
	kinds  map[string]string
	conc   map[string]chan struct{}
}

// NewSet creates an empty set.
func NewSet() *Set {
	return &Set{byName: map[string]Provider{}, kinds: map[string]string{}, conc: map[string]chan struct{}{}}
}

// Add registers a provider of a kind with a concurrency bound (concurrent
// requests across the pipeline).
func (s *Set) Add(p Provider, kind string, concurrency int) {
	s.byName[p.Name()] = p
	s.kinds[p.Name()] = kind
	s.conc[p.Name()] = make(chan struct{}, max(concurrency, 1))
}

// For returns the provider a generation was built with. A provider that is
// no longer configured, or is now configured with another kind, is an
// error: its vectors could not be compared with new ones.
func (s *Set) For(name, kind string) (Provider, error) {
	if name == "" {
		name, kind = "local", "local"
	}
	p, ok := s.byName[name]
	if !ok {
		return nil, fmt.Errorf("embedding provider %q is not configured (a generation was built with it)", name)
	}
	if kind != "" && s.kinds[name] != kind {
		return nil, fmt.Errorf("embedding provider %q is now of kind %q but a generation was built with kind %q", name, s.kinds[name], kind)
	}
	return p, nil
}

// Slot bounds concurrent requests to a provider: it blocks until a slot is
// free (or ctx ends) and returns the release function.
func (s *Set) Slot(ctx context.Context, name string) (func(), error) {
	c, ok := s.conc[name]
	if !ok {
		return func() {}, nil
	}
	select {
	case c <- struct{}{}:
		return func() { <-c }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
