package app

import (
	"fmt"
	"os"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
)

// telemetry records per-provider requests, retries, tokens and the
// ESTIMATED cost (tokens x the configured price, labelled with the pricing
// source the price came from).
type telemetry struct {
	pricing map[string]config.Pricing
}

func (t *telemetry) Request(provider, outcome string, d time.Duration, inputs int) {
	metrics.ProviderRequests.WithLabelValues(provider, outcome).Inc()
	metrics.ProviderRequestSeconds.WithLabelValues(provider).Observe(d.Seconds())
	// The original, provider-less series keep counting every request.
	metrics.EmbedRequests.WithLabelValues(outcome).Inc()
	metrics.EmbedRequestSeconds.Observe(d.Seconds())
}

func (t *telemetry) Retry(provider, reason string, wait time.Duration) {
	metrics.ProviderRetries.WithLabelValues(provider, reason).Inc()
	metrics.ProviderRetryWait.WithLabelValues(provider).Add(wait.Seconds())
	metrics.Retries.WithLabelValues("embedder").Inc()
}

func (t *telemetry) Embedded(provider string, inputs int, u embedclient.Usage) {
	metrics.ProviderInputs.WithLabelValues(provider).Add(float64(inputs))
	src := "estimated"
	if u.Reported {
		src = "reported"
	}
	metrics.ProviderTokens.WithLabelValues(provider, src).Add(float64(u.Tokens))
	if p, ok := t.pricing[provider]; ok && p.USDPerMillionTokens > 0 {
		metrics.ProviderCost.WithLabelValues(provider, p.Source).Add(float64(u.Tokens) * p.USDPerMillionTokens / 1e6)
	}
}

// buildProviders registers the built-in embedder ("local") and every
// configured provider. API keys are read from the named environment
// variables here and never logged.
func buildProviders(cfg *config.Config, local *embedclient.Client) (*embedclient.Set, error) {
	tel := &telemetry{pricing: map[string]config.Pricing{}}
	set := embedclient.NewSet()
	set.Add(local, config.LocalProvider, cfg.Embedder.Concurrency)
	for name, p := range cfg.Providers {
		tel.pricing[name] = p.Pricing
		key := ""
		if p.APIKeyEnv != "" {
			key = os.Getenv(p.APIKeyEnv)
			if key == "" && usesProvider(cfg, name) {
				return nil, fmt.Errorf("provider %s: environment variable %s (api_key_env) is not set", name, p.APIKeyEnv)
			}
		}
		hp, err := newHTTPProvider(name, p, key, tel)
		if err != nil {
			return nil, fmt.Errorf("provider %s: %w", name, err)
		}
		set.Add(hp, p.Kind, p.Concurrency)
	}
	return set, nil
}

func usesProvider(cfg *config.Config, name string) bool {
	for _, m := range cfg.Models {
		if m.ProviderName() == name {
			return true
		}
	}
	return false
}
