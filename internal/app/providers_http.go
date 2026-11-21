package app

import (
	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
)

// newHTTPProvider builds an OpenAI-compatible or Gemini provider.
func newHTTPProvider(name string, p config.ProviderDef, apiKey string, tel embedclient.Telemetry) (embedclient.Provider, error) {
	return embedclient.NewHTTP(embedclient.HTTPConfig{
		Name: name, Kind: p.Kind, BaseURL: p.ProviderURL(), APIKey: apiKey, Timeout: p.Timeout.Duration,
		MaxBatchInputs: p.BatchSize, MaxBatchTokens: p.MaxBatchTokens, RequestsPerSecond: p.RequestsPerSecond, Burst: p.Burst,
		TokensPerMinute: p.TokensPerMinute, InitialBackoff: p.Retry.InitialBackoff.Duration, MaxBackoff: p.Retry.MaxBackoff.Duration,
		MaxRetryAfter: p.MaxRetryAfter.Duration,
	}, tel), nil
}
