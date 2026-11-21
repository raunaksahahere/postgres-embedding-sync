package app

import (
	"errors"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
)

// newHTTPProvider builds an OpenAI-compatible or Gemini provider.
func newHTTPProvider(name string, p config.ProviderDef, apiKey string, tel embedclient.Telemetry) (embedclient.Provider, error) {
	return nil, errors.New("HTTP providers are not available in this build")
}
