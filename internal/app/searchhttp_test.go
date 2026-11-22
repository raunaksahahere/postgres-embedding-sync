package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
)

// Request errors answered before any search runs (no database needed).
func TestServeSearchRequestErrors(t *testing.T) {
	s := &search.Searcher{RRFK: 60}
	big := `{"query": "` + strings.Repeat("x", maxSearchBody) + `"}`
	for _, c := range []struct {
		name, method, body string
		status             int
		code               string
	}{
		{"unknown field", http.MethodPost, `{"query":"x","nope":1}`, 400, "invalid_request"},
		{"trailing object", http.MethodPost, `{"query":"x"}{"query":"y"}`, 400, "invalid_request"},
		{"trailing garbage", http.MethodPost, `{"query":"x"} junk`, 400, "invalid_request"},
		{"wrong type", http.MethodPost, `{"query":"x","k":"ten"}`, 400, "invalid_request"},
		{"too large", http.MethodPost, big, 413, "body_too_large"},
		{"method", http.MethodPut, ``, 405, "method_not_allowed"},
		{"empty query", http.MethodPost, `{"query":""}`, 400, "invalid_request"},
	} {
		req := httptest.NewRequest(c.method, "/search", strings.NewReader(c.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		serveSearch(w, req, s, nil)
		var got struct{ Code, Error string }
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		if w.Code != c.status || got.Code != c.code || got.Error == "" {
			t.Errorf("%s: %d %s", c.name, w.Code, w.Body.String())
		}
	}
	// GET keeps the legacy fallback for k and validates the new parameters.
	w := httptest.NewRecorder()
	serveSearch(w, httptest.NewRequest(http.MethodGet, "/search?q=x&rrf_k=abc", nil), s, nil)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"rrf_k"`) {
		t.Errorf("rrf_k: %d %s", w.Code, w.Body.String())
	}
}
