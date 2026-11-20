package embedclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetriesTransientThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var req Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		out := Response{Model: req.Model, Revision: req.Revision, Dimension: 2}
		for range req.Inputs {
			out.Embeddings = append(out.Embeddings, []float32{1, 0})
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	var retries atomic.Int32
	c := New(srv.URL, time.Second, 0, 1, time.Millisecond, 5*time.Millisecond, Hooks{OnRetry: func(string) { retries.Add(1) }})
	v, rej, err := c.Embed(context.Background(), Request{Model: "m", Revision: "r", Inputs: []string{"a", "b"}})
	if err != nil || len(v) != 2 || len(rej) != 0 || retries.Load() != 2 {
		t.Fatalf("v=%d rej=%v err=%v retries=%d", len(v), rej, err, retries.Load())
	}
}

func TestRejectedInputsAreIsolated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		var bad []int
		for i, s := range req.Inputs {
			if strings.Contains(s, "POISON") {
				bad = append(bad, i)
			}
		}
		if len(bad) > 0 {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": map[string]any{"error": "nope", "rejected": bad}})
			return
		}
		out := Response{Model: req.Model, Revision: req.Revision, Dimension: 1}
		for range req.Inputs {
			out.Embeddings = append(out.Embeddings, []float32{1})
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second, 0, 1, time.Millisecond, time.Millisecond, Hooks{})
	v, rej, err := c.Embed(context.Background(), Request{Model: "m", Revision: "r", Inputs: []string{"ok", "POISON", "fine", "POISON too"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 || v[0] == nil || v[2] == nil || len(rej) != 2 || rej[1] == "" || rej[3] == "" {
		t.Fatalf("vectors=%v rejected=%v", v, rej)
	}
}

func TestModelMismatchIsFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Response{Model: "other", Revision: "r", Embeddings: [][]float32{{1}}})
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second, 0, 1, time.Millisecond, time.Millisecond, Hooks{})
	if _, _, err := c.Embed(context.Background(), Request{Model: "m", Revision: "r", Inputs: []string{"x"}}); err == nil || !strings.Contains(err.Error(), "served other") {
		t.Fatalf("expected mismatch error, got %v", err)
	}
}

func TestBackoffIsCapped(t *testing.T) {
	c := New("http://x", time.Second, 0, 1, 100*time.Millisecond, time.Second, Hooks{})
	for i := 0; i < 50; i++ {
		if d := c.Backoff(i); d < 0 || d > time.Second {
			t.Fatalf("backoff %v out of range", d)
		}
	}
}

func TestFormatVector(t *testing.T) {
	if s := FormatVector([]float32{1, -0.5, 0.25}); s != "[1,-0.5,0.25]" {
		t.Fatal(s)
	}
}
