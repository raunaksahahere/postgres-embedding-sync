package setup

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
)

func TestModelKey(t *testing.T) {
	taken := map[string]bool{"minilm": true, "bge_small_en_v1_5": true}
	cases := map[string]string{
		"sentence-transformers/all-MiniLM-L6-v2":         "all_minilm_l6_v2",
		"BAAI/bge-small-en-v1.5":                         "bge_small_en_v1_5_2",
		"intfloat/2-model":                               "m_2_model",
		"org/a-very-long-model-name-that-keeps-going-on": "a_very_long_model_name_that",
	}
	for in, want := range cases {
		got := ModelKey(in, taken)
		if got != want {
			t.Errorf("ModelKey(%q) = %q, want %q", in, got, want)
		}
		if len(got) > 31 {
			t.Errorf("key %q too long", got)
		}
	}
}

func TestResolveHF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/org/model/revision/main":
			_, _ = w.Write([]byte(`{"id":"org/model","sha":"0123456789abcdef0123456789abcdef01234567"}`))
		case "/api/models/org/model/revision/v2":
			_, _ = w.Write([]byte(`{"sha":"fedcba9876543210fedcba9876543210fedcba98"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := hfAPI
	hfAPI = srv.URL
	defer func() { hfAPI = old }()
	ctx := context.Background()
	name, sha, err := ResolveHF(ctx, "org/model")
	if err != nil || name != "org/model" || !strings.HasPrefix(sha, "0123") {
		t.Fatalf("got %q %q %v", name, sha, err)
	}
	if _, sha, err = ResolveHF(ctx, "org/model@v2"); err != nil || !strings.HasPrefix(sha, "fedc") {
		t.Fatalf("revision: %q %v", sha, err)
	}
	if _, _, err = ResolveHF(ctx, "org/missing"); err == nil || !strings.Contains(err.Error(), "HF_TOKEN") {
		t.Fatalf("missing model: %v", err)
	}
	if _, _, err = ResolveHF(ctx, "not a model"); err == nil {
		t.Fatal("invalid id accepted")
	}
}

func TestRenderedConfigIsValid(t *testing.T) {
	y := renderConfig(table{Schema: "public", Name: "docs", Key: "id"}, []string{"title", "body"},
		[]modelChoice{{"minilm", "sentence-transformers/all-MiniLM-L6-v2", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41", 384, ""},
			{"bge", "BAAI/bge-small-en-v1.5", "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a", 384, "Represent this: "}}, "minilm")
	c, err := config.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Embedder.Managed || c.Source.Table != "public.docs" || c.Models["bge"].QueryPrefix != "Represent this: " ||
		c.Transform.Template != "{{.title}}\n\n{{.body}}" || c.Metrics.Listen != "127.0.0.1:9187" {
		t.Fatalf("unexpected config: %+v", c)
	}
}

func TestAddModelKeepsCommentsAndValidates(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("EMBEDSYNC_MODELS_DIR", dir)
	manifest := `{"variant":"full","models":[{"key":"bge","name":"BAAI/bge-small-en-v1.5",
		"revision":"5c38ec7c405ec4b44b94cc5a9bb96e735b38267a","dimension":384,"query_prefix":"Q: "}]}`
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "embedsync.yaml")
	y := renderConfig(table{Schema: "public", Name: "docs", Key: "id"}, []string{"body"},
		[]modelChoice{{"minilm", "sentence-transformers/all-MiniLM-L6-v2", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41", 384, ""}}, "minilm")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	p := NewPrompter(strings.NewReader(""), &bytes.Buffer{}, false)
	key, err := AddModel(context.Background(), p, path, "bge", "", "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if key != "bge" || c.Models["bge"].QueryPrefix != "Q: " || c.InitialGeneration != "minilm" || len(c.Models) != 2 {
		t.Fatalf("key %q, models %+v", key, c.Models)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "# only these columns are ever read or published") {
		t.Fatal("comments were lost")
	}
	if _, err := AddModel(context.Background(), p, path, "bge", "", ""); err == nil {
		t.Fatal("adding the same model twice must fail")
	}
}
