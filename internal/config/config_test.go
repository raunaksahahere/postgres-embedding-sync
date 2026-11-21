package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExampleConfigParses(t *testing.T) {
	b, err := os.ReadFile("../../examples/embedsync.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if c.Models["bge"].QueryPrefix == "" || c.Alerts.RetainedWALCriticalBytes != 1536<<20 {
		t.Fatalf("unexpected parse: %+v", c.Alerts)
	}
	if len(c.Pipelines) != 1 || c.Pipelines[0].Name != DefaultPipeline {
		t.Fatalf("the single-table form must become one %q pipeline: %+v", DefaultPipeline, c.Pipelines)
	}
	if got := strings.Join(c.Pipelines[0].PublishedColumns(), ","); got != "id,title,body" {
		t.Fatalf("published columns %s", got)
	}
}

func TestRenameKeepsFingerprint(t *testing.T) {
	base := `
source: {table: public.d, key: id, fields: [{name: body, column: body}]}
transform: {template: "{{.body}}"}
embedder: {url: http://x}
models: {m: {name: a, revision: b, dimension: 3}}
initial_generation: m
`
	a, err := Parse([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse([]byte(strings.Replace(base, "column: body", "column: content", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if a.Pipelines[0].TransformFingerprint() != b.Pipelines[0].TransformFingerprint() {
		t.Fatal("renaming a physical column must not change the transform fingerprint")
	}
	c, _ := Parse([]byte(strings.Replace(base, `"{{.body}}"`, `"x {{.body}}"`, 1)))
	if c.Pipelines[0].TransformFingerprint() == a.Pipelines[0].TransformFingerprint() {
		t.Fatal("template change must change the fingerprint")
	}
}

func TestValidation(t *testing.T) {
	_, err := Parse([]byte(`
source: {table: documents, key: id, fields: []}
transform: {template: ""}
models: {M: {name: a, dimension: 0}}
initial_generation: nope
unknown_key: 1
`))
	if err == nil {
		t.Fatal("expected errors")
	}
}

func TestEnvFile(t *testing.T) {
	dir := t.TempDir()
	p := EnvFilePath(filepath.Join(dir, "embedsync.yaml"))
	if err := WriteEnvFile(p, "test", [][2]string{{"ES_TEST_A", "postgres://u:p@h/db?x=1"}, {"ES_TEST_B", "new"}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ES_TEST_B", "from-env")
	if err := LoadEnvFile(p); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("ES_TEST_A"); got != "postgres://u:p@h/db?x=1" {
		t.Fatalf("A=%q", got)
	}
	if got := os.Getenv("ES_TEST_B"); got != "from-env" {
		t.Fatalf("real environment must win, B=%q", got)
	}
	os.Unsetenv("ES_TEST_A")
	if err := LoadEnvFile(filepath.Join(dir, "missing.env")); err != nil {
		t.Fatal(err)
	}
}

func TestManagedEmbedderNeedsNoURL(t *testing.T) {
	base := `
source: {table: public.d, key: id, fields: [{name: body, column: body}]}
transform: {template: "{{.body}}"}
embedder: {%s}
models: {m: {name: a, revision: b, dimension: 3}}
initial_generation: m
`
	if _, err := Parse([]byte(fmt.Sprintf(base, "managed: true"))); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse([]byte(fmt.Sprintf(base, "managed: false"))); err == nil {
		t.Fatal("an unmanaged embedder without url must be rejected")
	}
}

// The single-table form must keep the fingerprint every existing generation
// was built with (sha256 over fields, transform and version 1).
func TestLegacyFingerprintIsStable(t *testing.T) {
	c, err := Parse([]byte(`
source: {table: public.documents, key: id, fields: [{name: title, column: title}, {name: body, column: body}]}
transform: {template: "{{.title}}\n{{.body}}", chunk_words: 20, chunk_overlap: 4}
embedder: {url: http://x}
models: {m: {name: a, revision: b, dimension: 3}}
initial_generation: m
`))
	if err != nil {
		t.Fatal(err)
	}
	// The exact payload v0.2.0's Config.TransformFingerprint hashed.
	got := c.Pipelines[0].TransformFingerprint()
	if got != legacyFingerprint(`{"fields":["title","body"],"transform":{"template":"{{.title}}\n{{.body}}","chunk_words":20,"chunk_overlap":4},"version":1}`) {
		t.Fatalf("fingerprint formula changed: %s", got)
	}
	if c.CombinedFingerprint() != got {
		t.Fatal("a single default pipeline must keep the single-table generation fingerprint")
	}
}

func TestPipelines(t *testing.T) {
	cfg := `
source: {publication: p, slot: s}
transform: {template: "{{.body}}", chunk_words: 50}
pipelines:
  - name: docs
    table: public.documents
    key: id
    fields: [{name: body, column: body}]
  - name: tickets
    table: support.tickets
    key: [tenant_id, id]
    fields: [{name: subject, column: subject}, {name: body, column: body}]
    transform: {template: "{{.subject}} {{.body}}"}
embedder: {url: http://x}
models: {m: {name: a, revision: b, dimension: 3}}
initial_generation: m
`
	c, err := Parse([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.PipelineNames(), ","); got != "docs,tickets" {
		t.Fatal(got)
	}
	tk := c.Pipe("tickets")
	if strings.Join(tk.Key, ",") != "tenant_id,id" || strings.Join(tk.PublishedColumns(), ",") != "tenant_id,id,subject,body" {
		t.Fatalf("tickets key/columns: %v %v", tk.Key, tk.PublishedColumns())
	}
	if tk.Transform.ChunkWords != 120 || c.Pipe("docs").Transform.ChunkWords != 50 {
		t.Fatalf("transform inheritance/defaults: %+v %+v", tk.Transform, c.Pipe("docs").Transform)
	}
	if c.Pipe("docs").TransformFingerprint() == tk.TransformFingerprint() {
		t.Fatal("pipelines with different transforms share a fingerprint")
	}
	for name, bad := range map[string]string{
		"both forms":     strings.Replace(cfg, "source: {publication: p, slot: s}", "source: {publication: p, slot: s, table: public.x, key: id}", 1),
		"duplicate name": strings.Replace(cfg, "name: tickets", "name: docs", 1),
		"same table":     strings.Replace(cfg, "table: support.tickets", "table: public.documents", 1),
		"bad name":       strings.Replace(cfg, "name: tickets", "name: Tickets", 1),
		"dup key column": strings.Replace(cfg, "[tenant_id, id]", "[id, id]", 1),
		"empty key":      strings.Replace(cfg, "[tenant_id, id]", "[]", 1),
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("%s: expected a validation error", name)
		}
	}
}

func legacyFingerprint(payload string) string {
	h := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(h[:16])
}

func TestMultiExampleConfigParses(t *testing.T) {
	b, err := os.ReadFile("../../examples/embedsync-multi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.PipelineNames(), ","); got != "default,tickets" {
		t.Fatalf("pipelines %s", got)
	}
	if c.Pipe("tickets").Transform.ChunkWords != 120 || c.Pipe("default").Transform.ChunkWords != 160 {
		t.Fatal("transform inheritance")
	}
}
