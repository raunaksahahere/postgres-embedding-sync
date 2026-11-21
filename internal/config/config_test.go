package config

import (
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
	if got := strings.Join(c.PublishedColumns(), ","); got != "id,title,body" {
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
	if a.TransformFingerprint() != b.TransformFingerprint() {
		t.Fatal("renaming a physical column must not change the transform fingerprint")
	}
	c, _ := Parse([]byte(strings.Replace(base, `"{{.body}}"`, `"x {{.body}}"`, 1)))
	if c.TransformFingerprint() == a.TransformFingerprint() {
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
