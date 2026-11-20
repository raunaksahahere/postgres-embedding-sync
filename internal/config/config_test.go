package config

import (
	"os"
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
