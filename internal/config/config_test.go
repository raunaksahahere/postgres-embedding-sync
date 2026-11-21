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

func TestViewsExampleParses(t *testing.T) {
	b, err := os.ReadFile("../../examples/embedsync-views.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.DocNames(), ","); got != "kb" {
		t.Fatalf("doc names %s", got)
	}
	comps := c.Components()
	var names []string
	for _, comp := range comps {
		names = append(names, comp.Def.Name)
	}
	if got := strings.Join(names, ","); got != "kb:root,kb:sections,kb:tags,kb:tags.target" {
		t.Fatalf("components %s", got)
	}
	sec := c.Component("kb:sections")
	if sec == nil || sec.Kind != CompChild {
		t.Fatalf("sections component: %+v", sec)
	}
	hidden := map[string]string{}
	for _, f := range sec.Def.Fields {
		if IsHiddenField(f.Name) {
			hidden[f.Name] = f.Column
		}
	}
	if len(hidden) != 2 || hidden[HiddenField("document_id")] != "document_id" || hidden[HiddenField("position")] != "position" {
		t.Fatalf("sections hidden fields %v", hidden)
	}
	link := c.Component("kb:tags")
	if link == nil || link.Kind != CompLink || len(link.Refs) != 2 {
		t.Fatalf("link component: %+v", link)
	}
	if r := link.Refs[0]; r.Name != "parent" || r.To != "kb:root" || strings.Join(r.Columns, ",") != "tenant_id,document_id" {
		t.Fatalf("link parent ref %+v", r)
	}
	if r := link.Refs[1]; r.Name != "target" || r.To != "kb:tags.target" || strings.Join(r.Columns, ",") != "tenant_id,tag_id" {
		t.Fatalf("link target ref %+v", r)
	}
	if strings.Join(link.OrderFields, ",") != "name" || c.Component("kb:tags.target").Kind != CompTarget {
		t.Fatalf("link order %v", link.OrderFields)
	}
	if n := len(c.TableDefs()); n != 4 {
		t.Fatalf("table defs %d", n)
	}
	if c.PipelineFingerprints()["kb"] == "" {
		t.Fatal("view fingerprint missing")
	}
	v := c.View("kb")
	if v.MaxChildren != 500 || v.MaxFanoutPerTxn != 10000 {
		t.Fatalf("limits %d %d", v.MaxChildren, v.MaxFanoutPerTxn)
	}
}

// viewCfg is a valid config with one pipeline and one view; tests derive
// variants by replacing one fragment.
const viewCfg = `
transform: {template: "{{.body}}"}
pipelines:
  - name: docs
    table: public.documents
    key: id
    fields: [{name: body, column: body}]
views:
  - name: kb
    tenant: tenant_id
    table: public.kb_documents
    key: [tenant_id, id]
    fields: [{name: title, column: title}]
    children:
      - name: sections
        table: public.kb_sections
        key: [tenant_id, id]
        parent: [tenant_id, document_id]
        order_by: [position]
        fields: [{name: heading, column: heading}, {name: body, column: body}]
    links:
      - name: tags
        table: public.kb_document_tags
        key: [tenant_id, document_id, tag_id]
        parent: [tenant_id, document_id]
        target: [tenant_id, tag_id]
        target_table: public.kb_tags
        target_key: [tenant_id, id]
        target_fields: [{name: name, column: name}]
        order_by: [name]
    transform: {template: "{{.title}}{{range .sections}} {{.heading}}{{end}}"}
    max_children: 100
embedder: {url: http://x}
models: {m: {name: a, revision: b, dimension: 3}}
initial_generation: m
`

func viewVariant(t *testing.T, old, repl string) string {
	t.Helper()
	if !strings.Contains(viewCfg, old) {
		t.Fatalf("fragment %q not in viewCfg", old)
	}
	return strings.Replace(viewCfg, old, repl, 1)
}

func TestViewValidation(t *testing.T) {
	if _, err := Parse([]byte(viewCfg)); err != nil {
		t.Fatalf("base config: %v", err)
	}
	for _, tc := range []struct{ name, old, repl string }{
		{"key without tenant first", "key: [tenant_id, document_id, tag_id]", "key: [document_id, tenant_id, tag_id]"},
		{"parent without tenant first", "parent: [tenant_id, document_id]", "parent: [document_id, tenant_id]"},
		{"parent length", "parent: [tenant_id, document_id]", "parent: [tenant_id]"},
		{"table in pipeline and view", "table: public.documents", "table: public.kb_tags"},
		{"table twice in view", "table: public.kb_document_tags", "table: public.kb_sections"},
		{"link order_by not a target field", "order_by: [name]", "order_by: [tag_id]"},
		{"child named like root field", "name: sections", "name: title"},
		{"view named like pipeline", "name: docs", "name: kb"},
		{"hidden field name", "{name: heading, column: heading}", "{name: __heading, column: heading}"},
	} {
		if _, err := Parse([]byte(viewVariant(t, tc.old, tc.repl))); err == nil {
			t.Errorf("%s: expected a validation error", tc.name)
		}
	}
}

func TestViewFingerprint(t *testing.T) {
	fp := func(cfg string) string {
		t.Helper()
		c, err := Parse([]byte(cfg))
		if err != nil {
			t.Fatal(err)
		}
		return c.View("kb").Fingerprint()
	}
	base := fp(viewCfg)
	if fp(viewVariant(t, "max_children: 100", "max_children: 7")) != base {
		t.Fatal("max_children must not change the fingerprint")
	}
	for name, cfg := range map[string]string{
		"order_by": viewVariant(t, "order_by: [position]", "order_by: [heading]"),
		"field":    viewVariant(t, "{name: heading, column: heading}", "{name: head, column: heading}"),
		"parent":   viewVariant(t, "parent: [tenant_id, document_id]", "parent: [tenant_id, doc_id]"),
		"template": viewVariant(t, `{{.title}}{{range .sections}}`, `{{.title}}:{{range .sections}}`),
	} {
		if fp(cfg) == base {
			t.Errorf("changing the %s must change the fingerprint", name)
		}
	}
}

func TestComponentValue(t *testing.T) {
	c, err := Parse([]byte(viewCfg))
	if err != nil {
		t.Fatal(err)
	}
	sec := c.Component("kb:sections")
	str := func(s string) *string { return &s }
	key := []string{"1", "7"}
	fields := map[string]*string{
		HiddenField("document_id"): str("3"),
		"heading":                  str("Intro"),
	}
	for col, want := range map[string]string{"tenant_id": "1", "id": "7", "document_id": "3", "heading": "Intro"} {
		v, ok := sec.Value(col, key, fields)
		if !ok || v == nil || *v != want {
			t.Errorf("Value(%q) = %v, %v; want %q", col, v, ok, want)
		}
	}
	if v, ok := sec.Value("position", key, fields); !ok || v != nil {
		t.Errorf("Value(position) = %v, %v; want a carried column with a NULL value", v, ok)
	}
	if _, ok := sec.Value("nope", key, fields); ok {
		t.Error("Value of an unknown column must report ok=false")
	}
}
