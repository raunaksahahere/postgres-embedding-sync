package verify

import (
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

func sp(s string) *string { return &s }

var testDecls = []config.MetaField{
	{Name: "tenant", Column: "tenant_id", Type: "int"},
	{Name: "lang", Column: "lang", Type: "text"},
	{Name: "score", Column: "score", Type: "float"},
	{Name: "public", Column: "is_public", Type: "bool"},
	{Name: "published", Column: "published_at", Type: "timestamp"},
	{Name: "tags", Column: "tags", Type: "text_array"},
}

func metaOf(doc string) []config.MetaField {
	if doc == "docs" || doc == "kb" {
		return testDecls
	}
	return nil
}

func TestExpectedMetaEveryType(t *testing.T) {
	tr, err := transform.New("public.docs", "{{.body}}", []string{"body"}, 3, 0, "fp")
	if err != nil {
		t.Fatal(err)
	}
	trPlain, err := transform.New("public.notes", "{{.body}}", []string{"body"}, 50, 0, "fp")
	if err != nil {
		t.Fatal(err)
	}
	p := config.MetaFieldPrefix
	rows := Rows{
		"docs": {
			{Key: "1", Fields: map[string]*string{"body": sp("one two three four five six seven"),
				p + "tenant": sp("42"), p + "lang": sp("en"), p + "score": sp("1.5"), p + "public": sp("t"),
				p + "published": sp("2026-01-02 03:04:05+00"), p + "tags": sp(`{a,"b c"}`)}},
			// NULL and unparsable values are left out of meta.
			{Key: "2", Fields: map[string]*string{"body": sp("short"), p + "tenant": nil, p + "lang": sp("de"), p + "score": sp("x")}},
		},
		"notes": {{Key: "9", Fields: map[string]*string{"body": sp("no metadata here")}}},
	}
	exp, poison, err := Expected(map[string]*transform.Transformer{"docs": tr, "notes": trPlain}, transform.ModelIdentity{Name: "m"}, rows, 1<<20, metaOf)
	if err != nil || len(poison) != 0 {
		t.Fatalf("expected: %v poison=%v", err, poison)
	}
	want1 := `{"lang":"en","public":true,"published":1767323045,"score":1.5,"tags":["a","b c"],"tenant":42}`
	n1 := 0
	for ck, v := range exp {
		switch ck.pipeline + "/" + ck.key {
		case "docs/1":
			n1++
			if !JSONEqual(v.meta, want1) {
				t.Errorf("docs/1 chunk %d meta %s, want %s", ck.no, v.meta, want1)
			}
		case "docs/2":
			if !JSONEqual(v.meta, `{"lang":"de"}`) {
				t.Errorf("docs/2 meta %s", v.meta)
			}
		case "notes/9":
			if v.meta != "{}" {
				t.Errorf("notes/9 meta %s, want {}", v.meta)
			}
		}
	}
	if n1 < 2 {
		t.Fatalf("docs/1 should span several chunks (each carrying the meta), got %d", n1)
	}
	// Without declarations every chunk's meta is {}.
	exp, _, _ = Expected(map[string]*transform.Transformer{"docs": tr, "notes": trPlain}, transform.ModelIdentity{Name: "m"}, rows, 1<<20, nil)
	for ck, v := range exp {
		if v.meta != "{}" {
			t.Errorf("%v: meta %s without declarations", ck, v.meta)
		}
	}
}

func TestExpectedViewsRootMeta(t *testing.T) {
	tr, err := transform.NewView("kb", "{{.title}}", []string{"title"}, nil, 50, 0, "fp")
	if err != nil {
		t.Fatal(err)
	}
	p := config.MetaFieldPrefix
	views := map[string]ViewDocs{"kb": {MaxChildren: 10, Docs: map[string]*viewdoc.Doc{
		"1": {Key: "1", Root: map[string]*string{"title": sp("Hello"), p + "tenant": sp("7"), p + "tags": sp("{}")}, Lists: map[string][]viewdoc.Row{}},
	}}}
	out, poison := map[chunkKey]chunkVal{}, map[chunkKey]bool{}
	if err := ExpectedViews(map[string]*transform.Transformer{"kb": tr}, transform.ModelIdentity{Name: "m"}, views, 1<<20, metaOf, out, poison); err != nil {
		t.Fatal(err)
	}
	v, ok := out[chunkKey{"kb", "1", 0}]
	if !ok {
		t.Fatalf("no chunk: %v", out)
	}
	if v.text != "Hello" {
		t.Errorf("hidden metadata fields must not be rendered: %q", v.text)
	}
	if !JSONEqual(v.meta, `{"tenant":7,"tags":[]}`) {
		t.Errorf("view meta %s", v.meta)
	}
}

func TestViewRootFieldsCarryMetadata(t *testing.T) {
	v := &config.ViewDef{Fields: []config.Field{{Name: "title", Column: "title"}},
		Metadata: []config.MetaField{{Name: "tenant", Column: "tenant_id", Type: "int"}}}
	fs := viewRootFields(v)
	if len(fs) != 2 || fs[1].Name != config.MetaFieldPrefix+"tenant" || fs[1].Column != "tenant_id" {
		t.Fatalf("root fields %+v", fs)
	}
	if len(v.Fields) != 1 {
		t.Fatal("viewRootFields modified the view's fields")
	}
}

func TestJSONEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{`{"a":1,"b":"x"}`, `{"b": "x", "a": 1}`, true},
		{`{"t":1.767323045e+09}`, `{"t": 1767323045}`, true},
		{`{"f":1.5}`, `{"f": 1.50}`, true},
		{`{"s":"<a&b>"}`, `{"s":"<a&b>"}`, true},
		{`{"n":9007199254740993}`, `{"n":9007199254740992}`, false},
		{`{"a":[1,2]}`, `{"a":[2,1]}`, false},
		{`{"a":1}`, `{"a":"1"}`, false},
		{`{}`, `{"a":null}`, false},
		{`{}`, `{}`, true},
		{`not json`, `{}`, false},
	}
	for _, c := range cases {
		if got := JSONEqual(c.a, c.b); got != c.want {
			t.Errorf("JSONEqual(%s, %s) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestOKCountsSearchColumns(t *testing.T) {
	if !(GenReport{}).OK() {
		t.Fatal("empty report should be OK")
	}
	if (GenReport{MetaStale: 1}).OK() || (GenReport{TSVStale: 1}).OK() {
		t.Fatal("stale search columns must fail OK")
	}
	// Building is reported, not a failure.
	if !(GenReport{SearchBuilding: true}).OK() {
		t.Fatal("search columns building must not fail OK")
	}
}

func TestSearchCheckFor(t *testing.T) {
	cfg := &config.Config{Search: config.SearchConfig{TextConfig: "english"},
		Pipelines: []config.PipelineDef{{Name: "docs", Metadata: testDecls}}}
	sc := SearchCheckFor(cfg)
	if sc.TextConfig != "english" || sc.Fingerprint != cfg.SearchFingerprint() || len(sc.meta("docs")) != len(testDecls) {
		t.Fatalf("search check %+v", sc)
	}
	g := &store.Generation{SearchFingerprint: "old"}
	if g.SearchReady(sc.Fingerprint) {
		t.Fatal("a generation built for another fingerprint is still building")
	}
	if (SearchCheck{}).meta("docs") != nil {
		t.Fatal("zero SearchCheck has no declarations")
	}
}
