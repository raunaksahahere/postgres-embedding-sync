//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

var articleSetup = []string{
	`CREATE TABLE public.articles (id bigint PRIMARY KEY, title text, body text, lang text, year int, tags text[],
		published timestamptz, org text, rating float8, featured bool, internal text)`,
}

const articlePipeline = `  - name: articles
    table: public.articles
    key: id
    fields: [{name: title, column: title}, {name: body, column: body}]
    transform: {template: "{{.title}}\n{{.body}}", chunk_words: 20, chunk_overlap: 4}
`

const articleMetadata = `    metadata:
      - {name: lang, column: lang, type: text}
      - {name: year, column: year, type: int}
      - {name: tags, column: tags, type: text_array}
      - {name: published, column: published, type: timestamp}
      - {name: org, column: org, type: text}
      - {name: rating, column: rating, type: float}
      - {name: featured, column: featured, type: bool}
`

const docsPipeline = `  - name: docs
    table: public.documents
    key: id
    fields: [{name: title, column: title}, {name: body, column: body}]
`

// Vocabulary of seeded articles: no synonym of the fake encoder and no
// word of the special documents below.
var articleWords = strings.Fields(`amber basil cedar delta ember fjord grove harbor island juniper kelp lagoon meadow nectar orchard
	prairie quartz ridge summit thicket upland valley willow yarrow zenith`)

const (
	exactIDDoc    = 1000 // the only document containing SKU-48213
	paraphraseDoc = 1001 // "car" (the fake encoder maps automobile -> car)
	nArticles     = 60
)

func (e *env) seedArticles() {
	e.t.Helper()
	langs := []string{"en", "de", "fr"}
	for i := 1; i <= nArticles; i++ {
		var body []string
		for j := 0; j < 12; j++ {
			body = append(body, articleWords[(i*7+j*3)%len(articleWords)])
		}
		body = append(body, "shared")
		e.exec(`INSERT INTO articles (id, title, body, lang, year, tags, published, org, rating, featured, internal)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'hidden')`,
			i, fmt.Sprintf("article %d", i), strings.Join(body, " "), langs[i%3], 2015+i%10,
			[]string{fmt.Sprintf("t%d", i%4), "all"}, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i),
			fmt.Sprintf("org%d", i%2), float64(i)/10, i%5 == 0)
	}
	e.exec(`INSERT INTO articles (id, title, body, lang, year, tags, published, org, rating, featured) VALUES
		($1, 'order note', 'Order reference SKU-48213 was shipped on time', 'en', 2023, '{orders}', '2023-05-01', 'org1', 1.0, false),
		($2, 'road trip', 'my car broke down on the highway near the summit', 'en', 2024, '{travel}', '2024-02-02 10:00:00+05:30', 'org0', 2.0, true)`,
		exactIDDoc, paraphraseDoc)
}

type apiHit struct {
	Pipeline string         `json:"pipeline"`
	Key      string         `json:"key"`
	ChunkNo  int            `json:"chunk_no"`
	Score    float64        `json:"score"`
	Content  string         `json:"content"`
	Ranks    map[string]int `json:"ranks"`
}

type apiResp struct {
	Generation string   `json:"generation"`
	Mode       string   `json:"mode"`
	Hits       []apiHit `json:"hits"`
	Code       string   `json:"code"`
	Field      string   `json:"field"`
	Error      string   `json:"error"`
	raw        map[string]any
	header     http.Header
}

func (r apiResp) keys() []string {
	out := []string{}
	for _, h := range r.Hits {
		out = append(out, h.Pipeline+"/"+h.Key)
	}
	return out
}

func (e *env) searchReq(method, path string, body any, headers map[string]string) (int, apiResp) {
	e.t.Helper()
	var rd *bytes.Reader
	switch b := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case string:
		rd = bytes.NewReader([]byte(b))
	default:
		j, _ := json.Marshal(b)
		rd = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", e.port, path), rd)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	var r apiResp
	if err := json.Unmarshal(buf.Bytes(), &r); err != nil {
		e.t.Fatalf("%s %s: status %d, body %q: %v", method, path, resp.StatusCode, buf.String(), err)
	}
	_ = json.Unmarshal(buf.Bytes(), &r.raw)
	r.header = resp.Header
	return resp.StatusCode, r
}

func (e *env) post(body map[string]any, headers map[string]string) (int, apiResp) {
	return e.searchReq(http.MethodPost, "/search", body, headers)
}

func (e *env) mustPost(body map[string]any, headers map[string]string) apiResp {
	e.t.Helper()
	code, r := e.post(body, headers)
	if code != 200 {
		e.t.Fatalf("POST /search %v: %d %+v", body, code, r)
	}
	return r
}

func (e *env) sqlKeys(pipeline, q string) []string {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(), q)
	if err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, pipeline+"/"+k)
	}
	return out
}

func sameSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string{}, got...)
	w := append([]string{}, want...)
	sort.Strings(g)
	sort.Strings(w)
	if strings.Join(g, ",") != strings.Join(w, ",") {
		t.Fatalf("%s:\n got  %v\n want %v", label, g, w)
	}
}

func (e *env) searchState() string {
	e.t.Helper()
	for _, g := range e.status()["generations"].([]any) {
		gm := g.(map[string]any)
		if gm["serving"] == true {
			s, _ := gm["search"].(string)
			return s
		}
	}
	return ""
}

func (e *env) waitSearchReady() {
	e.t.Helper()
	waitFor(e.t, 90*time.Second, "search columns ready", func() bool { return e.searchState() == "ready" })
}

func (e *env) docEmbedInputs() int {
	e.emb.mu.Lock()
	defer e.emb.mu.Unlock()
	n := 0
	for _, l := range e.emb.log {
		f := strings.Fields(l)
		if len(f) == 3 && f[1] != "query" {
			var c int
			fmt.Sscan(f[2], &c)
			n += c
		}
	}
	return n
}

// Filtered, full-text and hybrid search: every filter operator returns
// exactly the matching documents, an exact identifier wins through the text
// branch, a paraphrase through the vector branch, metadata-only updates
// change results without re-embedding, and errors are typed.
func TestSearchFiltersHybridAcceptance(t *testing.T) {
	e := newEnv(t, envOpts{pipelines: docsPipeline + articlePipeline + articleMetadata, setupSQL: articleSetup})
	e.seed(20, 6)
	e.seedArticles()
	p := e.start()
	defer p.stop(t)
	e.waitReady()
	waitFor(t, 60*time.Second, "backfill", e.jobsDone)
	e.waitSearchReady()
	e.requireConsistent("search columns built")

	art := func(where string) []string {
		return e.sqlKeys("articles", `SELECT id::text FROM articles WHERE `+where)
	}
	allDocs := e.sqlKeys("docs", `SELECT id::text FROM documents`)
	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		{"eq", `{"field":"lang","op":"eq","value":"en"}`, art(`lang = 'en'`)},
		{"range and ne", `{"and":[{"field":"year","op":"range","gte":2018,"lt":2021},{"field":"lang","op":"ne","value":"de"}]}`,
			art(`year >= 2018 AND year < 2021 AND lang <> 'de'`)},
		{"contains", `{"field":"tags","op":"contains","value":"t1"}`, art(`'t1' = ANY(tags)`)},
		{"in", `{"field":"lang","op":"in","values":["fr","de"]}`, art(`lang IN ('fr','de')`)},
		{"timestamp range", `{"field":"published","op":"range","gte":"2020-02-01","lt":"2024-02-02T04:30:01Z"}`,
			art(`published >= '2020-02-01' AND published < '2024-02-02T04:30:01Z'`)},
		{"or bool float", `{"or":[{"field":"featured","op":"eq","value":true},{"field":"rating","op":"range","gt":5.5}]}`,
			art(`featured OR rating > 5.5`)},
		{"not includes missing", `{"not":{"field":"org","op":"eq","value":"org0"}}`, append(art(`org <> 'org0'`), allDocs...)},
		{"exists", `{"field":"year","op":"exists"}`, art(`year IS NOT NULL`)},
	}
	for _, c := range cases {
		for _, mode := range []string{"vector", "hybrid"} {
			r := e.mustPost(map[string]any{"query": "shared meadow", "k": 100, "mode": mode, "exact": true, "filter": json.RawMessage(c.filter)}, nil)
			sameSet(t, c.name+" "+mode, r.keys(), c.want)
		}
		r := e.mustPost(map[string]any{"query": "shared", "k": 100, "mode": "text", "pipelines": []string{"articles"}, "filter": json.RawMessage(c.filter)}, nil)
		var wantArt []string
		for _, k := range c.want {
			if strings.HasPrefix(k, "articles/") && k != fmt.Sprintf("articles/%d", exactIDDoc) && k != fmt.Sprintf("articles/%d", paraphraseDoc) {
				wantArt = append(wantArt, k)
			}
		}
		sameSet(t, c.name+" text", r.keys(), wantArt)
	}
	// Pipelines restrict; a field no document of the pipeline has matches nothing.
	if r := e.mustPost(map[string]any{"query": "shared", "k": 100, "pipelines": []string{"docs"}, "filter": json.RawMessage(`{"field":"lang","op":"exists"}`)}, nil); len(r.Hits) != 0 {
		t.Fatalf("docs have no lang: %v", r.keys())
	}
	r := e.mustPost(map[string]any{"query": "seed body word", "k": 100, "pipelines": []string{"docs"}}, nil)
	sameSet(t, "pipelines=docs", r.keys(), allDocs)

	// A selective filter through the ANN index still fills k.
	r = e.mustPost(map[string]any{"query": "shared meadow", "k": 5, "filter": json.RawMessage(`{"field":"lang","op":"eq","value":"fr"}`)}, nil)
	if len(r.Hits) != 5 {
		t.Fatalf("filtered ANN returned %d hits: %v", len(r.Hits), r.keys())
	}
	for _, k := range r.keys() {
		if !strings.HasPrefix(k, "articles/") || e.scalar(`SELECT lang FROM articles WHERE id = `+strings.TrimPrefix(k, "articles/")) != "fr" {
			t.Fatalf("filtered ANN hit %s is not fr", k)
		}
	}

	// An exact identifier: found by the text branch, first in text and hybrid.
	for _, mode := range []string{"text", "hybrid"} {
		r := e.mustPost(map[string]any{"query": "SKU-48213", "k": 5, "mode": mode}, nil)
		if len(r.Hits) == 0 || r.Hits[0].Key != fmt.Sprint(exactIDDoc) {
			t.Fatalf("%s: exact id not first: %+v", mode, r.Hits)
		}
		if mode == "hybrid" && r.Hits[0].Ranks["text"] != 1 {
			t.Fatalf("hybrid ranks %v", r.Hits[0].Ranks)
		}
	}
	// A paraphrase: no shared word, so only the vector branch finds it.
	r = e.mustPost(map[string]any{"query": "automobile", "k": 5, "mode": "text"}, nil)
	for _, h := range r.Hits {
		if h.Key == fmt.Sprint(paraphraseDoc) {
			t.Fatal("the text branch matched a paraphrase")
		}
	}
	r = e.mustPost(map[string]any{"query": "automobile", "k": 5, "mode": "hybrid"}, nil)
	if len(r.Hits) == 0 || r.Hits[0].Key != fmt.Sprint(paraphraseDoc) || r.Hits[0].Ranks["vector"] != 1 {
		t.Fatalf("paraphrase not first in hybrid: %+v", r.Hits)
	}
	if _, ok := r.Hits[0].Ranks["text"]; ok {
		t.Fatalf("paraphrase has a text rank: %v", r.Hits[0].Ranks)
	}
	if want := 1.0 / 61; r.Hits[0].Score < want-1e-9 || r.Hits[0].Score > want+1e-9 {
		t.Fatalf("rrf score %v, want %v", r.Hits[0].Score, want)
	}

	// Deterministic: the same hybrid request answers the same.
	a := e.mustPost(map[string]any{"query": "shared summit", "k": 20, "mode": "hybrid"}, nil)
	b := e.mustPost(map[string]any{"query": "shared summit", "k": 20, "mode": "hybrid"}, nil)
	if fmt.Sprint(a.Hits) != fmt.Sprint(b.Hits) {
		t.Fatal("hybrid results differ between identical requests")
	}

	// Legacy GET is unchanged: vector search, same hits as POST.
	code, g := e.searchReq(http.MethodGet, "/search?q="+url.QueryEscape("shared summit")+"&k=7", nil, nil)
	pv := e.mustPost(map[string]any{"query": "shared summit", "k": 7}, nil)
	if code != 200 || g.Mode != "vector" || fmt.Sprint(g.Hits) != fmt.Sprint(pv.Hits) {
		t.Fatalf("legacy GET: %d %+v vs %+v", code, g.Hits, pv.Hits)
	}
	for _, k := range []string{"generation", "model", "revision", "provider", "exact", "hits", "embed_ms", "query_ms"} {
		if _, ok := g.raw[k]; !ok {
			t.Fatalf("legacy response lost %q", k)
		}
	}
	// GET takes the new parameters too.
	code, g = e.searchReq(http.MethodGet, "/search?q=shared&k=100&mode=text&pipeline=articles&filter="+url.QueryEscape(`{"field":"lang","op":"eq","value":"de"}`), nil, nil)
	if code != 200 || len(g.Hits) != len(art(`lang = 'de'`)) {
		t.Fatalf("GET with filter: %d %v", code, g.keys())
	}

	// Metadata-only update: results change, nothing is re-embedded.
	before := e.chunkRows("chunks_v1", "articles")
	embeds := e.docEmbedInputs()
	e.exec(`UPDATE articles SET lang = 'xx', featured = NOT featured WHERE id IN (3, 4)`)
	waitFor(t, 30*time.Second, "metadata update visible", func() bool {
		r := e.mustPost(map[string]any{"query": "shared", "k": 10, "mode": "text", "filter": json.RawMessage(`{"field":"lang","op":"eq","value":"xx"}`)}, nil)
		return fmt.Sprint(r.keys()) == "[articles/3 articles/4]" || fmt.Sprint(r.keys()) == "[articles/4 articles/3]"
	})
	if n := e.docEmbedInputs() - embeds; n != 0 {
		t.Fatalf("a metadata-only update embedded %d inputs", n)
	}
	after := e.chunkRows("chunks_v1", "articles")
	if len(before) != len(after) {
		t.Fatal("chunk count changed")
	}
	for i := range before {
		if before[i].Key != after[i].Key || before[i].FP != after[i].FP || before[i].Emb != after[i].Emb {
			t.Fatalf("chunk %s/%s changed: %+v -> %+v", before[i].Key, before[i].No, before[i], after[i])
		}
	}
	// A text change re-embeds as before.
	e.exec(`UPDATE articles SET body = 'fresh quartz zenith shared prose' WHERE id = 5`)
	waitFor(t, 30*time.Second, "text update", func() bool { return e.docEmbedInputs() > embeds })

	// Errors: 400 with code and field, 404 for unknown names.
	for _, c := range []struct {
		body         any
		status       int
		code, field  string
		method, path string
	}{
		{map[string]any{"query": "x", "filter": json.RawMessage(`{"field":"year","op":"eq","value":"x"}`)}, 400, "invalid_value", "year", "", ""},
		{map[string]any{"query": "x", "filter": json.RawMessage(`{"field":"nope","op":"exists"}`)}, 400, "unknown_field", "nope", "", ""},
		{map[string]any{"query": "x", "filter": json.RawMessage(`{"field":"tags","op":"range","gt":"a"}`)}, 400, "unsupported_op", "tags", "", ""},
		{map[string]any{"query": "x", "mode": "fuzzy"}, 400, "invalid_request", "mode", "", ""},
		{map[string]any{"query": "x", "k": 1000}, 400, "invalid_request", "k", "", ""},
		{map[string]any{"query": ""}, 400, "invalid_request", "query", "", ""},
		{`{"query": "x", "unknown": 1}`, 400, "invalid_request", "", "", ""},
		{`{"query": 7}`, 400, "invalid_request", "query", "", ""},
		{map[string]any{"query": "x", "generation": "nope"}, 404, "unknown_generation", "generation", "", ""},
		{map[string]any{"query": "x", "pipelines": []string{"nope"}}, 404, "unknown_pipeline", "pipelines", "", ""},
		{nil, 400, "invalid_filter", "", http.MethodGet, "/search?q=x&filter=" + url.QueryEscape(`{"and":[]}`)},
		{nil, 405, "method_not_allowed", "", http.MethodDelete, "/search"},
	} {
		method, path := c.method, c.path
		if method == "" {
			method, path = http.MethodPost, "/search"
		}
		code, r := e.searchReq(method, path, c.body, nil)
		if code != c.status || r.Code != c.code || r.Field != c.field || r.Error == "" {
			t.Errorf("%s %s %v: got %d %+v, want %d %s %q", method, path, c.body, code, r, c.status, c.code, c.field)
		}
	}
	checkOpenAPI(t, e)
	e.requireConsistent("after metadata updates")
}

// checkOpenAPI checks live responses against api/openapi.yaml: every field
// a response carries is declared, and the declared required ones are present.
func checkOpenAPI(t *testing.T, e *env) {
	t.Helper()
	b, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Required   []string               `yaml:"required"`
				Properties map[string]interface{} `yaml:"properties"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	check := func(schema string, obj map[string]any) {
		s, ok := spec.Components.Schemas[schema]
		if !ok {
			t.Fatalf("openapi has no schema %s", schema)
		}
		for k := range obj {
			if _, ok := s.Properties[k]; !ok {
				t.Errorf("response field %q is not declared in %s", k, schema)
			}
		}
		for _, k := range s.Required {
			if _, ok := obj[k]; !ok {
				t.Errorf("%s requires %q, response lacks it", schema, k)
			}
		}
	}
	r := e.mustPost(map[string]any{"query": "SKU-48213 shared", "k": 3, "mode": "hybrid"}, nil)
	check("SearchResponse", r.raw)
	for _, h := range r.raw["hits"].([]any) {
		check("Hit", h.(map[string]any))
	}
	_, bad := e.post(map[string]any{"query": "x", "filter": json.RawMessage(`{"field":"nope","op":"exists"}`)}, nil)
	check("Error", bad.raw)
}

// Tenant scope from a trusted gateway header: required, forced on both
// branches, and not overridable by a client filter.
func TestSearchTenantScope(t *testing.T) {
	e := newEnv(t, envOpts{pipelines: docsPipeline + articlePipeline + articleMetadata, setupSQL: articleSetup,
		search: "  tenant_filter: {field: org, header: X-Tenant-ID}\n"})
	e.seed(10, 6)
	e.seedArticles()
	p := e.start()
	defer p.stop(t)
	e.waitReady()
	waitFor(t, 60*time.Second, "backfill", e.jobsDone)
	e.waitSearchReady()

	if code, r := e.post(map[string]any{"query": "shared"}, nil); code != 400 || r.Code != "tenant_required" || r.Field != "X-Tenant-ID" {
		t.Fatalf("no header: %d %+v", code, r)
	}
	if code, r := e.searchReq(http.MethodGet, "/search?q=shared", nil, nil); code != 400 || r.Code != "tenant_required" {
		t.Fatalf("legacy GET without header: %d %+v", code, r)
	}
	h := map[string]string{"X-Tenant-ID": "org0"}
	if code, r := e.post(map[string]any{"query": "shared", "filter": json.RawMessage(`{"or":[{"field":"org","op":"eq","value":"org1"},{"field":"lang","op":"exists"}]}`)}, h); code != 400 || r.Code != "tenant_filter_forbidden" {
		t.Fatalf("filter on the tenant field: %d %+v", code, r)
	}
	want := e.sqlKeys("articles", `SELECT id::text FROM articles WHERE org = 'org0'`)
	for _, mode := range []string{"vector", "hybrid"} {
		r := e.mustPost(map[string]any{"query": "shared meadow seed", "k": 100, "mode": mode, "exact": true}, h)
		sameSet(t, "tenant org0 "+mode, r.keys(), want)
	}
	r := e.mustPost(map[string]any{"query": "shared", "k": 100, "mode": "text"}, h)
	var wantText []string
	for _, k := range want {
		if k != fmt.Sprintf("articles/%d", paraphraseDoc) {
			wantText = append(wantText, k)
		}
	}
	sameSet(t, "tenant org0 text", r.keys(), wantText)
	r = e.mustPost(map[string]any{"query": "shared", "k": 100, "mode": "text", "filter": json.RawMessage(`{"field":"lang","op":"eq","value":"en"}`)},
		map[string]string{"X-Tenant-ID": "org1"})
	sameSet(t, "tenant org1 + lang", r.keys(), e.sqlKeys("articles", fmt.Sprintf(`SELECT id::text FROM articles WHERE org = 'org1' AND lang = 'en' AND id <> %d`, exactIDDoc)))
	code, g := e.searchReq(http.MethodGet, "/search?q=shared&k=100", nil, h)
	if code != 200 {
		t.Fatalf("GET with header: %d %+v", code, g)
	}
	for _, k := range g.keys() {
		if !strings.HasPrefix(k, "articles/") || e.scalar(`SELECT org FROM articles WHERE id = `+strings.TrimPrefix(k, "articles/")) != "org0" {
			t.Fatalf("GET leaked %s across tenants", k)
		}
	}
	if code, r := e.post(map[string]any{"query": "shared"}, map[string]string{"X-Tenant-ID": "nobody"}); code != 200 || len(r.Hits) != 0 {
		t.Fatalf("unknown tenant: %d %v", code, r.keys())
	}
}

// Upgrading a populated v0.5.0 installation: legacy GET answers exactly as
// before throughout, the search columns build in the background without
// re-embedding (filtered requests answer 503 meanwhile), then filters work.
func TestUpgradeFrom050BuildsSearchColumns(t *testing.T) {
	old := binaryAt(t, "v0.5.0")
	e := newEnv(t, envOpts{binary: old, batch: 5, pipelines: docsPipeline + articlePipeline, setupSQL: articleSetup})
	e.seed(30, 6)
	e.seedArticles()
	p := e.start()
	e.waitReady()
	waitFor(t, 60*time.Second, "v0.5.0 backfill", e.jobsDone)
	e.requireConsistent("v0.5.0 installation")
	queries := []string{"shared summit", "seed body word 7", "SKU-48213"}
	golden := map[string]string{}
	for _, q := range queries {
		code, r := e.searchReq(http.MethodGet, "/search?q="+url.QueryEscape(q)+"&k=8", nil, nil)
		if code != 200 || len(r.Hits) == 0 {
			t.Fatalf("v0.5.0 search %q: %d %+v", q, code, r)
		}
		golden[q] = fmt.Sprint(r.Hits)
	}
	before := append(e.chunkRows("chunks_v1", "docs"), e.chunkRows("chunks_v1", "articles")...)
	p.stop(t)

	e.bin = binary
	e.opts.pipelines = docsPipeline + articlePipeline + articleMetadata
	e.writeConfig()
	out, err := e.cli("init")
	if err != nil || !strings.Contains(out, "sink: applied sink/0005_search.sql") {
		t.Fatalf("upgrade init: %v %s", err, out)
	}
	embeds := e.docEmbedInputs()
	p = e.start("EMBEDSYNC_PAUSEPOINTS=backfill_window=700ms")
	defer p.stop(t)
	e.waitReady()
	sawBuilding := false
	for i := 0; i < 50 && e.searchState() != "ready"; i++ {
		code, r := e.post(map[string]any{"query": "shared", "filter": json.RawMessage(`{"field":"lang","op":"eq","value":"en"}`)}, nil)
		if code == 503 {
			if r.Code != "search_index_building" || r.header.Get("Retry-After") == "" {
				t.Fatalf("building: %d %+v", code, r)
			}
			sawBuilding = true
		}
		for _, q := range queries {
			code, g := e.searchReq(http.MethodGet, "/search?q="+url.QueryEscape(q)+"&k=8", nil, nil)
			if code != 200 || fmt.Sprint(g.Hits) != golden[q] {
				t.Fatalf("legacy GET %q changed during the upgrade: %d\n got  %v\n want %s", q, code, g.Hits, golden[q])
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !sawBuilding {
		t.Fatal("never observed the search columns building")
	}
	e.waitSearchReady()
	if n := e.docEmbedInputs() - embeds; n != 0 {
		t.Fatalf("building the search columns embedded %d inputs", n)
	}
	after := append(e.chunkRows("chunks_v1", "docs"), e.chunkRows("chunks_v1", "articles")...)
	if len(after) != len(before) {
		t.Fatalf("chunks %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i].Key != after[i].Key || before[i].FP != after[i].FP || before[i].Emb != after[i].Emb {
			t.Fatalf("upgrade changed chunk %+v -> %+v", before[i], after[i])
		}
	}
	r := e.mustPost(map[string]any{"query": "shared", "k": 100, "mode": "hybrid", "exact": true, "filter": json.RawMessage(`{"field":"lang","op":"eq","value":"en"}`)}, nil)
	sameSet(t, "filter after upgrade", r.keys(), e.sqlKeys("articles", `SELECT id::text FROM articles WHERE lang = 'en'`))
	for _, q := range queries {
		_, g := e.searchReq(http.MethodGet, "/search?q="+url.QueryEscape(q)+"&k=8", nil, nil)
		if fmt.Sprint(g.Hits) != golden[q] {
			t.Fatalf("legacy GET %q changed after the upgrade", q)
		}
	}
	e.requireConsistent("after building search columns")

	// Changing a metadata declaration rebuilds the columns, again without embedding.
	p.stop(t)
	e.opts.pipelines = docsPipeline + articlePipeline + articleMetadata + "      - {name: note, column: internal, type: text}\n"
	e.writeConfig()
	if out, err := e.cli("init"); err != nil {
		t.Fatalf("init adding metadata: %v %s", err, out)
	}
	p = e.start()
	e.waitReady()
	e.waitSearchReady()
	r = e.mustPost(map[string]any{"query": "shared", "k": 100, "mode": "text", "filter": json.RawMessage(`{"field":"note","op":"eq","value":"hidden"}`)}, nil)
	sameSet(t, "new metadata field", r.keys(), e.sqlKeys("articles", fmt.Sprintf(`SELECT id::text FROM articles WHERE internal = 'hidden' AND id NOT IN (%d, %d)`, exactIDDoc, paraphraseDoc)))
	if n := e.docEmbedInputs() - embeds; n != 0 {
		t.Fatalf("adding a metadata field embedded %d inputs", n)
	}
	e.requireConsistent("after adding a metadata field")
}

// A promotion (here a rollback) between embedding the query and running it:
// the request answers entirely from the generation serving when it runs,
// with a query embedded by that generation's encoder.
func TestSearchPinnedAcrossPromotion(t *testing.T) {
	dir := t.TempDir()
	q, qr := writeEval(t, dir)
	e := newEnv(t, envOpts{evalQueries: q, evalQrels: qr})
	seedTopics(e)
	p := e.start("EMBEDSYNC_PAUSEPOINTS=search_after_embed=3s")
	defer p.stop(t)
	e.waitReady()
	if out, err := e.cli("migrate", "--to", "v2", "--json"); err != nil {
		t.Fatalf("migrate v2: %v\n%s", err, out)
	}
	topic := ""
	for i := range topics {
		if goodTopics()[i] {
			topic = topics[i]
			break
		}
	}
	query := url.QueryEscape("facts about the " + topic)
	type result struct {
		code int
		r    apiResp
	}
	done := make(chan result, 1)
	go func() {
		code, r := e.searchReq(http.MethodGet, "/search?q="+query+"&k=5", nil, nil)
		done <- result{code, r}
	}()
	waitFor(t, 10*time.Second, "query embedded for v2", func() bool {
		e.emb.mu.Lock()
		defer e.emb.mu.Unlock()
		for _, l := range e.emb.log {
			if strings.HasPrefix(l, "fake/bowalt query") {
				return true
			}
		}
		return false
	})
	if out, err := e.cli("migrate", "rollback"); err != nil {
		t.Fatalf("rollback: %v %s", err, out)
	}
	res := <-done
	if res.code != 200 || res.r.Generation != "v1" {
		t.Fatalf("pinned search: %d %+v", res.code, res.r)
	}
	code, fresh := e.searchReq(http.MethodGet, "/search?q="+query+"&k=5", nil, nil)
	if code != 200 || fmt.Sprint(fresh.Hits) != fmt.Sprint(res.r.Hits) {
		t.Fatalf("pinned result differs from a fresh v1 search:\n %v\n %v", res.r.Hits, fresh.Hits)
	}
	if len(res.r.Hits) == 0 || !strings.Contains(res.r.Hits[0].Content, topic) {
		t.Fatalf("v1 hits do not match the topic %q: %v", topic, res.r.Hits)
	}
}

// The LangChain and LlamaIndex examples retrieve through a running service
// (skipped without uv).
func TestRAGExamples(t *testing.T) {
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv is not installed")
	}
	e := newEnv(t, envOpts{pipelines: articlePipeline + articleMetadata, setupSQL: articleSetup})
	e.seedArticles()
	p := e.start()
	defer p.stop(t)
	e.waitReady()
	waitFor(t, 60*time.Second, "backfill", e.jobsDone)
	e.waitSearchReady()
	for _, mode := range []string{"hybrid", "vector"} {
		cmd := exec.Command("bash", "examples/run-integrations.sh", fmt.Sprintf("http://127.0.0.1:%d", e.port), "SKU-48213 order", mode)
		cmd.Dir = "../.."
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("examples (%s): %v\n%s", mode, err, out)
		}
		if !strings.Contains(string(out), "SKU-48213") {
			t.Fatalf("examples (%s) did not retrieve the order note:\n%s", mode, out)
		}
	}
}
