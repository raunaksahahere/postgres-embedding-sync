package search

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/filter"
)

func TestFuseRRF(t *testing.T) {
	vec := []Hit{{Pipeline: "a", Key: "1", ChunkNo: 0}, {Pipeline: "a", Key: "2", ChunkNo: 1}, {Pipeline: "b", Key: "9"}}
	txt := []Hit{{Pipeline: "a", Key: "2", ChunkNo: 3}, {Pipeline: "c", Key: "5"}}
	got := Fuse(vec, txt, 60, 10)
	if len(got) != 4 {
		t.Fatalf("got %d hits", len(got))
	}
	// a/2: 1/62 + 1/61 beats a/1: 1/61.
	if got[0].Key != "2" || got[0].ChunkNo != 1 || got[0].Ranks["vector"] != 2 || got[0].Ranks["text"] != 1 {
		t.Fatalf("first = %+v", got[0])
	}
	want := 1.0/62 + 1.0/61
	if d := got[0].Score - want; d > 1e-12 || d < -1e-12 {
		t.Fatalf("score %v want %v", got[0].Score, want)
	}
	// a/1 (vector rank 1) and c/5 (text rank 2): 1/61 vs 1/62.
	if got[1].Key != "1" || got[2].Key != "5" || got[3].Key != "9" {
		t.Fatalf("order = %+v", got)
	}
	if _, ok := got[1].Ranks["text"]; ok {
		t.Fatal("a/1 has no text rank")
	}
	// Equal scores sort by pipeline, then key.
	tie := Fuse([]Hit{{Pipeline: "b", Key: "1"}}, []Hit{{Pipeline: "a", Key: "1"}}, 60, 10)
	if tie[0].Pipeline != "a" {
		t.Fatalf("tie order = %+v", tie)
	}
	if n := len(Fuse(vec, txt, 60, 2)); n != 2 {
		t.Fatalf("k not applied: %d", n)
	}
}

func TestValidate(t *testing.T) {
	s := &Searcher{RRFK: 60, Docs: map[string]bool{"docs": true},
		Meta: filter.Schema{"lang": {Name: "lang", Type: filter.Text}, "org": {Name: "org", Type: filter.Int}}}
	str := func(v string) *string { return &v }
	cases := []struct {
		name   string
		query  string
		o      Options
		status int
		code   string
		field  string
	}{
		{"empty query", "", Options{}, 400, "invalid_request", "query"},
		{"bad mode", "x", Options{Mode: "fuzzy"}, 400, "invalid_request", "mode"},
		{"k too big", "x", Options{K: 101}, 400, "invalid_request", "k"},
		{"rrf too big", "x", Options{RRFK: 5000}, 400, "invalid_request", "rrf_k"},
		{"unknown pipeline", "x", Options{Pipelines: []string{"nope"}}, 404, "unknown_pipeline", "pipelines"},
		{"unknown field", "x", Options{Filter: json.RawMessage(`{"field":"zz","op":"exists"}`)}, 400, "unknown_field", "zz"},
		{"bad value", "x", Options{Filter: json.RawMessage(`{"field":"org","op":"eq","value":"a"}`)}, 400, "invalid_value", "org"},
	}
	for _, c := range cases {
		_, err := s.validate(c.query, c.o)
		e := AsError(err)
		if err == nil || e.Status != c.status || e.Code != c.code || e.Field != c.field {
			t.Errorf("%s: got %v (%+v)", c.name, err, e)
		}
	}
	r, err := s.validate("x", Options{})
	if err != nil || r.mode != ModeVector || r.k != 10 || r.rrfK != 60 || r.restricted() {
		t.Fatalf("defaults: %+v %v", r, err)
	}

	s.TenantField, s.TenantHeader = "org", "X-Tenant-ID"
	for _, c := range []struct {
		o    Options
		code string
	}{
		{Options{}, "tenant_required"},
		{Options{Tenant: str(" ")}, "tenant_required"},
		{Options{Tenant: str("abc")}, "invalid_value"},
		{Options{Tenant: str("7"), Filter: json.RawMessage(`{"not":{"field":"org","op":"eq","value":8}}`)}, "tenant_filter_forbidden"},
	} {
		_, err := s.validate("x", c.o)
		if e := AsError(err); err == nil || e.Code != c.code || e.Status != http.StatusBadRequest {
			t.Errorf("tenant %+v: got %v", c.o, err)
		}
	}
	r, err = s.validate("x", Options{Tenant: str("7"), Filter: json.RawMessage(`{"field":"lang","op":"eq","value":"en"}`)})
	if err != nil || r.tenant == nil || !r.restricted() {
		t.Fatalf("tenant request: %+v %v", r, err)
	}
	var args []any
	sql := r.where(&args)
	if len(args) != 4 || args[1] != int64(7) && args[1] != float64(7) {
		t.Fatalf("where %s args %v", sql, args)
	}
}

func TestAsErrorBackend(t *testing.T) {
	e := AsError(errors.New("connection refused"))
	if e.Status != http.StatusServiceUnavailable || e.Code != "backend" {
		t.Fatalf("%+v", e)
	}
}
