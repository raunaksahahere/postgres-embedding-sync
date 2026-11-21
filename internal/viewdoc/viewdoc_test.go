package viewdoc

import (
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
)

func s(v string) *string { return &v }

func TestCompareValues(t *testing.T) {
	cases := []struct {
		k    Kind
		a, b *string
		want int
	}{
		{Int, s("9"), s("10"), -1},
		{Text, s("B"), s("a"), -1},
		{Text, s("é"), s("z"), 1},
		{Int, nil, s("1"), 1}, // NULLs last
		{Int, s("1"), nil, -1},
		{Time, s("2026-01-01 10:00:00+05:30"), s("2026-01-01 05:00:00+00"), -1}, // 04:30Z < 05:00Z
		{Time, s("2026-01-02"), s("2026-01-01"), 1},
		{Time, s("-infinity"), s("1900-01-01"), -1},
		{Time, s("infinity"), s("2999-01-01 00:00:00"), 1},
	}
	for _, c := range cases {
		if got := CompareValues(c.k, c.a, c.b); got != c.want {
			t.Fatalf("CompareValues(%s, %v, %v) = %d, want %d", c.k, deref(c.a), deref(c.b), got, c.want)
		}
	}
}

func TestSortRowsAndData(t *testing.T) {
	codec := keys.NewCodec(keys.Int, keys.Int)
	k := func(a, b string) string { e, _ := codec.Encode([]string{a, b}); return e }
	rows := []Row{
		{Key: k("1", "3"), Fields: map[string]*string{"__position": s("2"), "body": s("c")}},
		{Key: k("1", "10"), Fields: map[string]*string{"__position": s("1"), "body": s("b")}},
		{Key: k("1", "2"), Fields: map[string]*string{"__position": s("1"), "body": s("a")}},
		{Key: k("1", "1"), Fields: map[string]*string{"__position": nil, "body": s("z")}},
	}
	SortRows(rows, []Kind{Int}, func(r Row) []*string { return []*string{r.Fields["__position"]} }, codec)
	var order string
	for _, r := range rows {
		order += *r.Fields["body"]
	}
	if order != "abcz" { // position 1 (key 2 before key 10), position 2, NULL last
		t.Fatalf("order %q", order)
	}
	d := &Doc{Root: map[string]*string{"title": s("T"), "__tenant": s("1")}, Lists: map[string][]Row{"sections": rows}}
	data := d.Data()
	if _, hidden := data["__tenant"]; hidden || data["title"] != "T" {
		t.Fatalf("data %v", data)
	}
	secs := data["sections"].([]map[string]string)
	if len(secs) != 4 || secs[3]["body"] != "z" || secs[0]["__position"] != "" {
		t.Fatalf("sections %v", secs)
	}
	if _, ok := secs[0]["__position"]; ok {
		t.Fatal("hidden fields must not reach the template")
	}
	sig := "tenant_id:23,id:20,position:23,at:1184"
	ks, err := KindsFromSignature(sig, []string{"position", "at"})
	if err != nil || ks[0] != Int || ks[1] != Time {
		t.Fatalf("kinds %v %v", ks, err)
	}
	if _, err := KindsFromSignature(sig, []string{"nope"}); err == nil {
		t.Fatal("missing column must fail")
	}
}
