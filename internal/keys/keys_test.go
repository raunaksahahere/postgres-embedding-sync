package keys

import "testing"

func TestCompare(t *testing.T) {
	if Int.Compare("9", "10") != -1 || Int.Compare("-5", "3") != -1 || Int.Compare("7", "7") != 0 {
		t.Fatal("int keys must compare numerically")
	}
	if Text.Compare("9", "10") != 1 || Text.Compare("B", "a") != -1 {
		t.Fatal("text keys must compare bytewise (COLLATE \"C\")")
	}
	if UUID.Compare("0b4e6a1c-0000-4000-8000-000000000000", "a0000000-0000-4000-8000-000000000000") != -1 {
		t.Fatal("uuid order")
	}
	for _, oid := range []uint32{20, 21, 23, 25, 1043, 2950} {
		if _, err := FromTypeOID(oid); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := FromTypeOID(1700); err == nil {
		t.Fatal("numeric keys are unsupported")
	}
}

func TestCodecEncodingIsCollisionFree(t *testing.T) {
	single := NewCodec(Int)
	if s, _ := single.Encode([]string{"42"}); s != "42" {
		t.Fatalf("single-column keys must keep their legacy encoding, got %q", s)
	}
	c := NewCodec(Int, Text)
	tricky := [][]string{
		{"1", "a,b"}, {"1,a", "b"}, {"1", `a"b`}, {"1", `a\"b`}, {"1", "[\"x\"]"}, {"1", ""}, {"", "1"},
		{"1", "<&>"}, {"1", "naïve"}, {"1", "a\nb"}, {"-1", "x"},
	}
	seen := map[string][]string{}
	for _, v := range tricky {
		enc, err := c.Encode(v)
		if err != nil {
			t.Fatal(err)
		}
		if prev, dup := seen[enc]; dup {
			t.Fatalf("%v and %v both encode to %q", prev, v, enc)
		}
		seen[enc] = v
		dec, err := c.Decode(enc)
		if err != nil || len(dec) != 2 || dec[0] != v[0] || dec[1] != v[1] {
			t.Fatalf("round trip %v -> %q -> %v (%v)", v, enc, dec, err)
		}
	}
	if _, err := c.Encode([]string{"1", "\xff"}); err == nil {
		t.Fatal("invalid UTF-8 must be rejected, not replaced")
	}
	if _, err := c.Encode([]string{"1"}); err == nil {
		t.Fatal("wrong arity must be rejected")
	}
}

func TestCodecCompareIsLexicographicByColumn(t *testing.T) {
	c := NewCodec(Int, Text)
	enc := func(a, b string) string { s, _ := c.Encode([]string{a, b}); return s }
	cases := []struct {
		a, b string
		want int
	}{
		{enc("9", "z"), enc("10", "a"), -1}, // first column numeric, not text
		{enc("2", "B"), enc("2", "a"), -1},  // second column bytewise (COLLATE "C")
		{enc("-5", "x"), enc("3", "x"), -1},
		{enc("7", "é"), enc("7", "z"), 1}, // UTF-8 bytes: 0xC3 > 'z'
		{enc("7", "a"), enc("7", "a"), 0},
		{enc("7", "a"), enc("7", "ab"), -1},
	}
	for _, tc := range cases {
		if got := c.Compare(tc.a, tc.b); got != tc.want {
			t.Fatalf("Compare(%s, %s) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
		if got := c.Compare(tc.b, tc.a); got != -tc.want {
			t.Fatalf("Compare not antisymmetric for %s %s", tc.a, tc.b)
		}
	}
	p, err := ParseCodec(c.String())
	if err != nil || p.String() != "int,text" {
		t.Fatalf("ParseCodec: %v %v", p, err)
	}
}

func TestCodecSQL(t *testing.T) {
	var args []any
	single := NewCodec(Int)
	s, err := single.SinkCompare("source_key", ">", "5", &args)
	if err != nil || s != `source_key::bigint > $1::bigint` || len(args) != 1 {
		t.Fatalf("single-column sink SQL must stay unchanged: %s %v %v", s, args, err)
	}
	args = nil
	c := NewCodec(Int, Text)
	enc, _ := c.Encode([]string{"3", "x"})
	s, err = c.SinkCompare("source_key", "<=", enc, &args)
	want := `(((source_key::jsonb)->>0)::bigint, ((source_key::jsonb)->>1) COLLATE "C") <= ($1::bigint, $2::text COLLATE "C")`
	if err != nil || s != want || len(args) != 2 || args[0] != "3" || args[1] != "x" {
		t.Fatalf("composite sink SQL:\n got %s %v %v\nwant %s", s, args, err, want)
	}
	args = []any{"already"}
	s, err = c.SourceCompare([]string{`t."tenant_id"`, `t."slug"`}, ">", enc, &args)
	want = `(t."tenant_id", t."slug" COLLATE "C") > ($2::bigint, $3::text COLLATE "C")`
	if err != nil || s != want {
		t.Fatalf("composite source SQL:\n got %s\nwant %s", s, want)
	}
	if ob := c.SourceOrderBy([]string{"t.a", "t.b"}); ob != `t.a, t.b COLLATE "C"` {
		t.Fatalf("order by %s", ob)
	}
}
