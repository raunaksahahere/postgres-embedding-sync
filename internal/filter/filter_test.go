package filter

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var testSchema = Schema{
	"title":   {Name: "title", Type: Text},
	"year":    {Name: "year", Type: Int},
	"score":   {Name: "score", Type: Float},
	"draft":   {Name: "draft", Type: Bool},
	"created": {Name: "created", Type: Timestamp},
	"tags":    {Name: "tags", Type: TextArray},
}

func TestParseEmpty(t *testing.T) {
	for _, raw := range []string{"", "  ", "null", " null\n"} {
		n, err := Parse([]byte(raw), testSchema)
		if n != nil || err != nil {
			t.Errorf("Parse(%q) = %v, %v; want nil, nil", raw, n, err)
		}
	}
	if n, err := Parse(nil, testSchema); n != nil || err != nil {
		t.Errorf("Parse(nil) = %v, %v", n, err)
	}
	var args []any
	var nilNode *Node
	if got := nilNode.SQL("c.meta", &args); got != "TRUE" || len(args) != 0 {
		t.Errorf("nil SQL = %q, %v", got, args)
	}
}

func TestSQLGolden(t *testing.T) {
	cases := []struct {
		name   string
		filter string
		sql    string
		args   []any
	}{
		{"eq text", `{"field":"title","op":"eq","value":"Go"}`,
			`((c.meta->>$1::text) = $2::text)`, []any{"title", "Go"}},
		{"eq int", `{"field":"year","op":"eq","value":2024}`,
			`((c.meta->>$1::text)::numeric = $2::numeric)`, []any{"year", int64(2024)}},
		{"eq int integral float", `{"field":"year","op":"eq","value":2.0e3}`,
			`((c.meta->>$1::text)::numeric = $2::numeric)`, []any{"year", int64(2000)}},
		{"eq float", `{"field":"score","op":"eq","value":1.5}`,
			`((c.meta->>$1::text)::numeric = $2::numeric)`, []any{"score", 1.5}},
		{"eq float from int", `{"field":"score","op":"eq","value":-3}`,
			`((c.meta->>$1::text)::numeric = $2::numeric)`, []any{"score", -3.0}},
		{"eq bool", `{"field":"draft","op":"eq","value":false}`,
			`((c.meta->>$1::text)::boolean = $2::boolean)`, []any{"draft", false}},
		{"eq timestamp date", `{"field":"created","op":"eq","value":"1970-01-02"}`,
			`((c.meta->>$1::text)::float8 = $2::float8)`, []any{"created", 86400.0}},
		{"ne text", `{"field":"title","op":"ne","value":"x"}`,
			`(jsonb_exists(c.meta, $1::text) AND (c.meta->>$1::text) <> $2::text)`, []any{"title", "x"}},
		{"ne bool", `{"field":"draft","op":"ne","value":true}`,
			`(jsonb_exists(c.meta, $1::text) AND (c.meta->>$1::text)::boolean <> $2::boolean)`, []any{"draft", true}},
		{"in text", `{"field":"title","op":"in","values":["a","b"]}`,
			`((c.meta->>$1::text) = ANY($2::text[]))`, []any{"title", []string{"a", "b"}}},
		{"in int", `{"field":"year","op":"in","values":[1,2,3]}`,
			`((c.meta->>$1::text)::numeric = ANY($2::numeric[]))`, []any{"year", []int64{1, 2, 3}}},
		{"in float", `{"field":"score","op":"in","values":[0.5,1]}`,
			`((c.meta->>$1::text)::numeric = ANY($2::numeric[]))`, []any{"score", []float64{0.5, 1}}},
		{"in bool", `{"field":"draft","op":"in","values":[true]}`,
			`((c.meta->>$1::text)::boolean = ANY($2::boolean[]))`, []any{"draft", []bool{true}}},
		{"in timestamp", `{"field":"created","op":"in","values":["1970-01-01T00:01:00Z","1970-01-01T01:00:00+01:00"]}`,
			`((c.meta->>$1::text)::float8 = ANY($2::float8[]))`, []any{"created", []float64{60, 0}}},
		{"range int", `{"field":"year","op":"range","gte":2000,"lt":2010}`,
			`((c.meta->>$1::text)::numeric >= $2::numeric AND (c.meta->>$1::text)::numeric < $3::numeric)`,
			[]any{"year", int64(2000), int64(2010)}},
		{"range float one bound", `{"field":"score","op":"range","gt":0.5}`,
			`((c.meta->>$1::text)::numeric > $2::numeric)`, []any{"score", 0.5}},
		{"range timestamp", `{"field":"created","op":"range","gt":"1970-01-01T00:00:10Z","lte":"1970-01-02"}`,
			`((c.meta->>$1::text)::float8 > $2::float8 AND (c.meta->>$1::text)::float8 <= $3::float8)`,
			[]any{"created", 10.0, 86400.0}},
		{"range text", `{"field":"title","op":"range","lte":"m"}`,
			`((c.meta->>$1::text) COLLATE "C" <= $2::text)`, []any{"title", "m"}},
		{"contains", `{"field":"tags","op":"contains","value":"go"}`,
			`jsonb_exists(c.meta->$1::text, $2::text)`, []any{"tags", "go"}},
		{"exists", `{"field":"tags","op":"exists"}`,
			`jsonb_exists(c.meta, $1::text)`, []any{"tags"}},
		{"not", `{"not":{"field":"draft","op":"eq","value":true}}`,
			`(NOT COALESCE(((c.meta->>$1::text)::boolean = $2::boolean), false))`, []any{"draft", true}},
		{"and single child collapses", `{"and":[{"field":"year","op":"exists"}]}`,
			`jsonb_exists(c.meta, $1::text)`, []any{"year"}},
		{"nested", `{"and":[{"field":"year","op":"range","gte":2000},{"or":[{"field":"tags","op":"contains","value":"go"},{"not":{"field":"title","op":"in","values":["a"]}}]}]}`,
			`(((c.meta->>$1::text)::numeric >= $2::numeric) AND (jsonb_exists(c.meta->$3::text, $4::text) OR (NOT COALESCE(((c.meta->>$5::text) = ANY($6::text[])), false))))`,
			[]any{"year", int64(2000), "tags", "go", "title", []string{"a"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := Parse([]byte(c.filter), testSchema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			var args []any
			got := n.SQL("c.meta", &args)
			if got != c.sql {
				t.Errorf("SQL\n got: %s\nwant: %s", got, c.sql)
			}
			if !reflect.DeepEqual(args, c.args) {
				t.Errorf("args = %#v; want %#v", args, c.args)
			}
		})
	}
}

func TestSQLPlaceholdersContinue(t *testing.T) {
	n, err := Parse([]byte(`{"or":[{"field":"title","op":"eq","value":"a"},{"field":"year","op":"exists"}]}`), testSchema)
	if err != nil {
		t.Fatal(err)
	}
	args := []any{"vec", 10}
	got := n.SQL("ch.meta", &args)
	want := `(((ch.meta->>$3::text) = $4::text) OR jsonb_exists(ch.meta, $5::text))`
	if got != want {
		t.Errorf("got %s\nwant %s", got, want)
	}
	if !reflect.DeepEqual(args, []any{"vec", 10, "title", "a", "year"}) {
		t.Errorf("args = %#v", args)
	}
	// Rendering twice is independent and deterministic.
	var a1, a2 []any
	if n.SQL("m", &a1) != n.SQL("m", &a2) || !reflect.DeepEqual(a1, a2) {
		t.Error("SQL is not deterministic")
	}
}

// Field names and values chosen to look like SQL must only ever be bound.
func TestSQLNoInterpolation(t *testing.T) {
	evilField := `x'); DROP TABLE t; --`
	evilValue := `y' OR '1'='1`
	s := Schema{
		evilField: {Name: evilField, Type: Text},
		"arr$1":   {Name: "arr$1", Type: TextArray},
		"n":       {Name: "n", Type: Int},
	}
	filter := fmt.Sprintf(`{"and":[
		{"field":%q,"op":"eq","value":%q},
		{"field":%q,"op":"ne","value":%q},
		{"field":%q,"op":"in","values":[%q,"zz_in_value"]},
		{"field":%q,"op":"range","gt":%q},
		{"not":{"field":"arr$1","op":"contains","value":"zz_contains"}},
		{"field":"n","op":"eq","value":987654321}
	]}`, evilField, evilValue, evilField, evilValue, evilField, evilValue, evilField, evilValue)
	n, err := Parse([]byte(filter), s)
	if err != nil {
		t.Fatal(err)
	}
	var args []any
	sql := n.SQL("c.meta", &args)
	for _, bad := range []string{evilField, evilValue, "DROP", "'", "zz_", "arr", "987654321"} {
		if strings.Contains(sql, bad) {
			t.Errorf("SQL contains %q: %s", bad, sql)
		}
	}
	// Strip all the expected SQL tokens; anything left over would be leaked text.
	rest := sql
	for _, tok := range []string{"jsonb_exists", "COALESCE", "COLLATE", `"C"`, "numeric", "text", "ANY",
		"NOT", "AND", "OR", "false", "c.meta", "->>", "->", "::", "[]", "<>", ">=", "<=", "=", ">", "<", "(", ")", ",", " "} {
		rest = strings.ReplaceAll(rest, tok, "")
	}
	rest = strings.Map(func(r rune) rune {
		if r == '$' || (r >= '0' && r <= '9') {
			return -1
		}
		return r
	}, rest)
	if rest != "" {
		t.Errorf("unexpected text left in SQL: %q (from %s)", rest, sql)
	}
	if len(args) != 12 {
		t.Errorf("got %d args: %#v", len(args), args)
	}
}

func TestParseErrors(t *testing.T) {
	deep := `{"field":"year","op":"exists"}`
	for i := 0; i < MaxDepth; i++ { // MaxDepth+1 levels
		deep = `{"not":` + deep + `}`
	}
	okDeep := `{"field":"year","op":"exists"}`
	for i := 0; i < MaxDepth-1; i++ {
		okDeep = `{"not":` + okDeep + `}`
	}
	if _, err := Parse([]byte(okDeep), testSchema); err != nil {
		t.Errorf("depth %d rejected: %v", MaxDepth, err)
	}
	conds := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = `{"field":"year","op":"exists"}`
		}
		return `{"or":[` + strings.Join(parts, ",") + `]}`
	}
	if _, err := Parse([]byte(conds(MaxConditions)), testSchema); err != nil {
		t.Errorf("%d conditions rejected: %v", MaxConditions, err)
	}
	vals := func(k int) string {
		parts := make([]string, k)
		for i := range parts {
			parts[i] = fmt.Sprint(i)
		}
		return `{"field":"year","op":"in","values":[` + strings.Join(parts, ",") + `]}`
	}
	if _, err := Parse([]byte(vals(MaxInValues)), testSchema); err != nil {
		t.Errorf("%d values rejected: %v", MaxInValues, err)
	}

	cases := []struct {
		filter string
		code   string
		field  string
	}{
		// invalid_filter
		{`{`, CodeInvalidFilter, ""},
		{`[]`, CodeInvalidFilter, ""},
		{`"x"`, CodeInvalidFilter, ""},
		{`{}`, CodeInvalidFilter, ""},
		{`{"foo":1}`, CodeInvalidFilter, ""},
		{`{"and":[]}`, CodeInvalidFilter, ""},
		{`{"or":[]}`, CodeInvalidFilter, ""},
		{`{"and":{}}`, CodeInvalidFilter, ""},
		{`{"and":null}`, CodeInvalidFilter, ""},
		{`{"not":[]}`, CodeInvalidFilter, ""},
		{`{"not":null}`, CodeInvalidFilter, ""},
		{`{"and":[1]}`, CodeInvalidFilter, ""},
		{`{"and":[{"field":"year","op":"exists"}],"or":[{"field":"year","op":"exists"}]}`, CodeInvalidFilter, ""},
		{`{"not":{"field":"year","op":"exists"},"field":"year"}`, CodeInvalidFilter, ""},
		{`{"field":"year","op":"exists","field":"title"}`, CodeInvalidFilter, ""},
		{`{"field":1,"op":"exists"}`, CodeInvalidFilter, ""},
		{`{"field":"","op":"exists"}`, CodeInvalidFilter, ""},
		{`{"field":"year"}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":1}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"eq"}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"eq","value":1,"values":[1]}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"exists","value":1}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"range","gt":1,"bogus":2}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"in"}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"in","values":1}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"in","values":[]}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"range"}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"range","gt":1,"gte":2}`, CodeInvalidFilter, "year"},
		{`{"field":"year","op":"range","lt":1,"lte":2}`, CodeInvalidFilter, "year"},
		{`{"field":"tags","op":"contains"}`, CodeInvalidFilter, "tags"},
		// unknown_field
		{`{"field":"nope","op":"eq","value":1}`, CodeUnknownField, "nope"},
		{`{"and":[{"field":"year","op":"exists"},{"not":{"field":"Year","op":"exists"}}]}`, CodeUnknownField, "Year"},
		// unsupported_op
		{`{"field":"year","op":"like","value":1}`, CodeUnsupportedOp, "year"},
		{`{"field":"year","op":"EQ","value":1}`, CodeUnsupportedOp, "year"},
		{`{"field":"draft","op":"range","gt":true}`, CodeUnsupportedOp, "draft"},
		{`{"field":"tags","op":"range","gt":"a"}`, CodeUnsupportedOp, "tags"},
		{`{"field":"title","op":"contains","value":"a"}`, CodeUnsupportedOp, "title"},
		{`{"field":"year","op":"contains","value":"a"}`, CodeUnsupportedOp, "year"},
		{`{"field":"tags","op":"eq","value":"a"}`, CodeUnsupportedOp, "tags"},
		{`{"field":"tags","op":"in","values":["a"]}`, CodeUnsupportedOp, "tags"},
		// invalid_value
		{`{"field":"title","op":"eq","value":1}`, CodeInvalidValue, "title"},
		{`{"field":"title","op":"eq","value":null}`, CodeInvalidValue, "title"},
		{`{"field":"title","op":"eq","value":["a"]}`, CodeInvalidValue, "title"},
		{`{"field":"year","op":"eq","value":1.5}`, CodeInvalidValue, "year"},
		{`{"field":"year","op":"eq","value":"1"}`, CodeInvalidValue, "year"},
		{`{"field":"year","op":"eq","value":9223372036854775808}`, CodeInvalidValue, "year"},
		{`{"field":"score","op":"eq","value":"1.5"}`, CodeInvalidValue, "score"},
		{`{"field":"score","op":"eq","value":1e999}`, CodeInvalidValue, "score"},
		{`{"field":"draft","op":"eq","value":"true"}`, CodeInvalidValue, "draft"},
		{`{"field":"draft","op":"eq","value":1}`, CodeInvalidValue, "draft"},
		{`{"field":"created","op":"eq","value":"last week"}`, CodeInvalidValue, "created"},
		{`{"field":"created","op":"eq","value":1700000000}`, CodeInvalidValue, "created"},
		{`{"field":"year","op":"in","values":[1,"2"]}`, CodeInvalidValue, "year"},
		{`{"field":"year","op":"range","gt":1,"lt":"x"}`, CodeInvalidValue, "year"},
		{`{"field":"tags","op":"contains","value":1}`, CodeInvalidValue, "tags"},
		{`{"field":"title","op":"ne","value":{}}`, CodeInvalidValue, "title"},
		// too_complex
		{deep, CodeTooComplex, ""},
		{conds(MaxConditions + 1), CodeTooComplex, ""},
		{vals(MaxInValues + 1), CodeTooComplex, "year"},
	}
	for _, c := range cases {
		n, err := Parse([]byte(c.filter), testSchema)
		var fe *Error
		if !errors.As(err, &fe) {
			t.Errorf("Parse(%.80s) = %v, %v; want *Error %s", c.filter, n, err, c.code)
			continue
		}
		if fe.Code != c.code || fe.Field != c.field {
			t.Errorf("Parse(%.80s) = code %q field %q (%v); want code %q field %q",
				c.filter, fe.Code, fe.Field, fe, c.code, c.field)
		}
		if fe.Message == "" {
			t.Errorf("Parse(%.80s): empty message", c.filter)
		}
	}
}

func TestErrorString(t *testing.T) {
	e := &Error{Code: CodeUnknownField, Field: "x", Message: "field is not declared"}
	if got := e.Error(); got != `unknown_field: field "x": field is not declared` {
		t.Errorf("Error() = %q", got)
	}
	e = &Error{Code: CodeTooComplex, Message: "too deep"}
	if got := e.Error(); got != "too_complex: too deep" {
		t.Errorf("Error() = %q", got)
	}
}

func TestNodeFields(t *testing.T) {
	s := Schema{"a": {Name: "a", Type: Text}, "b": {Name: "b", Type: Int}}
	n, err := Parse([]byte(`{"or":[{"field":"a","op":"eq","value":"x"},{"not":{"and":[{"field":"b","op":"exists"},{"field":"a","op":"exists"}]}}]}`), s)
	if err != nil {
		t.Fatal(err)
	}
	got := n.Fields()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("Fields = %v", got)
	}
	if (*Node)(nil).Fields() != nil {
		t.Fatal("nil node has fields")
	}
}
