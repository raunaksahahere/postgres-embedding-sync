package filter

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func sp(s string) *string { return &s }

func TestParseType(t *testing.T) {
	for _, s := range []string{"text", "int", "float", "bool", "timestamp", "text_array"} {
		got, err := ParseType(s)
		if err != nil || string(got) != s {
			t.Errorf("ParseType(%q) = %q, %v", s, got, err)
		}
	}
	if got, err := ParseType("  Text_Array "); err != nil || got != TextArray {
		t.Errorf("ParseType case/space = %q, %v", got, err)
	}
	for _, s := range []string{"", "string", "integer", "json"} {
		if _, err := ParseType(s); err == nil {
			t.Errorf("ParseType(%q) succeeded", s)
		}
	}
}

func TestTypeForOID(t *testing.T) {
	cases := map[uint32]Type{
		21: Int, 23: Int, 20: Int,
		700: Float, 701: Float, 1700: Float,
		16: Bool,
		25: Text, 1043: Text, 1042: Text,
		1082: Timestamp, 1114: Timestamp, 1184: Timestamp,
		1009: TextArray, 1015: TextArray,
	}
	for oid, want := range cases {
		got, err := TypeForOID(oid)
		if err != nil || got != want {
			t.Errorf("TypeForOID(%d) = %q, %v; want %q", oid, got, err, want)
		}
	}
	for _, oid := range []uint32{0, 114, 3802, 2950, 1007, 17} {
		if _, err := TypeForOID(oid); err == nil {
			t.Errorf("TypeForOID(%d) succeeded", oid)
		}
	}
}

func unix(s string) float64 {
	tm, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return float64(tm.Unix()) + float64(tm.Nanosecond())/1e9
}

func TestNormalize(t *testing.T) {
	cases := []struct {
		typ  Type
		in   *string
		want any
	}{
		{Text, nil, nil},
		{Int, nil, nil},
		{TextArray, nil, nil},
		{Text, sp(""), ""},
		{Text, sp(" a\tb "), " a\tb "},
		{Int, sp("42"), int64(42)},
		{Int, sp("-7"), int64(-7)},
		{Int, sp("-9223372036854775808"), int64(-9223372036854775808)},
		{Float, sp("1.50"), 1.5},
		{Float, sp("-0.25"), -0.25},
		{Float, sp("3"), 3.0},
		{Float, sp("1e-5"), 1e-5},
		{Float, sp("123456789.123456789"), 123456789.123456789},
		{Bool, sp("t"), true},
		{Bool, sp("f"), false},
		{Bool, sp("true"), true},
		{Bool, sp("FALSE"), false},
		{Timestamp, sp("2024-03-05"), unix("2024-03-05T00:00:00Z")},
		{Timestamp, sp("2024-03-05 10:20:30"), unix("2024-03-05T10:20:30Z")},
		{Timestamp, sp("2024-03-05 10:20:30.5"), unix("2024-03-05T10:20:30.5Z")},
		{Timestamp, sp("2024-03-05 10:20:30.123456"), unix("2024-03-05T10:20:30.123456Z")},
		{Timestamp, sp("2024-03-05 10:20:30+00"), unix("2024-03-05T10:20:30Z")},
		{Timestamp, sp("2024-03-05 10:20:30+05"), unix("2024-03-05T05:20:30Z")},
		{Timestamp, sp("2024-03-05 10:20:30-03"), unix("2024-03-05T13:20:30Z")},
		{Timestamp, sp("2024-03-05 10:20:30.25+05:30"), unix("2024-03-05T04:50:30.25Z")},
		{Timestamp, sp("1850-03-05 10:20:30+05:30:00"), unix("1850-03-05T04:50:30Z")},
		{Timestamp, sp("2024-03-05T10:20:30Z"), unix("2024-03-05T10:20:30Z")},
		{Timestamp, sp("2024-03-05T10:20:30.75-02:00"), unix("2024-03-05T12:20:30.75Z")},
		{Timestamp, sp("1969-12-31"), -86400.0},
		{Timestamp, sp("infinity"), 1e18},
		{Timestamp, sp("-infinity"), -1e18},
		{TextArray, sp("{}"), []string{}},
		{TextArray, sp("{a}"), []string{"a"}},
		{TextArray, sp("{a,b,c}"), []string{"a", "b", "c"}},
		{TextArray, sp(`{"a b","c,d","e\"f","g\\h",""}`), []string{"a b", "c,d", `e"f`, `g\h`, ""}},
		{TextArray, sp(`{x,NULL,"NULL",null,y}`), []string{"x", "NULL", "y"}},
		{TextArray, sp(`{NULL}`), []string{}},
		{TextArray, sp(`{ a , b }`), []string{"a", "b"}},
		{TextArray, sp(`{a\,b,\NULL}`), []string{"a,b", "NULL"}},
		{TextArray, sp(`{"{}","é ü"}`), []string{"{}", "é ü"}},
		{TextArray, sp(`[0:1]={a,b}`), []string{"a", "b"}},
	}
	for _, c := range cases {
		got, err := c.typ.Normalize(c.in)
		if err != nil {
			t.Errorf("%s.Normalize(%v): %v", c.typ, deref(c.in), err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s.Normalize(%v) = %#v; want %#v", c.typ, deref(c.in), got, c.want)
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return "<NULL>"
	}
	return *s
}

func TestNormalizeErrors(t *testing.T) {
	cases := []struct {
		typ Type
		in  string
	}{
		{Int, ""}, {Int, "1.5"}, {Int, "abc"}, {Int, "9223372036854775808"},
		{Float, ""}, {Float, "NaN"}, {Float, "Infinity"}, {Float, "-Infinity"}, {Float, "x"},
		{Bool, ""}, {Bool, "yes"}, {Bool, "1"},
		{Timestamp, ""}, {Timestamp, "2024-13-01"}, {Timestamp, "0044-03-15 BC"}, {Timestamp, "yesterday"},
		{Timestamp, "2024-03-05 10:20"},
		{TextArray, ""}, {TextArray, "a,b"}, {TextArray, "{a,b"}, {TextArray, "{{a,b},{c,d}}"},
		{TextArray, `{"a}`}, {TextArray, "{a,,b}"}, {TextArray, `{a"b}`}, {TextArray, `{"a"b}`},
		{TextArray, "{a,}"}, {TextArray, `{a\}`}, {TextArray, "[0:1]{a}"},
		{Type("json"), "{}"},
	}
	for _, c := range cases {
		if got, err := c.typ.Normalize(sp(c.in)); err == nil {
			t.Errorf("%s.Normalize(%q) = %#v; want error", c.typ, c.in, got)
		}
	}
}

// Normalized values must be JSON-encodable as the expected JSON types.
func TestNormalizeJSON(t *testing.T) {
	cases := []struct {
		typ  Type
		in   string
		want string
	}{
		{Int, "-12", "-12"},
		{Float, "1.50", "1.5"},
		{Bool, "t", "true"},
		{Timestamp, "1970-01-02", "86400"},
		{TextArray, "{}", "[]"},
		{TextArray, `{a,"b c"}`, `["a","b c"]`},
	}
	for _, c := range cases {
		v, err := c.typ.Normalize(sp(c.in))
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(v)
		if err != nil || string(b) != c.want {
			t.Errorf("json(%s.Normalize(%q)) = %s, %v; want %s", c.typ, c.in, b, err, c.want)
		}
	}
}
