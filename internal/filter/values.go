// Package filter implements embedsync's metadata layer for filtered search:
// the declared metadata types, normalization of PostgreSQL text values into
// the JSON stored in each chunk row's `meta jsonb` column, and a small JSON
// filter language that is validated against the declared fields and compiled
// to a parameterized SQL boolean expression over that column.
package filter

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Type is the declared type of a metadata field.
type Type string

// Supported metadata types.
const (
	Text      Type = "text"
	Int       Type = "int"
	Float     Type = "float"
	Bool      Type = "bool"
	Timestamp Type = "timestamp"
	TextArray Type = "text_array"
)

// InfinityUnix is the Unix-seconds value stored for PostgreSQL's "infinity"
// timestamp; "-infinity" is stored as its negation.
const InfinityUnix = 1e18

// ParseType parses a declared type name (case-insensitive, surrounding
// whitespace ignored).
func ParseType(s string) (Type, error) {
	switch t := Type(strings.ToLower(strings.TrimSpace(s))); t {
	case Text, Int, Float, Bool, Timestamp, TextArray:
		return t, nil
	}
	return "", fmt.Errorf("filter: unknown metadata type %q (want text, int, float, bool, timestamp or text_array)", s)
}

// PostgreSQL type OIDs understood by TypeForOID.
const (
	oidBool        = 16
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidFloat4      = 700
	oidFloat8      = 701
	oidTextArray   = 1009
	oidVarcharArr  = 1015
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidNumeric     = 1700
)

// TypeForOID maps a PostgreSQL column type OID to the metadata type used for
// it. Unsupported column types are an error.
func TypeForOID(oid uint32) (Type, error) {
	switch oid {
	case oidInt2, oidInt4, oidInt8:
		return Int, nil
	case oidFloat4, oidFloat8, oidNumeric:
		return Float, nil
	case oidBool:
		return Bool, nil
	case oidText, oidVarchar, oidBpchar:
		return Text, nil
	case oidDate, oidTimestamp, oidTimestamptz:
		return Timestamp, nil
	case oidTextArray, oidVarcharArr:
		return TextArray, nil
	}
	return "", fmt.Errorf("filter: column type oid %d is not supported as metadata", oid)
}

// Normalize converts a column's PostgreSQL text value (nil = SQL NULL) to the
// JSON value stored in meta:
//
//   - Text -> string (unchanged)
//   - Int -> int64
//   - Float -> float64 (NaN and infinities are rejected: JSON cannot hold them)
//   - Bool -> bool ("t", "f", "true", "false", case-insensitive)
//   - Timestamp -> float64 Unix seconds (UTC); values without an offset are
//     taken as UTC; "infinity"/"-infinity" -> +/-InfinityUnix
//   - TextArray -> []string (never nil) parsed from an array literal; NULL
//     elements are dropped
//
// NULL yields (nil, nil); the caller omits the key from meta.
func (t Type) Normalize(text *string) (any, error) {
	if text == nil {
		return nil, nil
	}
	s := *text
	switch t {
	case Text:
		return s, nil
	case Int:
		v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("filter: invalid int %q", s)
		}
		return v, nil
	case Float:
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("filter: invalid or non-finite float %q", s)
		}
		return v, nil
	case Bool:
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "t", "true":
			return true, nil
		case "f", "false":
			return false, nil
		}
		return nil, fmt.Errorf("filter: invalid bool %q", s)
	case Timestamp:
		return parseTimestamp(s)
	case TextArray:
		return parseTextArray(s)
	}
	return nil, fmt.Errorf("filter: unknown metadata type %q", string(t))
}

// tsLayouts are tried in order; fractional seconds are accepted after the
// seconds field even though the layouts do not spell them out.
var tsLayouts = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05Z07",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05Z07:00:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07",
	"2006-01-02T15:04:05Z07:00", // RFC 3339
	"2006-01-02T15:04:05Z07:00:00",
}

// parseTimestamp parses PostgreSQL (ISO DateStyle) date/timestamp/timestamptz
// output and RFC 3339 into Unix seconds.
func parseTimestamp(s string) (float64, error) {
	v := strings.TrimSpace(s)
	switch strings.ToLower(v) {
	case "infinity", "+infinity":
		return InfinityUnix, nil
	case "-infinity":
		return -InfinityUnix, nil
	}
	for _, layout := range tsLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return float64(t.Unix()) + float64(t.Nanosecond())/1e9, nil
		}
	}
	return 0, fmt.Errorf("filter: invalid timestamp %q", s)
}

// parseTextArray parses a one-dimensional PostgreSQL array literal such as
// {a,"b c","d\"e",NULL}. An optional dimension decoration ("[1:2]={...}") is
// skipped. Multi-dimensional arrays are rejected.
func parseTextArray(s string) ([]string, error) {
	bad := func(why string) error { return fmt.Errorf("filter: invalid array literal %q: %s", s, why) }
	v := strings.TrimSpace(s)
	if strings.HasPrefix(v, "[") {
		i := strings.Index(v, "=")
		if i < 0 {
			return nil, bad("dimension decoration without '='")
		}
		v = strings.TrimSpace(v[i+1:])
	}
	if len(v) < 2 || v[0] != '{' || v[len(v)-1] != '}' {
		return nil, bad("must be enclosed in braces")
	}
	body := v[1 : len(v)-1]
	out := []string{}
	if strings.TrimSpace(body) == "" {
		return out, nil
	}
	i, n := 0, len(body)
	isSpace := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	for {
		for i < n && isSpace(body[i]) {
			i++
		}
		if i >= n {
			return nil, bad("missing element")
		}
		var b strings.Builder
		switch body[i] {
		case '{':
			return nil, bad("multi-dimensional arrays are not supported")
		case '"':
			i++
			closed := false
			for i < n {
				c := body[i]
				if c == '\\' {
					if i+1 >= n {
						return nil, bad("dangling backslash")
					}
					b.WriteByte(body[i+1])
					i += 2
					continue
				}
				if c == '"' {
					closed = true
					i++
					break
				}
				b.WriteByte(c)
				i++
			}
			if !closed {
				return nil, bad("unterminated quoted element")
			}
			for i < n && isSpace(body[i]) {
				i++
			}
			out = append(out, b.String())
		default:
			escaped := false
			pendingSpace := 0 // trailing unescaped whitespace is trimmed
			for i < n && body[i] != ',' {
				c := body[i]
				switch {
				case c == '"' || c == '{' || c == '}':
					return nil, bad("unexpected character in unquoted element")
				case c == '\\':
					if i+1 >= n {
						return nil, bad("dangling backslash")
					}
					b.WriteString(strings.Repeat(" ", pendingSpace))
					pendingSpace = 0
					b.WriteByte(body[i+1])
					escaped = true
					i += 2
					continue
				case isSpace(c):
					pendingSpace++
				default:
					b.WriteString(strings.Repeat(" ", pendingSpace))
					pendingSpace = 0
					b.WriteByte(c)
				}
				i++
			}
			elem := b.String()
			if elem == "" && !escaped {
				return nil, bad("empty unquoted element")
			}
			if !escaped && strings.EqualFold(elem, "NULL") {
				break // NULL elements are dropped
			}
			out = append(out, elem)
		}
		if i >= n {
			return out, nil
		}
		if body[i] != ',' {
			return nil, bad("expected ',' between elements")
		}
		i++
	}
}
