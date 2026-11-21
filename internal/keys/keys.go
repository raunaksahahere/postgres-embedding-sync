// Package keys gives Go and SQL the same total order over primary-key values.
//
// Backfill windows are ranges over the source primary key. The range deletes
// that remove stale sink rows, and the planner's "has backfill passed this
// key?" check, must agree with PostgreSQL's ORDER BY. Keys travel as text, so
// each supported key type declares how to compare and how to cast in SQL.
//
// A pipeline's key may span several columns. Codec gives such keys one
// collision-free text encoding (stored in the sink's source_key column) and
// orders them lexicographically, column by column, exactly like a PostgreSQL
// row comparison over the typed source columns.
package keys

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Kind is the comparison family of a single primary-key column.
type Kind string

const (
	// Int covers int2, int4 and int8.
	Int Kind = "int"
	// Text covers text, varchar and bpchar compared with COLLATE "C".
	Text Kind = "text"
	// UUID compares canonical lowercase UUID strings (same as byte order).
	UUID Kind = "uuid"
)

// FromTypeOID maps a PostgreSQL type OID to a key kind.
func FromTypeOID(oid uint32) (Kind, error) {
	switch oid {
	case 20, 21, 23: // int8, int2, int4
		return Int, nil
	case 25, 1043: // text, varchar (not bpchar: pgoutput sends char(n) padded while ::text trims it)
		return Text, nil
	case 2950:
		return UUID, nil
	}
	return "", fmt.Errorf("unsupported primary key type oid %d (supported: int2/int4/int8, text/varchar, uuid; char(n) is not supported)", oid)
}

// Parse validates a stored kind.
func Parse(s string) (Kind, error) {
	switch Kind(s) {
	case Int, Text, UUID:
		return Kind(s), nil
	}
	return "", fmt.Errorf("unknown key kind %q", s)
}

// Compare returns -1, 0 or 1. Malformed integers sort as text so a bug never panics.
func (k Kind) Compare(a, b string) int {
	if k == Int {
		x, err1 := strconv.ParseInt(a, 10, 64)
		y, err2 := strconv.ParseInt(b, 10, 64)
		if err1 == nil && err2 == nil {
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			}
			return 0
		}
	}
	return strings.Compare(a, b)
}

// SQLExpr returns an SQL expression that orders the text column like the source key.
func (k Kind) SQLExpr(col string) string {
	switch k {
	case Int:
		return col + "::bigint"
	case UUID:
		return col + "::uuid"
	}
	return col + ` COLLATE "C"`
}

// SQLParam casts a text parameter placeholder to the comparison type.
func (k Kind) SQLParam(p string) string {
	switch k {
	case Int:
		return p + "::bigint"
	case UUID:
		return p + "::uuid"
	}
	return p + `::text COLLATE "C"`
}

// SourceOrder returns the ORDER BY / comparison expression for the source key column.
func (k Kind) SourceOrder(col string) string {
	if k == Text {
		return col + ` COLLATE "C"`
	}
	return col
}

// Codec encodes, decodes and orders the key of one pipeline. Kinds lists the
// key columns' kinds in key order (the configured column order).
//
// Encoding: a single-column key is its column's text value unchanged (the
// format every installation before composite keys used, so existing sink rows
// stay valid). A composite key is a JSON array of the column text values,
// e.g. ["7","a\"b"]. Both are injective for a fixed column count, and every
// pipeline's keys share one column count, so (pipeline, encoded key) never
// collides.
type Codec struct {
	Kinds []Kind
}

// NewCodec builds a codec.
func NewCodec(kinds ...Kind) Codec { return Codec{Kinds: append([]Kind(nil), kinds...)} }

// ParseCodec parses the stored form produced by String ("int" or "int,text").
func ParseCodec(s string) (Codec, error) {
	var c Codec
	for _, p := range strings.Split(s, ",") {
		k, err := Parse(strings.TrimSpace(p))
		if err != nil {
			return Codec{}, err
		}
		c.Kinds = append(c.Kinds, k)
	}
	return c, nil
}

// String renders the kinds as stored in the sink ("int", "int,text").
func (c Codec) String() string {
	parts := make([]string, len(c.Kinds))
	for i, k := range c.Kinds {
		parts[i] = string(k)
	}
	return strings.Join(parts, ",")
}

// Composite reports whether the key has more than one column.
func (c Codec) Composite() bool { return len(c.Kinds) > 1 }

// EncodeValues encodes column text values without knowing their kinds (the
// encoding depends only on the column count).
func EncodeValues(vals []string) (string, error) {
	if len(vals) == 0 {
		return "", fmt.Errorf("empty key")
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	for _, v := range vals {
		// JSON would silently replace invalid UTF-8 with U+FFFD, mapping two
		// different keys to one encoding.
		if !utf8.ValidString(v) {
			return "", fmt.Errorf("key column value is not valid UTF-8")
		}
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(vals); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

// Encode encodes one key's column values (in key order).
func (c Codec) Encode(vals []string) (string, error) {
	if len(vals) != len(c.Kinds) {
		return "", fmt.Errorf("key has %d values, expected %d", len(vals), len(c.Kinds))
	}
	return EncodeValues(vals)
}

// Decode splits an encoded key into its column values.
func (c Codec) Decode(s string) ([]string, error) {
	return DecodeN(s, len(c.Kinds))
}

// DecodeN decodes a key of n columns.
func DecodeN(s string, n int) ([]string, error) {
	if n <= 1 {
		return []string{s}, nil
	}
	var vals []string
	if err := json.Unmarshal([]byte(s), &vals); err != nil {
		return nil, fmt.Errorf("decode composite key %q: %w", s, err)
	}
	if len(vals) != n {
		return nil, fmt.Errorf("composite key %q has %d values, expected %d", s, len(vals), n)
	}
	return vals, nil
}

// Compare orders two encoded keys lexicographically by column, like a
// PostgreSQL row comparison. Undecodable keys sort as text so a bug never panics.
func (c Codec) Compare(a, b string) int {
	if !c.Composite() {
		return c.kind(0).Compare(a, b)
	}
	x, err1 := c.Decode(a)
	y, err2 := c.Decode(b)
	if err1 != nil || err2 != nil {
		return strings.Compare(a, b)
	}
	for i, k := range c.Kinds {
		if r := k.Compare(x[i], y[i]); r != 0 {
			return r
		}
	}
	return 0
}

func (c Codec) kind(i int) Kind {
	if i < len(c.Kinds) {
		return c.Kinds[i]
	}
	return Text
}

// SourceOrderBy returns the ORDER BY list over the typed source columns.
// cols are already-quoted column expressions in key order.
func (c Codec) SourceOrderBy(cols []string) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = c.kind(i).SourceOrder(col)
	}
	return strings.Join(parts, ", ")
}

func rowOf(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// params appends the decoded values of enc to args and returns the typed placeholders.
func (c Codec) params(enc string, args *[]any) ([]string, error) {
	vals, err := c.Decode(enc)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		*args = append(*args, v)
		out[i] = c.kind(i).SQLParam(fmt.Sprintf("$%d", len(*args)))
	}
	return out, nil
}

// SourceCompare renders "<typed source key> op <bound>" for a backfill scan.
// op is one of < <= > >=; a composite key compares as a row (lexicographic).
func (c Codec) SourceCompare(cols []string, op, enc string, args *[]any) (string, error) {
	lhs := make([]string, len(cols))
	for i, col := range cols {
		lhs[i] = c.kind(i).SourceOrder(col)
	}
	rhs, err := c.params(enc, args)
	if err != nil {
		return "", err
	}
	return rowOf(lhs) + " " + op + " " + rowOf(rhs), nil
}

// SinkExprs returns typed expressions over the encoded text column col, in key order.
func (c Codec) SinkExprs(col string) []string {
	if !c.Composite() {
		return []string{c.kind(0).SQLExpr(col)}
	}
	out := make([]string, len(c.Kinds))
	for i, k := range c.Kinds {
		out[i] = k.SQLExpr(fmt.Sprintf("((%s::jsonb)->>%d)", col, i))
	}
	return out
}

// SinkCompare renders "<decoded col> op <bound>" over the sink's encoded key
// column. Callers must restrict it to rows of this codec's pipeline before
// the casts run (see the committer's CASE guard).
func (c Codec) SinkCompare(col, op, enc string, args *[]any) (string, error) {
	rhs, err := c.params(enc, args)
	if err != nil {
		return "", err
	}
	return rowOf(c.SinkExprs(col)) + " " + op + " " + rowOf(rhs), nil
}
