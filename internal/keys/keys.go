// Package keys gives Go and SQL the same total order over primary-key values.
//
// Backfill windows are ranges over the source primary key. The range deletes
// that remove stale sink rows, and the planner's "has backfill passed this
// key?" check, must agree with PostgreSQL's ORDER BY. Keys travel as text, so
// each supported key type declares how to compare and how to cast in SQL.
package keys

import (
	"fmt"
	"strconv"
	"strings"
)

// Kind is the comparison family of a single-column primary key.
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
