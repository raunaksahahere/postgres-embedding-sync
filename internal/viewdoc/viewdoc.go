// Package viewdoc is the assembled form of a view document (a root row plus
// ordered child rows and linked target rows) and the ordering rules that the
// stream path, DLQ replay, migration coverage and verify share.
//
// Ordering: a child list is ordered by its order columns, then by the child
// key; a link list by the target's order fields, then by the target key.
// Values compare by type (integers numerically, text bytewise like
// COLLATE "C", uuids as canonical strings, timestamps and dates as instants)
// with NULLs last; keys compare with keys.Codec. verify assembles the same
// documents with SQL ORDER BY, independently of this code.
package viewdoc

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
)

// Row is one child row or one linked target row.
type Row struct {
	Key    string             // encoded key of the child (or target) row
	Fields map[string]*string // logical fields (hidden carrier fields included)
}

// Doc is one assembled view document.
type Doc struct {
	Key   string             // encoded root key = document key
	Root  map[string]*string // root row's fields
	Lists map[string][]Row   // child or link name -> ordered rows
}

// Data is the template input: root fields and, per list, row maps. Hidden
// carrier fields are dropped and NULL renders as the empty string.
func (d *Doc) Data() map[string]any {
	out := map[string]any{}
	for k, v := range d.Root {
		if !config.IsHiddenField(k) {
			out[k] = deref(v)
		}
	}
	for name, rows := range d.Lists {
		items := make([]map[string]string, 0, len(rows))
		for _, r := range rows {
			m := map[string]string{}
			for k, v := range r.Fields {
				if !config.IsHiddenField(k) {
					m[k] = deref(v)
				}
			}
			items = append(items, m)
		}
		out[name] = items
	}
	return out
}

// MaxList returns the longest list's name and length.
func (d *Doc) MaxList() (string, int) {
	name, n := "", 0
	for k, rows := range d.Lists {
		if len(rows) > n || (len(rows) == n && k < name) {
			name, n = k, len(rows)
		}
	}
	return name, n
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Kind is how an order value compares.
type Kind string

// Order kinds.
const (
	Int  Kind = "int"
	Text Kind = "text"
	UUID Kind = "uuid"
	Time Kind = "time" // timestamp, timestamptz, date
)

// KindFromOID maps a column type to its order kind.
func KindFromOID(oid uint32) (Kind, error) {
	switch oid {
	case 20, 21, 23:
		return Int, nil
	case 25, 1043:
		return Text, nil
	case 2950:
		return UUID, nil
	case 1082, 1114, 1184:
		return Time, nil
	}
	return "", fmt.Errorf("unsupported order column type oid %d (supported: integers, text/varchar, uuid, date, timestamp, timestamptz)", oid)
}

// KindsFromSignature reads the order kinds of cols from an accepted relation
// signature ("col:typeoid,..."). Missing columns are an error.
func KindsFromSignature(sig string, cols []string) ([]Kind, error) {
	types := map[string]uint32{}
	for _, part := range strings.Split(sig, ",") {
		name, t, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(t, 10, 32); err == nil {
			types[name] = uint32(n)
		}
	}
	out := make([]Kind, len(cols))
	for i, c := range cols {
		oid, ok := types[c]
		if !ok {
			return nil, fmt.Errorf("column %q is not in the accepted relation signature", c)
		}
		k, err := KindFromOID(oid)
		if err != nil {
			return nil, fmt.Errorf("order column %q: %w", c, err)
		}
		out[i] = k
	}
	return out, nil
}

var timeLayouts = []string{
	"2006-01-02 15:04:05.999999-07:00:00", "2006-01-02 15:04:05.999999-07:00", "2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999", "2006-01-02",
}

func parseTime(s string) (time.Time, bool) {
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// CompareValues orders two values of one kind, NULLs last. Values that do
// not parse as their kind sort as text, so a bug never panics.
func CompareValues(k Kind, a, b *string) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	}
	switch k {
	case Int:
		return keys.Int.Compare(*a, *b)
	case Time:
		special := func(s string) int {
			switch s {
			case "-infinity":
				return -1
			case "infinity":
				return 1
			}
			return 0
		}
		sa, sb := special(*a), special(*b)
		if sa != 0 || sb != 0 {
			return cmpInt(sa, sb)
		}
		x, ok1 := parseTime(*a)
		y, ok2 := parseTime(*b)
		if ok1 && ok2 {
			return x.Compare(y)
		}
	}
	return strings.Compare(*a, *b)
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// SortRows orders rows by the values valueOf returns (one per order kind),
// then by key with codec.
func SortRows(rows []Row, kinds []Kind, valueOf func(Row) []*string, codec keys.Codec) {
	vals := make([][]*string, len(rows))
	for i := range rows {
		vals[i] = valueOf(rows[i])
	}
	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(x, y int) bool {
		a, b := idx[x], idx[y]
		for i, k := range kinds {
			if c := CompareValues(k, vals[a][i], vals[b][i]); c != 0 {
				return c < 0
			}
		}
		return codec.Compare(rows[a].Key, rows[b].Key) < 0
	})
	sorted := make([]Row, len(rows))
	for i, j := range idx {
		sorted[i] = rows[j]
	}
	copy(rows, sorted)
}
