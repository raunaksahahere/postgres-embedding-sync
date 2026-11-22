package filter

// Filter language (JSON):
//
//	filter    := {"and": [filter...]} | {"or": [filter...]} | {"not": filter} | condition
//	condition := {"field": NAME, "op": OP, ...operands}
//
//	OP: "eq"       {"value": v}
//	    "ne"       {"value": v}
//	    "in"       {"values": [v...]}            1..MaxInValues values
//	    "range"    {"gt"|"gte"|"lt"|"lte": v}    at least one; not gt+gte or lt+lte
//	    "contains" {"value": "x"}                text_array fields only
//	    "exists"   {}                            true when the key is present
//
// Operand values must match the field type: text -> JSON string; int -> an
// integral JSON number; float -> JSON number; bool -> JSON bool; timestamp ->
// string (RFC 3339, "2006-01-02", or any form Normalize accepts), compared as
// Unix seconds. text_array fields support only "contains" and "exists"; bool
// fields do not support "range". Text ranges compare bytewise (COLLATE "C").
//
// Missing keys (SQL NULL semantics are mapped to two-valued logic):
//
//   - Every comparison on a key the document lacks is false.
//   - "ne" therefore never matches a document lacking the field; use
//     {"not": {eq}} to also match such documents.
//   - "not" negates its operand under that rule: the operand is wrapped in
//     COALESCE(..., false), so {"not": {"field":"f","op":"eq",...}} matches
//     documents without f.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
)

// Limits on filter size.
const (
	MaxDepth      = 8    // nesting levels; a bare condition is depth 1
	MaxConditions = 100  // leaf conditions per filter
	MaxInValues   = 1000 // values in one "in" condition
)

// Error codes reported in Error.Code.
const (
	CodeInvalidFilter = "invalid_filter"
	CodeUnknownField  = "unknown_field"
	CodeInvalidValue  = "invalid_value"
	CodeUnsupportedOp = "unsupported_op"
	CodeTooComplex    = "too_complex"
)

// Field is a declared metadata field.
type Field struct {
	Name string
	Type Type
}

// Schema maps field names to their declarations.
type Schema map[string]Field

// Error is a client error with a stable code (one of the Code* constants) and
// the field it concerns ("" when it is not about one field).
type Error struct {
	Code    string
	Field   string
	Message string
}

func (e *Error) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: field %q: %s", e.Code, e.Field, e.Message)
	}
	return e.Code + ": " + e.Message
}

func errf(code, field, format string, a ...any) *Error {
	return &Error{Code: code, Field: field, Message: fmt.Sprintf(format, a...)}
}

type nodeKind int

const (
	kindAnd nodeKind = iota
	kindOr
	kindNot
	kindCond
)

type op string

const (
	opEq       op = "eq"
	opNe       op = "ne"
	opIn       op = "in"
	opRange    op = "range"
	opContains op = "contains"
	opExists   op = "exists"
)

// bound is one side of a range condition.
type bound struct {
	sqlOp string // ">", ">=", "<", "<="
	value any
}

// Node is a validated filter. Its zero value is not meaningful; obtain one
// from Parse. A nil *Node means "no filter".
type Node struct {
	kind     nodeKind
	children []*Node

	field  string
	typ    Type
	op     op
	value  any     // eq, ne, contains
	values any     // in: []string, []int64, []float64 or []bool
	bounds []bound // range
}

// Parse validates a filter against the schema. A nil, empty or JSON null raw
// filter yields (nil, nil). All errors are *Error.
func Parse(raw []byte, s Schema) (*Node, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}
	if !json.Valid(trimmed) {
		return nil, errf(CodeInvalidFilter, "", "filter is not valid JSON")
	}
	p := &parser{schema: s}
	n, err := p.parse(trimmed, 1)
	if err != nil {
		return nil, err
	}
	return n, nil
}

type parser struct {
	schema Schema
	conds  int
}

// object decodes a JSON object into its members, rejecting duplicate keys.
func object(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, false
	}
	m := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := tok.(string)
		if !ok {
			return nil, false
		}
		if _, dup := m[key]; dup {
			return nil, false
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, false
		}
		m[key] = v
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return m, true
}

func (p *parser) parse(raw json.RawMessage, depth int) (*Node, error) {
	if depth > MaxDepth {
		return nil, errf(CodeTooComplex, "", "filter nesting exceeds %d levels", MaxDepth)
	}
	m, ok := object(raw)
	if !ok {
		return nil, errf(CodeInvalidFilter, "", "each filter must be a JSON object without duplicate keys")
	}
	for _, k := range []string{"and", "or", "not"} {
		sub, has := m[k]
		if !has {
			continue
		}
		if len(m) != 1 {
			return nil, errf(CodeInvalidFilter, "", "%q must be the only key of its object", k)
		}
		if k == "not" {
			child, err := p.parse(sub, depth+1)
			if err != nil {
				return nil, err
			}
			return &Node{kind: kindNot, children: []*Node{child}}, nil
		}
		var items []json.RawMessage
		if err := json.Unmarshal(sub, &items); err != nil || items == nil {
			return nil, errf(CodeInvalidFilter, "", "%q must be an array of filters", k)
		}
		if len(items) == 0 {
			return nil, errf(CodeInvalidFilter, "", "%q must not be empty", k)
		}
		kind := kindAnd
		if k == "or" {
			kind = kindOr
		}
		n := &Node{kind: kind, children: make([]*Node, 0, len(items))}
		for _, it := range items {
			child, err := p.parse(it, depth+1)
			if err != nil {
				return nil, err
			}
			n.children = append(n.children, child)
		}
		return n, nil
	}
	if _, has := m["field"]; !has {
		return nil, errf(CodeInvalidFilter, "", `a filter must have "and", "or", "not" or "field"`)
	}
	p.conds++
	if p.conds > MaxConditions {
		return nil, errf(CodeTooComplex, "", "filter has more than %d conditions", MaxConditions)
	}
	return p.condition(m)
}

// allowedKeys lists the operand keys each op accepts besides field and op.
var allowedKeys = map[op][]string{
	opEq:       {"value"},
	opNe:       {"value"},
	opIn:       {"values"},
	opRange:    {"gt", "gte", "lt", "lte"},
	opContains: {"value"},
	opExists:   {},
}

func (p *parser) condition(m map[string]json.RawMessage) (*Node, error) {
	var name string
	if err := json.Unmarshal(m["field"], &name); err != nil || name == "" {
		return nil, errf(CodeInvalidFilter, "", `"field" must be a non-empty string`)
	}
	f, ok := p.schema[name]
	if !ok {
		return nil, errf(CodeUnknownField, name, "field is not declared")
	}
	rawOp, has := m["op"]
	if !has {
		return nil, errf(CodeInvalidFilter, name, `missing "op"`)
	}
	var opName string
	if err := json.Unmarshal(rawOp, &opName); err != nil {
		return nil, errf(CodeInvalidFilter, name, `"op" must be a string`)
	}
	o := op(opName)
	allowed, known := allowedKeys[o]
	if !known {
		return nil, errf(CodeUnsupportedOp, name, "unknown op %q", opName)
	}
	switch {
	case o == opRange && (f.Type == Bool || f.Type == TextArray):
		return nil, errf(CodeUnsupportedOp, name, "range is not supported for %s fields", f.Type)
	case o == opContains && f.Type != TextArray:
		return nil, errf(CodeUnsupportedOp, name, "contains is only supported for text_array fields")
	case f.Type == TextArray && o != opContains && o != opExists:
		return nil, errf(CodeUnsupportedOp, name, "text_array fields support only contains and exists")
	}
	for k := range m {
		if k == "field" || k == "op" {
			continue
		}
		if !contains(allowed, k) {
			return nil, errf(CodeInvalidFilter, name, "unknown key %q for op %q", k, opName)
		}
	}

	n := &Node{kind: kindCond, field: name, typ: f.Type, op: o}
	switch o {
	case opEq, opNe, opContains:
		raw, has := m["value"]
		if !has {
			return nil, errf(CodeInvalidFilter, name, `op %q requires "value"`, opName)
		}
		valType := f.Type
		if o == opContains {
			valType = Text
		}
		v, err := scalar(raw, valType, name)
		if err != nil {
			return nil, err
		}
		n.value = v
	case opIn:
		raw, has := m["values"]
		if !has {
			return nil, errf(CodeInvalidFilter, name, `op "in" requires "values"`)
		}
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil || items == nil {
			return nil, errf(CodeInvalidFilter, name, `"values" must be an array`)
		}
		if len(items) == 0 {
			return nil, errf(CodeInvalidFilter, name, `"values" must not be empty`)
		}
		if len(items) > MaxInValues {
			return nil, errf(CodeTooComplex, name, `"values" has more than %d entries`, MaxInValues)
		}
		vals, err := list(items, f.Type, name)
		if err != nil {
			return nil, err
		}
		n.values = vals
	case opRange:
		_, gt := m["gt"]
		_, gte := m["gte"]
		_, lt := m["lt"]
		_, lte := m["lte"]
		if !gt && !gte && !lt && !lte {
			return nil, errf(CodeInvalidFilter, name, `range needs at least one of "gt", "gte", "lt", "lte"`)
		}
		if (gt && gte) || (lt && lte) {
			return nil, errf(CodeInvalidFilter, name, `range may not combine "gt" with "gte" or "lt" with "lte"`)
		}
		for _, b := range []struct{ key, sqlOp string }{{"gt", ">"}, {"gte", ">="}, {"lt", "<"}, {"lte", "<="}} {
			raw, has := m[b.key]
			if !has {
				continue
			}
			v, err := scalar(raw, f.Type, name)
			if err != nil {
				return nil, err
			}
			n.bounds = append(n.bounds, bound{sqlOp: b.sqlOp, value: v})
		}
	case opExists:
	}
	return n, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// scalar converts one JSON operand to the Go value bound for a field of type t:
// string (text), int64 (int), float64 (float, timestamp) or bool (bool).
func scalar(raw json.RawMessage, t Type, field string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errf(CodeInvalidValue, field, "invalid JSON value")
	}
	wrong := func() error {
		return errf(CodeInvalidValue, field, "value %s is not a valid %s", truncate(raw), t)
	}
	switch t {
	case Text:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case Int:
		if num, ok := v.(json.Number); ok {
			r, ok := new(big.Rat).SetString(string(num))
			if ok && r.IsInt() && r.Num().IsInt64() {
				return r.Num().Int64(), nil
			}
		}
	case Float:
		if num, ok := v.(json.Number); ok {
			if f, err := strconv.ParseFloat(string(num), 64); err == nil {
				return f, nil
			}
		}
	case Bool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case Timestamp:
		if s, ok := v.(string); ok {
			if f, err := parseTimestamp(s); err == nil {
				return f, nil
			}
		}
	}
	return nil, wrong()
}

// list converts the operands of an "in" condition to a typed slice.
func list(items []json.RawMessage, t Type, field string) (any, error) {
	switch t {
	case Text:
		out := make([]string, 0, len(items))
		for _, it := range items {
			v, err := scalar(it, t, field)
			if err != nil {
				return nil, err
			}
			out = append(out, v.(string))
		}
		return out, nil
	case Int:
		out := make([]int64, 0, len(items))
		for _, it := range items {
			v, err := scalar(it, t, field)
			if err != nil {
				return nil, err
			}
			out = append(out, v.(int64))
		}
		return out, nil
	case Float, Timestamp:
		out := make([]float64, 0, len(items))
		for _, it := range items {
			v, err := scalar(it, t, field)
			if err != nil {
				return nil, err
			}
			out = append(out, v.(float64))
		}
		return out, nil
	case Bool:
		out := make([]bool, 0, len(items))
		for _, it := range items {
			v, err := scalar(it, t, field)
			if err != nil {
				return nil, err
			}
			out = append(out, v.(bool))
		}
		return out, nil
	}
	return nil, errors.New("filter: unreachable: in on " + string(t))
}

func truncate(raw json.RawMessage) string {
	s := string(raw)
	if len(s) > 64 {
		s = s[:61] + "..."
	}
	return s
}

// SQL renders the node as a parenthesized boolean SQL expression over the
// jsonb column col (an already-quoted SQL expression such as `c.meta`),
// appending bound parameters to *args; placeholders continue from len(*args).
// Field names and values are always bound parameters, never SQL text. A nil
// node renders as "TRUE".
func (n *Node) SQL(col string, args *[]any) string {
	if n == nil {
		return "TRUE"
	}
	switch n.kind {
	case kindAnd, kindOr:
		if len(n.children) == 1 {
			return n.children[0].SQL(col, args)
		}
		sep := " AND "
		if n.kind == kindOr {
			sep = " OR "
		}
		parts := make([]string, len(n.children))
		for i, c := range n.children {
			parts[i] = c.SQL(col, args)
		}
		return "(" + strings.Join(parts, sep) + ")"
	case kindNot:
		return "(NOT COALESCE(" + n.children[0].SQL(col, args) + ", false))"
	}
	return n.condSQL(col, args)
}

func bind(args *[]any, v any) string {
	*args = append(*args, v)
	return "$" + strconv.Itoa(len(*args))
}

// sqlCast is the SQL type a field's values are compared as ("" for text).
func sqlCast(t Type) string {
	switch t {
	case Int, Float:
		return "numeric"
	case Bool:
		return "boolean"
	case Timestamp:
		return "float8"
	}
	return "text"
}

func (n *Node) condSQL(col string, args *[]any) string {
	key := bind(args, n.field) + "::text"
	cast := sqlCast(n.typ)
	// lhs is the field's value as the comparison type.
	lhs := "(" + col + "->>" + key + ")"
	if cast != "text" {
		lhs += "::" + cast
	}
	switch n.op {
	case opExists:
		return "jsonb_exists(" + col + ", " + key + ")"
	case opContains:
		return "jsonb_exists(" + col + "->" + key + ", " + bind(args, n.value) + "::text)"
	case opEq:
		return "(" + lhs + " = " + bind(args, n.value) + "::" + cast + ")"
	case opNe:
		return "(jsonb_exists(" + col + ", " + key + ") AND " + lhs + " <> " + bind(args, n.value) + "::" + cast + ")"
	case opIn:
		return "(" + lhs + " = ANY(" + bind(args, n.values) + "::" + cast + "[]))"
	case opRange:
		if n.typ == Text {
			lhs += ` COLLATE "C"`
		}
		parts := make([]string, len(n.bounds))
		for i, b := range n.bounds {
			parts[i] = lhs + " " + b.sqlOp + " " + bind(args, b.value) + "::" + cast
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	}
	panic("filter: unreachable op " + string(n.op))
}

// Fields returns the field names a filter refers to (nil node: none).
func (n *Node) Fields() []string {
	if n == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	var walk func(*Node)
	walk = func(x *Node) {
		if x.field != "" && !seen[x.field] {
			seen[x.field] = true
			out = append(out, x.field)
		}
		for _, c := range x.children {
			walk(c)
		}
	}
	walk(n)
	return out
}
