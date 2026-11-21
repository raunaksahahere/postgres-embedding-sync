package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ViewDef assembles one document per row of a root table from related
// tables: ordered child rows (e.g. sections) and rows reached through a link
// table (e.g. tags). Every table carries the tenant column first in its key
// and in every reference, so no join can cross tenants. Each table belongs to
// exactly one pipeline or view.
//
//	views:
//	  - name: kb
//	    tenant: tenant_id
//	    table: public.documents
//	    key: [tenant_id, id]
//	    fields: [{name: title, column: title}]
//	    children:
//	      - name: sections
//	        table: public.sections
//	        key: [tenant_id, id]
//	        parent: [tenant_id, document_id]
//	        order_by: [position]
//	        fields: [{name: heading, column: heading}, {name: body, column: body}]
//	    links:
//	      - name: tags
//	        table: public.document_tags
//	        key: [tenant_id, document_id, tag_id]
//	        parent: [tenant_id, document_id]
//	        target: [tenant_id, tag_id]
//	        target_table: public.tags
//	        target_key: [tenant_id, id]
//	        target_fields: [{name: name, column: name}]
//	        order_by: [name]
//	    transform: {template: "{{.title}}{{range .tags}} #{{.name}}{{end}}{{range .sections}}\n{{.heading}}\n{{.body}}{{end}}"}
type ViewDef struct {
	Name     string     `yaml:"name" json:"name"`
	Tenant   string     `yaml:"tenant" json:"tenant"`
	Table    string     `yaml:"table" json:"table"`
	Key      KeyColumns `yaml:"key" json:"key"`
	Fields   []Field    `yaml:"fields" json:"fields"`
	Children []ChildDef `yaml:"children" json:"children"`
	Links    []LinkDef  `yaml:"links" json:"links"`
	// Transform renders the document; nil inherits the top-level transform.
	// The template sees the root fields, and each child and link as a list
	// of maps: {{range .sections}}{{.heading}}{{end}}.
	Transform *Transform `yaml:"transform" json:"transform"`
	// MaxChildren caps the rows one document may assemble per child or link;
	// a document over it is quarantined (too_many_children).
	MaxChildren int `yaml:"max_children" json:"max_children"`
	// MaxFanoutPerTxn caps the documents one source transaction may change
	// in this view (e.g. renaming a tag linked everywhere); above it the
	// pipeline pauses before acknowledging, keeping the transaction atomic.
	MaxFanoutPerTxn int `yaml:"max_fanout_per_txn" json:"max_fanout_per_txn"`
}

// ChildDef is a table whose rows belong to one root row through Parent.
type ChildDef struct {
	Name    string     `yaml:"name" json:"name"`
	Table   string     `yaml:"table" json:"table"`
	Key     KeyColumns `yaml:"key" json:"key"`
	Parent  KeyColumns `yaml:"parent" json:"parent"`     // columns referencing the root key, in root key order
	OrderBy []string   `yaml:"order_by" json:"order_by"` // columns; ties broken by the child key
	Fields  []Field    `yaml:"fields" json:"fields"`
}

// LinkDef reaches target rows through a link table (many-to-many).
type LinkDef struct {
	Name         string     `yaml:"name" json:"name"`
	Table        string     `yaml:"table" json:"table"`
	Key          KeyColumns `yaml:"key" json:"key"`
	Parent       KeyColumns `yaml:"parent" json:"parent"` // link columns referencing the root key
	Target       KeyColumns `yaml:"target" json:"target"` // link columns referencing the target key
	TargetTable  string     `yaml:"target_table" json:"target_table"`
	TargetKey    KeyColumns `yaml:"target_key" json:"target_key"`
	TargetFields []Field    `yaml:"target_fields" json:"target_fields"`
	OrderBy      []string   `yaml:"order_by" json:"order_by"` // target field names; ties broken by the target key
}

// ComponentKind is the role of one table inside a view.
type ComponentKind string

// Component kinds.
const (
	CompRoot   ComponentKind = "root"
	CompChild  ComponentKind = "child"
	CompLink   ComponentKind = "link"
	CompTarget ComponentKind = "target"
)

// Ref names a reference from a component's rows to another component's key.
type Ref struct {
	Name    string   // "parent" or "target"
	Columns []string // referencing columns, in the referenced key's order
	To      string   // component name the reference points at
}

// Component is one table of a view, presented as a table pipeline (Def) so
// that catalog checks, publication, grants, relation signatures, watermark
// backfill and source_state work as for any pipeline. Def.Name is
// "<view>:root", "<view>:<child>", "<view>:<link>" or "<view>:<link>.target";
// a colon cannot appear in a pipeline name, so names never collide.
type Component struct {
	Def   PipelineDef
	View  *ViewDef
	Kind  ComponentKind
	Child *ChildDef
	Link  *LinkDef
	Refs  []Ref
	// OrderCols orders a child's rows (columns, possibly key columns);
	// OrderFields orders a link's targets (target field names). Ties are
	// broken by the child or target key.
	OrderCols   []string
	OrderFields []string
}

// Value returns column col of a row of this component: from its decoded
// key if col is a key column, otherwise from the field that carries it.
// ok is false if the component does not carry col.
func (c *Component) Value(col string, key []string, fields map[string]*string) (v *string, ok bool) {
	for i, k := range c.Def.Key {
		if k == col && i < len(key) {
			s := key[i]
			return &s, true
		}
	}
	for _, f := range c.Def.Fields {
		if f.Column == col {
			return fields[f.Name], true
		}
	}
	return nil, false
}

// hiddenPrefix marks fields added to carry reference and order columns.
const hiddenPrefix = "__"

// HiddenField is the logical field name that carries column col.
func HiddenField(col string) string { return hiddenPrefix + col }

// IsHiddenField reports whether a logical field only carries a reference or order column.
func IsHiddenField(name string) bool { return strings.HasPrefix(name, hiddenPrefix) }

// ComponentName builds a component's pipeline name.
func ComponentName(view, part string) string { return view + ":" + part }

func compDef(name, table string, key KeyColumns, fields []Field, extra []string) PipelineDef {
	fs := append([]Field(nil), fields...)
	have := map[string]bool{}
	for _, k := range key {
		have[k] = true
	}
	for _, f := range fields {
		have[f.Column] = true
	}
	for _, c := range extra {
		if !have[c] {
			have[c] = true
			fs = append(fs, Field{Name: HiddenField(c), Column: c})
		}
	}
	return PipelineDef{Name: name, Table: table, Key: key, Fields: fs, Transform: &Transform{}}
}

// Components returns the tables of every view as table pipelines.
func (c *Config) Components() []*Component {
	var out []*Component
	for i := range c.Views {
		out = append(out, c.Views[i].Components()...)
	}
	return out
}

// Components returns the view's tables: root, children, then for each link
// the link table and its target table.
func (v *ViewDef) Components() []*Component {
	root := &Component{View: v, Kind: CompRoot, Def: compDef(ComponentName(v.Name, "root"), v.Table, v.Key, v.Fields, nil)}
	out := []*Component{root}
	for i := range v.Children {
		ch := &v.Children[i]
		comp := &Component{View: v, Kind: CompChild, Child: ch,
			Def:       compDef(ComponentName(v.Name, ch.Name), ch.Table, ch.Key, ch.Fields, append(append([]string{}, ch.Parent...), ch.OrderBy...)),
			Refs:      []Ref{{Name: "parent", Columns: ch.Parent, To: root.Def.Name}},
			OrderCols: ch.OrderBy}
		out = append(out, comp)
	}
	for i := range v.Links {
		l := &v.Links[i]
		target := &Component{View: v, Kind: CompTarget, Link: l,
			Def: compDef(ComponentName(v.Name, l.Name+".target"), l.TargetTable, l.TargetKey, l.TargetFields, nil)}
		link := &Component{View: v, Kind: CompLink, Link: l,
			Def:         compDef(ComponentName(v.Name, l.Name), l.Table, l.Key, nil, append(append([]string{}, l.Parent...), l.Target...)),
			Refs:        []Ref{{Name: "parent", Columns: l.Parent, To: root.Def.Name}, {Name: "target", Columns: l.Target, To: target.Def.Name}},
			OrderFields: l.OrderBy}
		out = append(out, link, target)
	}
	return out
}

// Component returns the named component or nil.
func (c *Config) Component(name string) *Component {
	for _, comp := range c.Components() {
		if comp.Def.Name == name {
			return comp
		}
	}
	return nil
}

// View returns the named view or nil.
func (c *Config) View(name string) *ViewDef {
	for i := range c.Views {
		if c.Views[i].Name == name {
			return &c.Views[i]
		}
	}
	return nil
}

// TableDefs lists every replicated table as a table pipeline: standalone
// pipelines, then every view's components.
func (c *Config) TableDefs() []*PipelineDef {
	out := make([]*PipelineDef, 0, len(c.Pipelines))
	for i := range c.Pipelines {
		out = append(out, &c.Pipelines[i])
	}
	for _, comp := range c.Components() {
		d := comp.Def
		out = append(out, &d)
	}
	return out
}

// DocNames lists every document pipeline (standalone pipelines and views).
func (c *Config) DocNames() []string {
	out := c.PipelineNames()
	for _, v := range c.Views {
		out = append(out, v.Name)
	}
	return out
}

// Fingerprint hashes everything that affects a view's document text: the
// assembly (tables, keys, references, order, fields) and the transform. The
// limits are left out: they decide whether a document is processed, not
// what it says. Physical column names are included (order and reference
// columns decide the text), so renaming a view's column means a new
// generation, unlike a standalone pipeline's field column.
func (v *ViewDef) Fingerprint() string {
	d := *v
	d.MaxChildren, d.MaxFanoutPerTxn = 0, 0
	b, _ := json.Marshal(struct {
		View    ViewDef `json:"view"`
		Version int     `json:"version"`
	}{d, 1})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:16])
}

func (c *Config) validateViews(add func(string, ...any), tables, names map[string]bool) {
	for i := range c.Views {
		v := &c.Views[i]
		where := fmt.Sprintf("view %q", v.Name)
		if !pipeRe.MatchString(v.Name) {
			add("view name %q must match %s", v.Name, pipeRe)
		}
		if names[v.Name] {
			add("%s: name already used by another pipeline or view", where)
		}
		names[v.Name] = true
		if !identRe.MatchString(v.Tenant) {
			add("%s: tenant must name the tenant column present in every table's key", where)
		}
		useTable := func(t, what string) {
			schema, table, ok := strings.Cut(t, ".")
			if !ok || !identRe.MatchString(schema) || !identRe.MatchString(table) {
				add("%s: %s table must be schema-qualified lowercase identifiers, got %q", where, what, t)
			}
			if tables[t] {
				add("%s: table %s is already used by another pipeline or view (one consumer per table)", where, t)
			}
			if schema == c.Source.ControlSchema {
				add("%s: %s table must not live in the control schema %q", where, what, c.Source.ControlSchema)
			}
			tables[t] = true
		}
		checkKey := func(what string, key KeyColumns) {
			if len(key) == 0 || len(key) > 8 {
				add("%s: %s key must list 1 to 8 primary-key columns", where, what)
				return
			}
			seen := map[string]bool{}
			for _, k := range key {
				if !identRe.MatchString(k) || seen[k] {
					add("%s: %s key column %q is invalid or repeated", where, what, k)
				}
				seen[k] = true
			}
			if key[0] != v.Tenant {
				add("%s: %s key must start with the tenant column %q (tenant boundaries)", where, what, v.Tenant)
			}
		}
		checkRef := func(what string, ref, to KeyColumns) {
			if len(ref) != len(to) {
				add("%s: %s has %d columns but the key it references has %d", where, what, len(ref), len(to))
				return
			}
			seen := map[string]bool{}
			for _, c := range ref {
				if !identRe.MatchString(c) || seen[c] {
					add("%s: %s column %q is invalid or repeated", where, what, c)
				}
				seen[c] = true
			}
			if len(ref) > 0 && ref[0] != v.Tenant {
				add("%s: %s must start with the tenant column %q (a join may not cross tenants)", where, what, v.Tenant)
			}
		}
		checkFields := func(what string, fs []Field, required bool) map[string]bool {
			if required && len(fs) == 0 {
				add("%s: %s fields must list at least one allowlisted column", where, what)
			}
			seen := map[string]bool{}
			for _, f := range fs {
				if !identRe.MatchString(f.Name) || !identRe.MatchString(f.Column) || IsHiddenField(f.Name) {
					add("%s: %s field %+v needs lowercase identifier name/column (names may not start with %q)", where, what, f, hiddenPrefix)
				}
				if seen[f.Name] {
					add("%s: %s duplicate field %q", where, what, f.Name)
				}
				seen[f.Name] = true
			}
			return seen
		}
		useTable(v.Table, "root")
		checkKey("root", v.Key)
		rootFields := checkFields("root", v.Fields, true)
		parts := map[string]bool{}
		part := func(name string) {
			if !pipeRe.MatchString(name) {
				add("%s: child/link name %q must match %s", where, name, pipeRe)
			}
			if parts[name] || rootFields[name] {
				add("%s: name %q is used twice (children, links and root fields share the template namespace)", where, name)
			}
			parts[name] = true
		}
		for _, ch := range v.Children {
			part(ch.Name)
			useTable(ch.Table, "child "+ch.Name)
			checkKey("child "+ch.Name, ch.Key)
			checkRef("child "+ch.Name+" parent", ch.Parent, v.Key)
			checkFields("child "+ch.Name, ch.Fields, true)
			for _, o := range ch.OrderBy {
				if !identRe.MatchString(o) {
					add("%s: child %s order_by %q must be a column", where, ch.Name, o)
				}
			}
		}
		for _, l := range v.Links {
			part(l.Name)
			useTable(l.Table, "link "+l.Name)
			useTable(l.TargetTable, "link "+l.Name+" target")
			checkKey("link "+l.Name, l.Key)
			checkKey("link "+l.Name+" target", l.TargetKey)
			checkRef("link "+l.Name+" parent", l.Parent, v.Key)
			checkRef("link "+l.Name+" target", l.Target, l.TargetKey)
			tf := checkFields("link "+l.Name+" target", l.TargetFields, true)
			for _, o := range l.OrderBy {
				if !tf[o] {
					add("%s: link %s order_by %q must name one of its target_fields", where, l.Name, o)
				}
			}
		}
		if v.MaxChildren < 1 || v.MaxFanoutPerTxn < 1 {
			add("%s: max_children and max_fanout_per_txn must be positive", where)
		}
		if t := v.Transform; t != nil {
			if t.Template == "" {
				add("%s: transform.template is required", where)
			}
			if t.ChunkWords < 8 {
				add("%s: transform.chunk_words must be >= 8", where)
			}
			if t.ChunkOverlap < 0 || t.ChunkOverlap >= t.ChunkWords {
				add("%s: transform.chunk_overlap must be in [0, chunk_words)", where)
			}
		}
	}
}
