package viewdoc

import (
	"context"
	"fmt"
	"sort"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/keys"
)

// Source reads materialized view state: the allowlisted rows of each
// component (its pipeline is the component name) and the reference index.
// The stream path implements it over uncommitted units layered on the sink;
// replay and migration coverage read the sink directly. Implementations
// never query the source tables, so a document is always assembled from the
// state at one stream position.
type Source interface {
	// States returns the fields of the given keys that exist.
	States(ctx context.Context, pipeline string, keys []string) (map[string]map[string]*string, error)
	// Children returns, per parent key, the keys of rows whose reference ref
	// points at it.
	Children(ctx context.Context, pipeline, ref string, parents []string) (map[string][]string, error)
}

// CompSpec is a component with what assembly needs from the catalog.
type CompSpec struct {
	Comp       *config.Component
	Codec      keys.Codec
	OrderKinds []Kind // children: kinds of OrderCols; links: kinds of the target's OrderFields
}

// LinkSpec pairs a link table with its target table.
type LinkSpec struct {
	Link   *CompSpec
	Target *CompSpec
}

// ViewSpec is everything needed to assemble one view's documents.
type ViewSpec struct {
	View     *config.ViewDef
	Root     *CompSpec
	Children []*CompSpec
	Links    []LinkSpec
}

// Relation is what the sink records about a component's table: key kinds
// and the accepted column signature (types of every published column).
type Relation struct {
	KeyKinds  string
	Signature string
}

// Build makes a ViewSpec from the config and each component's accepted relation.
func Build(v *config.ViewDef, rels map[string]Relation) (*ViewSpec, error) {
	comps := v.Components()
	byName := map[string]*CompSpec{}
	for _, c := range comps {
		r, ok := rels[c.Def.Name]
		if !ok {
			return nil, fmt.Errorf("view %s: component %s is not registered yet", v.Name, c.Def.Name)
		}
		codec, err := keys.ParseCodec(r.KeyKinds)
		if err != nil {
			return nil, fmt.Errorf("view %s: component %s: %w", v.Name, c.Def.Name, err)
		}
		byName[c.Def.Name] = &CompSpec{Comp: c, Codec: codec}
	}
	vs := &ViewSpec{View: v}
	for _, c := range comps {
		cs := byName[c.Def.Name]
		switch c.Kind {
		case config.CompRoot:
			vs.Root = cs
		case config.CompChild:
			k, err := KindsFromSignature(rels[c.Def.Name].Signature, c.OrderCols)
			if err != nil {
				return nil, fmt.Errorf("view %s: child %s: %w", v.Name, c.Child.Name, err)
			}
			cs.OrderKinds = k
			vs.Children = append(vs.Children, cs)
		case config.CompLink:
			target := byName[config.ComponentName(v.Name, c.Link.Name+".target")]
			cols := make([]string, len(c.OrderFields))
			for i, f := range c.OrderFields {
				for _, tf := range c.Link.TargetFields {
					if tf.Name == f {
						cols[i] = tf.Column
					}
				}
			}
			k, err := KindsFromSignature(rels[target.Comp.Def.Name].Signature, cols)
			if err != nil {
				return nil, fmt.Errorf("view %s: link %s: %w", v.Name, c.Link.Name, err)
			}
			cs.OrderKinds = k
			vs.Links = append(vs.Links, LinkSpec{Link: cs, Target: target})
		}
	}
	return vs, nil
}

// Refs computes a component row's references (ref name -> encoded key of
// the referenced row). A reference with a NULL column is absent.
func Refs(c *config.Component, codec keys.Codec, key string, fields map[string]*string) map[string]string {
	if len(c.Refs) == 0 {
		return nil
	}
	kv, err := codec.Decode(key)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, r := range c.Refs {
		vals := make([]string, len(r.Columns))
		ok := true
		for i, col := range r.Columns {
			v, has := c.Value(col, kv, fields)
			if !has || v == nil {
				ok = false
				break
			}
			vals[i] = *v
		}
		if !ok {
			continue
		}
		if enc, err := keys.EncodeValues(vals); err == nil {
			out[r.Name] = enc
		}
	}
	return out
}

// Assemble builds the documents of the given root keys from src. Keys whose
// root row does not exist are absent from the result.
func Assemble(ctx context.Context, src Source, vs *ViewSpec, rootKeys []string) (map[string]*Doc, error) {
	if len(rootKeys) == 0 {
		return map[string]*Doc{}, nil
	}
	roots, err := src.States(ctx, vs.Root.Comp.Def.Name, rootKeys)
	if err != nil {
		return nil, err
	}
	docs := map[string]*Doc{}
	present := make([]string, 0, len(roots))
	for _, k := range rootKeys {
		f, ok := roots[k]
		if !ok {
			continue
		}
		d := &Doc{Key: k, Root: f, Lists: map[string][]Row{}}
		for _, ch := range vs.Children {
			d.Lists[ch.Comp.Child.Name] = []Row{}
		}
		for _, l := range vs.Links {
			d.Lists[l.Link.Comp.Link.Name] = []Row{}
		}
		docs[k] = d
		present = append(present, k)
	}
	if len(present) == 0 {
		return docs, nil
	}
	for _, ch := range vs.Children {
		name := ch.Comp.Def.Name
		kids, err := src.Children(ctx, name, "parent", present)
		if err != nil {
			return nil, err
		}
		all := flatten(kids)
		st, err := src.States(ctx, name, all)
		if err != nil {
			return nil, err
		}
		for parent, ks := range kids {
			d := docs[parent]
			if d == nil {
				continue
			}
			rows := make([]Row, 0, len(ks))
			for _, k := range ks {
				if f, ok := st[k]; ok {
					rows = append(rows, Row{Key: k, Fields: f})
				}
			}
			comp, codec := ch.Comp, ch.Codec
			SortRows(rows, ch.OrderKinds, func(r Row) []*string {
				kv, _ := codec.Decode(r.Key)
				out := make([]*string, len(comp.OrderCols))
				for i, col := range comp.OrderCols {
					out[i], _ = comp.Value(col, kv, r.Fields)
				}
				return out
			}, codec)
			d.Lists[comp.Child.Name] = rows
		}
	}
	for _, l := range vs.Links {
		name := l.Link.Comp.Def.Name
		links, err := src.Children(ctx, name, "parent", present)
		if err != nil {
			return nil, err
		}
		all := flatten(links)
		lst, err := src.States(ctx, name, all)
		if err != nil {
			return nil, err
		}
		// Resolve each link row to its target key.
		targetOf := map[string]string{}
		var tkeys []string
		seenT := map[string]bool{}
		for _, k := range all {
			f, ok := lst[k]
			if !ok {
				continue
			}
			t, ok := Refs(l.Link.Comp, l.Link.Codec, k, f)["target"]
			if !ok {
				continue
			}
			targetOf[k] = t
			if !seenT[t] {
				seenT[t] = true
				tkeys = append(tkeys, t)
			}
		}
		tst, err := src.States(ctx, l.Target.Comp.Def.Name, tkeys)
		if err != nil {
			return nil, err
		}
		fields := l.Link.Comp.OrderFields
		for parent, ks := range links {
			d := docs[parent]
			if d == nil {
				continue
			}
			rows := make([]Row, 0, len(ks))
			for _, k := range ks {
				t, ok := targetOf[k]
				if !ok {
					continue
				}
				if f, ok := tst[t]; ok {
					rows = append(rows, Row{Key: t, Fields: f})
				}
			}
			SortRows(rows, l.Link.OrderKinds, func(r Row) []*string {
				out := make([]*string, len(fields))
				for i, f := range fields {
					out[i] = r.Fields[f]
				}
				return out
			}, l.Target.Codec)
			d.Lists[l.Link.Comp.Link.Name] = rows
		}
	}
	return docs, nil
}

func flatten(m map[string][]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, ks := range m {
		for _, k := range ks {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}
