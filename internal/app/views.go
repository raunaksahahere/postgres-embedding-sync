package app

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/verify"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/viewdoc"
)

// viewSpec builds a view's assembly spec from the sink's accepted relations
// (commands other than `run` have no source catalog).
func viewSpec(ctx context.Context, e *Env, spec *pipeline.Spec) (*viewdoc.ViewSpec, error) {
	rels, err := e.Store.Relations(ctx)
	if err != nil {
		return nil, err
	}
	vr := map[string]viewdoc.Relation{}
	for n, r := range rels {
		vr[n] = viewdoc.Relation{KeyKinds: r.KeyKinds, Signature: r.RelationSignature}
	}
	return viewdoc.Build(spec.ViewDef, vr)
}

// replayViewDoc retries a quarantined view document: it is assembled from
// the sink's state and rendered again. It is applied only if the record is
// unchanged since it was read: any later render of the document by the
// stream (which supersedes or re-quarantines the record) owns the outcome.
func replayViewDoc(ctx context.Context, e *Env, d store.DLQEntry, g *store.Generation, spec *pipeline.Spec, out io.Writer, res *ReplayResult) error {
	vs, err := viewSpec(ctx, e, spec)
	if err != nil {
		return err
	}
	docs, err := viewdoc.Assemble(ctx, verify.StateSource(e.Store.Pool, e.Store.Schema), vs, []string{d.SourceKey})
	if err != nil {
		return err
	}
	doc := docs[d.SourceKey]
	bump := func() {
		res.StillBad++
		_, _ = e.Store.Pool.Exec(ctx, `UPDATE `+e.Store.T("dlq")+` SET attempts = attempts + 1, updated_at = now() WHERE id = $1`, d.ID)
	}
	if doc == nil {
		res.Superseded++
		fmt.Fprintf(out, "skipped dlq #%d: the document no longer exists\n", d.ID)
		return nil
	}
	if _, n := doc.MaxList(); n > spec.ViewDef.MaxChildren {
		bump()
		return nil
	}
	text, err := spec.Tr.RenderData(doc.Data())
	if err != nil || len(text) > e.Cfg.Pipeline.MaxDocumentBytes {
		bump()
		return nil
	}
	cs := spec.Tr.Chunks(d.SourceKey, text, ModelIdentity(g))
	inputs := make([]string, len(cs))
	for i, c := range cs {
		inputs[i] = g.DocumentPrefix + c.Text
	}
	vecs := map[int][]float32{}
	if len(inputs) > 0 {
		prov, err := e.Providers.For(g.Provider.Name, g.Provider.Kind)
		if err != nil {
			return err
		}
		v, rej, err := prov.Embed(ctx, embedclient.Request{Model: g.ModelName, Revision: g.ModelRevision, Kind: embedclient.Document, Inputs: inputs,
			Dimensions: g.Provider.RequestDimensions, Expect: g.Dimension, InputType: g.Provider.DocumentInputType})
		if err != nil {
			return err
		}
		if len(rej) > 0 {
			bump()
			return nil
		}
		vecs = v
	}
	applied := false
	err = pgx.BeginFunc(ctx, e.Store.Pool, func(tx pgx.Tx) error {
		var state string
		var attempts int
		if err := tx.QueryRow(ctx, `SELECT state, attempts FROM `+e.Store.T("dlq")+` WHERE id = $1 FOR UPDATE`, d.ID).Scan(&state, &attempts); err != nil {
			return err
		}
		if state != "open" || attempts != d.Attempts {
			return nil // a newer render of the document owns the outcome
		}
		tbl := pgx.Identifier{e.Store.Schema, g.ChunkTable}.Sanitize()
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl+` WHERE pipeline = $1 AND source_key = $2`, d.Pipeline, d.SourceKey); err != nil {
			return err
		}
		for i, c := range cs {
			if _, err := tx.Exec(ctx, `INSERT INTO `+tbl+` (pipeline, source_key, chunk_no, chunk_id, fingerprint, content, embedding, source_lsn, meta)
				VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::pg_lsn, $9::jsonb)`, d.Pipeline, d.SourceKey, c.No, c.ID, c.Fingerprint, c.Text,
				embedclient.FormatVector(vecs[i]), d.SourceLSN, pipeline.MetaJSON(spec.ViewDef.Metadata, doc.Root)); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE `+e.Store.T("dlq")+` SET state = 'resolved', resolved_at = now(), updated_at = now(), attempts = attempts + 1 WHERE id = $1`, d.ID); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return err
	}
	if applied {
		res.Resolved++
		fmt.Fprintf(out, "resolved dlq #%d (generation %s, view %s)\n", d.ID, d.GenerationID, d.Pipeline)
	} else {
		res.Superseded++
		fmt.Fprintf(out, "skipped dlq #%d: superseded by a newer change\n", d.ID)
	}
	return nil
}
