// Package search answers semantic queries against one generation.
//
// Every request is pinned: the generation (from the serving alias or an
// explicit override) determines the encoder name, revision, query prompt,
// dimension and chunk table. Two generations with equal dimensions can never
// be mixed because the query vector is produced by the generation's own encoder
// and the response from the embedding service is checked against it.
package search

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

// Searcher runs queries. The query vector comes from the provider that
// built the searched generation (as recorded with it), with the same
// request settings, so promotion and rollback switch providers together
// with the index.
type Searcher struct {
	Pool      *pgxpool.Pool
	Schema    string
	Providers *embedclient.Set
}

// Options control one search.
type Options struct {
	K          int
	Generation string // empty = serving alias
	Exact      bool   // bypass the ANN index (sequential scan)
}

// Hit is one document (best chunk per pipeline and source key). Key is the
// encoded source key: the column value for a single-column key, a JSON array
// of the column values for a composite key.
type Hit struct {
	Pipeline string  `json:"pipeline"`
	Key      string  `json:"key"`
	ChunkNo  int     `json:"chunk_no"`
	Score    float64 `json:"score"`
	Content  string  `json:"content"`
}

// Result is a search response.
type Result struct {
	Generation string  `json:"generation"`
	Model      string  `json:"model"`
	Revision   string  `json:"revision"`
	Provider   string  `json:"provider"`
	Exact      bool    `json:"exact"`
	Hits       []Hit   `json:"hits"`
	EmbedMS    float64 `json:"embed_ms"`
	QueryMS    float64 `json:"query_ms"`
}

type genRow struct {
	id, model, revision, prefix, table, distance string
	dim, efSearch                                int
	provider                                     store.ProviderIdentity
}

func (s *Searcher) loadGen(ctx context.Context, id string) (*genRow, error) {
	g := &genRow{}
	var params struct {
		EFSearch int `json:"ef_search"`
	}
	q := `SELECT g.id, g.model_name, g.model_revision, g.query_prefix, g.chunk_table, g.distance, g.dimension, g.index_params, g.provider
		FROM ` + pgx.Identifier{s.Schema, "generations"}.Sanitize() + ` g `
	var row pgx.Row
	if id == "" {
		row = s.Pool.QueryRow(ctx, q+`JOIN `+pgx.Identifier{s.Schema, "serving"}.Sanitize()+` s ON s.generation_id = g.id WHERE s.name = 'default'`)
	} else {
		row = s.Pool.QueryRow(ctx, q+`WHERE g.id = $1`, id)
	}
	if err := row.Scan(&g.id, &g.model, &g.revision, &g.prefix, &g.table, &g.distance, &g.dim, &params, &g.provider); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("generation %q not found", id)
		}
		return nil, err
	}
	g.efSearch = params.EFSearch
	return g, nil
}

// Search embeds the query with the generation's encoder and returns the top-K documents.
func (s *Searcher) Search(ctx context.Context, query string, o Options) (res *Result, err error) {
	start := time.Now()
	genLabel := "unknown" // never a caller-supplied string: label values must stay bounded
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		if res != nil {
			genLabel = res.Generation
		}
		metrics.SearchRequests.WithLabelValues(genLabel, outcome).Inc()
		metrics.SearchSeconds.Observe(time.Since(start).Seconds())
	}()
	if o.K <= 0 {
		o.K = 10
	}
	g, err := s.loadGen(ctx, o.Generation)
	if err != nil {
		return nil, err
	}
	prov, err := s.Providers.For(g.provider.Name, g.provider.Kind)
	if err != nil {
		return nil, err
	}
	var resp *embedclient.Response
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = prov.EmbedOnce(ctx, g.queryRequest(query))
		var tr *embedclient.TransientError
		if err == nil || !errors.As(err, &tr) {
			break
		}
		time.Sleep(prov.Backoff(attempt))
	}
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if len(resp.Embeddings) != 1 || len(resp.Embeddings[0]) != g.dim {
		return nil, fmt.Errorf("query embedding has wrong dimension for generation %s", g.id)
	}
	vec := embedclient.FormatVector(resp.Embeddings[0])
	embedDone := time.Now()
	hits, err := s.query(ctx, nil, g, vec, o.K, o.Exact)
	if err != nil {
		return nil, err
	}
	return &Result{Generation: g.id, Model: g.model, Revision: g.revision, Provider: g.provider.Name, Exact: o.Exact, Hits: hits,
		EmbedMS: float64(embedDone.Sub(start).Microseconds()) / 1000, QueryMS: float64(time.Since(embedDone).Microseconds()) / 1000}, nil
}

// SearchVector runs a search with a precomputed query vector (used by the migration gate).
func (s *Searcher) SearchVector(ctx context.Context, genID, vec string, k int, exact bool) ([]Hit, error) {
	g, err := s.loadGen(ctx, genID)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, nil, g, vec, k, exact)
}

// SearchVectorIn is SearchVector inside the caller's transaction (a savepoint
// scopes the planner settings), so several generations can be compared at one
// snapshot.
func (s *Searcher) SearchVectorIn(ctx context.Context, parent pgx.Tx, genID, vec string, k int, exact bool) ([]Hit, error) {
	g, err := s.loadGen(ctx, genID)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, parent, g, vec, k, exact)
}

func (g *genRow) queryRequest(texts ...string) embedclient.Request {
	in := make([]string, len(texts))
	for i, t := range texts {
		in[i] = g.prefix + t
	}
	return embedclient.Request{Model: g.model, Revision: g.revision, Kind: embedclient.Query, Inputs: in,
		Dimensions: g.provider.RequestDimensions, Expect: g.dim, InputType: g.provider.QueryInputType}
}

// QueryEncoder is how a generation's queries are embedded.
type QueryEncoder struct {
	Model, Revision string
	Dim             int
	Provider        embedclient.Provider
	g               *genRow
}

// Request builds the provider request for query texts (prompt applied).
func (q *QueryEncoder) Request(texts ...string) embedclient.Request { return q.g.queryRequest(texts...) }

// Encoder returns how a generation's queries are embedded: its model,
// prompt, provider and request settings.
func (s *Searcher) Encoder(ctx context.Context, genID string) (*QueryEncoder, error) {
	g, err := s.loadGen(ctx, genID)
	if err != nil {
		return nil, err
	}
	prov, err := s.Providers.For(g.provider.Name, g.provider.Kind)
	if err != nil {
		return nil, err
	}
	return &QueryEncoder{Model: g.model, Revision: g.revision, Dim: g.dim, Provider: prov, g: g}, nil
}

func (s *Searcher) query(ctx context.Context, parent pgx.Tx, g *genRow, vec string, k int, exact bool) ([]Hit, error) {
	op := "<=>"
	switch g.distance {
	case "l2":
		op = "<->"
	case "ip":
		op = "<#>"
	}
	var tx pgx.Tx
	var err error
	if parent != nil {
		tx, err = parent.Begin(ctx) // savepoint: rolling back undoes SET LOCAL
	} else {
		tx, err = s.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	}
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if exact {
		for _, q := range []string{`SET LOCAL enable_indexscan = off`, `SET LOCAL enable_bitmapscan = off`} {
			if _, err := tx.Exec(ctx, q); err != nil {
				return nil, err
			}
		}
	} else if g.efSearch > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL hnsw.ef_search = %d`, g.efSearch)); err != nil {
			return nil, err
		}
	}
	candidates := k * 4
	// The inner ORDER BY distance LIMIT is what the HNSW index serves; the
	// outer order breaks exact (and float-rounding) ties by key so results are
	// deterministic. Exact search also breaks ties inside the LIMIT.
	inner := `embedding ` + op + ` $1::vector`
	if exact {
		inner += `, pipeline, source_key, chunk_no`
	}
	rows, err := tx.Query(ctx, `SELECT pipeline, source_key, chunk_no, content, d FROM (SELECT pipeline, source_key, chunk_no, content, embedding `+op+` $1::vector AS d FROM `+
		pgx.Identifier{s.Schema, g.table}.Sanitize()+` ORDER BY `+inner+` LIMIT $2) c ORDER BY round(d::numeric, 6), pipeline, source_key, chunk_no`, vec, candidates)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var hits []Hit
	for rows.Next() {
		var h Hit
		var d float64
		if err := rows.Scan(&h.Pipeline, &h.Key, &h.ChunkNo, &h.Content, &d); err != nil {
			return nil, err
		}
		doc := h.Pipeline + "\x00" + h.Key
		if seen[doc] {
			continue
		}
		seen[doc] = true
		switch g.distance {
		case "cosine":
			h.Score = 1 - d
		case "ip":
			h.Score = -d
		default:
			h.Score = -d
		}
		if len(hits) < k {
			hits = append(hits, h)
		}
	}
	return hits, rows.Err()
}
