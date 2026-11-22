// Package search answers queries against one generation: vector (semantic),
// full-text, or hybrid (both fused with reciprocal rank fusion), optionally
// restricted by metadata filters, pipelines and a gateway-supplied tenant.
//
// Every request is pinned: the generation (from the serving alias or an
// explicit override) determines the encoder name, revision, query prompt,
// dimension and chunk table, and both branches of a hybrid query read the
// same snapshot. Two generations with equal dimensions can never be mixed
// because the query vector is produced by the generation's own encoder and
// the response from the embedding service is checked against it.
package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/failpoint"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/filter"
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

	// Meta is the declared metadata fields filters may use.
	Meta filter.Schema
	// TextConfig is the text search configuration of the full-text branch
	// (it must be the one the chunks' tsv was built with).
	TextConfig string
	// RRFK is the default reciprocal rank fusion constant.
	RRFK int
	// SearchFingerprint is what a generation's search columns must be
	// complete for before filtered, text or hybrid queries may use it.
	SearchFingerprint string
	// Docs is the document pipelines and views requests may name (nil: no check).
	Docs map[string]bool
	// TenantField/TenantHeader force a tenant predicate (empty: off).
	TenantField, TenantHeader string
}

// Mode selects the retrieval branches.
type Mode string

// Modes.
const (
	ModeVector Mode = "vector"
	ModeText   Mode = "text"
	ModeHybrid Mode = "hybrid"
)

// Limits.
const (
	MaxK        = 100
	MaxQueryLen = 4096
	MaxRRFK     = 1000
)

// Options control one search.
type Options struct {
	K          int
	Generation string // empty = serving alias
	Exact      bool   // bypass the ANN index (sequential scan)
	Mode       Mode   // empty = vector
	// Pipelines restricts results to these document pipelines/views (empty: all).
	Pipelines []string
	// Filter is the JSON metadata filter (empty: none).
	Filter json.RawMessage
	// RRFK overrides the fusion constant (0: the configured default).
	RRFK int
	// Tenant is the tenant header value (nil: header absent).
	Tenant *string
}

// Error is a request the searcher refuses: Status is the HTTP status
// (400 invalid request, 404 unknown generation or pipeline, 503 index not
// built yet or a backend failure), Code a stable identifier.
type Error struct {
	Status     int    `json:"-"`
	Code       string `json:"code"`
	Field      string `json:"field,omitempty"`
	Message    string `json:"error"`
	RetryAfter int    `json:"-"` // seconds, for 503 search_index_building
	cause      error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.cause }

func badRequest(code, field, format string, a ...any) *Error {
	return &Error{Status: http.StatusBadRequest, Code: code, Field: field, Message: fmt.Sprintf(format, a...)}
}

// AsError classifies any search failure: client errors keep their status;
// everything else (provider, database) is 503 backend.
func AsError(err error) *Error {
	var se *Error
	if errors.As(err, &se) {
		return se
	}
	var fe *filter.Error
	if errors.As(err, &fe) {
		return &Error{Status: http.StatusBadRequest, Code: fe.Code, Field: fe.Field, Message: fe.Error(), cause: err}
	}
	return &Error{Status: http.StatusServiceUnavailable, Code: "backend", Message: err.Error(), cause: err}
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
	// Ranks is the document's 1-based rank in each hybrid branch that
	// returned it ("vector", "text").
	Ranks map[string]int `json:"ranks,omitempty"`
}

// Result is a search response.
type Result struct {
	Generation string  `json:"generation"`
	Model      string  `json:"model"`
	Revision   string  `json:"revision"`
	Provider   string  `json:"provider"`
	Mode       Mode    `json:"mode"`
	Exact      bool    `json:"exact"`
	Hits       []Hit   `json:"hits"`
	EmbedMS    float64 `json:"embed_ms"`
	QueryMS    float64 `json:"query_ms"`
}

type genRow struct {
	id, model, revision, prefix, table, distance, indexType string
	dim, efSearch                                           int
	provider                                                store.ProviderIdentity
	searchFP                                                string
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Searcher) loadGen(ctx context.Context, db querier, id string) (*genRow, error) {
	g := &genRow{}
	var params struct {
		EFSearch int `json:"ef_search"`
	}
	q := `SELECT g.id, g.model_name, g.model_revision, g.query_prefix, g.chunk_table, g.distance, g.index_type, g.dimension, g.index_params, g.provider,
		coalesce(g.search_fingerprint, '')
		FROM ` + pgx.Identifier{s.Schema, "generations"}.Sanitize() + ` g `
	var row pgx.Row
	if id == "" {
		row = db.QueryRow(ctx, q+`JOIN `+pgx.Identifier{s.Schema, "serving"}.Sanitize()+` s ON s.generation_id = g.id WHERE s.name = 'default'`)
	} else {
		row = db.QueryRow(ctx, q+`WHERE g.id = $1`, id)
	}
	if err := row.Scan(&g.id, &g.model, &g.revision, &g.prefix, &g.table, &g.distance, &g.indexType, &g.dim, &params, &g.provider, &g.searchFP); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if id == "" {
				return nil, &Error{Status: http.StatusServiceUnavailable, Code: "backend", Message: "no generation is serving yet"}
			}
			return nil, &Error{Status: http.StatusNotFound, Code: "unknown_generation", Field: "generation", Message: fmt.Sprintf("generation %q not found", id)}
		}
		return nil, err
	}
	g.efSearch = params.EFSearch
	return g, nil
}

// request is a validated search.
type request struct {
	mode      Mode
	k, rrfK   int
	filter    *filter.Node
	tenant    *filter.Node
	pipelines []string
}

func (r *request) restricted() bool {
	return r.filter != nil || r.tenant != nil
}

func (s *Searcher) validate(query string, o Options) (*request, error) {
	r := &request{mode: o.Mode, k: o.K, rrfK: o.RRFK}
	if query == "" || len(query) > MaxQueryLen {
		return nil, badRequest("invalid_request", "query", "query is required (max %d bytes)", MaxQueryLen)
	}
	switch r.mode {
	case "":
		r.mode = ModeVector
	case ModeVector, ModeText, ModeHybrid:
	default:
		return nil, badRequest("invalid_request", "mode", "mode must be vector, text or hybrid")
	}
	if r.k == 0 {
		r.k = 10
	}
	if r.k < 1 || r.k > MaxK {
		return nil, badRequest("invalid_request", "k", "k must be in [1, %d]", MaxK)
	}
	if r.rrfK == 0 {
		r.rrfK = max(s.RRFK, 1)
	}
	if r.rrfK < 1 || r.rrfK > MaxRRFK {
		return nil, badRequest("invalid_request", "rrf_k", "rrf_k must be in [1, %d]", MaxRRFK)
	}
	seen := map[string]bool{}
	for _, p := range o.Pipelines {
		if s.Docs != nil && !s.Docs[p] {
			return nil, &Error{Status: http.StatusNotFound, Code: "unknown_pipeline", Field: "pipelines", Message: fmt.Sprintf("pipeline %q is not configured", p)}
		}
		if !seen[p] {
			seen[p] = true
			r.pipelines = append(r.pipelines, p)
		}
	}
	f, err := filter.Parse(o.Filter, s.Meta)
	if err != nil {
		return nil, err
	}
	r.filter = f
	if s.TenantField != "" {
		if o.Tenant == nil || strings.TrimSpace(*o.Tenant) == "" {
			return nil, badRequest("tenant_required", s.TenantHeader, "the %s header is required (it must be set by the gateway)", s.TenantHeader)
		}
		for _, name := range f.Fields() {
			if name == s.TenantField {
				return nil, badRequest("tenant_filter_forbidden", name, "filters may not refer to the tenant field %q; it is set from the %s header", name, s.TenantHeader)
			}
		}
		val, err := s.tenantValue(*o.Tenant)
		if err != nil {
			return nil, err
		}
		cond, _ := json.Marshal(map[string]any{"field": s.TenantField, "op": "eq", "value": val})
		if r.tenant, err = filter.Parse(cond, s.Meta); err != nil {
			return nil, badRequest("invalid_value", s.TenantHeader, "the %s header is not a valid tenant", s.TenantHeader)
		}
	}
	return r, nil
}

func (s *Searcher) tenantValue(v string) (any, error) {
	if s.Meta[s.TenantField].Type == filter.Int {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return nil, badRequest("invalid_value", s.TenantHeader, "the %s header must be an integer", s.TenantHeader)
		}
		return n, nil
	}
	return v, nil
}

// Search runs a query against the requested (or serving) generation.
func (s *Searcher) Search(ctx context.Context, query string, o Options) (res *Result, err error) {
	start := time.Now()
	genLabel := "unknown" // never a caller-supplied string: label values must stay bounded
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
			if AsError(err).Status < 500 {
				outcome = "rejected"
			}
		}
		if res != nil {
			genLabel = res.Generation
		}
		metrics.SearchRequests.WithLabelValues(genLabel, outcome).Inc()
		metrics.SearchSeconds.Observe(time.Since(start).Seconds())
	}()
	r, err := s.validate(query, o)
	if err != nil {
		return nil, err
	}
	needVec := r.mode != ModeText

	// Embed with the generation named now, then pin: one read-only
	// repeatable-read transaction re-reads the generation and runs every
	// branch, so a promotion between embedding and querying is seen as a
	// whole (the query is re-embedded for the newly serving generation).
	var vec string
	var embedded *genRow
	if needVec {
		if embedded, err = s.loadGen(ctx, s.Pool, o.Generation); err != nil {
			return nil, err
		}
		if vec, err = s.embed(ctx, embedded, query); err != nil {
			return nil, err
		}
		failpoint.Pause("search_after_embed")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	g, err := s.loadGen(ctx, tx, o.Generation)
	if err != nil {
		return nil, err
	}
	if needVec && g.id != embedded.id {
		if vec, err = s.embed(ctx, g, query); err != nil {
			return nil, err
		}
	}
	embedDone := time.Now()
	if (r.mode != ModeVector || r.restricted()) && g.searchFP != s.SearchFingerprint {
		return nil, &Error{Status: http.StatusServiceUnavailable, Code: "search_index_building", RetryAfter: 30,
			Message: fmt.Sprintf("generation %s is still building its metadata and full-text columns; vector search without filters works meanwhile", g.id)}
	}

	var hits []Hit
	switch r.mode {
	case ModeVector:
		hits, err = s.vectorBranch(ctx, tx, g, vec, r, r.k, r.k*4)
	case ModeText:
		hits, err = s.textBranch(ctx, tx, g, query, r, r.k)
	case ModeHybrid:
		depth := max(4*r.k, 50)
		var vh, th []Hit
		if vh, err = s.vectorBranch(ctx, tx, g, vec, r, depth, depth*2); err != nil {
			break
		}
		if th, err = s.textBranch(ctx, tx, g, query, r, depth); err != nil {
			break
		}
		hits = Fuse(vh, th, r.rrfK, r.k)
	}
	if err != nil {
		return nil, err
	}
	if hits == nil {
		hits = []Hit{}
	}
	return &Result{Generation: g.id, Model: g.model, Revision: g.revision, Provider: g.provider.Name, Mode: r.mode, Exact: o.Exact, Hits: hits,
		EmbedMS: float64(embedDone.Sub(start).Microseconds()) / 1000, QueryMS: float64(time.Since(embedDone).Microseconds()) / 1000}, nil
}

func (s *Searcher) embed(ctx context.Context, g *genRow, query string) (string, error) {
	prov, err := s.Providers.For(g.provider.Name, g.provider.Kind)
	if err != nil {
		return "", err
	}
	var resp *embedclient.Response
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = prov.EmbedOnce(ctx, g.queryRequest(query))
		var tr *embedclient.TransientError
		if err == nil || !errors.As(err, &tr) || attempt == 2 {
			break
		}
		// A throttled query waits as asked, but never longer than a
		// search should take.
		wait := max(prov.Backoff(attempt), min(embedclient.RetryAfterOf(err), 5*time.Second))
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return "", ctx.Err()
		}
	}
	if err != nil {
		return "", fmt.Errorf("embed query: %w", err)
	}
	if len(resp.Embeddings) != 1 || len(resp.Embeddings[0]) != g.dim {
		return "", fmt.Errorf("query embedding has wrong dimension for generation %s", g.id)
	}
	return embedclient.FormatVector(resp.Embeddings[0]), nil
}

// where renders the request's restrictions over chunk alias c (TRUE if none).
func (r *request) where(args *[]any) string {
	var parts []string
	if len(r.pipelines) > 0 {
		*args = append(*args, r.pipelines)
		parts = append(parts, fmt.Sprintf("c.pipeline = ANY($%d::text[])", len(*args)))
	}
	if r.tenant != nil {
		parts = append(parts, r.tenant.SQL("c.meta", args))
	}
	if r.filter != nil {
		parts = append(parts, r.filter.SQL("c.meta", args))
	}
	if len(parts) == 0 {
		return "TRUE"
	}
	return strings.Join(parts, " AND ")
}

// SearchVector runs a search with a precomputed query vector (used by the migration gate).
func (s *Searcher) SearchVector(ctx context.Context, genID, vec string, k int, exact bool) ([]Hit, error) {
	g, err := s.loadGen(ctx, s.Pool, genID)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, nil, g, vec, &request{k: k}, k, k*4, exact)
}

// SearchVectorIn is SearchVector inside the caller's transaction (a savepoint
// scopes the planner settings), so several generations can be compared at one
// snapshot.
func (s *Searcher) SearchVectorIn(ctx context.Context, parent pgx.Tx, genID, vec string, k int, exact bool) ([]Hit, error) {
	g, err := s.loadGen(ctx, parent, genID)
	if err != nil {
		return nil, err
	}
	return s.query(ctx, parent, g, vec, &request{k: k}, k, k*4, exact)
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
func (q *QueryEncoder) Request(texts ...string) embedclient.Request {
	return q.g.queryRequest(texts...)
}

// Encoder returns how a generation's queries are embedded: its model,
// prompt, provider and request settings.
func (s *Searcher) Encoder(ctx context.Context, genID string) (*QueryEncoder, error) {
	g, err := s.loadGen(ctx, s.Pool, genID)
	if err != nil {
		return nil, err
	}
	prov, err := s.Providers.For(g.provider.Name, g.provider.Kind)
	if err != nil {
		return nil, err
	}
	return &QueryEncoder{Model: g.model, Revision: g.revision, Dim: g.dim, Provider: prov, g: g}, nil
}

func (s *Searcher) vectorBranch(ctx context.Context, tx pgx.Tx, g *genRow, vec string, r *request, k, candidates int) ([]Hit, error) {
	return s.query(ctx, tx, g, vec, r, k, candidates, false)
}

// query is the vector branch: the top documents by distance among the
// chunks matching the request's restrictions.
func (s *Searcher) query(ctx context.Context, parent pgx.Tx, g *genRow, vec string, r *request, k, candidates int, exact bool) ([]Hit, error) {
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
	} else {
		if g.efSearch > 0 && g.indexType != "ivfflat" {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL hnsw.ef_search = %d`, g.efSearch)); err != nil {
				return nil, err
			}
		}
		if r.restricted() || len(r.pipelines) > 0 {
			// A restricted ANN scan keeps walking the index until enough rows
			// pass the filter (pgvector 0.8+). Older pgvector lacks the
			// setting: the scan then returns what the first candidates hold.
			s.iterativeScan(ctx, tx, g)
		}
	}
	args := []any{vec, candidates}
	where := r.where(&args)
	// The inner ORDER BY distance LIMIT is what the ANN index serves; the
	// outer order breaks exact (and float-rounding) ties by key so results are
	// deterministic. Exact search also breaks ties inside the LIMIT.
	inner := `c.embedding ` + op + ` $1::vector`
	if exact {
		inner += `, c.pipeline, c.source_key, c.chunk_no`
	}
	rows, err := tx.Query(ctx, `SELECT pipeline, source_key, chunk_no, content, d FROM (SELECT c.pipeline, c.source_key, c.chunk_no, c.content, c.embedding `+op+` $1::vector AS d FROM `+
		pgx.Identifier{s.Schema, g.table}.Sanitize()+` c WHERE `+where+` ORDER BY `+inner+` LIMIT $2) c ORDER BY round(d::numeric, 6), pipeline, source_key, chunk_no`, args...)
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
		default:
			h.Score = -d
		}
		if len(hits) < k {
			hits = append(hits, h)
		}
	}
	return hits, rows.Err()
}

// iterativeScan enables pgvector's iterative index scan for a filtered
// query when the extension supports it (in a savepoint, so an unknown
// setting on an older pgvector is harmless).
func (s *Searcher) iterativeScan(ctx context.Context, tx pgx.Tx, g *genRow) {
	setting := "hnsw.iterative_scan"
	if g.indexType == "ivfflat" {
		setting = "ivfflat.iterative_scan"
	}
	sp, err := tx.Begin(ctx)
	if err != nil {
		return
	}
	if _, err := sp.Exec(ctx, `SET LOCAL `+setting+` = relaxed_order`); err != nil {
		_ = sp.Rollback(ctx)
		return
	}
	_ = sp.Commit(ctx)
}

// textBranch is the full-text branch: the top documents by ts_rank_cd of
// their best chunk among chunks matching the query and restrictions.
func (s *Searcher) textBranch(ctx context.Context, tx pgx.Tx, g *genRow, query string, r *request, k int) ([]Hit, error) {
	args := []any{s.TextConfig, query}
	where := r.where(&args)
	// Rank per chunk, then keep each document's best chunk (DISTINCT ON),
	// then order documents; every order ends with the key so ties are stable.
	rows, err := tx.Query(ctx, `SELECT pipeline, source_key, chunk_no, content, r FROM (
			SELECT DISTINCT ON (c.pipeline, c.source_key) c.pipeline, c.source_key, c.chunk_no, c.content, ts_rank_cd(c.tsv, q) AS r
			FROM `+pgx.Identifier{s.Schema, g.table}.Sanitize()+` c, websearch_to_tsquery($1::regconfig, $2) q
			WHERE c.tsv @@ q AND `+where+`
			ORDER BY c.pipeline, c.source_key, ts_rank_cd(c.tsv, q) DESC, c.chunk_no) d
		ORDER BY round(r::numeric, 6) DESC, pipeline, source_key LIMIT `+strconv.Itoa(k), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []Hit
	for rows.Next() {
		var h Hit
		var rank float32
		if err := rows.Scan(&h.Pipeline, &h.Key, &h.ChunkNo, &h.Content, &rank); err != nil {
			return nil, err
		}
		h.Score = float64(rank)
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

// Fuse combines ranked vector and text hits with reciprocal rank fusion:
// a document scores Σ 1/(rrfK + rank) over the branches that returned it.
// The chunk shown is the vector branch's when it has one. Ties sort by
// pipeline, then key.
func Fuse(vector, text []Hit, rrfK, k int) []Hit {
	type acc struct {
		h     Hit
		score float64
	}
	byDoc := map[string]*acc{}
	var order []string
	add := func(branch string, hits []Hit) {
		for i, h := range hits {
			id := h.Pipeline + "\x00" + h.Key
			a, ok := byDoc[id]
			if !ok {
				h.Ranks = map[string]int{}
				a = &acc{h: h}
				byDoc[id] = a
				order = append(order, id)
			}
			a.h.Ranks[branch] = i + 1
			a.score += 1 / float64(rrfK+i+1)
		}
	}
	add("vector", vector)
	add("text", text)
	out := make([]Hit, 0, len(order))
	for _, id := range order {
		a := byDoc[id]
		a.h.Score = a.score
		out = append(out, a.h)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].Pipeline != out[j].Pipeline {
			return out[i].Pipeline < out[j].Pipeline
		}
		return out[i].Key < out[j].Key
	})
	if len(out) > k {
		out = out[:k]
	}
	return out
}
