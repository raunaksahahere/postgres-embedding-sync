package app

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/filter"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/search"
)

// NewSearcher builds the searcher for the configuration: declared metadata,
// text search configuration, fusion constant, searchable pipelines and the
// tenant rule.
func NewSearcher(e *Env, pool *pgxpool.Pool) *search.Searcher {
	meta := filter.Schema{}
	for name, t := range e.Cfg.MetaSchema() {
		meta[name] = filter.Field{Name: name, Type: filter.Type(t)}
	}
	docs := map[string]bool{}
	for _, n := range e.Cfg.DocNames() {
		docs[n] = true
	}
	s := &search.Searcher{Pool: pool, Schema: e.Cfg.Sink.Schema, Providers: e.Providers, Meta: meta,
		TextConfig: e.Cfg.Search.TextConfig, RRFK: e.Cfg.Search.RRFK, SearchFingerprint: e.Cfg.SearchFingerprint(), Docs: docs}
	if tf := e.Cfg.Search.TenantFilter; tf != nil {
		s.TenantField, s.TenantHeader = tf.Field, tf.Header
	}
	return s
}

// searchBody is the POST /search request.
type searchBody struct {
	Query      string          `json:"query"`
	K          int             `json:"k"`
	Mode       search.Mode     `json:"mode"`
	Pipelines  []string        `json:"pipelines"`
	Generation string          `json:"generation"`
	Exact      bool            `json:"exact"`
	Filter     json.RawMessage `json:"filter"`
	RRFK       int             `json:"rrf_k"`
}

const maxSearchBody = 1 << 20

// serveSearch answers GET /search (q, k, generation, exact as before, plus
// mode, pipeline, filter, rrf_k) and POST /search (a JSON body).
func serveSearch(w http.ResponseWriter, r *http.Request, s *search.Searcher, tf *config.TenantFilter) {
	var body searchBody
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		q := r.URL.Query()
		body.Query = q.Get("q")
		// Legacy GET: an out-of-range k falls back to 10, as before.
		if k, _ := strconv.Atoi(q.Get("k")); k > 0 && k <= search.MaxK {
			body.K = k
		}
		body.Generation = q.Get("generation")
		body.Exact = q.Get("exact") == "true"
		body.Mode = search.Mode(q.Get("mode"))
		if p := q.Get("pipeline"); p != "" {
			body.Pipelines = strings.Split(p, ",")
		}
		if f := q.Get("filter"); f != "" {
			body.Filter = json.RawMessage(f)
		}
		if v := q.Get("rrf_k"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				writeSearchError(w, &search.Error{Status: http.StatusBadRequest, Code: "invalid_request", Field: "rrf_k", Message: "rrf_k must be an integer"})
				return
			}
			body.RRFK = n
		}
	case http.MethodPost:
		dec := json.NewDecoder(io.LimitReader(r.Body, maxSearchBody))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			field := ""
			var te *json.UnmarshalTypeError
			if errors.As(err, &te) {
				field = te.Field
			}
			writeSearchError(w, &search.Error{Status: http.StatusBadRequest, Code: "invalid_request", Field: field, Message: "invalid request body: " + err.Error()})
			return
		}
		if body.K == 0 {
			body.K = 10
		}
	default:
		w.Header().Set("Allow", "GET, POST")
		writeSearchError(w, &search.Error{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Message: "use GET or POST"})
		return
	}
	o := search.Options{K: body.K, Generation: body.Generation, Exact: body.Exact, Mode: body.Mode, Pipelines: body.Pipelines, Filter: body.Filter, RRFK: body.RRFK}
	if tf != nil {
		if vals, ok := r.Header[http.CanonicalHeaderKey(tf.Header)]; ok && len(vals) > 0 {
			v := vals[0]
			o.Tenant = &v
		}
	}
	res, err := s.Search(r.Context(), body.Query, o)
	if err != nil {
		writeSearchError(w, search.AsError(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func writeSearchError(w http.ResponseWriter, e *search.Error) {
	w.Header().Set("Content-Type", "application/json")
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
	}
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(e)
}
