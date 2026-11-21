package pipeline

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/embedclient"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/metrics"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/transform"
)

// vectorCache is a small LRU from fingerprint to pgvector text.
type vectorCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

type cacheItem struct{ fp, vec string }

func newVectorCache(capacity int) *vectorCache {
	return &vectorCache{cap: capacity, ll: list.New(), items: map[string]*list.Element{}}
}

func (c *vectorCache) get(fp string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[fp]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*cacheItem).vec, true
	}
	return "", false
}

func (c *vectorCache) put(fp, vec string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[fp]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.items[fp] = c.ll.PushFront(&cacheItem{fp, vec})
	for c.ll.Len() > c.cap {
		e := c.ll.Back()
		c.ll.Remove(e)
		delete(c.items, e.Value.(*cacheItem).fp)
	}
}

// EmbedStage computes vectors for every chunk a unit needs. Units are
// forwarded immediately so several can embed concurrently; the committer
// waits for each unit's ready channel in order.
type EmbedStage struct {
	Providers *embedclient.Set
	Store     *store.Store
	Specs     Specs
	BatchSize int
	Log       *slog.Logger

	cache *vectorCache
}

// NewEmbedStage embeds through each generation's provider; the providers
// bound their own concurrency and batch sizes.
func NewEmbedStage(ps *embedclient.Set, st *store.Store, specs Specs, batch int, log *slog.Logger) *EmbedStage {
	return &EmbedStage{Providers: ps, Store: st, Specs: specs, BatchSize: batch, Log: log, cache: newVectorCache(4096)}
}

// Run forwards units from in to out after dispatching their embedding work.
func (e *EmbedStage) Run(ctx context.Context, in <-chan *Unit, out chan<- *Unit) error {
	defer close(out)
	for u := range in {
		e.dispatch(ctx, u)
		metrics.QueueDepth.WithLabelValues("commit").Set(float64(len(out)))
		select {
		case out <- u:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type job struct {
	gen    *GenInfo
	prov   embedclient.Provider
	fps    []string
	inputs []string
}

func (e *EmbedStage) dispatch(ctx context.Context, u *Unit) {
	if u.Pause != nil {
		close(u.ready)
		return
	}
	var jobs []job
	for genID, gi := range u.TargetGens() {
		eff := u.Effective(genID)
		chunks := map[DocID][]transform.Chunk{}
		need := map[string]string{} // fingerprint -> text
		for id, d := range eff {
			if d.Delete || d.Poison != "" {
				continue
			}
			cs := e.Specs[id.Pipeline].Tr.Chunks(id.Key, d.Text, gi.Model)
			chunks[id] = cs
			for _, c := range cs {
				need[c.Fingerprint] = c.Text
			}
		}
		u.Chunks[genID] = chunks
		vecs := map[string]string{}
		u.Vectors[genID] = vecs
		u.Rejected[genID] = map[string]string{}
		missing := []string{}
		for fp := range need {
			if v, ok := e.cache.get(fp); ok {
				vecs[fp] = v
				metrics.EmbeddingsSkipped.WithLabelValues(genID, "cache").Inc()
				continue
			}
			missing = append(missing, fp)
		}
		if len(missing) > 0 {
			found, err := e.lookup(ctx, gi.Gen, missing)
			if err != nil {
				e.Log.Warn("fingerprint lookup failed; embedding instead", "generation", genID, "err", err)
			}
			rest := missing[:0:0]
			for _, fp := range missing {
				if v, ok := found[fp]; ok {
					vecs[fp] = v
					e.cache.put(fp, v)
					metrics.EmbeddingsSkipped.WithLabelValues(genID, "sink").Inc()
				} else {
					rest = append(rest, fp)
				}
			}
			missing = rest
		}
		prov, err := e.Providers.For(gi.Gen.Provider.Name, gi.Gen.Provider.Kind)
		if err != nil {
			u.err = err
			close(u.ready)
			return
		}
		batch := max(1, min(e.BatchSize, prov.MaxBatch()))
		for i := 0; i < len(missing); i += batch {
			end := min(i+batch, len(missing))
			j := job{gen: gi, prov: prov}
			for _, fp := range missing[i:end] {
				j.fps = append(j.fps, fp)
				j.inputs = append(j.inputs, gi.Model.DocumentPrefix+need[fp])
			}
			jobs = append(jobs, j)
		}
	}
	if len(jobs) == 0 {
		close(u.ready)
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			release, err := e.Providers.Slot(ctx, j.prov.Name())
			if err != nil {
				mu.Lock()
				u.err = err
				mu.Unlock()
				return
			}
			defer release()
			g := j.gen.Gen
			vecs, rejected, err := j.prov.Embed(ctx, embedclient.Request{
				Model: g.ModelName, Revision: g.ModelRevision, Kind: embedclient.Document, Inputs: j.inputs,
				Dimensions: g.Provider.RequestDimensions, Expect: g.Dimension, InputType: g.Provider.DocumentInputType})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if u.err == nil {
					u.err = err
				}
				return
			}
			gid := j.gen.Gen.ID
			for i, fp := range j.fps {
				if v, ok := vecs[i]; ok {
					if len(v) != j.gen.Gen.Dimension {
						if u.err == nil {
							u.err = errors.New("embedding dimension does not match generation")
						}
						return
					}
					s := embedclient.FormatVector(v)
					u.Vectors[gid][fp] = s
					e.cache.put(fp, s)
				} else if msg, ok := rejected[i]; ok {
					u.Rejected[gid][fp] = msg
				}
			}
			metrics.Embeddings.WithLabelValues(gid).Add(float64(len(vecs)))
		}(j)
	}
	go func() {
		wg.Wait()
		close(u.ready)
	}()
}

// lookup reuses vectors already stored for identical fingerprints, which covers
// unchanged chunks of an updated row and replays after a crash.
func (e *EmbedStage) lookup(ctx context.Context, g *store.Generation, fps []string) (map[string]string, error) {
	rows, err := e.Store.Pool.Query(ctx, `SELECT DISTINCT ON (fingerprint) fingerprint, embedding::text FROM `+
		pgx.Identifier{e.Store.Schema, g.ChunkTable}.Sanitize()+` WHERE fingerprint = ANY($1)`, fps)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var fp, v string
		if err := rows.Scan(&fp, &v); err != nil {
			return nil, err
		}
		out[fp] = v
	}
	return out, rows.Err()
}
