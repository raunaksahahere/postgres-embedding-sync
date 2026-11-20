package migrate

import (
	"math"
	"math/rand/v2"
	"sort"
)

// RecallAt returns |relevant ∩ top-k| / |relevant| (0 when nothing is relevant).
func RecallAt(ranked []string, rel map[string]float64, k int) float64 {
	n := 0
	for _, g := range rel {
		if g > 0 {
			n++
		}
	}
	if n == 0 {
		return 0
	}
	hit := 0
	for i, d := range ranked {
		if i >= k {
			break
		}
		if rel[d] > 0 {
			hit++
		}
	}
	return float64(hit) / float64(n)
}

// NDCGAt returns nDCG@k with linear gains (BEIR/pytrec_eval convention).
func NDCGAt(ranked []string, rel map[string]float64, k int) float64 {
	dcg := 0.0
	for i, d := range ranked {
		if i >= k {
			break
		}
		dcg += rel[d] / math.Log2(float64(i)+2)
	}
	gains := make([]float64, 0, len(rel))
	for _, g := range rel {
		if g > 0 {
			gains = append(gains, g)
		}
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(gains)))
	idcg := 0.0
	for i, g := range gains {
		if i >= k {
			break
		}
		idcg += g / math.Log2(float64(i)+2)
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

// Overlap returns |a ∩ b| / |b| for top-k lists (ANN recall against exact search).
func Overlap(approx, exact []string) float64 {
	if len(exact) == 0 {
		return 1
	}
	set := map[string]bool{}
	for _, d := range approx {
		set[d] = true
	}
	hit := 0
	for _, d := range exact {
		if set[d] {
			hit++
		}
	}
	return float64(hit) / float64(len(exact))
}

// TieAwareOverlap is ANN recall against exact search when scores can tie: an
// approximate result counts if it is in the exact top-k or scores at least as
// well as the exact k-th result (within eps), since any member of a tie at the
// boundary is an equally correct exact answer. Scores are higher-is-better.
func TieAwareOverlap(approxKeys []string, approxScores []float64, exactKeys []string, exactScores []float64, eps float64) float64 {
	if len(exactKeys) == 0 {
		return 1
	}
	set := map[string]bool{}
	for _, d := range exactKeys {
		set[d] = true
	}
	kth := exactScores[len(exactScores)-1]
	hit := 0
	for i, d := range approxKeys {
		if set[d] || approxScores[i] >= kth-eps {
			hit++
		}
	}
	return math.Min(1, float64(hit)/float64(len(exactKeys)))
}

// Mean of a slice.
func Mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// CI is a paired bootstrap confidence interval for mean(b - a).
type CI struct {
	Mean  float64 `json:"mean"`
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

// PairedBootstrap resamples queries with replacement and returns the
// percentile interval of mean(b[i] - a[i]).
func PairedBootstrap(a, b []float64, samples int, level float64, seed int64) CI {
	n := len(a)
	if n == 0 || n != len(b) {
		return CI{}
	}
	d := make([]float64, n)
	for i := range a {
		d[i] = b[i] - a[i]
	}
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	means := make([]float64, samples)
	for s := 0; s < samples; s++ {
		sum := 0.0
		for i := 0; i < n; i++ {
			sum += d[rng.IntN(n)]
		}
		means[s] = sum / float64(n)
	}
	sort.Float64s(means)
	alpha := (1 - level) / 2
	lo := means[int(math.Floor(alpha*float64(samples-1)))]
	hi := means[int(math.Ceil((1-alpha)*float64(samples-1)))]
	return CI{Mean: Mean(d), Lower: lo, Upper: hi}
}
