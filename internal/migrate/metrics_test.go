package migrate

import (
	"math"
	"testing"
)

func TestRecallNDCG(t *testing.T) {
	rel := map[string]float64{"a": 1, "b": 1}
	if r := RecallAt([]string{"x", "a", "y"}, rel, 10); r != 0.5 {
		t.Fatalf("recall %v", r)
	}
	if n := NDCGAt([]string{"a", "b"}, rel, 10); math.Abs(n-1) > 1e-12 {
		t.Fatalf("perfect ndcg %v", n)
	}
	// relevant at rank 2 only: (1/log2(3)) / (1/log2(2) + 1/log2(3))
	want := (1 / math.Log2(3)) / (1 + 1/math.Log2(3))
	if n := NDCGAt([]string{"x", "a"}, rel, 10); math.Abs(n-want) > 1e-12 {
		t.Fatalf("ndcg %v want %v", n, want)
	}
	if o := Overlap([]string{"a", "b", "c"}, []string{"a", "c", "d", "e"}); o != 0.5 {
		t.Fatalf("overlap %v", o)
	}
}

func TestPairedBootstrapDetectsShift(t *testing.T) {
	a := make([]float64, 200)
	b := make([]float64, 200)
	for i := range a {
		a[i] = float64(i%10) / 10
		b[i] = a[i] - 0.05
	}
	ci := PairedBootstrap(a, b, 2000, 0.95, 7)
	if math.Abs(ci.Mean+0.05) > 1e-9 || ci.Upper > -0.049 || ci.Lower < -0.051 {
		t.Fatalf("constant shift CI %+v", ci)
	}
	same := PairedBootstrap(a, a, 500, 0.95, 7)
	if same.Lower != 0 || same.Upper != 0 {
		t.Fatalf("identical systems must give zero CI, got %+v", same)
	}
	again := PairedBootstrap(a, b, 2000, 0.95, 7)
	if again != ci {
		t.Fatal("bootstrap must be deterministic for a seed")
	}
}

func TestTieAwareOverlap(t *testing.T) {
	// Exact top-2 is {a, b} with scores 0.9, 0.5; c ties b at the boundary.
	if o := TieAwareOverlap([]string{"a", "c"}, []float64{0.9, 0.5}, []string{"a", "b"}, []float64{0.9, 0.5}, 1e-6); o != 1 {
		t.Fatalf("a tie at the boundary is a correct answer, got %v", o)
	}
	if o := TieAwareOverlap([]string{"a", "d"}, []float64{0.9, 0.4}, []string{"a", "b"}, []float64{0.9, 0.5}, 1e-6); o != 0.5 {
		t.Fatalf("a worse result is a miss, got %v", o)
	}
}

func TestEvalDocID(t *testing.T) {
	// One pipeline: qrels keep using the bare source key (single-table format).
	if got := EvalDocID(1, "default", "42"); got != "42" {
		t.Fatalf("single pipeline id %q", got)
	}
	// Several pipelines: the same key in two tables are different documents.
	a, b := EvalDocID(2, "docs", "42"), EvalDocID(2, "notes", "42")
	if a == b || a != "docs/42" {
		t.Fatalf("multi-pipeline ids %q %q", a, b)
	}
}
