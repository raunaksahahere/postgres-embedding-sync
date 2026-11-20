package transform

import (
	"strings"
	"testing"
)

func tr(t *testing.T, words, overlap int, fp string) *Transformer {
	t.Helper()
	x, err := New("public.documents", "{{.title}}\n\n{{.body}}", []string{"title", "body"}, words, overlap, fp)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestSplitOverlapAndCoverage(t *testing.T) {
	x := tr(t, 10, 3, "fp")
	words := make([]string, 25)
	for i := range words {
		words[i] = string(rune('a' + i))
	}
	parts := x.Split(strings.Join(words, "  \n "))
	if len(parts) != 4 { // starts 0,7,14,21
		t.Fatalf("got %d chunks: %q", len(parts), parts)
	}
	if !strings.HasPrefix(parts[1], "h i j") || !strings.HasSuffix(parts[3], "y") {
		t.Fatalf("unexpected windows %q", parts)
	}
	if x.Split("   ") != nil {
		t.Fatal("blank text must produce no chunks")
	}
}

func TestDeterministicIDsAndFingerprints(t *testing.T) {
	m := ModelIdentity{Name: "m", Revision: "r1"}
	a := tr(t, 8, 2, "fp1").Chunks("42", "one two three four five six seven eight nine ten", m)
	b := tr(t, 8, 2, "fp1").Chunks("42", "one two  three four five six seven eight nine ten", m)
	if len(a) != len(b) || a[0].ID != b[0].ID || a[0].Fingerprint != b[0].Fingerprint {
		t.Fatal("whitespace-only change must not change chunks")
	}
	c := tr(t, 8, 2, "fp1").Chunks("42", "one two three four five six seven eight nine ten", ModelIdentity{Name: "m", Revision: "r2"})
	if c[0].Fingerprint == a[0].Fingerprint || c[0].ID != a[0].ID {
		t.Fatal("model revision must change fingerprint but not chunk id")
	}
	d := tr(t, 8, 2, "fp2").Chunks("42", "one two three four five six seven eight nine ten", m)
	if d[0].Fingerprint == a[0].Fingerprint {
		t.Fatal("transform config must change fingerprint")
	}
	e := tr(t, 8, 2, "fp1").Chunks("42", "one two three four five six seven eight nine ten", ModelIdentity{Name: "m", Revision: "r1", DocumentPrefix: "passage: "})
	if e[0].Fingerprint == a[0].Fingerprint {
		t.Fatal("document prompt must change fingerprint")
	}
	if ChunkID("public.documents", "42", 0) == ChunkID("public.documents", "43", 0) {
		t.Fatal("chunk ids must differ by key")
	}
}

func TestTemplateAllowlist(t *testing.T) {
	if _, err := New("t", "{{.secret}}", []string{"title"}, 10, 1, "x"); err == nil {
		t.Fatal("template referencing a non-allowlisted field must fail")
	}
	x := tr(t, 10, 1, "x")
	s, err := x.Render(map[string]string{"title": "T", "body": "B", "secret": "nope"})
	if err != nil || s != "T\n\nB" {
		t.Fatalf("render = %q, %v", s, err)
	}
}
