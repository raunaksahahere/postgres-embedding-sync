// Package transform turns allowlisted source columns into deterministic chunks.
//
// The functions here are pure: the same row, transform configuration and model
// revision always produce the same chunk IDs, texts and fingerprints. This is
// what lets the pipeline skip unchanged embedding work and lets the verifier
// recompute the expected sink from the source.
package transform

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"text/template"
	"unicode/utf8"
)

// Chunk is one embeddable unit of a document.
type Chunk struct {
	No          int
	ID          string
	Text        string
	Fingerprint string
}

// Transformer renders and chunks documents.
type Transformer struct {
	tmpl         *template.Template
	fieldNames   []string
	chunkWords   int
	chunkOverlap int
	table        string
	transformFP  string
}

// New builds a Transformer. fieldNames are the logical names available to the template.
func New(table, tmpl string, fieldNames []string, chunkWords, chunkOverlap int, transformFP string) (*Transformer, error) {
	t, err := template.New("doc").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	tr := &Transformer{tmpl: t, fieldNames: fieldNames, chunkWords: chunkWords, chunkOverlap: chunkOverlap, table: table, transformFP: transformFP}
	// Validate the template only references allowlisted fields.
	probe := map[string]string{}
	for _, f := range fieldNames {
		probe[f] = "x"
	}
	if _, err := tr.Render(probe); err != nil {
		return nil, fmt.Errorf("template references a field outside the allowlist: %w", err)
	}
	return tr, nil
}

// Render executes the template over allowlisted field values. Missing values
// (SQL NULL) must be passed as empty strings by the caller.
func (t *Transformer) Render(fields map[string]string) (string, error) {
	data := make(map[string]string, len(t.fieldNames))
	for _, f := range t.fieldNames {
		data[f] = fields[f]
	}
	var buf bytes.Buffer
	if err := t.tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	s := buf.String()
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("rendered document is not valid UTF-8")
	}
	return s, nil
}

// Split cuts text into overlapping word windows. Whitespace is normalised so
// formatting-only edits do not change chunk text.
func (t *Transformer) Split(text string) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	step := t.chunkWords - t.chunkOverlap
	var out []string
	for start := 0; ; start += step {
		end := start + t.chunkWords
		if end > len(words) {
			end = len(words)
		}
		out = append(out, strings.Join(words[start:end], " "))
		if end == len(words) {
			break
		}
	}
	return out
}

// ModelIdentity identifies the encoder in fingerprints.
type ModelIdentity struct {
	Name           string
	Revision       string
	DocumentPrefix string
}

// Chunks renders, splits and fingerprints a document for one model.
func (t *Transformer) Chunks(key string, text string, m ModelIdentity) []Chunk {
	parts := t.Split(text)
	out := make([]Chunk, len(parts))
	for i, p := range parts {
		out[i] = Chunk{No: i, ID: ChunkID(t.table, key, i), Text: p, Fingerprint: Fingerprint(t.transformFP, m, p)}
	}
	return out
}

// ChunkID is a stable identifier for (table, key, chunk number).
func ChunkID(table, key string, no int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d", table, key, no)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// Fingerprint hashes chunk text with the transform configuration and model
// revision. A change to any of them forces re-embedding; otherwise the stored
// vector is reused.
func Fingerprint(transformFP string, m ModelIdentity, text string) string {
	h := sha256.New()
	fmt.Fprintf(h, "v1\x00%s\x00%s\x00%s\x00%s\x00%s", transformFP, m.Name, m.Revision, m.DocumentPrefix, text)
	return hex.EncodeToString(h.Sum(nil)[:20])
}
