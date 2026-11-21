package setup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// hfAPI is the Hugging Face Hub API base (overridable for tests).
var hfAPI = "https://huggingface.co"

var hfIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ResolveHF pins a Hugging Face model id (optionally "id@revision") to a commit SHA.
func ResolveHF(ctx context.Context, spec string) (name, sha string, err error) {
	name, rev, _ := strings.Cut(strings.TrimSpace(spec), "@")
	if !hfIDRe.MatchString(name) {
		return "", "", fmt.Errorf("%q is not a Hugging Face model id (expected owner/name, e.g. sentence-transformers/all-MiniLM-L6-v2)", name)
	}
	if rev == "" {
		rev = "main"
	}
	u := fmt.Sprintf("%s/api/models/%s/revision/%s", hfAPI, name, url.PathEscape(rev))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", "", err
	}
	if tok := os.Getenv("HF_TOKEN"); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("contact Hugging Face: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return "", "", fmt.Errorf("model %s (revision %s) not found on Hugging Face; if it is private or gated, set HF_TOKEN", name, rev)
	default:
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", "", fmt.Errorf("hugging face API: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var body struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || len(body.SHA) < 7 {
		return "", "", fmt.Errorf("hugging face API returned no commit for %s", name)
	}
	return name, body.SHA, nil
}

var nonKey = regexp.MustCompile(`[^a-z0-9]+`)

// ModelKey derives a config/generation key (^[a-z][a-z0-9_]{0,30}$) from a model id.
func ModelKey(name string, taken map[string]bool) string {
	base := name
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	k := strings.Trim(nonKey.ReplaceAllString(strings.ToLower(base), "_"), "_")
	if k == "" || k[0] < 'a' || k[0] > 'z' {
		k = "m_" + k
	}
	if len(k) > 28 {
		k = strings.TrimRight(k[:28], "_")
	}
	out := k
	for i := 2; taken[out]; i++ {
		out = fmt.Sprintf("%s_%d", k, i)
	}
	return out
}
