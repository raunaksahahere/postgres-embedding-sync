package setup

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
	"github.com/raunaksahahere/postgres-embedding-sync/internal/local"
)

// ListModels prints bundled and configured models.
func ListModels(out io.Writer, configPath string) error {
	man, err := local.LoadManifest()
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Bundled with this installation (%s):\n", man.Variant)
	if len(man.Models) == 0 {
		fmt.Fprintln(out, "  none: add any Hugging Face model with `embedsync model add <owner/name>`")
	}
	for _, m := range man.Models {
		fmt.Fprintf(out, "  %-10s %s (%d dimensions)\n", m.Key, m.Name, m.Dimension)
	}
	b, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintf(out, "\nNo config at %s yet: run `embedsync setup`.\n", configPath)
		return nil
	}
	cfg, err := config.Parse(b)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "\nIn %s (usable with `embedsync migrate --to <key>`):\n", configPath)
	keys := make([]string, 0, len(cfg.Models))
	for k := range cfg.Models {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m := cfg.Models[k]
		mark := ""
		if k == cfg.InitialGeneration {
			mark = "  (initial generation)"
		}
		fmt.Fprintf(out, "  %-10s %s@%.12s (%d dimensions)%s\n", k, m.Name, m.Revision, m.Dimension, mark)
	}
	return nil
}

// AddModel resolves a bundled key or Hugging Face id, downloads it if needed,
// and appends it to the config's models section (comments are preserved).
func AddModel(ctx context.Context, p *Prompter, configPath, spec, key, queryPrefix string) (string, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("read config: %w (run `embedsync setup` first)", err)
	}
	cfg, err := config.Parse(raw)
	if err != nil {
		return "", err
	}
	taken := map[string]bool{}
	for k, m := range cfg.Models {
		taken[k] = true
		if m.Name == spec || k == spec {
			return "", fmt.Errorf("model %s is already configured as %q", m.Name, k)
		}
	}
	var mc modelChoice
	man, err := local.LoadManifest()
	if err != nil {
		return "", err
	}
	found := false
	for _, b := range man.Models {
		if spec == b.Key || spec == b.Name {
			mc = modelChoice{b.Key, b.Name, b.Revision, b.Dimension, b.QueryPrefix}
			found = true
		}
	}
	if !found {
		if mc, err = ResolveModel(ctx, p, spec, queryPrefix, taken); err != nil {
			return "", err
		}
	}
	if key != "" {
		mc.Key = key
	} else if taken[mc.Key] {
		mc.Key = ModelKey(mc.Name, taken)
	}
	if taken[mc.Key] {
		return "", fmt.Errorf("model key %q is already used", mc.Key)
	}
	for _, m := range cfg.Models {
		if m.Name == mc.Name && m.Revision == mc.Revision {
			return "", fmt.Errorf("%s@%s is already configured", mc.Name, mc.Revision)
		}
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return "", fmt.Errorf("%s: unexpected config layout", configPath)
	}
	root := doc.Content[0]
	var models *yaml.Node
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "models" {
			models = root.Content[i+1]
		}
	}
	if models == nil || models.Kind != yaml.MappingNode {
		return "", fmt.Errorf("%s: no models section", configPath)
	}
	str := func(v string) *yaml.Node { return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v} }
	entry := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		str("name"), str(mc.Name),
		str("revision"), str(mc.Revision),
		str("dimension"), {Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(mc.Dimension)},
		str("distance"), str("cosine"),
	}}
	if mc.QueryPrefix != "" {
		entry.Content = append(entry.Content, str("query_prefix"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str",
			Value: mc.QueryPrefix, Style: yaml.DoubleQuotedStyle})
	}
	models.Content = append(models.Content, str(mc.Key), entry)
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return "", err
	}
	if _, err := config.Parse(out); err != nil {
		return "", fmt.Errorf("internal error, updated config is invalid: %w", err)
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, configPath); err != nil {
		return "", err
	}
	return mc.Key, nil
}
