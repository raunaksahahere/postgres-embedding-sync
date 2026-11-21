// Package local supports installed (packaged) embedsync: where the installer put
// the bundled embedder and models, the per-user config and cache directories,
// and starting the bundled embedder as a child process.
//
// Installed layout, relative to the directory holding the embedsync binary:
//
//	embedsync[.exe]
//	embedder/embedsync-embedder[.exe]   frozen Python embedding service
//	models/manifest.json                models shipped with this installer variant
//	models/<org>__<name>/<revision>/    one sentence-transformers model directory
package local

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// BundledModel is one model shipped with the installer.
type BundledModel struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Revision    string `json:"revision"`
	Dimension   int    `json:"dimension"`
	QueryPrefix string `json:"query_prefix"`
}

// Manifest describes the installer variant and its models.
type Manifest struct {
	Variant string         `json:"variant"`
	Models  []BundledModel `json:"models"`
}

// InstallDir is the directory of the running embedsync binary, symlinks resolved.
func InstallDir() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}
	return filepath.Dir(exe), nil
}

// EmbedderBinary locates the bundled embedder ($EMBEDSYNC_EMBEDDER_BIN overrides).
func EmbedderBinary() (string, error) {
	if p := os.Getenv("EMBEDSYNC_EMBEDDER_BIN"); p != "" {
		return p, nil
	}
	dir, err := InstallDir()
	if err != nil {
		return "", err
	}
	name := "embedsync-embedder"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	p := filepath.Join(dir, "embedder", name)
	if _, err := os.Stat(p); err != nil {
		return "", fmt.Errorf("bundled embedder not found at %s: this embedsync was not installed from an "+
			"installer package. Install a package from the GitHub releases page, set EMBEDSYNC_EMBEDDER_BIN, "+
			"or set embedder.url to a running embedder", p)
	}
	return p, nil
}

// ModelsDir is where bundled models live ($EMBEDSYNC_MODELS_DIR overrides); it may not exist.
func ModelsDir() string {
	if p := os.Getenv("EMBEDSYNC_MODELS_DIR"); p != "" {
		return p
	}
	dir, err := InstallDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "models")
}

// BundledModelDir is where an installer puts one model (same rule as the embedder).
func BundledModelDir(name, revision string) string {
	return filepath.Join(ModelsDir(), strings.ReplaceAll(name, "/", "__"), revision)
}

// LoadManifest reads the bundled model manifest. A missing manifest means no
// bundled models (an empty manifest is returned).
func LoadManifest() (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(ModelsDir(), "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &Manifest{Variant: "none"}, nil
	}
	if err != nil {
		return nil, err
	}
	m := &Manifest{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("models/manifest.json: %w", err)
	}
	return m, nil
}

// ConfigDir is the per-user configuration directory (created if missing):
// ~/.config/embedsync, ~/Library/Application Support/embedsync, %AppData%\embedsync.
func ConfigDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	d := filepath.Join(base, "embedsync")
	return d, os.MkdirAll(d, 0o700)
}

// CacheDir is the per-user cache directory for downloaded models and embedder logs.
func CacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	d := filepath.Join(base, "embedsync")
	return d, os.MkdirAll(d, 0o700)
}

// DefaultConfigPath is the config file written by `embedsync setup`.
func DefaultConfigPath() string {
	base, err := os.UserConfigDir()
	if err != nil {
		return "embedsync.yaml"
	}
	return filepath.Join(base, "embedsync", "embedsync.yaml")
}
