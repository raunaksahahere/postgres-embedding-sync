package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EnvFilePath is the credentials file that sits next to a config file.
func EnvFilePath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), "embedsync.env")
}

// LoadEnvFile sets KEY=VALUE pairs from path for variables that are not
// already set, so the real environment always wins. A missing file is fine.
// Blank lines and lines starting with # are ignored; values are taken
// literally (no quotes or expansion).
func LoadEnvFile(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, n)
		}
		if _, set := os.LookupEnv(k); !set {
			if err := os.Setenv(k, v); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// WriteEnvFile writes KEY=VALUE pairs readable only by the current user.
func WriteEnvFile(path string, header string, kv [][2]string) error {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimRight(header, "\n"), "\n") {
		b.WriteString("# " + l + "\n")
	}
	for _, p := range kv {
		b.WriteString(p[0] + "=" + p[1] + "\n")
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
