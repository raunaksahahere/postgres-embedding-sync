package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ModelRef names one model the embedder must serve.
type ModelRef struct {
	Name     string `json:"name"`
	Revision string `json:"revision"`
}

func (m ModelRef) String() string { return m.Name + "@" + m.Revision }

// Options control starting the bundled embedder.
type Options struct {
	Models []ModelRef
	// Output receives the embedder's own log (model downloads, errors). Nil
	// writes it to <cache>/embedder.log.
	Output io.Writer
	// Progress, when set, is called periodically while waiting for readiness.
	Progress func(msg string)
	// ReadyTimeout bounds waiting for the models to download and load (default 30m).
	ReadyTimeout time.Duration
}

// Embedder is a running embedding service: either a child process we started
// or an instance owned by another embedsync process (shared through a state
// file, so `embedsync search` reuses the one `embedsync run` started).
type Embedder struct {
	URL     string
	LogPath string

	cmd       *exec.Cmd
	exited    chan struct{}
	exitErr   error
	logFile   *os.File
	stateFile string
	stopOnce  sync.Once
}

type state struct {
	URL    string     `json:"url"`
	PID    int        `json:"pid"`
	Models []ModelRef `json:"models"`
}

func stateFile() string {
	d, err := CacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(d, "embedder.json")
}

// Ensure returns a running embedder serving every requested model, reusing one
// published by another embedsync process when possible.
func Ensure(ctx context.Context, opts Options) (*Embedder, error) {
	if sf := stateFile(); sf != "" {
		if b, err := os.ReadFile(sf); err == nil {
			var st state
			if json.Unmarshal(b, &st) == nil && st.URL != "" {
				if served, err := health(ctx, st.URL); err == nil && covers(served, opts.Models) {
					return &Embedder{URL: st.URL}, nil
				}
			}
		}
	}
	return Start(ctx, opts)
}

func covers(served []string, want []ModelRef) bool {
	have := map[string]bool{}
	for _, s := range served {
		have[s] = true
	}
	for _, m := range want {
		if !have[m.String()] {
			return false
		}
	}
	return true
}

// Start launches the bundled embedder on a free localhost port and waits until
// all models are loaded (downloading any that are not bundled or cached).
func Start(ctx context.Context, opts Options) (*Embedder, error) {
	bin, err := EmbedderBinary()
	if err != nil {
		return nil, err
	}
	if len(opts.Models) == 0 {
		return nil, errors.New("no models to serve: add one with `embedsync model add` or run `embedsync setup`")
	}
	if opts.ReadyTimeout == 0 {
		opts.ReadyTimeout = 30 * time.Minute
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	cache, err := CacheDir()
	if err != nil {
		return nil, err
	}
	refs := make([]string, len(opts.Models))
	for i, m := range opts.Models {
		refs[i] = m.String()
	}
	env := append(os.Environ(),
		"EMBEDDER_HOST=127.0.0.1",
		"EMBEDDER_PORT="+strconv.Itoa(port),
		"EMBEDDER_MODELS="+strings.Join(refs, ","),
		"EMBEDDER_BUNDLED_MODELS_DIR="+ModelsDir(),
		"EMBEDDER_PARENT_PID="+strconv.Itoa(os.Getpid()),
		"PYTHONUNBUFFERED=1",
	)
	if os.Getenv("HF_HOME") == "" {
		env = append(env, "HF_HOME="+filepath.Join(cache, "hf"))
	}
	e := &Embedder{URL: "http://127.0.0.1:" + strconv.Itoa(port), exited: make(chan struct{})}
	out := opts.Output
	if out == nil {
		e.LogPath = filepath.Join(cache, "embedder.log")
		f, err := os.Create(e.LogPath)
		if err != nil {
			return nil, err
		}
		e.logFile = f
		out = f
	}
	cmd := exec.Command(bin)
	cmd.Env = env
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Dir = cache
	setProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		e.closeLog()
		return nil, fmt.Errorf("start embedder %s: %w", bin, err)
	}
	e.cmd = cmd
	go func() {
		e.exitErr = cmd.Wait()
		close(e.exited)
	}()
	if err := e.waitReady(ctx, opts); err != nil {
		e.Stop()
		return nil, err
	}
	return e, nil
}

func (e *Embedder) waitReady(ctx context.Context, opts Options) error {
	deadline := time.Now().Add(opts.ReadyTimeout)
	started := time.Now()
	lastNote := time.Now()
	for {
		hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := health(hctx, e.URL)
		cancel()
		if err == nil {
			return nil
		}
		var fatal fatalErr
		if errors.As(err, &fatal) {
			return fmt.Errorf("embedder failed to load models: %s%s", fatal.msg, e.logHint())
		}
		select {
		case <-e.exited:
			return fmt.Errorf("embedder exited during startup (%v)%s", e.exitErr, e.logHint())
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("embedder not ready after %s%s", opts.ReadyTimeout, e.logHint())
		}
		if opts.Progress != nil && time.Since(lastNote) > 15*time.Second {
			lastNote = time.Now()
			opts.Progress(fmt.Sprintf("still loading models (%s); first use downloads models that are not bundled",
				time.Since(started).Round(time.Second)))
		}
	}
}

func (e *Embedder) logHint() string {
	if e.LogPath == "" {
		return ""
	}
	b, err := os.ReadFile(e.LogPath)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > 15 {
		lines = lines[len(lines)-15:]
	}
	return fmt.Sprintf("\nembedder log (%s):\n  %s", e.LogPath, strings.Join(lines, "\n  "))
}

// Publish records this embedder so other embedsync commands can reuse it.
func (e *Embedder) Publish(models []ModelRef) error {
	if e.cmd == nil {
		return nil
	}
	sf := stateFile()
	if sf == "" {
		return nil
	}
	b, _ := json.Marshal(state{URL: e.URL, PID: e.cmd.Process.Pid, Models: models})
	if err := os.WriteFile(sf, b, 0o600); err != nil {
		return err
	}
	e.stateFile = sf
	return nil
}

// Exited is closed when a child embedder process ends (nil channel when shared).
func (e *Embedder) Exited() <-chan struct{} { return e.exited }

// Stop terminates a child embedder; a shared one is left running.
func (e *Embedder) Stop() {
	e.stopOnce.Do(func() {
		if e.stateFile != "" {
			_ = os.Remove(e.stateFile)
		}
		if e.cmd != nil && e.cmd.Process != nil {
			select {
			case <-e.exited:
			default:
				terminate(e.cmd.Process)
				select {
				case <-e.exited:
				case <-time.After(10 * time.Second):
					_ = e.cmd.Process.Kill()
					<-e.exited
				}
			}
		}
		e.closeLog()
	})
}

func (e *Embedder) closeLog() {
	if e.logFile != nil {
		_ = e.logFile.Close()
		e.logFile = nil
	}
}

type fatalErr struct{ msg string }

func (f fatalErr) Error() string { return f.msg }

// health returns the served models, a fatalErr when model loading failed, or
// another error while still starting.
func health(ctx context.Context, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/healthz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Models []string `json:"models"`
		Detail string   `json:"detail"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body)
	switch resp.StatusCode {
	case http.StatusOK:
		return body.Models, nil
	case http.StatusInternalServerError:
		return nil, fatalErr{body.Detail}
	default:
		return nil, fmt.Errorf("embedder status %d", resp.StatusCode)
	}
}

// Dimensions asks a running embedder for each served model's vector dimension.
func Dimensions(ctx context.Context, url string) (map[string]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Models []struct {
			Model     string `json:"model"`
			Revision  string `json:"revision"`
			Dimension int    `json:"dimension"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, m := range body.Models {
		out[m.Model+"@"+m.Revision] = m.Dimension
	}
	return out, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func published(ctx context.Context) (*state, []string) {
	sf := stateFile()
	if sf == "" {
		return nil, nil
	}
	b, err := os.ReadFile(sf)
	if err != nil {
		return nil, nil
	}
	var st state
	if json.Unmarshal(b, &st) != nil || st.URL == "" {
		return nil, nil
	}
	served, err := health(ctx, st.URL)
	if err != nil {
		return nil, nil
	}
	return &st, served
}

// RunPublished reports whether an `embedsync run` embedder is up and published.
func RunPublished() bool {
	st, _ := published(context.Background())
	return st != nil
}

// RunCovers reports whether the published embedder serves every model.
func RunCovers(models []ModelRef) bool {
	_, served := published(context.Background())
	return covers(served, models)
}
