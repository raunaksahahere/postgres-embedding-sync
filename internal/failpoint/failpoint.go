// Package failpoint lets crash tests kill the process at precise points.
//
// EMBEDSYNC_FAILPOINTS="before_sink_commit=3,after_commit_before_feedback=1"
// exits the process with status 86 on the Nth hit of a named point. It is a
// no-op when the variable is unset, which is the production default.
//
// EMBEDSYNC_PAUSEPOINTS="search_after_embed=3s" makes Pause sleep at a named
// point (tests use it to change state in the middle of a request).
package failpoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ExitCode is the status used when a failpoint fires.
const ExitCode = 86

var (
	once    sync.Once
	mu      sync.Mutex
	targets map[string]int
	hits    map[string]int
	pauses  map[string]time.Duration
)

func load() {
	targets = map[string]int{}
	hits = map[string]int{}
	pauses = map[string]time.Duration{}
	for _, part := range strings.Split(os.Getenv("EMBEDSYNC_PAUSEPOINTS"), ",") {
		name, d, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		if v, err := time.ParseDuration(d); err == nil && v > 0 {
			pauses[name] = v
		}
	}
	for _, part := range strings.Split(os.Getenv("EMBEDSYNC_FAILPOINTS"), ",") {
		name, n, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			targets[name] = v
		}
	}
}

// Hit counts a pass through the named point and exits if it is the configured hit.
func Hit(name string) {
	once.Do(load)
	if len(targets) == 0 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	hits[name]++
	if want, ok := targets[name]; ok && hits[name] == want {
		fmt.Fprintf(os.Stderr, "failpoint %s fired on hit %d\n", name, want)
		os.Exit(ExitCode)
	}
}

// Pause sleeps at the named point for the configured duration (none by default).
func Pause(name string) {
	once.Do(load)
	if d, ok := pauses[name]; ok {
		fmt.Fprintf(os.Stderr, "pausepoint %s: sleeping %s\n", name, d)
		time.Sleep(d)
	}
}
