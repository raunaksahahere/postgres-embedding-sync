// Package failpoint lets crash tests kill the process at precise points.
//
// EMBEDSYNC_FAILPOINTS="before_sink_commit=3,after_commit_before_feedback=1"
// exits the process with status 86 on the Nth hit of a named point. It is a
// no-op when the variable is unset, which is the production default.
package failpoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// ExitCode is the status used when a failpoint fires.
const ExitCode = 86

var (
	once    sync.Once
	mu      sync.Mutex
	targets map[string]int
	hits    map[string]int
)

func load() {
	targets = map[string]int{}
	hits = map[string]int{}
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
