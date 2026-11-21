//go:build windows

package local

import (
	"os"
	"os/exec"
)

// The embedder watches EMBEDDER_PARENT_PID and exits with its parent.
func setProcAttr(*exec.Cmd) {}

func terminate(p *os.Process) { _ = p.Kill() }
