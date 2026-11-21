//go:build !windows

package local

import (
	"os"
	"os/exec"
	"syscall"
)

func setProcAttr(cmd *exec.Cmd) {
	// Own process group: a Ctrl-C in the terminal reaches embedsync, which then
	// stops the embedder in order. If embedsync dies abruptly, the embedder
	// notices through EMBEDDER_PARENT_PID and exits.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminate(p *os.Process) { _ = p.Signal(syscall.SIGTERM) }
