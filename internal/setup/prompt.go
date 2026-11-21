// Package setup implements the guided first-run experience of an installed
// embedsync: `embedsync setup`, `embedsync doctor` and `embedsync model`.
package setup

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/term"
)

// ErrNeedInput is returned in non-interactive mode when a required answer has
// no flag and no safe default.
var ErrNeedInput = errors.New("missing input in non-interactive mode")

// Prompter asks questions on a terminal, or answers with defaults when not
// interactive (--yes).
type Prompter struct {
	in          *bufio.Reader
	inFile      *os.File
	Out         io.Writer
	Interactive bool
}

// NewPrompter reads from in and writes to out.
func NewPrompter(in io.Reader, out io.Writer, interactive bool) *Prompter {
	p := &Prompter{in: bufio.NewReader(in), Out: out, Interactive: interactive}
	if f, ok := in.(*os.File); ok {
		p.inFile = f
	}
	return p
}

// Say prints a line.
func (p *Prompter) Say(format string, a ...any) { fmt.Fprintf(p.Out, format+"\n", a...) }

func (p *Prompter) line() (string, error) {
	s, err := p.in.ReadString('\n')
	if err != nil && (s == "" || !errors.Is(err, io.EOF)) {
		if errors.Is(err, io.EOF) {
			return "", errors.New("input closed")
		}
		return "", err
	}
	return strings.TrimSpace(s), nil
}

// Ask returns the answer, or def for an empty answer.
func (p *Prompter) Ask(q, def string) (string, error) {
	if !p.Interactive {
		return def, nil
	}
	if def != "" {
		fmt.Fprintf(p.Out, "%s [%s]: ", q, def)
	} else {
		fmt.Fprintf(p.Out, "%s: ", q)
	}
	s, err := p.line()
	if err != nil {
		return "", err
	}
	if s == "" {
		return def, nil
	}
	return s, nil
}

// Secret reads a value without echo when stdin is a terminal.
func (p *Prompter) Secret(q string) (string, error) {
	if !p.Interactive {
		return "", nil
	}
	fmt.Fprintf(p.Out, "%s: ", q)
	if p.inFile != nil && term.IsTerminal(int(p.inFile.Fd())) {
		b, err := term.ReadPassword(int(p.inFile.Fd()))
		fmt.Fprintln(p.Out)
		return string(b), err
	}
	return p.line()
}

// Confirm asks a yes/no question.
func (p *Prompter) Confirm(q string, def bool) (bool, error) {
	if !p.Interactive {
		return def, nil
	}
	hint := "y/N"
	if def {
		hint = "Y/n"
	}
	for {
		fmt.Fprintf(p.Out, "%s [%s]: ", q, hint)
		s, err := p.line()
		if err != nil {
			return false, err
		}
		switch strings.ToLower(s) {
		case "":
			return def, nil
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		}
	}
}

// Choose shows numbered options and returns the chosen index.
func (p *Prompter) Choose(q string, options []string, def int) (int, error) {
	if !p.Interactive {
		return def, nil
	}
	p.Say("%s", q)
	for i, o := range options {
		p.Say("  %d) %s", i+1, o)
	}
	for {
		s, err := p.Ask("Choose 1-"+strconv.Itoa(len(options)), strconv.Itoa(def+1))
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(s)
		if err == nil && n >= 1 && n <= len(options) {
			return n - 1, nil
		}
		p.Say("  please enter a number between 1 and %d", len(options))
	}
}

// ChooseMany returns indexes from a comma-separated answer like "1,3".
func (p *Prompter) ChooseMany(q string, options []string, def []int) ([]int, error) {
	if !p.Interactive {
		return def, nil
	}
	p.Say("%s", q)
	for i, o := range options {
		p.Say("  %d) %s", i+1, o)
	}
	defS := make([]string, len(def))
	for i, d := range def {
		defS[i] = strconv.Itoa(d + 1)
	}
	for {
		s, err := p.Ask("Numbers separated by commas", strings.Join(defS, ","))
		if err != nil {
			return nil, err
		}
		var out []int
		ok := true
		seen := map[int]bool{}
		for _, part := range strings.Split(s, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 1 || n > len(options) {
				ok = false
				break
			}
			if !seen[n] {
				seen[n] = true
				out = append(out, n-1)
			}
		}
		if ok && len(out) > 0 {
			return out, nil
		}
		p.Say("  please enter numbers between 1 and %d, e.g. 1,2", len(options))
	}
}
