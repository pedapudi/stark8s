package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// The reward is the Lean kernel. It cannot be gamed by prose, it cannot drift,
// and it needs no judge and no reward model — but it costs about four seconds
// of CPU per completion, which is what makes this example about scaling rather
// than about learning.
//
// That cost is the whole argument for putting it in its own operation. Left in
// the same process as generation it would idle an accelerator: measured on one
// L4 and eight CPUs, generating 64 completions takes 178s and verifying them
// takes 419s serially. As a separate operation with its own replica count,
// verification runs on cheap CPU pods while the accelerator only ever
// generates.
type verifier interface {
	Verify(ctx context.Context, proof string) (bool, string)
}

// leanVerifier shells out to a Mathlib checkout. `import Mathlib` dominates the
// four seconds; a persistent REPL with Mathlib already loaded would cut it, at
// the cost of holding state in an operation that is otherwise stateless.
type leanVerifier struct {
	Root    string
	Timeout time.Duration
}

func (v leanVerifier) Verify(ctx context.Context, proof string) (bool, string) {
	src := "import Mathlib\nset_option maxHeartbeats 200000\n" + unfence(proof) + "\n"
	f, err := os.CreateTemp(v.Root, "cand-*.lean")
	if err != nil {
		return false, err.Error()
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(src); err != nil {
		return false, err.Error()
	}
	f.Close()

	to := v.Timeout
	if to == 0 {
		to = 3 * time.Minute
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	cmd := exec.CommandContext(cctx, "lake", "env", "lean", filepath.Base(f.Name()))
	cmd.Dir = v.Root
	out, err := cmd.CombinedOutput()
	text := string(out)
	// A proof that leaves `sorry` type-checks and proves nothing, so the exit
	// code alone is not the answer.
	if err != nil || strings.Contains(text, "error") || strings.Contains(text, "sorry") {
		return false, firstLine(text)
	}
	return true, ""
}

// unfence strips a markdown code fence. A model asked for Lean very often
// returns correct Lean inside ```lean ... ```, and rejecting that measures the
// wrapper rather than the proof.
func unfence(text string) string {
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, "```") {
		return t
	}
	if i := strings.IndexByte(t, '\n'); i >= 0 {
		t = t[i+1:]
	}
	if j := strings.LastIndex(t, "```"); j >= 0 {
		t = t[:j]
	}
	return strings.TrimSpace(t)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}
