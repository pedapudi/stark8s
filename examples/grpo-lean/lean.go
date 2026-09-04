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
	Score(ctx context.Context, statement, tactic string) (float64, string)
}

// leanVerifier shells out to a Mathlib checkout. `import Mathlib` dominates the
// four seconds; a persistent REPL with Mathlib already loaded would cut it, at
// the cost of holding state in an operation that is otherwise stateless.
type leanVerifier struct {
	Root    string
	Timeout time.Duration
}

// Score compiles the candidate and grades what the kernel said about it.
//
// A binary reward is what this task offers by default, and it is the reason
// the first attempt trained on nothing: a proof compiles or it does not, so an
// eight-completion group collapses to all-zero or all-one and GRPO sees no
// gradient. Measured over 136 completions from a 2B model, 15 of 17 groups
// were degenerate under a binary reward and 2 of 17 under the tiers below.
//
// The tiers are not a judgement about proof quality. Each one is a distinct
// thing the compiler reported, and they order the ways a candidate can be
// wrong: a tactic that parses and leaves goals open is nearer a proof than one
// naming a lemma that does not exist, which is nearer than text that is not
// Lean at all.
func (v leanVerifier) Score(ctx context.Context, statement, tactic string) (float64, string) {
	if tactic == "" {
		return 0, "empty"
	}
	src := "import Mathlib\nset_option maxHeartbeats 200000\n" +
		statement + " := by\n  " + tactic + "\n"
	f, err := os.CreateTemp(v.Root, "cand-*.lean")
	if err != nil {
		return 0, "harness: " + err.Error()
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(src); err != nil {
		return 0, "harness: " + err.Error()
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
	if cctx.Err() != nil {
		return 0, "timeout"
	}
	return classify(text, err == nil)
}

// classify turns one compiler run into a reward. It is separate from the
// process handling so it can be tested without a Lean toolchain.
func classify(text string, exitOK bool) (float64, string) {
	// A proof that leaves `sorry` type-checks and proves nothing, so the exit
	// code alone is not the answer.
	if exitOK && !strings.Contains(text, "error") && !strings.Contains(text, "sorry") {
		return 1.0, "proved"
	}
	switch {
	case strings.Contains(text, "unsolved goals"):
		return 0.4, "unsolved"
	case strings.Contains(text, "unknown identifier"),
		strings.Contains(text, "unknown constant"):
		return 0.1, "unknown-lemma"
	case strings.Contains(text, "unexpected token"):
		return 0.0, "parse-error"
	}
	return 0.1, "other-error"
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
