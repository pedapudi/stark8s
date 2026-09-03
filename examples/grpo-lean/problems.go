package main

// A self-contained sample of statements so the example runs without a dataset
// download. Each is a Mathlib-provable goal whose proof is one tactic, which
// is what the prompt asks for: the statement is already known to the graph, so
// nothing is spent making the model re-emit it.
//
// The measured runs behind the README used Lean-Workbook statements in the
// same shape. Swapping the source is a change to this file alone.
var problems = []problem{
	{"comm-add", `theorem lw_comm_add (a b : ℝ) : a + b = b + a`},
	{"add-zero", `theorem lw_add_zero (n : ℕ) : n + 0 = n`},
	{"subst-lin", `theorem lw_subst_lin (x : ℝ) (h : x = 2) : x + 3 = 5`},
	{"mul-one", `theorem lw_mul_one (a : ℝ) : a * 1 = a`},
	{"sub-nonpos", `theorem lw_sub_nonpos (a b : ℝ) (h : a ≤ b) : a - b ≤ 0`},
	{"two-mul", `theorem lw_two_mul (n : ℕ) : 2 * n = n + n`},
	{"sq-nonneg", `theorem lw_sq_nonneg (x : ℝ) : 0 ≤ x ^ 2`},
	{"lt-trans", `theorem lw_lt_trans (a b c : ℝ) (h₁ : a < b) (h₂ : b < c) : a < c`},
}

// problem is one goal. Statement is valid Lean up to but excluding `:=`.
type problem struct {
	ID        string `json:"id"`
	Statement string `json:"statement"`
}

// prompt asks for a tactic and nothing else. An earlier version asked the
// model to reproduce the statement and then prove it, which spent most of a
// 256-token budget on a header that the graph already had: 67% of completions
// were truncated mid-proof and every one of them scored zero. Asking for the
// tactic alone makes 80 tokens ample.
func prompt(p problem) string {
	return "Prove this Lean 4 goal using Mathlib. Reply with the tactic only, " +
		"on one line, with no code fence and no explanation.\n\n" +
		p.Statement + " := by\n"
}

// source is the file handed to the kernel. The statement comes from the graph
// and the model supplies only what follows `:= by`, so a model cannot score by
// restating an easier goal.
func source(p problem, tactic string) string {
	return p.Statement + " := by\n  " + tactic + "\n"
}
