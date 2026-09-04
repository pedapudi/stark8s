package main

// The full candidate set, not a selection from it. Every statement that
// compiles with a known reference tactic is here, including the ones the model
// never proves and the ones it always proves, because choosing which goals to
// train on by how the reward comes out is how a benchmark stops measuring
// anything.
//
// Each statement was compiled against Mathlib v4.8.0 with a reference tactic
// before being added; a statement that does not parse scores zero for every
// completion and reads as a model failure.
var problems = []problem{
	{"comm-add", `theorem lw_comm_add (a b : ℝ) : a + b = b + a`},
	{"add-zero", `theorem lw_add_zero (n : ℕ) : n + 0 = n`},
	{"subst-lin", `theorem lw_subst_lin (x : ℝ) (h : x = 2) : x + 3 = 5`},
	{"mul-one", `theorem lw_mul_one (a : ℝ) : a * 1 = a`},
	{"sub-nonpos", `theorem lw_sub_nonpos (a b : ℝ) (h : a ≤ b) : a - b ≤ 0`},
	{"two-mul", `theorem lw_two_mul (n : ℕ) : 2 * n = n + n`},
	{"sq-nonneg", `theorem lw_sq_nonneg (x : ℝ) : 0 ≤ x ^ 2`},
	{"lt-trans", `theorem lw_lt_trans (a b c : ℝ) (h₁ : a < b) (h₂ : b < c) : a < c`},
	{"amgm2", `theorem lw_amgm2 (a b : ℝ) : a * b ≤ (a ^ 2 + b ^ 2) / 2`},
	{"inv-pos", `theorem lw_inv_pos (x : ℝ) (h : 0 < x) : 0 < x⁻¹`},
	{"solve-lin", `theorem lw_solve_lin (a b c : ℝ) (h : a + b = c) : a = c - b`},
	{"sq-mono", `theorem lw_sq_mono (x y : ℝ) (hx : 0 ≤ x) (h : x ≤ y) : x ^ 2 ≤ y ^ 2`},
	{"one-lt-sq", `theorem lw_one_lt_sq (a : ℝ) (h : 1 < a) : 1 < a ^ 2`},
	{"mul-nonneg", `theorem lw_mul_nonneg (a b : ℝ) (ha : 0 ≤ a) (hb : 0 ≤ b) : 0 ≤ a * b`},
	{"apply-hyp", `theorem lw_apply_hyp (f : ℕ → ℕ) (h : ∀ n, f n = n) : f 3 = 3`},
	{"cube-nonneg", `theorem lw_cube_nonneg (x : ℝ) (h : 0 ≤ x) : 0 ≤ x ^ 3`},
	{"add-sq", `theorem lw_add_sq (a b : ℝ) : (a + b) ^ 2 = a ^ 2 + 2 * a * b + b ^ 2`},
}

// problem is one goal. Statement is valid Lean up to but excluding `:=`.
type problem struct {
	ID        string `json:"id"`
	Statement string `json:"statement"`
}

// The prompt names the tactics that exist. Without that line the model reaches
// for lemma names it invents — `mul_nonneg_mul_nonneg`, `nonneg_of_square` —
// and almost never for the automation that closes these goals: measured over
// 136 completions, naming the tactics cut groups with no gradient from 5 to 2.
//
// It asks for the tactic and not the theorem. An earlier version asked the
// model to restate the goal and then prove it inside a 256-token budget; the
// header consumed the budget, 67% of completions were truncated mid-proof, and
// every one of them scored zero.
func prompt(p problem) string {
	return "Prove this Lean 4 goal using Mathlib. Reply with the tactic only, on one " +
		"line, with no code fence and no explanation.\n" +
		"Useful tactics: ring, simp, norm_num, linarith, nlinarith, positivity, " +
		"omega, rfl, assumption, exact.\n\n" +
		p.Statement + " := by\n"
}

// source is the file handed to the kernel. The statement comes from the graph
// and the model supplies only what follows `:= by`, so a model cannot score by
// restating an easier goal.
func source(p problem, tactic string) string {
	return p.Statement + " := by\n  " + tactic + "\n"
}
