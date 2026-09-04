# grpo-lm: post-training that improves on data it never saw

GRPO over Gemma 4 E2B against verifiable constraints. Each instance asks for
one sentence about some subject, subject to three requirements a program can
check: an exact word count, a word that must not appear, and a term that must
appear. The reward is that checker. Nothing calls a judge, so it cannot drift
and cannot be gamed by pleasing a scorer.

The point of this example is the measurement. Held-out constraint satisfaction
rose from **0.145 to 0.220** over 30 steps, against a base band measured five
times on the same instances, on a set the training never touched.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^      (1x L4)         (2x CPU)               |
                      \-------------weights------------\       advantages
                         (Broadcast, feedback: a URI)   \            |
                                                         \----- learner
                              eval (Retained)         metrics (Retained)
                                                                 (1x L4)
```

## Measuring learning rather than fitting

Two designs each look like they measure learning and do not. Both were run on
this model before this one:

- **A fixed pool of tasks.** Train reward rose 0.805 to 0.861 while held-out
  reward fell 0.845 to 0.831. The model fit the pool.
- **A fresh instance every step, scored on itself.** Reward fell 0.813 to
  0.742, and nothing in that number distinguishes a worse policy from a harder
  draw, because every step scores different problems.

So this example does both at once. Training draws fresh instances every step,
so there is nothing to memorize. Measurement uses a fixed set of 32 instances,
disjoint from every training tag, scored with identical instances at every
evaluation, so a change in the number is a change in the policy.

The base band is part of the design rather than an afterthought. Step 0 always
samples the untrained policy, and the evaluation runs five times there, which
gives the spread any later number has to clear.

## The result

One L4 for `rollout`, one for `learner`, two CPU replicas each for `reward` and
`advantage`. 16 fresh instances per step, 8 completions each; the held-out set
is 32 instances at 8 completions, so every row below is 256 samples.

| step | all constraints met | graded reward | exact word count | includes term |
|---|---|---|---|---|
| base (5 reps) | **0.145 ± 0.025** | 0.822 ± 0.009 | 0.150 | 0.932 |
| 3 | 0.203 | 0.839 | 0.203 | 0.922 |
| 6 | 0.164 | 0.851 | 0.176 | 0.941 |
| 9 | 0.199 | 0.851 | 0.203 | 0.938 |
| 12 | 0.160 | 0.844 | 0.168 | 0.918 |
| 15 | 0.152 | 0.861 | 0.156 | 0.965 |
| 18 | 0.238 | 0.865 | 0.258 | 0.922 |
| 21 | 0.234 | 0.853 | 0.242 | 0.914 |
| 24 | 0.211 | 0.868 | 0.223 | 0.961 |
| 27 | 0.215 | 0.869 | 0.238 | 0.949 |

Averaged over the last three evaluations, held-out satisfaction is 0.220,
which is 3.1 standard deviations above the base band. Pooling the counts —
185 of 1280 base samples against 169 of 768 final samples — gives a
two-proportion `z` of 4.38.

The improvement is where the headroom was. Exact word count went from 0.150 to
0.238, a relative gain of about 60%. The other two constraints did not pay for
it: the required term held between 0.91 and 0.97 throughout, and the forbidden
word was avoided in every one of the 3,584 completions scored. That check
matters, because the cheapest way to hit a word count is to stop writing
sentences, and the model did not.

Training reward on fresh instances moved 0.833 to 0.875 over the same 30 steps,
which agrees with the held-out curve. No step had more than one of its 16
groups without gradient.

## Calibrate before spending an accelerator

A task the model always satisfies and a task it never satisfies both produce
groups with no spread, zero advantages, and no gradient. The run looks healthy
either way. So the binary has a `score` mode that reads completions and reports
how often each constraint is met, and the first version of this task failed
that check:

```
instances=24 completions=192 mean reward=0.935
constraint        met
avoid          100.0%
exactwords      15.1%
include         90.1%
```

Mean reward 0.935 with an exact-count rate of 15% means partial credit was
doing nearly all the work. Credit for a near miss was scaled to the word budget
— two words out of twenty scored 0.9 — so the reward was already near its
ceiling and had almost nothing left to give. Scaling it to a fixed tolerance of
four words instead dropped the base to 0.806, and the strict rate of 14.6% is
what the training then had room to move.

The saturated version would have trained on almost nothing while every metric
looked fine. Rescoring the same 192 completions cost nothing, because the
calibration sampling had already been saved.

## What this does and does not show

It shows that GRPO on this graph improves a policy on instances it was never
trained on, at a margin well outside the base band, without damage to the
constraints that were already satisfied.

It does not show that the group-relative baseline is what did it. The
comparison is against the untrained policy rather than against the same
pipeline with the advantages shuffled or zeroed. What is established is that
the updates moved the policy the right way; whether GRPO's particular baseline
is responsible remains untested.

Nor does it generalize past this task family. Held-out here means unseen
combinations of subject, word budget, forbidden word and required term. A model
better at hitting a word count is not thereby better at anything else.

## Running it

```sh
go test ./examples/grpo-lm/
```

The test drives the whole graph in-process against a stub model whose skill is
a parameter, so every path runs on CPU in a few seconds.

For the cluster run, build the worker image from the repository `Dockerfile`
and the sidecar image from [examples/sidecar](../sidecar). Set
`CHECKPOINT_PREFIX` in `workload.yaml` to a bucket the pods can write, then
apply. Base weights are baked into the sidecar image and `MODEL` points at that
path: naming a hub repository there instead makes every pod start depend on an
external download.

To calibrate before training, dump the held-out prompts, sample them against a
bare sidecar, and score the result:

```sh
grpo-lm -held=24 dump > prompts.json     # {tag: prompt}
# ... sample each prompt n times against /generate ...
grpo-lm score < completions.json         # {tag: [completion, ...]}
```

One operational note that cost a run. The worker waits for its sidecar to
accept a connection before consuming anything. A worker that starts first takes
a segment, fails its request, and exits. The container restarts inside a pod
that is still alive, and the coordinator returns in-flight segments to the
pending set only when a pod is gone, so those records are never redelivered.
Here the wait paid off for a different reason. A duplicated `env:` key in
the manifest left the learner's sidecar listening on the generator's port, so
the learner blocked visibly instead of training against a model with no
optimizer attached.
