# ai-token-usage-dashboard autoresearch program

This program applies the `autoresearch` style to the AI Token Usage Dashboard repo.

The core idea is:
- keep the evaluation harness fixed
- optimize one primary metric
- make one small change per iteration
- keep changes only when they improve the metric without breaking correctness

This is **not** the normal feature-delivery mode for this repo. Use this mode when the feature scope is already understood and the team is trying to improve the implementation empirically.

---

## Objective

Optimize the dashboard's refresh/recalc pipeline while preserving correctness.

### Primary metric

**Median successful `/recalc` latency in milliseconds** over 5 runs on a **fixed benchmark fixture set**.

Lower is better.

### Hard gates

An experiment is automatically discarded if any of these fail:

1. `python3 -m unittest discover -s scripts/tests`
2. Benchmark run returns a valid payload and rewrites runtime HTML successfully
3. Token and cost totals remain correct for the fixed benchmark scenario
4. Required HTML/data hooks remain present in the rendered output

### Secondary guardrails

Track these, but do not optimize them directly unless the user changes the goal:
- payload shape stability
- peak memory
- pricing warning count
- code complexity / diff size
- readability / maintainability

---

## Team roles

### Architect
Acts as the **program owner** and **keeper**.

Responsibilities:
- maintain this `program.md`
- define the benchmark fixture and evaluation rules
- propose exactly one experiment at a time
- decide keep/discard after tester reports results
- keep the experiment queue small and ranked

### Builder
Acts as the **mutator**.

Responsibilities:
- implement exactly one experiment at a time
- keep diffs minimal and focused
- do not broaden scope
- do not change the benchmark harness unless the architect explicitly authorizes a dedicated harness-update iteration

### Tester
Acts as the **evaluator**.

Responsibilities:
- run the fixed gates and benchmark
- report exact commands, exact before/after numbers, and exact failures
- recommend `keep`, `discard`, or `crash`
- update `results.tsv`

---

## In-scope files

By default, experiments may edit only these files unless the architect explicitly narrows the set further for a given iteration:

- `scripts/dashboard_core/collectors.py`
- `scripts/dashboard_core/aggregation.py`
- `scripts/dashboard_core/pipeline.py`
- `scripts/dashboard_core/render.py`
- `dashboard/index.html`

Only when necessary and explicitly approved for the current iteration:
- `scripts/dashboard_core/config.py`
- `scripts/dashboard_core/models.py`
- `scripts/dashboard_core/pricing.py`
- `README.md`

---

## Immutable harness

These are read-only during the optimization loop unless the architect starts a dedicated harness-update iteration and the human approves it:

- `scripts/tests/*`
- benchmark fixture data
- benchmark measurement logic
- benchmark golden expectations
- benchmark HTML template/hooks

Reason: if the harness changes during optimization, metric comparisons become untrustworthy.

---

## Benchmark harness

The benchmark must run against a **fixed representative fixture set**, not live data in:
- `~/.codex/sessions`
- `~/.claude/projects`
- `~/.pi/agent`

### Preferred benchmark command

Once available, use a dedicated script like:

```bash
python3 scripts/benchmark_recalc.py --repeat 5
```

The script should:
- build or load a fixed fixture pack covering Codex, Claude, and PI
- point `DashboardConfig` at those fixture roots
- run `recalc_dashboard(...)` 5 times
- print per-run timings and the median in ms
- verify the rendered HTML still includes required hooks
- exit non-zero on failure

### Current harness status

`scripts/benchmark_recalc.py` is the frozen benchmark harness for this repo's autoresearch mode.

It was created from the existing deterministic pipeline scenario in `scripts/tests/test_harness_contracts.py` and should remain immutable during optimization unless the human explicitly approves a dedicated harness-update change.

---

## Setup

Before the experiment loop begins, do the following:

1. **Agree on a run tag**
   - Example: `latency-mar9`

2. **Create the branch**
   - From current `main`
   - Use a branch name like:

   ```bash
   git checkout -b autoresearch/latency-mar9
   ```

3. **Read the in-scope files fully**
   - `README.md`
   - `program.md`
   - `scripts/dashboard_core/collectors.py`
   - `scripts/dashboard_core/aggregation.py`
   - `scripts/dashboard_core/pipeline.py`
   - `scripts/dashboard_core/render.py`
   - `dashboard/index.html`
   - `scripts/tests/test_harness_contracts.py`
   - `scripts/tests/test_usage_aggregation.py`
   - `scripts/tests/test_runtime_html.py`

4. **Verify benchmark harness exists**
   - If `scripts/benchmark_recalc.py` does not exist yet, create it in a dedicated setup change and commit it before beginning optimization.
   - Record the benchmark fixture/golden behavior and do not change it afterward.

5. **Initialize `results.tsv`**
   - Create with this header:

   ```tsv
   commit	median_recalc_ms	gates	status	description
   ```

6. **Run the baseline**
   - Run the full gate suite
   - Run the benchmark
   - Log the baseline row in `results.tsv`

7. **Confirm baseline is green**
   - No optimization iterations begin until the baseline passes all gates

---

## Experiment design rules

Each iteration must test **one hypothesis only**.

Good examples:
- reduce repeated date slicing in pipeline aggregation
- precompute provider breakdown merges once instead of rebuilding repeatedly
- avoid expensive HTML rewrite work when only the dataset payload changes
- reduce repeated pricing/model lookup overhead
- eliminate unnecessary row sorting passes

Bad examples:
- refactor the whole dashboard pipeline
- clean up unrelated code while also optimizing latency
- redesign UI and optimize backend in the same iteration
- change tests and implementation together to chase a speedup

### Simplicity criterion

All else equal, simpler is better.

A small speedup is not worth a large amount of brittle complexity.
A neutral or near-neutral latency result may still be worth keeping **only** if the code becomes materially simpler and all gates pass.
That decision belongs to the architect.

---

## Output and logging

Every experiment must be logged in `results.tsv`.

Columns:
- `commit`: short git hash
- `median_recalc_ms`: measured median latency in ms
- `gates`: `pass` or `fail`
- `status`: `keep`, `discard`, or `crash`
- `description`: short description of the single experiment

Example:

```tsv
commit	median_recalc_ms	gates	status	description
abc1234	118.4	pass	keep	baseline
bcd2345	111.7	pass	keep	precompute provider summary merges
cde3456	124.9	pass	discard	extra normalization pass before render
3456def	0	fail	crash	memoization bug in breakdown cache key
```

---

## Evaluation commands

### Gate suite

```bash
python3 -m unittest discover -s scripts/tests
```

### Benchmark

Preferred:

```bash
python3 scripts/benchmark_recalc.py --repeat 5
```

The tester should report:
- 5 individual run times
- median latency
- whether payload and HTML checks passed

---

## Keep / discard policy

### Keep
Keep the experiment if:
- all hard gates pass
- median `/recalc` latency improves meaningfully versus the current kept baseline
- code complexity remains acceptable

### Discard
Discard the experiment if:
- any hard gate fails
- median latency is worse
- latency is flat and the code is more complex

### Borderline case
If latency is effectively flat but the code becomes clearly simpler or more maintainable, the architect may keep it, but must explicitly note that in `results.tsv` and in the handoff message.

---

## The experiment loop

Repeat the following forever until the human interrupts:

1. **Architect** reviews current kept baseline and proposes the next single hypothesis.
2. **Architect** tells the builder exactly which files may be edited for this iteration.
3. **Builder** implements only that hypothesis.
4. **Builder** commits the change.
5. **Tester** runs the full gate suite.
6. **Tester** runs the benchmark.
7. **Tester** reports:
   - exact commands run
   - per-run timings
   - median latency
   - pass/fail gates
   - keep/discard recommendation
8. **Architect** decides:
   - keep the commit as the new baseline, or
   - discard it and reset to the previous kept commit
9. **Tester** logs the result in `results.tsv`.
10. **Architect** proposes the next experiment.

Do **not** pause to ask the human whether to continue after every iteration.
The loop is autonomous until interrupted.

---

## Git discipline

For each experiment:

1. start from the current kept commit
2. make one focused change
3. commit once with a short experiment description
4. evaluate
5. keep or reset

If discarded, reset back to the previous kept commit before starting the next experiment.

---

## Crash policy

If the experiment crashes:
- log it as `crash`
- record `median_recalc_ms` as `0`
- include the short cause in `description`
- revert to the previous kept commit
- move on

If the crash is due to a trivial mistake in the current experiment, the builder may fix it once and retry before declaring the experiment a crash.

---

## What this mode is for

Use this autoresearch mode when the team is optimizing:
- recalc performance
- aggregation efficiency
- payload generation efficiency
- rendering throughput
- code simplification under a stable harness

Do **not** use this mode as the primary workflow for:
- ambiguous feature work
- product design exploration
- changing requirements
- UI redesign with subjective goals

For those, use the normal architect / builder / tester delivery loop first.
After the feature is stable, switch into autoresearch mode for optimization.

---

## First recommended run for this repo

The first autoresearch track for `ai-token-usage-dashboard` should be:

**Goal:** reduce median `/recalc` latency while preserving current token/cost correctness and rendered dashboard behavior.

**Suggested first experiment ideas:**
1. precompute and reuse provider summary totals where possible
2. reduce repeated sorting and repeated row-materialization work
3. avoid unnecessary HTML work when only JSON payload content changes
4. cut repeated model/pricing lookup overhead in collectors
5. reduce temporary object creation in daily/breakdown merges

Start with the smallest, most measurable idea.
