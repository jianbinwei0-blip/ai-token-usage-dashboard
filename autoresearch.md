# Autoresearch: live dashboard refresh to 200 ms

## Objective
Reduce the real browser-visible `/recalc` latency for the local AI token usage dashboard on the user's actual Codex, Claude, and PI data.

Current focus: **cold-start latency after daemon restart**, while preserving the already-achieved warm-refresh wins.

This session optimizes the live local workload, not the tiny fixed-fixture synthetic benchmark used previously. We must not overfit to the benchmark and must not cheat by skipping real parsing work unless we can prove unchanged inputs and preserve correctness.

## Metrics
- **Primary**: `cold_recalc_ms` (ms, lower is better) — median first successful recalc latency over 5 **fresh Python processes** after one unmeasured prep recalc populates any legitimate persistent caches. This models restarting the daemon after prior successful use.
- **Secondary**:
  - warm-process latency (`live_recalc_ms`)
  - correctness/test pass rate
  - payload/html hook integrity
  - code complexity / maintainability

## How to Run
`./autoresearch.sh`

The script:
- writes runtime HTML to `tmp/autoresearch.runtime.html` so tracked `dashboard/index.html` stays clean
- runs a live recalc benchmark on the user's real local data roots
- performs one unmeasured prep recalc, then measures cold starts in fresh processes and warm runs in-process
- prints `METRIC live_recalc_ms=<number>` and `METRIC cold_recalc_ms=<number>` lines
- relies on `autoresearch.checks.sh` for correctness gates

## Files in Scope
- `scripts/dashboard_core/collectors.py` — dominant hotspot; scans/parses Codex, Claude, and PI logs
- `scripts/dashboard_core/pipeline.py` — orchestration and any persistent cache entry points
- `scripts/dashboard_core/aggregation.py` — low-cost aggregation helpers if needed
- `scripts/dashboard_core/render.py` — HTML rewrite path; likely minor but allowed
- `scripts/dashboard_core/config.py` — config plumbing if needed for cache behavior
- `scripts/dashboard_core/models.py` — only if cache/state data structures truly need it
- `dashboard/index.html` — UI hooks only if required, not a priority
- `scripts/benchmark_live_recalc.py` — live benchmark harness for this autoresearch target
- `autoresearch.sh` — benchmark entrypoint
- `autoresearch.checks.sh` — correctness gates

## Off Limits
- `scripts/tests/*` except to read them
- synthetic benchmark semantics in `scripts/benchmark_recalc.py`
- user data under `~/.codex`, `~/.claude`, `~/.pi`
- anything that fakes success by ignoring files, truncating history, or weakening correctness

## Constraints
- Do not cheat on the benchmark.
- Do not overfit to the tiny synthetic fixture benchmark.
- Keep live totals/costs correct for unchanged inputs.
- `python3 -m unittest discover -s scripts/tests` must pass.
- The recalc must still emit valid payloads and HTML hooks for the dashboard.
- Keep changes incremental: one hypothesis per experiment.
- Use runtime HTML in `tmp/` to avoid dirtying tracked dashboard files during experiments.

## Workload Notes
Current measured live workload before optimization:
- Codex root: ~675 JSONL files, ~2.03 GB
- Claude root: ~103 JSONL files, ~7.6 MB
- PI root: ~244 JSONL files, ~346.8 MB
- Live `/recalc`: ~5.5 s
- Hotspots: Codex collection ~4.0 s, PI collection ~1.1 s, Claude ~0.05 s, HTML rewrite ~0.007 s

Likely direction: persistent incremental caches keyed by file metadata/content stability, especially for append-only log files, so repeated browser refreshes reuse previously parsed state while preserving correctness when files change.

## What's Been Tried
- Measured the live pipeline directly instead of assuming the fixed fixture benchmark represented browser refreshes.
- Verified the bottleneck is collector I/O and JSON parsing, not HTML rewrite or frontend rendering.
- Added a metadata-keyed in-memory parse cache for Codex session files. Result: warm median dropped from ~5439 ms to ~1319 ms while cold start stayed ~5481 ms. This confirmed repeated browser refreshes can benefit heavily from persistent process caches.
- Added the same unchanged-file metadata cache pattern for PI session files by caching parsed assistant usage records per session file. Result: warm recalc fell to roughly ~225 ms median, showing PI was the remaining dominant hot path after Codex.
- Added cached per-file Claude request record parsing. Result: live median dropped to ~157 ms, beating the 200 ms warm target while preserving correctness checks.
- Retargeted the benchmark to cold starts after daemon restart: one unmeasured prep recalc populates legitimate persistent caches, then cold runs are measured in fresh Python processes. This models restarting the daemon after prior successful use.
- Added persistent on-disk provider parse caches keyed by file size and mtime. Result: restart cold median fell from multi-second territory to about ~221 ms while warm median stayed around ~160 ms.
- Changed PI persistent cache entries from raw assistant-message records to aggregated per-day/per-model session contributions. Result: restart cold median dropped further to ~68 ms and warm median to ~43 ms.
- Deduplicated Claude request events within each file before caching them. Result: restart cold median improved again to ~65 ms.
- Tested reusing file stat results from directory iteration to avoid redundant stat calls. Result: slight warm improvement but cold regression; discarded.
- Tested lazily exporting symbols from `dashboard_core.__init__` to cut import overhead. Result: import time improved, but benchmarked cold recalc still regressed slightly; discarded.
- Changed Codex persistent cache entries from raw session usage snapshots to fully priced per-file contributions keyed by pricing metadata. Result: restart cold median improved further to ~60.6 ms and warm median to ~37.4 ms.
- Tested the same higher-level priced contribution cache strategy for Claude. Result: regressed cold latency to ~74.6 ms; discarded.
- Tested the same higher-level priced contribution cache strategy for PI. Result: warm latency improved slightly but cold still regressed to ~62.3 ms; discarded.
- Combined lesson: persistent file-signature caches plus caching higher-level per-file contributions are enough to make both warm refreshes and restart cold starts fast without weakening correctness on changed files, but the strategy is not uniformly beneficial across providers.
