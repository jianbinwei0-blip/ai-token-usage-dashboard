# Harness Engineering Adoption

This repo now applies the core lessons from OpenAI's harness-engineering guidance to keep changes reliable as the dashboard grows.

## What Changed

1. Legible architecture with explicit boundaries
- `scripts/dashboard_core/config.py`: runtime config and env parsing.
- `scripts/dashboard_core/collectors.py`: provider-specific ingestion only.
- `scripts/dashboard_core/aggregation.py`: cross-provider math and date/window logic.
- `scripts/dashboard_core/render.py`: HTML mutation and dataset embedding.
- `scripts/dashboard_core/pipeline.py`: orchestration of end-to-end recalc flow.
- `scripts/ai_usage_recalc_server.py`: thin HTTP entrypoint with compatibility wrappers.

2. Repository guidance as system of record
- This file defines the operating model and validation loops.
- README now points to these architecture boundaries and validation commands.

3. Taste and quality enforced through tests
- Existing unit tests keep parser and range correctness checks.
- `scripts/tests/test_harness_contracts.py` adds harness-style invariants:
  - deterministic output under a fixed clock,
  - no duplicate dataset-tag injection across repeated runs,
  - Monday current-week clamp remains intact at full pipeline level.

## Validation Loop

Use this sequence before/after behavior changes:

```bash
python3 -m unittest scripts.tests.test_usage_aggregation scripts.tests.test_harness_contracts
curl -s http://127.0.0.1:8765/recalc | jq '.ok,.updated_at,.providers_available'
```

## Why This Matters

- Faster iteration: each module has a narrow purpose, so edits are localized.
- Safer changes: deterministic harness checks catch regressions that visual spot-checking misses.
- Better maintainability: local conventions are encoded in code + tests, not just tribal memory.
