# Append and retained history

A user sees appended usage counted once and still sees the last observed totals
when a previously observed session log disappears.

## Sub-features

- pstack-append adds one new assistant usage event to an owned synthetic PI log.
- pstack-retain removes that fixture log only after the app observed it.
- pstack-recheck recalculates again without losing or duplicating retained usage.

## How to get to it (user POV)

Usage changes when local sessions grow; refresh the dashboard or GET /recalc.
The retention case represents a log that was observed before it was removed.

## Driving it with pstack dashboard harness

Preconditions: A healthy run created by start. Run this after browser/CLI checks.
Never substitute real provider logs or user data for the helper's fixture.

- **Drive.** Run `python3 "$H" history "$RUN"`.
- **Append.** It appends exactly 100,000 tokens with $0.12 fixture cost, then uses
  the real /recalc route. Expected PI total becomes 450,000.
- **Retain.** It deletes only its owned PI JSONL file and recalculates again.
  PI must remain 450,000, and Combined becomes 2,510,000.
- **Evidence.** Inspect pstack-history.json, the earlier and later pstack-api*.json
  observations, and the retained server/recalc logs.
- **Reuse.** A successfully completed history pass can be checked again without
  appending another event. After a failed partial pass, stop and start fresh.

## Gotchas

- Retention cannot recover a log deleted before the app ever observed it.
- The parse cache is private to this run; deleting it would reset the history
  under test, so do not do so to force a pass.
- This is the sole flow that mutates source logs, and only synthetic ones.
- Do not run a fixed-baseline assertion afterward without considering the
  advanced ledger, or start a fresh run for a clean baseline.
