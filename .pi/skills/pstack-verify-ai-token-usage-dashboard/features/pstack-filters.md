# Provider and date filters

Users choose which provider and dates to inspect, preserve their provider on
reload, and reset back to the full available window without stale totals.

## Sub-features

- pstack-provider selects Combined or PI from the actual provider control.
- pstack-persistence preserves PI through a user reload.
- pstack-dates narrows the view to today's real fixture rows.
- pstack-empty selects a valid date with no usage and shows zero totals/spend.
- pstack-reset restores All Available and the complete combined total.

## How to get to it (user POV)

Use the dashboard's Provider, Range, From, To, and Reset controls.

## Driving it with pstack dashboard harness

Preconditions: A fresh or explicitly advanced owned run is healthy. Run the
browser helper once for this feature plus refresh and cost/model coverage.

- **Drive.** Run `node "$W" "$RUN"`. It operates #usageProvider,
  #usageRangePreset, #usageRangeFrom, #usageRangeTo, and #usageRangeClear through
  their real change/click handlers, not internal app functions.
- **PI.** Expect the exact PI ledger total and model list, still selected after
  a full reload/recalc cycle. Baseline PI is 350,000 tokens.
- **Dates/empty.** Expect today's ledger total when From=To=today. For the
  fixture gap date, both tokens and spend must be zero.
- **Reset.** Expect preset all and the full combined ledger total.
- **Evidence.** Read before/after action snapshots and the PI screenshot inside
  the unique browser evidence directory.

## Gotchas

- Each browser pass has a disposable profile; it never reads personal storage.
- Provider changes also clamp date bounds. The harness selects All Available
  explicitly before comparing provider totals or narrowing dates.
- An equivalent selected date interval can normalize to an existing preset;
  actual From/To bounds and visible totals are the assertion.
- History intentionally advances the ledger; fresh baselines require a new run.
