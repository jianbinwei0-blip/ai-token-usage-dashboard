# Pstack dashboard verification map

This is the maintained user-flow map for the web dashboard, recalc API, and
tmux formatter. Native macOS app flows have their own
[Usage Island map](../../pstack-verify-usage-island/features/README.md).
Run both skills for shared collector, formatter, pricing or account-contract changes.
Read the skill's Launch/Doctor instructions before any recipe.
Use only a run created by pstack_verify.py; the live 8765 instance is forbidden.

## Baseline

- Start creates synthetic Codex, Claude, and PI sessions, with no DSH sessions.
- The fresh All Available combined view has 2,410,000 tokens and $2.269 exact
  synthetic spend, displayed as $2.27; PI has 350,000 tokens and $0.31.
- Fixture dates follow the current local date. An old Codex record is 40 days
  back. Read pstack-fixture.json and pstack-run.json for the actual dates and
  expected ledger, rather than assuming a particular year/month.
- Baseline MTD has 1,860,000 tokens and $1.759 exact spend. Today depends on
  whether the fixture's Claude record is yesterday or, on month start, today.
- The pricing file is an isolated supported override, not an assertion about
  live provider prices. No real logs, billing account, or credentials are used.
- One coordinator drives a run serially. Separate start calls use independent
  ports, data, caches, and browser profiles, so parallel independent runs can
  coexist; do not double-drive the same run.

## Features

- [Refresh and usable data](pstack-refresh.md).
- [Provider and date filters](pstack-filters.md).
- [Spend and model breakdown](pstack-costs.md).
- [Append and retained history](pstack-history.md).
- [Tmux status CLI](pstack-tmux.md).

One browser command covers refresh, filters, and spend/model checks. Avoid
repeating it solely because these are separate map entries. Run history last
because it deliberately advances the fixture ledger.

## Proof and cleanup

Required proof pairs the real user action with the visible/API result and
side effects. API/CLI JSON and logs remain in pstack-evidence; each browser
pass has a unique pstack-browser-* folder with screenshots and action/results
JSON. Repeated snapshots do not overwrite older evidence. Preserve failures
as failures, even when a later corrected attempt passes.

Always doctor first. On failure, clean up the owned run and start a fresh one;
a healthy process does not prove a partially mutated test state is valid.
Stop removes scratch and closes the owned port, never evidence. Check
pstack-cleanup.json after teardown.

The companion Usage Island skill proves native builds, isolated adapter/account
fixtures and real-app interaction/presentation flows; native coverage is not
excluded from project verification. Live quota/account access, compressed DSH
logs and controlled five-minute scheduling remain unproven by this dashboard run.
Unit tests cover more code but do not replace live proof of those surfaces.
