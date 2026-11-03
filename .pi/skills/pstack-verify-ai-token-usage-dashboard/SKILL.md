---
name: pstack-verify-ai-token-usage-dashboard
description: Verifies the AI Token Usage Dashboard web UI, recalc API, retained session history, and tmux formatter using isolated synthetic logs and retained evidence, with companion pstack-verify-usage-island coverage for the native macOS app. Use when changing dashboard controls, collectors, pricing/rendering, refresh behavior, or CLI status, or when coordinating verification across the dashboard and Usage Island.
---

# Pstack verification for AI Token Usage Dashboard

Read [features/README.md](features/README.md) and the matching feature recipes.
This verifies real web/API/CLI surfaces, not live usage. For native app flows use
[pstack-verify-usage-island](../pstack-verify-usage-island/SKILL.md); run both skills
for shared collector, formatter, pricing or account-contract changes.

## Launch

Run from the repository root. Python 3.9+, Node.js 22+, and an installed
Chrome/Chromium are needed for full coverage. There are no new package installs.

Run this complete block in one shell; its EXIT trap cleans up on success/failure:
```bash
set -euo pipefail
H=.pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_verify.py
W=.pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_web.mjs
RUN="$(python3 "$H" start | python3 -c 'import json,sys; print(json.load(sys.stdin)["runDir"])')"
trap 'python3 "$H" stop "$RUN"' EXIT
python3 "$H" doctor "$RUN"
python3 "$H" api "$RUN"
node "$W" "$RUN"
python3 "$H" tmux "$RUN"
python3 "$H" history "$RUN"
```
For separate agent shell calls, use start alone without an EXIT trap, retain its
absolute runDir, then explicitly stop after the last drive or any failure.
Start uses the real launcher, an ephemeral loopback port, copied real HTML,
fixed test pricing, and synthetic logs; never 8765, real logs, or live accounts.

Readiness requires actual fixture aggregation and owned health/build/path
checks, not merely an open port. All config, HOME, parse/CLI caches, and browser
profiles are private to the run. Account mode is off and a fail-fast Codex stub
records any accidental lookup. Real usage data is never a fallback.

## Doctor

```bash
python3 "$H" doctor "$RUN"
```

Require ok:true, ownedSources:true, and sourceUnchanged:true. It checks PID
creation/command identity, private paths, usable dataset, source hashes, and
local date. Run before driving and after surprises. Date/source drift requires
a fresh run. Do not borrow or restart the existing 8765 service.

## Drive

```bash
python3 "$H" api "$RUN"
node "$W" "$RUN"
python3 "$H" tmux "$RUN"
python3 "$H" history "$RUN"
```

API proves real recalc/timing/HTML injection and 404 behavior. Browser drives
real controls for provider, reload persistence, dates, empty results, reset,
model rows, and spend. It uses disposable headless Chrome via CDP, blocks
non-fixture requests, and always closes that browser. Set PSTACK_CHROME_BIN to
an installed Chrome executable if needed; missing browser coverage is BLOCKED.
Tmux runs the real CLI with private caches and account usage off. Run history
last: it appends to/deletes only a synthetic PI log, proving retained history.
Use a new run after any failed or partially mutated drive, not weaker assertions.

## Evidence

Artifacts live in `$RUN/pstack-evidence/`: fixture ledger, doctor/API/CLI JSON,
server/recalc logs, history proof, and per-pass `pstack-browser-*/` screenshots,
actions, network timing, and JS-error records. Repeated evidence is append-only.
Only public DOM controls are driven; no internal app setters or test endpoints.
Synthetic pricing proves override math, not live invoices or current billing.

## Cleanup

```bash
python3 "$H" stop "$RUN"
```

This stops only the recorded owned PID, confirms its port closed, and removes
only pstack-scratch. Evidence and run records survive. Cleanup after every
failed attempt too; if identity changed, stop and report rather than kill by
process name. Confirm pstack-evidence/pstack-cleanup.json and screenshots remain.

## Checks and limits

```bash
python3 -m unittest discover -s scripts/tests
python3 .pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_verify_tests.py
```

Do not edit product code, install LaunchAgents, change tmux, commit, or publish
as a verification side effect. Native Usage Island builds, adapter/account-display
fixtures and real-app smoke flows are covered by the companion skill linked above.
Live account/quota access, compressed DSH ingestion and five-minute scheduling
are not proven by this synthetic dashboard run. Skill source under .pi/skills/
is version-controlled; tmp/ evidence and other local Pi state remain gitignored.
