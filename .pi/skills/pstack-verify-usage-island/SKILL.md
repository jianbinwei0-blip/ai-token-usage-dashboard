---
name: pstack-verify-usage-island
description: Verifies the native macOS Usage Island app with real builds, isolated synthetic collector/cache fixtures, packaged bridge checks, native mouse/Escape smoke flows, display anchoring, quota/reset details, freshness warnings, resource metrics, and retained screenshots. Use when changing apps/usage-island, shared formatter/account contracts, native interactions or packaging, or proving Usage Island user flows.
---

# Pstack verification for Usage Island

Read [features/README.md](features/README.md) and the matching feature recipes.
For shared collector/formatter/account changes, also run
[pstack-verify-ai-token-usage-dashboard](../pstack-verify-ai-token-usage-dashboard/SKILL.md).
No product code, installed app, real usage, credentials or live services are changed.

## Launch

Run from the repository root on macOS 14+ with Swift 6/CLT and Python 3.9+.
Full input proof needs an unlocked desktop; no package install is needed.
The complete sequence builds/checks, stages, drives and cleans its owned app:
```bash
set -euo pipefail
H=.pi/skills/pstack-verify-usage-island/scripts/pstack_island.py
RUN="$(python3 "$H" start | python3 -c 'import json,sys; print(json.load(sys.stdin)["runDir"])')"
trap 'python3 "$H" stop "$RUN"' EXIT
python3 "$H" doctor "$RUN"
python3 "$H" bridge "$RUN"
python3 "$H" native "$RUN"
```
For separate shell calls, start without the EXIT trap, retain the absolute runDir,
and explicitly stop after the last command or any failure. Start failure cleans
its own scratch/app and prints the retained evidence path. Do not run native
verifications in parallel; the helper rejects concurrent commands/recorded apps.

Start runs the existing check.sh (Python tests, native checks, real app build),
copies its package layout to an owned temporary .app, changes only the generated
bundle name/ID, and ad-hoc signs/verifies it. It launches that real executable
with private HOME, CFFIXED_USER_HOME, temp, preferences and data/cache roots.
The existing installed island can stay running; it is never restarted or replaced.
The staged app briefly presents its own overlay and restores focus/display state.

## Doctor

```bash
python3 "$H" doctor "$RUN"
```
Require ok:true, ownedApp:true, sourceUnchanged:true, installedAppUnchanged:true.
Readiness means the real packaged bridge has decoded the expected synthetic
cache and the app wrote matching health/timestamp/PID diagnostics in its private
home. It also verifies PID birth/executable, bundle hashes/ID, source inputs,
local date, installed executable/processes/preferences and account-stub silence.
Source/date/identity drift requires cleanup and a fresh run, never weaker checks.

## Drive

```bash
python3 "$H" bridge "$RUN"
python3 "$H" native "$RUN"
```
Bridge proves eleven account/freshness/failure cases and the real RAM/disk helper.
Native runs baseline and warning fixtures through the existing in-app smoke:
same-position wing clicks, detail clicks, actual Refresh dispatch, Escape,
compact/expanded focus/anchoring on all connected displays, and hide/show.
Locked/inactive sessions retain partial evidence but native returns BLOCKED (exit 2).
In a locked session replace native with preview: python3 "$H" preview "$RUN".
That proves offscreen rendering and hidden startup only; interactivePassed:false
is not full input coverage. Repeat native in a fresh unlocked run before claiming it.

The real collector first aggregates isolated synthetic sessions with fixed test
prices. Native account/warning scenarios then replay a normalized synthetic cache
at the production collector-subprocess boundary. The bundled adapter, formatter,
views, resource helper and native event loop are real; no UI setter is injected.
Refresh replay is not proof of real five-minute scheduling or live account access.

## Evidence

Retained at $RUN/pstack-evidence/: build/test/sign logs, fixture ledger, independent
collector proof, doctor, eleven-case adapter matrix, refresh-call ledger, resource
results, and per-case pstack-native-*/ smoke JSON, fixture, adapter, diagnostics and
own-view PNGs. Repeats append; failures remain failures. Inspect expanded-preview.png in both cases for values, warnings and clipping;
unlocked runs also retain expanded.png.
Only synthetic AI/account amounts are shown; RAM/disk reflect this machine.

## Cleanup

```bash
python3 "$H" stop "$RUN"
```
Only the recorded app PID with matching birth/executable is signalled. Identity
drift refuses signalling/deletion. Scratch is removed; all evidence survives.
Require pstack-cleanup.json: ownedAppExited, scratchRemoved, installedAppUnchanged.
Never kill by name, use the installed-app smoke.sh default, install, or publish.

## Helpers and proof limits
```bash
python3 .pi/skills/pstack-verify-usage-island/scripts/pstack_island_tests.py
```
[Feature map](features/README.md) distinguishes mouse/Escape evidence, controller
checks, callback interception and remaining physical/hardware journeys. Do not
report unexecuted menu/browser, global-key, lock-transition or hardware flows as
passed. Use /skill:pstack-maintain-verification-skill when app behavior changes.
Skill source is version-controlled; tmp/ evidence and local Pi state stay ignored.
