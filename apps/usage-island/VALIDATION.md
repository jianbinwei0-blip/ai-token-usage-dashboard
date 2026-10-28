# Validation

Validated locally through 2026-10-28 (macOS 26.7.1, Apple Silicon).

## Automated checks

`./scripts/check.sh`: **36 passing app checks**. The repository's full dashboard suite also passes **69 tests**, for **105 checks** total.

- 22 Python tests (including canonical formatter parity and reset-credit detail sanitization, capped/legacy/zero metadata, stale-data preservation, and unchanged existing fields): canonical values, weekly-only quota, credits, named model limits, signed-out accounts, stale usage versus stale quota, incomplete pricing, invalid/missing/non-finite metrics, failed collection, shared cache/cadence, resource validation and subprocess-group cleanup.
- 14 native checks (including optional legacy metadata, no-expiration versus unknown dates, capped detail counts, expiry boundaries, and local-time/DST formatting): JSON schema and duplicate fields, exact value preservation, date parsing, freshness boundaries, floating/notched presentation policy, locked/inactive-session privacy, missing resources, subprocess errors, timeout cleanup and bounded output.
- Debug and release app builds succeed with the SDK-selection wrapper.
- Shell scripts pass syntax checks; Info.plist passes plist validation.

## Live data parity

Compared the app adapter against SketchyBar's `snapshot_fields` with the same live snapshot and timestamp. All eight available fields matched exactly: plan, weekly, reset, credits, today, month, cost and refresh.

Initial implementation preserved the legacy SketchyBar configuration and tmux resource helper. During repository integration, SketchyBar was uninstalled and its configuration/logs were archived locally outside git. The app now generates its bundled formatter directly from `scripts/dashboard_core/tmux_status.py`, and its system-resource helper remains self-contained.

## Interactive presentation

The unlocked in-app smoke check passed on the connected Studio Display and physical MacBook notch:

- Floating compact summary and expanded details.
- Expanded panel receives keyboard focus.
- Escape collapses via the native event loop.
- Hide/show destroys and recreates the panel.
- Hardware-notch compact and expanded layouts, with content on either side of the cutout.
- Return to the original display.
- Control–Option–U registration succeeds without requesting Accessibility access.

Inspected four own-view PNG captures. Fixed a truncated Today caption and replaced the floating material's gray appearance with the existing charcoal palette. The checked captures are retained locally in `.artifacts/validation/` and are gitignored because they contain usage amounts.

These checks exercise the app's real presentation callbacks and native Escape handling. They are not an external mouse-automation or comprehensive accessibility audit. Global hotkey registration was checked; an external physical keypress was not synthesized.

## Initial installed release

Installed and locally signed `~/Applications/AI Usage Island.app`.

- `codesign --verify --deep --strict` passes.
- Binary linkage contains only system frameworks/libraries, not build-directory dylibs.
- Temporarily moved `.build` out of the way and cold-launched the installed app. Its bundled adapter decoded the existing cache successfully; the app does not need the development build directory.
- The Mac was locked during the final release check. Verified that a cold launch while locked creates no visible island window. Interactive focus/presentation checks were explicitly skipped in this final run, rather than treating macOS's focus refusal as an application failure.
- Added initial session-state detection and a lightweight fallback poll because lock notifications are not replayed to applications launched after locking. The UI stays below system security surfaces and returns after unlock.

The initial release report is `.artifacts/validation/final-release.json`. Earlier unlocked interaction results are in `unlocked-interaction.json`. These are private, ignored local artifacts rather than published repository files.

## Repository integration and SketchyBar removal

- Moved source into `apps/usage-island/`; the old standalone source directory no longer exists.
- A clean build and signed release package succeed from the new location. Checks/builds prepare the canonical formatter automatically; it is generated, not a second maintained source file.
- All 63 dashboard tests and 28 app checks pass. The live `/recalc` endpoint returns success for the existing provider scope.
- Reinstalled the release built from this repository. With the SketchyBar formula, service and active configuration absent, the installed app's refresh and RAM/disk helpers succeed. All eight live AI fields still match the archived legacy renderer under the same snapshot and clock.
- SketchyBar's launch service and formula were removed; its configuration/cache/log backup is local under `~/Library/Application Support/AIUsageIsland/Backups/`. The shared dashboard runtime, pricing and tmux helper were retained.
- The post-integration launch occurred while locked: bundled cache loading and hidden-window privacy checks passed; interactive presentation checks were explicitly skipped for that run.
- Build outputs, logs, private captures and generated resources remain ignored. Test credit amounts use a synthetic balance.

## Reset availability and expiration dates (v0.2.0)

- Confirmed the installed Codex protocol exposes an authoritative available count and optional/capped detail rows. Expiration timestamps use Unix seconds; explicit null means no expiration.
- Subscription schema v3 stores only normalized counts and grouped dates. Opaque credit IDs and backend titles/descriptions are not persisted. Non-available credits are excluded and repeated IDs are deduplicated before grouping.
- Version-2 caches refresh once for the new schema. Lookup failure preserves last-known values as stale without inventing dates; the terminal/compact field output stays unchanged.
- Updated only the subscription normalizer and cache gate in the background-safe runtime, after checking they matched the known baseline and backing them up locally.
- Rebuilt, locally signed and installed v0.2.0. The installed data source reports two available resets with two complete expiration groups.
- Inspected the offscreen expanded-view preview: available count, both local expiration dates/time zones, and the relabelled Weekly reset are readable without clipping. This renders the real SwiftUI details view without creating a visible window or changing focus.
- The Mac was locked during the installation smoke check. Cache loading, offscreen rendering and hidden-window privacy passed; interactive checks were explicitly skipped. No reset-consumption action was added or called.
- Private evidence is retained under `.artifacts/reset-expirations-*/`, which remains gitignored. Tests use synthetic timestamps, not live account data.

## Remaining coverage limits

Actual sleep/wake, fast user switching, every full-screen Space configuration, and live display unplug/replug were not exhaustively exercised. Policy checks, screen-change observation and display fallback are implemented. Login startup remains opt-in. The web dashboard and tmux presentation remain independent of the island.

For a fresh full interactive pass after unlocking, run `./scripts/smoke.sh`. Use a fresh output directory if specifying one explicitly.

## Toolchain notes

This machine's Command Line Tools lack SwiftUIMacros, PreviewsMacros, XCTest and Swift Testing. The project therefore uses the installed macOS 26.5 SDK, two documented DynamicNotchKit macro compatibility changes, and a standalone native check executable. Upstream onChange deprecation warnings and SwiftPM's native-builder deprecation warning remain non-fatal; no compiler errors remain.
