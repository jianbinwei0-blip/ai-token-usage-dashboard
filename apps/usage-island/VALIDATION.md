# Validation

Validated locally through 2026-10-30 (macOS 26.7.1, Apple Silicon).

## Automated checks

`./scripts/check.sh`: **36 passing app checks**. The repository's full dashboard suite also passes **69 tests**, for **105 checks** total.

- 22 Python tests (including canonical formatter parity and reset-credit detail sanitization, capped/legacy/zero metadata, stale-data preservation, and unchanged existing fields): canonical values, weekly-only quota, credits, named model limits, signed-out accounts, stale usage versus stale quota, incomplete pricing, invalid/missing/non-finite metrics, failed collection, shared cache/cadence, resource validation and subprocess-group cleanup.
- 14 native checks (including optional legacy metadata, no-expiration versus unknown dates, capped detail counts, expiry boundaries, and local-time/DST formatting): JSON schema and duplicate fields, exact value preservation, date parsing, freshness boundaries, display-independent compact/expanded presentation policy, locked/inactive-session privacy, missing resources, subprocess errors, timeout cleanup and bounded output.
- Debug and release app builds succeed with the SDK-selection wrapper.
- Shell scripts pass syntax checks; Info.plist passes plist validation.

## Live data parity

Compared the app adapter against SketchyBar's `snapshot_fields` with the same live snapshot and timestamp. All eight available fields matched exactly: plan, weekly, reset, credits, today, month, cost and refresh.

Initial implementation preserved the legacy SketchyBar configuration and tmux resource helper. During repository integration, SketchyBar was uninstalled and its configuration/logs were archived locally outside git. The app now generates its bundled formatter directly from `scripts/dashboard_core/tmux_status.py`, and its system-resource helper remains self-contained.

## Interactive presentation (initial release)

The unlocked in-app smoke check passed on the connected Studio Display and physical MacBook notch:

- Floating compact summary and expanded details.
- Expanded panel receives keyboard focus.
- Escape collapses via the native event loop.
- Hide/show destroys and recreates the panel.
- Hardware-notch compact and expanded layouts, with content on either side of the cutout.
- Return to the original display.
- Global hotkey registration succeeds without requesting Accessibility access. The current binding is Command–Control–U.

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

## Command–Control–U shortcut (v0.2.1)

- No enabled match was found among 230 effective macOS symbolic-hotkey entries. On-disk symbolic settings, custom menu/Services shortcuts and 233 standard Cocoa text bindings also showed no match.
- Inspected configured Karabiner, Rex, Ghostty, Cursor and Chrome extension bindings, plus relevant utility shortcut preferences. No Command–Control–U assignment was found in those settings. No other application's configuration was changed.
- A temporary Carbon registration of the new combination succeeded and was immediately released. No keypress was injected. Carbon permits shared registrations (the existing island binding could also be temporarily registered), so this probe confirms registration availability, not exclusive ownership or the absence of every other binding.
- App-local menus and global event-tap bindings are not exhaustively enumerable by these checks. The probe had no Accessibility permission; none was requested or granted. Treat the result as no conflict found, not a universal conflict-free guarantee.
- The app centralizes the shortcut name, key code and modifiers. Tooltips, menu text and smoke reports use that definition.
- Built and installed v0.2.1. Its live diagnostics report successful registration, virtual key code 32 and Carbon modifier mask 4352 (Command + Control, without Option).
- All 105 automated checks and the full unlocked presentation smoke check passed. Physical global-key dispatch was not synthesized; registration and installed parameters were verified without triggering another application's commands.

## Unified notch-style presentation (v0.2.2)

- All displays now use the existing native notch-style compact wings and expanded details, rather than switching non-notched screens to a floating pill. Physical-notch detection remains truthful and only affects cutout geometry and diagnostics.
- The app explicitly selects DynamicNotchKit's notch style. On non-notched screens, the unmodified library supplies a virtual centre gap and menu-bar height. No vendored UI or geometry changes were needed.
- Presentation policy is independent of hardware; the native test covers compact, expanded and explicit-hide precedence. Removed the obsolete floating summary view and made compact window bounds consistent across displays.
- All 105 automated checks passed (22 app Python, 14 native and 69 dashboard tests). Debug and release builds, shell syntax, Info.plist validation, and `git diff --check` passed.
- Installed locally signed v0.2.2, build 4, at `~/Applications/AI Usage Island.app`. The previous installation was backed up by the installer; login startup and other applications were not changed. `codesign --verify --deep --strict` passed, and binary linkage contains only system libraries/frameworks.
- The installed, unlocked smoke check passed on all three connected displays: Studio Display, Built-in Retina Display (physical notch), and Thunderbolt Display. Each display showed compact and expanded states anchored to its top centre; expanded panels received keyboard focus. Native Escape collapse, hide/show, offscreen rendering, cache loading and global shortcut registration also passed, with no skipped checks.
- Inspected the app's own compact captures on all three displays and expanded captures on the Studio Display and MacBook. Text remains readable at menu-bar height and expanded details remain below the physical cutout. No desktop capture or new permission was required.
- The smoke check now visits every connected display and restores the original display in compact mode, including cleanup after a failure. The installed app was left compact on Studio Display. Private captures, logs and the smoke report are retained under `.artifacts/unified-notch-20261030/` and remain gitignored.
- The centred overlay does not reserve space in the system menu bar. Long application menus or crowded status icons can overlap it. External mouse automation could not attach to the compact panel in this session; presentation callbacks, native Escape handling and own-view captures were checked instead. Physical global-key dispatch was not synthesized.

## Click details to collapse (v0.2.3)

- Added a standard tap gesture and rectangular content hit area to the expanded details. Clicking non-interactive text, metric rows or empty content space returns to compact mode. Child buttons retain their own actions; the collapse button and Escape remain available. No outside-click, hover or timer-based dismissal was added.
- The opt-in smoke check now sends native mouse-down/up events to the real SwiftUI hierarchy, using bounds reported by that hierarchy rather than fixed coordinates or direct calls to collapse. Layout probes are enabled only during smoke checks and do not receive input. The initial accessibility-based target lookup was unavailable because the in-process tree was empty; it was replaced with layout-based targeting, without adding a permission requirement.
- The staged release smoke check passed on Studio Display, Built-in Retina Display and Thunderbolt Display. It verified text/background/metric click collapse, the collapse button, native Escape, top-centre presentation, hide/show, keyboard focus and shortcut registration.
- Button preservation checks clicked the actual controls. Refresh invoked its real cached refresh path. Dashboard and More options used temporary callback counters to verify dispatch without opening a browser or menu; their normal callbacks were restored afterward.
- All 105 automated checks passed (22 app Python, 14 native and 69 dashboard tests), along with debug/release builds, shell syntax, plist validation and diff whitespace checks. Installed locally signed v0.2.3, build 5, with the previous app backed up. The installed binary matches the staged release, and deep/strict signature verification passed.
- The installed-release smoke check also passed all 16 checks with no skips on all three displays. The app was left compact on Studio Display. Logs, app-only captures and staged/installed reports are private local evidence under `.artifacts/click-collapse-20261030/`, which remains gitignored.
- Long reset-list scrolling and external physical mouse/global-key dispatch were not exhaustively exercised. The change uses a single-tap gesture, not a drag or scroll gesture.

## Same-position summary click fix (v0.2.4)

- Fixed the user-reported gap in v0.2.3: the detail-body tap gesture did not cover the reserved notch/menu-bar band, where the original compact wings had been clicked.
- An app-local mouse monitor now handles primary clicks in that top band of this app's expanded window. It records whether the panel was already expanded at mouse-down, so the opening click cannot immediately close it. A drag beyond the click tolerance cancels dismissal. Detail buttons and events in other windows are not intercepted.
- The native smoke check now measures both actual compact-wing positions, clicks each to expand, and clicks the identical original screen coordinate to collapse after the window resizes. Both wings passed on Studio Display, Built-in Retina Display and Thunderbolt Display.
- All 105 automated checks remain passing. The final debug/release builds, plist validation, signature verification and diff checks passed. Installed v0.2.4, build 6, with the previous app backed up; the installed binary matches the packaged release.
- The final installed smoke report passed all 19 checks without skips, including same-position toggles on every display, detail-text/background/metric click collapse, preserved button dispatch, Escape, focus and hide/show. The app was left compact on Studio Display.
- Current local evidence is `.artifacts/click-collapse-20261030/installed-same-position/`; earlier v0.2.3 and staged reports are retained separately. Tests synthesize app-local native mouse events, not external physical mouse input.

## Remaining coverage limits

Actual sleep/wake, fast user switching, menu-bar auto-hide, every full-screen Space configuration, and live display unplug/replug were not exhaustively exercised. Policy checks, screen-change observation and display fallback are implemented. Login startup remains opt-in. The web dashboard and tmux presentation remain independent of the island.

For a fresh full interactive pass after unlocking, run `./scripts/smoke.sh`. Use a fresh output directory if specifying one explicitly.

## Toolchain notes

This machine's Command Line Tools lack SwiftUIMacros, PreviewsMacros, XCTest and Swift Testing. The project therefore uses the installed macOS 26.5 SDK, two documented DynamicNotchKit macro compatibility changes, and a standalone native check executable. Upstream onChange deprecation warnings and SwiftPM's native-builder deprecation warning remain non-fatal; no compiler errors remain.
