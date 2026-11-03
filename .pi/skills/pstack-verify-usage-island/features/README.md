# Usage Island verification map

The native app is a first-class verification surface, alongside the
[web/API/tmux map](../../pstack-verify-ai-token-usage-dashboard/features/README.md).
Run both skills for shared collector, pricing, canonical formatter or account
schema changes. Native-only UI changes use this skill plus its real app checks.

## Features

- [Metrics, quota, credits and reset details](pstack-metrics.md).
- [Compact/expanded presentation and display anchoring](pstack-presentation.md).
- [Freshness, pricing and unavailable/error states](pstack-freshness.md).
- [Refresh ownership and resource metrics](pstack-refresh.md).
- [Visibility, privacy, shortcut and process ownership](pstack-controls.md).

## Fixture and proof boundaries

Start reuses the dashboard skill's synthetic session/pricing seed, then runs the
real collector and independently checks Today/MTD tokens and exact MTD spend.
No DSH logs are seeded. Normalized subscription fixtures add GPT Pro, 73% weekly
remaining, 1,234 credits, four resets (two future, one non-expiring, one unknown).
Actual adapter display values are in pstack-island-run.json expectedFields.

The native warning fixture combines ten-minute-old usage, twenty-minute-old
quota, 12% remaining, incomplete pricing, an expired group and incomplete reset
details. The eleven-case adapter matrix separately proves baseline, stale usage,
stale quota, partial pricing, low/zero quota, signed-out, missing/malformed cache,
failed collection and combined warnings. Relative dates follow the local run.

The UI's collector subprocess is an explicitly labelled replay fixture. It
records every invocation and never falls back to live data/accounts. It does
not replace the packaged adapter/formatter/views, and it does not certify
five-minute wall-clock cadence. Real resource percentages are read-only.

## Evidence quality

Native smoke posts real app-local mouse-down/up events to measured SwiftUI
bounds for same-position expand/collapse and detail/button hit-testing; Escape
uses the native event loop. No external physical input is synthesized.
Hide/show and screen changes use production controller actions. Dashboard and
More callbacks are temporarily counters: button dispatch is proven, actual
browser navigation and popup interaction are not. Global shortcut registration
and parameters are checked, not physical key delivery or exclusive ownership.

The app captures only its own views. Inspect both cases' preview PNGs (and
expanded PNGs when unlocked); screenshots do not replace matrix/action proof.
The preview command permits locked-session skips but only claims rendering and
hidden startup; its interactivePassed:false must stay visible in any report.
The native command instead returns BLOCKED/exit 2 when input checks are skipped.
After cleanup check nested PNGs, smoke reports and pstack-cleanup.json survive.
Run fresh after a failed drive; never weaken an assertion or overwrite failure.

## Additional environment journeys

These remain named, unclaimed checks, not a blanket exclusion of the native app:
- External menu navigation (Display, Hide/Show, Quit): use native UI tools on
  the owned staged app in an active desktop. The existing smoke proves the
  underlying controller paths, not external menu hit-testing. Dashboard's normal
  callback opens live 8765, so actual browser navigation stays opt-in rather than
  being silently included in the isolated fixture run.
- Physical Command–Control–U: opt-in, unlocked desktop; do not synthesize a global
  key into the user's session without checking other registrations.
- Lock/unlock, sleep/wake, fast user switching, menu-bar auto-hide/full-screen
  Spaces, and live display unplug/replug: controlled hardware/session checks.
  Core privacy/presentation policies are covered by the native checks; the smoke
  verifies hidden startup only when actually run locked, marking input as skipped.
- Long reset-list scrolling and comprehensive accessibility: targeted follow-up
  native user-flow checks when those surfaces change.

An unavailable precondition is BLOCKED for that journey, never evidence of PASS.
