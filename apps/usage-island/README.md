# AI Usage Island

A small native macOS companion for [AI Token Usage Dashboard](../../README.md), built with DynamicNotchKit. It reuses the repository's accounting and canonical formatter rather than adding a second set of token parsers or prices. SketchyBar is not required.

## Use

- Click the floating summary, either notch wing, or the menu-bar chart icon to expand.
- **Command–Control–U (`⌃⌘U`)** toggles details globally. **Escape** collapses the focused panel.
- Right-click the menu-bar icon, or use **…** in the detail panel, for options.
- **Display** selects the main display automatically or a specific connected screen. A disconnected selection falls back to the main display.
- **Refresh** updates the view using the collector's five-minute cache. It does not force an extra collection.
- **Dashboard** opens the existing dashboard at http://127.0.0.1:8765.
- **Hide island** leaves the menu-bar control available. **Quit** stops the app.

On a non-notched display the summary is a floating pill. On a notched MacBook, Today and Week left sit on opposite sides of the hardware cutout. The island hides while locked or on an inactive user session, including when first launched while locked, and returns after unlock.

Details show plan, weekly remaining quota, weekly reset countdown, credits, available resets and their expiration dates, Today/MTD tokens, estimated MTD cost, collection times, RAM and home-volume disk usage. Unavailable fields are not fabricated. Estimated cost is a usage valuation, not an extra subscription charge.

Reset expiration dates include the local time zone. Resets sharing an expiration are grouped; an explicit non-expiring reset shows **No expiration**, while missing dates show **Not reported**. The server's available count remains authoritative even when it supplies only part of the detail list. Cached or already-expired entries are labelled rather than silently treated as current. This display is read-only and never consumes a reset.

## Installed location

`~/Applications/AI Usage Island.app`

The app is locally ad-hoc signed, not Developer ID notarized. It does not require Accessibility, Screen Recording, or agent-hook installation. No telemetry is added.

The installer does not change login startup. To start the island automatically, add this app in macOS System Settings → General → Login Items. The web dashboard and tmux status integration remain independent. A legacy SketchyBar installation can be removed without affecting the island.

## Build, check, install

Requirements: macOS 14+, Swift 6 Command Line Tools or Xcode, Python 3, and the existing AI dashboard runtime. JetBrains Mono is used when installed, with native font fallback.

From the repository root:

```sh
cd apps/usage-island
./scripts/check.sh
./scripts/package.sh
./scripts/install.sh
```

Checks and builds first run `scripts/prepare-resources.sh`, which copies the repository's canonical formatter into the app's generated resources. That copy is gitignored; the installed bundle includes it and does not need the checkout.

Quit the installed app before reinstalling. Previous installed versions are retained under `~/Library/Application Support/AIUsageIsland/Backups/`.

`scripts/swift.sh` selects an installed SDK matching the current macOS major version, with `AI_USAGE_SDK` available as an override. On this machine it uses macOS 26.5 rather than the CLT's default macOS 27 SDK.

DynamicNotchKit is pinned and vendored at `cd0b3e52d537db115ad3a9d89601f20e0bee8d27`. Two small macro compatibility changes avoid requiring full Xcode. See `Vendor/DynamicNotchKit/UPSTREAM.md`. The MIT license is retained.

The native checks use a standalone executable because this CLT installation has neither XCTest nor Swift Testing. They exercise the real core module and subprocess runner, not copies of their implementation.

## Data and refresh ownership

- AI runtime: `~/.local/share/ai-token-usage-dashboard-daemon`
- Shared AI cache: `tmp/tmux_status.json` within that runtime
- Existing prices: `~/.config/ai-token-usage-dashboard/pricing.json`
- Formatter: repository-root `scripts/dashboard_core/tmux_status.py`, copied into the bundle at build time
- Resource formulas: bundled `Sources/UsageIslandApp/Resources/Bridge/status_resources.sh`; no runtime dependency on tmux or SketchyBar configuration

The app reads cached data immediately and invokes the existing collector every 30 seconds. That collector shares the same file lock and only recalculates on five-minute boundaries. Collection therefore continues without SketchyBar, while two running displays do not create two accounting pipelines. RAM refreshes every ten seconds; the resource helper caches disk usage for five minutes in this app's own cache directory.

The app preserves the formatter's weekly-only quota selection, credit rules, abbreviated numbers, model-specific quota fields and incomplete-pricing markers. Stale usage and stale account quotas are labelled separately. Helper failure retains prior values only with an explicit warning. Timeouts terminate the collector process group.

Reset details require subscription snapshot schema v3. If using a separate background runtime, deploy the matching `scripts/dashboard_core/chatgpt_subscription.py` and `scripts/render_tmux_status.py` updates there as well, preserving its caches and pricing. The next collection upgrades older snapshots; a failed lookup retains cached counts without inventing expiration dates.

The runtime is outside Documents to support locked-session access. The installed app bundles its adapter and formatter and does not depend on the source checkout or a running tmux session. If the daemon runtime is absent, the adapter can fall back to the existing Documents dashboard checkout, with that location's usual macOS access limitations.

Overrides: `AI_USAGE_DASHBOARD_DIR`, `AI_USAGE_CACHE_FILE`, `AI_USAGE_PYTHON`, and developer-only `AI_USAGE_BRIDGE_DIR`. Finder launches do not inherit arbitrary shell exports.

## Diagnostics and UI smoke checks

Presentation state: `~/Library/Caches/AIUsageIsland/presentation.json`. It records status, display selection, and the registered shortcut's name/key code/modifiers, not transcripts or credentials.

```sh
./scripts/smoke.sh
```

The opt-in smoke check restarts only this app, tests compact/expanded presentation, keyboard focus, Escape, hide/show, and an attached physical notch when available. It returns to the original display and leaves the app compact. It captures only the app's own views, not the desktop. It also renders an offscreen expanded-view preview without creating a window or changing focus. While locked, it verifies cache loading and hidden-window privacy and explicitly records interactive checks as skipped. Screenshots can contain usage amounts, so `.artifacts/` is gitignored.

See `VALIDATION.md` for the checks performed and remaining limitations.

## Source map

- `Sources/UsageIslandCore/`: bridge contract, freshness and subprocess lifecycle
- `Sources/UsageIslandApp/`: SwiftUI views, AppKit controller, global shortcut and refresh loops
- `Sources/UsageIslandApp/Resources/Bridge/`: Python adapter, system-resource helper and generated canonical formatter
- `Tests/`: native and Python checks
- `packaging/`, `scripts/`: reproducible local packaging, signing and installation
