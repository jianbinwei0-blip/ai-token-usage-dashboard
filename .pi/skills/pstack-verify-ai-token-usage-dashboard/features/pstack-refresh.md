# Refresh and usable data

A user opens or reloads the dashboard and sees current local usage, healthy
refresh timing, and a real rendered page rather than stale template values.

## Sub-features

- pstack-load opens the real dashboard against synthetic local sessions.
- pstack-refresh recalculates through the real HTTP route and reloads the page.
- pstack-timing exposes Server-Timing and visible refresh state.
- pstack-runtime preserves the tracked HTML template while writing owned caches.

## How to get to it (user POV)

- Open the URL returned by start, or reload that page in the isolated browser.
- Call GET /health or GET /recalc on that same owned instance for API diagnosis.

## Driving it with pstack dashboard harness

Preconditions: Launch completed, RUN is the returned run directory, and doctor
reports owned sources and an unchanged product build.

- **Doctor.** Run `python3 "$H" doctor "$RUN"`. Expect ok:true and owned paths.
- **Recalculate.** Run `python3 "$H" api "$RUN"`. Expect HTTP 200/ok:true,
  exact fixture totals, complete synthetic pricing, timings, one dataset tag,
  and a correct 404 for the deliberate unknown-route probe.
- **Open/reload.** Run `node "$W" "$RUN"`. The real page makes /recalc
  requests and reloads. Require successful timing-bearing responses, actual
  visible metrics, and no JS exceptions in the tested flow.
- **Evidence.** Inspect pstack-api*.json and the browser pass's
  pstack-browser-results.json and pstack-dashboard-combined.png.
  Confirm the original dashboard source hash remains unchanged at cleanup.

## Gotchas

- Port 8765 belongs to the live user instance; never use it for verification.
- The fresh-snapshot message is transient: scheduling immediately replaces it
  with next-refresh status. The harness waits for an actual recalc/navigation
  cycle, populated controls, and a non-placeholder last-refresh timestamp.
- A process or port alone is not readiness. Start first renders the real HTML
  through the production CLI and validates actual synthetic aggregation.
- API top-level totals are year-to-date; All Available can include an older year.
