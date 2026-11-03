# Freshness and failure honesty

## Sub-features

Usage stale independently of quota stale; incomplete-pricing asterisk; low and
exhausted weekly quota; signed-out omission; missing/malformed data as unavailable
rather than zero; failed refresh as an error; cached/expired reset warnings.

## How to get to it (user POV)

Open details and read the status line, quota freshness, cost qualifier and reset
notes. Unavailable metrics must not be fabricated as fresh values or zeros.

## Driving it with pstack island harness

Run bridge for the eleven independent cases. It launches the real packaged
adapter subprocess for each cache, invokes the production refresh boundary,
and retains actual payloads in pstack-bridge-matrix.json. The native warnings
pass (or preview while locked) renders combined stale usage/quota, partial
pricing and reset warnings.
Doctor matches actual app health/status/timestamp to the fixture. Inspect the
warning expanded capture, not just the adapter output.

## Gotchas

Only baseline and combined warnings get full UI smoke/captures; the other nine
cases are adapter-level proof plus the existing real core checks, not nine UI
journeys. Missing/failed data are not replaced by the real user's cache.
Elapsed-time fixtures prove policy/rendering, not a ten/fifteen-minute wait.
