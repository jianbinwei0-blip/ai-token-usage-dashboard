# Native metrics and account details

## Sub-features

Today/MTD tokens, estimated MTD cost, plan, weekly-only quota/reset countdown,
credits, authoritative available resets, local expiration/time-zone labels,
explicit No expiration versus Not reported, and incomplete/expired detail notes.

## How to get to it (user POV)

Click either compact wing to open details. Read token/cost and account sections;
resets are read-only and never consumed.

## Driving it with pstack island harness

Run start, doctor, bridge, native. pstack-collector-proof.json checks real
aggregation against the independent fixture ledger. The bridge baseline checks
six exact display fields plus four resets and distinct unknown/non-expiring
semantics. Native baseline captures the actual expanded view; preview can render
the same SwiftUI details offscreen while locked. Warning capture
adds stale, expired and incomplete reset details. Inspect dates and clipping in
expanded.png and expanded-preview.png; the native core suite covers date/DST
formatting. Require all bridge assertions and native smoke checks to pass.

## Gotchas

Subscription/cache values are synthetic at an existing production boundary,
not a live plan/credit lookup. No credentials or invoice validation. Display
rounding is intentional (fixture MTD cost displays $2 despite exact ledger
spend $1.759); collector proof retains the exact value. This is usage valuation,
not a subscription charge. Date/time-zone output is local to the current run.
