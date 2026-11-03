# Refresh and resources

## Sub-features

Cached startup, manual Refresh without collapse, periodic bridge ownership,
real RAM/disk values, bounded subprocesses, and private cache side effects.

## How to get to it (user POV)

Expand details, click Refresh and observe the panel remains expanded. Read
Collected, next scheduled time, RAM and Disk. Refresh respects the collector's
five-minute cache instead of forcing extra account collection.

## Driving it with pstack island harness

Start runs check.sh, including subprocess timeout/group-cleanup and shared-cache
tests, then the real collector against private sessions/prices. Bridge runs the
real packaged RAM/disk helper, checks valid percentages and its private disk
cache. Native smoke clicks the actual Refresh button and observes isRefreshing.
pstack-fixture-collector.jsonl records the refresh subprocess arguments. Require
real collector proof, matrix resources, native button checks and cleanup proof.

## Gotchas

UI refresh uses fixture replay at the collector subprocess boundary; the real
collector was proved separately, not during that replay. The existing suite
tests cadence/cache rules, but this flow does not certify a five-minute timer.
Dashboard and More dispatch counters do not prove real browser/menu navigation.
Resource amounts vary naturally and are not frozen synthetic metrics.
