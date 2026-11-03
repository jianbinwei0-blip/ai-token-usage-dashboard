# Native presentation and clicks

## Sub-features

Top-centre notch-style compact wings, expanded details/focus, physical cutout
detection, same-position toggles for both wings, detail-text/background/metric
collapse, collapse button, Escape, and return to the original display.

## How to get to it (user POV)

Click Today or Week left. Click the identical original summary location again
to collapse; alternatively click detail text/empty space or press Escape.
Select another connected screen from More → Display.

## Driving it with pstack island harness

Run native after doctor. Both baseline and warnings use the existing real-app
smoke with measured wing/detail geometry and native mouse events. It captures
compact/expanded views on every connected display, verifies top-centre anchoring,
focus and Escape, and restores the original display compact even on failure.
Require smoke passed:true, skipped:[], same-position and Escape check entries,
and per-display PNG evidence. Inspect original and hardware-notch captures.

## Gotchas

Display selection in smoke uses the controller action, not external menu clicks.
Only connected displays are proven; no unplug/replug is simulated. Own-view
captures cannot prove absence of overlap from another app's crowded menu bar.
Do not count native app-local input as external physical mouse/key delivery.
