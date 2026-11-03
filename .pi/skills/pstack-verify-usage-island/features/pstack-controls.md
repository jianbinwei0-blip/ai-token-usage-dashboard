# Visibility, privacy and ownership

## Sub-features

Hide/show, original display restoration, shortcut registration/parameters,
locked/inactive-session startup policy, clean shutdown and installed-app safety.

## How to get to it (user POV)

Use More or the menu-bar control to hide/show; Command–Control–U toggles details.
The island hides while locked/inactive and returns after unlock. Quit stops only
this app; the dashboard/tmux integrations are independent.

## Driving it with pstack island harness

Start stages a uniquely identified signed app. Doctor requires private-home
diagnostics from its exact PID/build and unchanged installed binary, installed
process identity and preferences. Native smoke exercises hide/show/controller
paths and verifies shortcut registration, then restores compact/original screen.
The native checks test locked/inactive policy. Stop signals only the owned PID
after checking birth and executable; require scratch removed and PNGs retained.

## Gotchas

The installed island is never killed, restarted or reinstalled. Do not use
smoke.sh's default installed target for this fixture flow. Two app instances can
register the same shortcut, so registration is not exclusive ownership proof;
do not send the global shortcut during this run. Locked smoke output marks
interactive checks skipped and the wrapper returns BLOCKED. Actual lock/unlock
transitions and external menu clicks require controlled follow-up checks.
