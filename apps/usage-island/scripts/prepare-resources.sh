#!/bin/bash
set -euo pipefail
APP_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$APP_ROOT/../.." && pwd)"
SOURCE="$REPO_ROOT/scripts/dashboard_core/tmux_status.py"
DESTINATION="$APP_ROOT/Sources/UsageIslandApp/Resources/Bridge/dashboard_status_formatter.py"
if [[ ! -f "$SOURCE" ]]; then
  printf 'Canonical formatter missing: %s\nBuild the app from the ai-token-usage-dashboard repository.\n' "$SOURCE" >&2
  exit 1
fi
# Keep one maintained formatter, but a self-contained installed app. Avoid
# touching the resource timestamp when its contents have not changed.
if ! cmp -s "$SOURCE" "$DESTINATION"; then
  cp "$SOURCE" "$DESTINATION"
fi
