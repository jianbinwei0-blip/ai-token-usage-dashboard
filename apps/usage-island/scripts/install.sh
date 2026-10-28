#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="$ROOT/dist/AI Usage Island.app"
DESTINATION="$HOME/Applications/AI Usage Island.app"
if [[ ! -x "$SOURCE/Contents/MacOS/UsageIsland" ]]; then
  printf 'Run ./scripts/package.sh first.\n' >&2
  exit 1
fi
if pgrep -f "$DESTINATION/Contents/MacOS/UsageIsland" >/dev/null; then
  printf 'Quit AI Usage Island from its menu before reinstalling.\n' >&2
  exit 1
fi
mkdir -p "$HOME/Applications"
if [[ -d "$DESTINATION" ]]; then
  BACKUPS="$HOME/Library/Application Support/AIUsageIsland/Backups"
  mkdir -p "$BACKUPS"
  mv "$DESTINATION" "$BACKUPS/AI Usage Island-$(date +%Y%m%d-%H%M%S).app"
fi
ditto "$SOURCE" "$DESTINATION"
codesign --verify --strict "$DESTINATION"
printf 'Installed: %s\nLogin startup is unchanged.\n' "$DESTINATION"
case "${1:-}" in
  "") open "$DESTINATION" ;;
  --no-open) ;;
  *) printf 'Installed, but unknown launch option: %s\\n' "$1" >&2; exit 2 ;;
esac
