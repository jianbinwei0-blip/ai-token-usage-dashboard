#!/bin/bash
set -euo pipefail

usage() {
  printf 'Usage: %s [--no-open]\nStages and verifies a replacement; preserves the previous app on failure.\n' "$0"
}
# Validate every argument before mkdir, staging, replacement or launch.
if [[ $# -gt 1 ]]; then usage >&2; exit 2; fi
case "${1:-}" in
  "") if [[ $# -ne 0 ]]; then usage >&2; exit 2; fi; LAUNCH=1 ;;
  --no-open) LAUNCH=0 ;;
  --help|-h) usage; exit 0 ;;
  *) usage >&2; printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="$ROOT/dist/AI Usage Island.app"
APPLICATIONS="$HOME/Applications"
DESTINATION="$APPLICATIONS/AI Usage Island.app"
BACKUPS="$HOME/Library/Application Support/AIUsageIsland/Backups"
LOCK="$APPLICATIONS/.AIUsageIsland-install.lock"
if [[ ! -x "$SOURCE/Contents/MacOS/UsageIsland" || ! -f "$SOURCE/Contents/Info.plist" ]]; then
  printf 'Run ./scripts/package.sh first; the source bundle is incomplete.\n' >&2
  exit 1
fi
for TOOL in ditto codesign pgrep; do
  if ! command -v "$TOOL" >/dev/null; then printf 'Required tool missing: %s\n' "$TOOL" >&2; exit 1; fi
done
if [[ -L "$DESTINATION" || ( -e "$DESTINATION" && ! -d "$DESTINATION" ) ]]; then
  printf 'Refusing to replace a symlink or non-directory at: %s\n' "$DESTINATION" >&2
  exit 1
fi
require_stopped_app() {
  local RESULT
  if pgrep -f "$DESTINATION/Contents/MacOS/UsageIsland" >/dev/null; then
    printf 'Quit AI Usage Island from its menu before reinstalling, then retry.\n' >&2
    return 1
  else
    RESULT=$?
    if [[ "$RESULT" -ne 1 ]]; then
      printf 'Cannot safely determine whether the app is running; installation was stopped.\n' >&2
      return 1
    fi
  fi
}
require_stopped_app

mkdir -p "$APPLICATIONS"
if ! mkdir "$LOCK" 2>/dev/null; then
  printf 'Another install may be active. If not, remove the empty install lock: %s\n' "$LOCK" >&2
  exit 1
fi
STAGING=""
BACKUP_DIRECTORY=""
BACKUP=""
PROMOTION_ATTEMPTED=0
COMMITTED=0
cleanup() {
  local STATUS=$?
  trap - EXIT HUP INT TERM
  if [[ "$COMMITTED" -ne 1 ]]; then
    if [[ "$PROMOTION_ATTEMPTED" -eq 1 && ( -e "$DESTINATION" || -L "$DESTINATION" ) ]]; then
      if ! rm -rf "$DESTINATION"; then
        printf 'Could not remove failed replacement: %s\n' "$DESTINATION" >&2
        STATUS=1
      fi
    fi
    # Check the backup itself, not just a flag: signals can arrive immediately
    # after the old bundle's rename and before the next shell statement.
    if [[ -n "$BACKUP" && -d "$BACKUP" ]]; then
      if [[ ! -e "$DESTINATION" && ! -L "$DESTINATION" ]] && mv "$BACKUP" "$DESTINATION"; then
        printf 'Previous app restored.\n' >&2
      else
        printf 'Automatic rollback failed. Previous app is preserved at: %s\nRestore it to: %s\n' "$BACKUP" "$DESTINATION" >&2
        STATUS=1
      fi
    fi
  fi
  if [[ -n "$STAGING" ]]; then
    if ! rm -rf "$STAGING"; then printf 'Remove leftover staging directory: %s\n' "$STAGING" >&2; STATUS=1; fi
  fi
  if [[ -n "$BACKUP_DIRECTORY" && -d "$BACKUP_DIRECTORY" ]]; then
    rmdir "$BACKUP_DIRECTORY" 2>/dev/null || true
  fi
  if ! rmdir "$LOCK"; then printf 'Remove leftover install lock: %s\n' "$LOCK" >&2; STATUS=1; fi
  exit "$STATUS"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Same-filesystem staging makes promotion a rename, never a partial bundle copy.
STAGING="$(mktemp -d "$APPLICATIONS/.AIUsageIsland-install.XXXXXX")"
STAGED="$STAGING/AI Usage Island.app"
ditto "$SOURCE" "$STAGED"
if [[ ! -x "$STAGED/Contents/MacOS/UsageIsland" || ! -f "$STAGED/Contents/Info.plist" ]]; then
  printf 'Staged app is incomplete; previous installation was not changed.\n' >&2
  exit 1
fi
codesign --verify --strict "$STAGED"
# Do not replace an app launched while the copy/signature check was in progress.
require_stopped_app
if [[ -L "$DESTINATION" || ( -e "$DESTINATION" && ! -d "$DESTINATION" ) ]]; then
  printf 'Destination changed during staging; refusing replacement.\n' >&2
  exit 1
fi
if [[ -d "$DESTINATION" ]]; then
  mkdir -p "$BACKUPS"
  BACKUP_DIRECTORY="$(mktemp -d "$BACKUPS/install-$(date +%Y%m%d-%H%M%S).XXXXXX")"
  BACKUP="$BACKUP_DIRECTORY/AI Usage Island.app"
  mv "$DESTINATION" "$BACKUP"
fi
PROMOTION_ATTEMPTED=1
mv "$STAGED" "$DESTINATION"
codesign --verify --strict "$DESTINATION"
if [[ "$LAUNCH" -eq 1 ]]; then open "$DESTINATION"; fi
COMMITTED=1
printf 'Installed: %s\nLogin startup is unchanged.\n' "$DESTINATION"
if [[ -n "$BACKUP" ]]; then printf 'Previous app retained: %s\n' "$BACKUP"; fi
