#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-release}"
"$ROOT/scripts/swift.sh" build -c "$CONFIGURATION" --product UsageIsland
BIN_DIR="$("$ROOT/scripts/swift.sh" build -c "$CONFIGURATION" --show-bin-path)"
mkdir -p "$ROOT/dist"
STAGING="$(mktemp -d "$ROOT/dist/package.XXXXXX")"
trap 'rm -rf "$STAGING"' EXIT
APP="$STAGING/AI Usage Island.app"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$ROOT/packaging/Info.plist" "$APP/Contents/Info.plist"
cp "$BIN_DIR/UsageIsland" "$APP/Contents/MacOS/UsageIsland"
ditto "$BIN_DIR/AIUsageIsland_UsageIslandApp.bundle" "$APP/Contents/Resources/AIUsageIsland_UsageIslandApp.bundle"
ditto "$ROOT/LICENSES" "$APP/Contents/Resources/LICENSES"
cp "$ROOT/../../LICENSE" "$APP/Contents/Resources/LICENSES/AIUsageIsland.txt"
chmod +x "$APP/Contents/MacOS/UsageIsland"
/usr/bin/codesign --force --sign - --options runtime "$APP"
/usr/bin/codesign --verify --strict "$APP"
TARGET="$ROOT/dist/AI Usage Island.app"
if [[ -e "$TARGET" ]]; then rm -rf "$TARGET"; fi
mv "$APP" "$TARGET"
printf '\nBuilt and locally signed: %s\n' "$TARGET"
