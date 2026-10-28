#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
"$ROOT/scripts/prepare-resources.sh"
if [[ -n "${AI_USAGE_SDK:-}" ]]; then
  SDK="$AI_USAGE_SDK"
else
  MAJOR="$(sw_vers -productVersion | cut -d. -f1)"
  DEVELOPER="$(xcode-select -p)"
  SDK="$DEVELOPER/SDKs/MacOSX$MAJOR.sdk"
  if [[ ! -d "$SDK" ]]; then
    SDK="$(xcrun --sdk macosx --show-sdk-path)"
  fi
fi
if [[ ! -d "$SDK" ]]; then
  printf 'SDK not found: %s\n' "$SDK" >&2
  exit 1
fi
# Native SwiftPM avoids requiring Xcode's build-service and test runner.
COMMAND="$1"
shift
exec swift "$COMMAND" --sdk "$SDK" --build-system native "$@"
