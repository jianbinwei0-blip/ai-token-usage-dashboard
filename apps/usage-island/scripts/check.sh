#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
"$ROOT/scripts/prepare-resources.sh"
PYTHON="${AI_USAGE_PYTHON:-/opt/homebrew/bin/python3}"
if [[ ! -x "$PYTHON" ]]; then PYTHON="$(command -v python3)"; fi
PYTHONDONTWRITEBYTECODE=1 "$PYTHON" -m unittest discover -s Tests -v
"$ROOT/scripts/swift.sh" run UsageIslandCoreChecks
"$ROOT/scripts/swift.sh" build --product UsageIsland
