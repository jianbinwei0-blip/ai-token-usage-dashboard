#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Do not synthesize shell defaults: they would override saved Finder settings.
exec "${AI_USAGE_PYTHON:-python3}" "$SCRIPT_DIR/start_local.py"
