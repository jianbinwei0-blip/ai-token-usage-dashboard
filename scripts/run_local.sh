#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOURCE_DASHBOARD_HTML="$REPO_ROOT/dashboard/index.html"
RUNTIME_DASHBOARD_HTML="$REPO_ROOT/tmp/index.runtime.html"

if [[ -z "${AI_USAGE_DASHBOARD_HTML:-}" ]]; then
  export AI_USAGE_DASHBOARD_HTML="$RUNTIME_DASHBOARD_HTML"
  /usr/bin/python3 "$SCRIPT_DIR/seed_runtime_html.py" "$SOURCE_DASHBOARD_HTML" "$AI_USAGE_DASHBOARD_HTML"
else
  export AI_USAGE_DASHBOARD_HTML
fi

export AI_USAGE_CODEX_SESSIONS_ROOT="${AI_USAGE_CODEX_SESSIONS_ROOT:-$HOME/.codex/sessions}"
export AI_USAGE_CLAUDE_PROJECTS_ROOT="${AI_USAGE_CLAUDE_PROJECTS_ROOT:-$HOME/.claude/projects}"
export AI_USAGE_SERVER_HOST="${AI_USAGE_SERVER_HOST:-127.0.0.1}"
export AI_USAGE_SERVER_PORT="${AI_USAGE_SERVER_PORT:-8765}"

exec /usr/bin/python3 "$SCRIPT_DIR/ai_usage_recalc_server.py"
