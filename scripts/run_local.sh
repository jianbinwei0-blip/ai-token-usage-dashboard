#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export CODEX_USAGE_DASHBOARD_HTML="${CODEX_USAGE_DASHBOARD_HTML:-$REPO_ROOT/dashboard/index.html}"
export CODEX_USAGE_SESSIONS_ROOT="${CODEX_USAGE_SESSIONS_ROOT:-$HOME/.codex/sessions}"
export CODEX_USAGE_CLAUDE_PROJECTS_ROOT="${CODEX_USAGE_CLAUDE_PROJECTS_ROOT:-$HOME/.claude/projects}"
export CODEX_USAGE_SERVER_HOST="${CODEX_USAGE_SERVER_HOST:-127.0.0.1}"
export CODEX_USAGE_SERVER_PORT="${CODEX_USAGE_SERVER_PORT:-8765}"

exec /usr/bin/python3 "$SCRIPT_DIR/codex_usage_recalc_server.py"
