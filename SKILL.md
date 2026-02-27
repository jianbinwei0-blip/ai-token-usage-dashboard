---
name: codex-token-usage-dashboard
description: Build, refresh, and customize a local Codex+Claude token usage dashboard served on 127.0.0.1:8765 from ~/.codex/sessions and ~/.claude/projects data. Use when the user asks for provider-specific token usage updates, chart/stat card changes, date-range/preset behavior, refresh cadence updates, or recalc-service troubleshooting.
---

# Codex Token Usage Dashboard

## Overview

Use this skill to maintain a local token-usage dashboard that:
- Reads session data from `~/.codex/sessions` and `~/.claude/projects`
- Recalculates stats/tables via a local Python service
- Serves the dashboard at `http://127.0.0.1:8765/`

Primary files:
- `dashboard/index.html`: dashboard UI and client-side behavior
- `scripts/codex_usage_recalc_server.py`: `/health`, `/recalc`, and HTML rewrite logic
- `scripts/run_local.sh`: local launcher

## Quick Commands

- Start local service:
  - `./scripts/run_local.sh`
- Health check:
  - `curl -s http://127.0.0.1:8765/health`
- Recalculate immediately:
  - `curl -s http://127.0.0.1:8765/recalc`
- Open dashboard:
  - `open http://127.0.0.1:8765/`

## Working Workflow

1. Confirm service is running (`/health`).
2. Trigger `/recalc` before reviewing token numbers.
3. Make UI changes in `dashboard/index.html`.
4. Make aggregation/range logic changes in `scripts/codex_usage_recalc_server.py` only when server-side totals must change.
5. Validate with:
   - `curl -s http://127.0.0.1:8765/recalc` returns JSON containing `"ok": true`
   - Dashboard reflects requested UI/number updates after refresh.

## Data/Behavior Notes

- `/recalc` rewrites the configured dashboard HTML file in place.
- Defaults:
  - Host: `127.0.0.1`
  - Port: `8765`
  - Codex sessions root: `~/.codex/sessions`
  - Claude projects root: `~/.claude/projects`
  - Dashboard HTML: `dashboard/index.html`
- Override with env vars:
  - `CODEX_USAGE_SERVER_HOST`
  - `CODEX_USAGE_SERVER_PORT`
  - `CODEX_USAGE_SESSIONS_ROOT`
  - `CODEX_USAGE_CLAUDE_PROJECTS_ROOT`
  - `CODEX_USAGE_DASHBOARD_HTML`

## Guardrails

- Preserve existing functionality unless the user explicitly requests behavior changes.
- Keep number formatting user-friendly (`toLocaleString("en-US")` for display).
- Prefer small, targeted UI edits over broad redesigns for dashboard maintenance requests.
