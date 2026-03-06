# AI Token Usage Dashboard

A local HTML dashboard for multi-provider AI token usage that auto-recalculates on refresh from local session data.
Supports Codex, Claude, and PI Coding Agent.

## Dashboard Screenshot

![AI Token Usage Dashboard](docs/images/dashboard-screenshot.png)

## Features

- Provider toggle filtered to providers present on the system (`Combined`, `Codex`, `Claude`, `PI`)
- YTD stats cards (`YTD total`, `days`, `sessions`, `highest day`)
- Today + calendar-week rollups
- Daily breakdown table sorted by highest total tokens
- Horizontal bar chart (with rank, total tokens, and session count) above the table
- Auto-recalc on browser refresh and every 5 minutes via local `localhost` endpoint

## Project Structure

- `dashboard/index.html`: Dashboard UI
- `scripts/ai_usage_recalc_server.py`: Thin local HTTP recalc service (`/health`, `/recalc`)
- `scripts/dashboard_core/config.py`: Runtime config/env resolution
- `scripts/dashboard_core/collectors.py`: Codex/Claude/PI usage ingestion
- `scripts/dashboard_core/aggregation.py`: Daily aggregation + date window logic
- `scripts/dashboard_core/render.py`: HTML rewrite + dataset injection
- `scripts/dashboard_core/pipeline.py`: End-to-end recalc orchestration
- `scripts/run_local.sh`: Convenience launcher for local development
- `scripts/tests/test_harness_contracts.py`: Deterministic pipeline/harness invariants
- `launchd/*.plist.example`: Optional macOS LaunchAgent template
- `docs/harness-engineering-adoption.md`: Harness-engineering rationale + validation loop

## Requirements

- macOS or Linux
- Python 3.9+
- Local Codex session logs in `~/.codex/sessions`
- Local Claude project logs in `~/.claude/projects` (optional; dashboard still works without Claude data)
- Local PI agent state in `~/.pi/agent` (optional; dashboard still works without PI data)

## Quick Start

1. Start the local recalc service:

```bash
cd /path/to/ai-token-usage-dashboard
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

2. Open the dashboard:

```bash
open http://127.0.0.1:8765/
```

3. Refresh the page.
   - On refresh, the dashboard calls `/recalc` on the same localhost server
   - By default, the service rewrites `tmp/index.runtime.html` (untracked), so git-tracked `dashboard/index.html` stays unchanged
   - The page reloads with fresh values after recalc completes

## Configuration

Environment variables:

- `AI_USAGE_SERVER_HOST` (default: `127.0.0.1`)
- `AI_USAGE_SERVER_PORT` (default: `8765`)
- `AI_USAGE_CODEX_SESSIONS_ROOT` (default: `~/.codex/sessions`)
- `AI_USAGE_CLAUDE_PROJECTS_ROOT` (default: `~/.claude/projects`)
- `AI_USAGE_PI_AGENT_ROOT` (default: `~/.pi/agent`)
- `AI_USAGE_DASHBOARD_HTML` (default via `scripts/run_local.sh`: `<repo>/tmp/index.runtime.html`, seeded from `<repo>/dashboard/index.html`)

## Optional: Run as LaunchAgent (macOS)

1. Copy and edit the template:

```bash
cp launchd/com.user.ai-token-usage-dashboard-recalc.plist.example \
  ~/Library/LaunchAgents/com.user.ai-token-usage-dashboard-recalc.plist
```

2. Replace placeholder absolute paths.

3. Load it:

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.user.ai-token-usage-dashboard-recalc.plist
launchctl kickstart -k gui/$(id -u)/com.user.ai-token-usage-dashboard-recalc
```

4. Verify:

```bash
curl http://127.0.0.1:8765/health
```

## Notes

- The dashboard is designed for local use and reads local AI provider session logs from Codex, Claude, and PI when present.
- Claude request usage is deduplicated by `(sessionId, requestId)` and keeps the highest observed `output_tokens` for the request.
- Claude total token metric is computed as `input_tokens + cache_creation_input_tokens + cache_read_input_tokens + output_tokens`.
- PI usage is read from `~/.pi/agent/sessions/**/*.jsonl` and sums assistant `usage.totalTokens` while counting unique session IDs per day.
- No third-party services are required.

## Validation

Run the full local harness checks:

```bash
python3 -m unittest discover -s scripts/tests
```

## Use as a Codex Skill

This repo is skill-ready and includes:
- `SKILL.md` (trigger metadata + workflow instructions)
- `agents/openai.yaml` (skill UI metadata)

### Local install (from filesystem)

```bash
mkdir -p ~/.codex/skills
ln -sfn /absolute/path/to/ai-token-usage-dashboard \
  ~/.codex/skills/ai-token-usage-dashboard
```

Restart Codex after installation.

### Invoke in Codex

Ask with the skill name, for example:

```text
Use $ai-token-usage-dashboard to recalc and update my usage dashboard.
```
