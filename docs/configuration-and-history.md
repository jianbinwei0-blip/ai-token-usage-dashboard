# Shared configuration and durable observed history

All presentation surfaces use one canonical collector and price catalog. Settings discovery and reads do not collect usage or create files.

## Settings v1

Default file:

- macOS: `~/Library/Application Support/ai-token-usage-dashboard/settings.json`
- Linux: `${XDG_CONFIG_HOME:-~/.config}/ai-token-usage-dashboard/settings.json`

Select another file explicitly with `AI_USAGE_SETTINGS_FILE`. A missing default is normal; an explicitly selected missing, invalid or incompatible file is an actionable error, not permission to fall back silently.

Example (create manually, replacing the runtime path):

```json
{
  "version": 1,
  "runtime_dir": "/path/to/ai-token-usage-dashboard",
  "dashboard_url": "http://127.0.0.1:9321/",
  "pricing_file": null,
  "history_file": "~/Library/Application Support/ai-token-usage-dashboard/history.v1.json",
  "codex_sessions_root": "~/.codex/sessions",
  "claude_projects_root": "~/.claude/projects",
  "pi_agent_root": "~/.pi/agent",
  "dsh_home": "~/.dsh"
}
```

Only `version` is required. `history_file` in the example is macOS-specific; omit it to use the platform default. Paths must be absolute or start with `~/`; relative paths, `$VARIABLE` expansion, unknown/duplicate keys and unsupported versions are rejected. `pricing_file: null` explicitly selects built-in prices. URL destinations must be credential-free local HTTP(S) addresses, without query or fragment. The server itself is HTTP; HTTPS/path-prefix destinations require a separately configured local proxy.

Precedence is explicit environment > saved settings > defaults. Existing source-root variables continue to work. New equivalents are `AI_USAGE_DASHBOARD_DIR`, `AI_USAGE_DASHBOARD_URL` and `AI_USAGE_HISTORY_FILE`. An explicit `AI_USAGE_PRICING_FILE=''` selects built-ins and is not replaced by the island bridge. `AI_USAGE_SERVER_HOST` / `AI_USAGE_SERVER_PORT` override binding and, unless a URL environment override is explicit, update the browser destination. Finder cannot inherit arbitrary shell-only environment values; save settings when Finder and terminal must agree.

The native build bundles the canonical settings reader and formatter verbatim. Its dashboard action resolves the validated saved URL, not a hard-coded port. Selected external runtimes are not automatically upgraded; use doctor to detect drift before changing them.

## Read-only doctor

```bash
python3 scripts/usage_doctor.py
python3 scripts/usage_doctor.py --json
```

Doctor reports effective paths and their existence/readability, source/runtime drift, schema compatibility, collection freshness, and recovery steps. It does not output usage amounts, source contents, credentials, account identity or raw settings; it never collects, contacts an account, writes settings or repairs history automatically. Effective root paths are intentionally visible in this explicit diagnostic command. Metadata reads are bounded, so a large history may be reported as too large to validate; use full backup validation separately. Exit codes: 0 healthy, 1 setup findings, 2 invalid settings.

## Durable history versus disposable output

Default durable file:

- macOS: `~/Library/Application Support/ai-token-usage-dashboard/history.v1.json`
- Linux: `${XDG_DATA_HOME:-~/.local/share}/ai-token-usage-dashboard/history.v1.json`

`AI_USAGE_HISTORY_FILE` overrides the location. `AI_USAGE_PARSE_CACHE_FILE` remains a deprecated explicit-path alias; the new variable wins if both are supplied. The file stores versioned observed contributions and parse offsets, not a database or a second accounting system. It contains private source locations/session identities internally, so backups should remain local and private. Files are created owner-readable/writable. Published dataset project/session labels and source descriptions do not expose those paths or raw identities.

On first collection at the new default, if history is missing and `<effective runtime>/tmp/recalc_parse_cache.json` exists, it is validated and copied. A timestamped backup is created beside the legacy file; the original is not removed. Existing v4/v5 records are accepted without deleting sessions whose source logs no longer exist. A corrupt, unreadable or incompatible file prevents collection from saving a replacement. Repair/restore explicitly rather than deleting it.

Generated `tmp/index.runtime.html`, its `.dataset` materialization, tmux display snapshots and timing logs are disposable. Clearing those does not clear retained history. Do not clear an explicitly configured history merely because its filename still says “cache”.

## Validate, export, restore

These commands do not scan source logs or call accounts. Quote paths containing spaces.

```bash
# Validate without writing.
python3 scripts/usage_history.py validate /private/backup.json

# Export configured history to a NEW file; refuses to overwrite a destination.
python3 scripts/usage_history.py export /private/new-backup.json

# Stop all collectors first. Restore to an absent destination.
python3 scripts/usage_history.py --history-file /private/history.json restore /private/backup.json

# Explicit replacement first preserves the previous file in a timestamped backup.
python3 scripts/usage_history.py --history-file /private/history.json restore /private/backup.json --replace
```

Restore validates before promotion and uses the same accounting lock as web, tmux and island collection. Back up first and stop collectors: if source logs have grown since a backup, normal collection must observe the new records afterward. Replacement is intentional, not an automatic merge of conflicting history. To deliberately start a new history, stop every collector, export the old file, and explicitly select a new history path. Logs deleted before initial observation cannot be recovered.

## Cost and identity semantics

- Dollars are estimated usage valuation, not an invoice, hard billing cap, or extra subscription charge. Native-reported amounts retain precedence. Unknown models retain tokens and an explicit unpriced/partial state.
- Each daily contribution carries cost-source coverage. Day/model, project/session, range explanations and terminal/native provenance sum the canonical amounts, without independently pricing them.
- PI headers with cwd are converted to anonymous project identifiers. Supported sessions use anonymous session identifiers. No transcripts or raw cwd/session identifiers are exported in those views. Existing retained rows without metadata stay Unassigned / legacy; live logs are reparsed to add metadata without invalidating deleted-log history.
- Models are sortable by estimated cost or tokens. PI sessions are ordered by cost inside an accessible native disclosure. Other collectors remain visible but do not claim project attribution.
- Across days, session totals count session-days. A session with multiple models can occur in multiple model buckets; those are not additive globally unique session counts.
- Calendar/rolling ranges end at the current local day. All Available ends at the last observed day. Matched comparisons show both windows and withhold percentages when the prior coverage is unavailable. Month/year comparisons cap at the previous period's end when necessary.
- Legacy deleted Codex records keep their original date/model allocation with a warning. Accurate event allocation cannot be reconstructed from an old singleton total. Ambiguous untimestamped/reordered usage events cannot yield guaranteed request identities.

## Native installation

Build/check/package before installing. The installer validates all arguments first, stages a private candidate, verifies its structure/signature, and only then backs up and promotes the bundle. Copy, verification, promotion and launch failures roll back to the previous bundle; rollback failure retains the named backup and prints recovery steps. A failed first install removes only its failed candidate. It never resets history/settings or enables login startup. A crash, SIGKILL or filesystem failure may require manual recovery of a stale install lock/backup. Automated tests use temporary fake bundles and injected failures, not the installed app.

## Local service boundary

Use the served local URL, not `file://`, to refresh. JSON routes no longer allow arbitrary origins, unrecognized Host headers and cross-site requests are refused, and responses prevent framing/sniffing. This is local-service hardening, not authentication or authorization for remote hosting. Do not expose the server publicly. Concurrent refreshes serialize at the shared history lock; HTML and history promotion are atomic.
