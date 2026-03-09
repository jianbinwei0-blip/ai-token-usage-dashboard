#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head><meta charset=\"UTF-8\" /><title>Benchmark Fixture</title></head>
<body>
  <main>
    <section class=\"panel range-panel\">
      <div class=\"usage-chart-control-group\">
        <label for=\"usageProvider\">Provider</label>
        <select id=\"usageProvider\" class=\"usage-chart-sort\" aria-label=\"Usage provider\">
          <option value=\"combined\">Combined</option>
          <option value=\"codex\">Codex</option>
          <option value=\"claude\">Claude</option>
          <option value=\"pi\">PI</option>
        </select>
      </div>
    </section>
    <section class=\"stats\">
      <article class=\"stat\"><div class=\"label\">placeholder</div><div class=\"value\">0</div></article>
    </section>
    <section class=\"table-wrap\">
      <table>
        <tbody id=\"dailyUsageTableBody\">
          <tr><td><span class=\"rank\">1</span></td><td>2026-01-01</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num total-col\">0</td></tr>
        </tbody>
      </table>
    </section>
    <section class=\"table-wrap\">
      <table>
        <tbody id=\"usageBreakdownTableBody\">
          <tr><td><span class=\"rank\">1</span></td><td>cli</td><td>model</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num total-col\">0</td></tr>
        </tbody>
      </table>
    </section>
  </main>
  <script id=\"usageDataset\" type=\"application/json\">{}</script>
</body>
</html>
"""


REQUIRED_HTML_SNIPPETS = [
    'id="usageDataset"',
    'id="dailyUsageTableBody"',
    'id="usageBreakdownTableBody"',
    'Total Cost',
    'Input Cost',
]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def read_dataset_from_html(html: str) -> dict:
    match = re.search(
        r'<script id="usageDataset" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if match is None:
        raise AssertionError("Rendered HTML is missing the usageDataset script tag")
    return json.loads(match.group(1))


def build_fixture_root(root: Path) -> tuple[DashboardConfig, datetime]:
    dashboard_path = root / "dashboard" / "index.html"
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

    codex_root = root / "codex"
    claude_root = root / "claude"
    pi_root = root / "pi-agent"

    write_jsonl(
        codex_root / "2026" / "03" / "03" / "session-a.jsonl",
        [
            {
                "type": "session_meta",
                "payload": {
                    "id": "codex-session-a",
                    "originator": "codex_cli_rs",
                },
            },
            {
                "type": "turn_context",
                "payload": {
                    "model": "gpt-5.2",
                },
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {
                            "input_tokens": 100,
                            "cached_input_tokens": 20,
                            "output_tokens": 20,
                            "total_tokens": 120,
                        }
                    },
                },
            },
        ],
    )

    write_jsonl(
        claude_root / "project-a" / "session.jsonl",
        [
            {
                "requestId": "claude-request-1",
                "sessionId": "claude-session-1",
                "timestamp": "2026-03-03T20:00:00Z",
                "message": {
                    "model": "claude-sonnet-4-6",
                    "usage": {
                        "input_tokens": 10,
                        "cache_creation_input_tokens": 3,
                        "cache_read_input_tokens": 2,
                        "output_tokens": 5,
                    },
                },
            }
        ],
    )

    write_jsonl(
        pi_root / "sessions" / "--Users-benchmark--" / "2026-03-03T21-00-00-000Z_session-pi.jsonl",
        [
            {
                "type": "session",
                "id": "pi-session-a",
                "timestamp": "2026-03-03T21:00:00.000Z",
                "cwd": "/Users/benchmark",
            },
            {
                "type": "model_change",
                "id": "pi-model-1",
                "timestamp": "2026-03-03T21:00:01.000Z",
                "provider": "openai-codex",
                "modelId": "gpt-5.4",
            },
            {
                "type": "message",
                "id": "pi-assistant-1",
                "timestamp": "2026-03-03T21:00:30.000Z",
                "message": {
                    "role": "assistant",
                    "usage": {
                        "input": 45,
                        "output": 5,
                        "cacheRead": 0,
                        "cacheWrite": 0,
                        "totalTokens": 50,
                    },
                },
            },
        ],
    )

    config = DashboardConfig(
        host="127.0.0.1",
        port=8765,
        dashboard_html=dashboard_path,
        sessions_root=codex_root,
        claude_projects_root=claude_root,
        pi_agent_root=pi_root,
    )
    fixed_now = datetime(2026, 3, 4, 15, 0, tzinfo=timezone.utc)
    return config, fixed_now


def expected_combined_rows() -> list[dict]:
    combined_day = datetime.fromisoformat("2026-03-03T20:00:00+00:00").astimezone().date().isoformat()
    return [
        {
            "date": combined_day,
            "sessions": 3,
            "input_tokens": 155,
            "output_tokens": 30,
            "cached_tokens": 25,
            "total_tokens": 190,
            "input_cost_usd": 0.0003925,
            "output_cost_usd": 0.00045,
            "cached_cost_usd": 0.00001685,
            "total_cost_usd": 0.00085935,
            "cost_complete": True,
            "cost_status": "complete",
            "breakdown_rows": [
                {
                    "agent_cli": "codex_cli_rs",
                    "model": "gpt-5.2",
                    "sessions": 1,
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "cached_tokens": 20,
                    "total_tokens": 120,
                    "input_cost_usd": 0.00025,
                    "output_cost_usd": 0.0003,
                    "cached_cost_usd": 0.000005,
                    "total_cost_usd": 0.000555,
                    "cost_complete": True,
                    "cost_status": "complete",
                },
                {
                    "agent_cli": "pi",
                    "model": "gpt-5.4",
                    "sessions": 1,
                    "input_tokens": 45,
                    "output_tokens": 5,
                    "cached_tokens": 0,
                    "total_tokens": 50,
                    "input_cost_usd": 0.0001125,
                    "output_cost_usd": 0.000075,
                    "cached_cost_usd": 0.0,
                    "total_cost_usd": 0.0001875,
                    "cost_complete": True,
                    "cost_status": "complete",
                },
                {
                    "agent_cli": "claude-code",
                    "model": "claude-sonnet-4-6",
                    "sessions": 1,
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "cached_tokens": 5,
                    "total_tokens": 20,
                    "input_cost_usd": 0.00003,
                    "output_cost_usd": 0.000075,
                    "cached_cost_usd": 0.00001185,
                    "total_cost_usd": 0.00011685,
                    "cost_complete": True,
                    "cost_status": "complete",
                },
            ],
        },
    ]


def validate_render(payload: dict, html: str, fixed_now: datetime) -> None:
    for snippet in REQUIRED_HTML_SNIPPETS:
        if snippet not in html:
            raise AssertionError(f"Rendered HTML missing required snippet: {snippet}")

    dataset = read_dataset_from_html(html)
    if dataset["generated_at"] != fixed_now.isoformat():
        raise AssertionError("Dataset generated_at timestamp is not deterministic")

    expected_flags = {"codex": True, "claude": True, "pi": True, "combined": True}
    if payload["providers_available"] != expected_flags:
        raise AssertionError(f"Unexpected providers_available: {payload['providers_available']}")
    if dataset["providers_available"] != expected_flags:
        raise AssertionError(f"Unexpected dataset providers_available: {dataset['providers_available']}")

    expected_scalars = {
        "input_tokens": 155,
        "output_tokens": 30,
        "cached_tokens": 25,
        "ytd_total_tokens": 190,
        "input_cost_usd": 0.0003925,
        "output_cost_usd": 0.00045,
        "cached_cost_usd": 0.00001685,
        "total_cost_usd": 0.00085935,
        "cost_complete": True,
        "cost_status": "complete",
    }
    for key, expected in expected_scalars.items():
        actual = payload[key]
        if actual != expected:
            raise AssertionError(f"Unexpected payload {key}: expected {expected!r}, got {actual!r}")

    if payload["pricing"]["warning_count"] != 0:
        raise AssertionError("Benchmark fixture unexpectedly produced pricing warnings")
    if payload["providers"]["combined"]["ytd_total_tokens"] != 190:
        raise AssertionError("Combined provider summary ytd_total_tokens mismatch")
    if payload["providers"]["combined"]["total_cost_usd"] != 0.00085935:
        raise AssertionError("Combined provider summary total_cost_usd mismatch")

    combined_rows = dataset["providers"]["combined"]["rows"]
    expected_rows = expected_combined_rows()
    if combined_rows != expected_rows:
        raise AssertionError(
            "Combined dataset rows differ from expected benchmark fixture output\n"
            f"expected={json.dumps(expected_rows, indent=2)}\n"
            f"actual={json.dumps(combined_rows, indent=2)}"
        )


def run_benchmark(repeat: int) -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        config, fixed_now = build_fixture_root(root)
        timings_ms: list[float] = []
        last_payload: dict | None = None

        for _ in range(repeat):
            started = time.perf_counter()
            payload = recalc_dashboard(config, now=fixed_now)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            html = config.dashboard_html.read_text(encoding="utf-8")
            validate_render(payload, html, fixed_now)
            timings_ms.append(elapsed_ms)
            last_payload = payload

        assert last_payload is not None
        return {
            "ok": True,
            "repeat": repeat,
            "timings_ms": timings_ms,
            "median_ms": statistics.median(timings_ms),
            "min_ms": min(timings_ms),
            "max_ms": max(timings_ms),
            "payload": last_payload,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark the dashboard /recalc pipeline on fixed fixtures")
    parser.add_argument("--repeat", type=int, default=5, help="Number of benchmark runs (default: 5)")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable text")
    args = parser.parse_args()

    if args.repeat < 1:
        print("--repeat must be >= 1", file=sys.stderr)
        return 2

    try:
        result = run_benchmark(args.repeat)
    except Exception as exc:  # noqa: BLE001
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"benchmark failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    print("benchmark: recalc on fixed Codex/Claude/PI fixtures")
    for idx, timing in enumerate(result["timings_ms"], start=1):
        print(f"run {idx}: {timing:.3f} ms")
    print(f"median_ms: {result['median_ms']:.3f}")
    print(f"min_ms:    {result['min_ms']:.3f}")
    print(f"max_ms:    {result['max_ms']:.3f}")
    print("gates:     pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
