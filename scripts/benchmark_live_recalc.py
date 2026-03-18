#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard

REQUIRED_HTML_SNIPPETS = [
    'id="usageDataset"',
    'id="dailyUsageTableBody"',
    'id="usageBreakdownTableBody"',
]


def validate_payload(payload: dict, html: str) -> None:
    if not payload.get("ok"):
        raise AssertionError("recalc payload not ok")
    for snippet in REQUIRED_HTML_SNIPPETS:
        if snippet not in html:
            raise AssertionError(f"Rendered HTML missing required snippet: {snippet}")
    if payload.get("providers_available", {}).get("combined") is not True:
        raise AssertionError("combined provider unavailable")
    if payload.get("ytd_total_tokens", 0) <= 0:
        raise AssertionError("expected positive total tokens on live workload")


def run_single_recalc(config: DashboardConfig) -> dict:
    started = time.perf_counter()
    payload = recalc_dashboard(config)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    html = config.dashboard_html.read_text(encoding="utf-8")
    validate_payload(payload, html)
    return {"elapsed_ms": elapsed_ms, "payload": payload}


def run_fresh_process_single(repo_root: Path) -> dict:
    command = [sys.executable, str(Path(__file__).resolve()), "--single-run-json"]
    result = subprocess.run(
        command,
        cwd=repo_root,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip() or f"exit {result.returncode}"
        raise AssertionError(f"fresh-process recalc failed: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"fresh-process recalc emitted invalid JSON: {exc}") from exc


def run_benchmark(repeat: int) -> dict:
    repo_root = Path(__file__).resolve().parents[1]
    config = DashboardConfig.from_env(repo_root)

    prep = run_single_recalc(config)
    cold_timings_ms: list[float] = []
    warm_timings_ms: list[float] = []
    last_payload = prep["payload"]

    for _ in range(repeat):
        cold_run = run_fresh_process_single(repo_root)
        cold_timings_ms.append(float(cold_run["elapsed_ms"]))
        if isinstance(cold_run.get("payload"), dict):
            last_payload = cold_run["payload"]

    for _ in range(repeat):
        warm_run = run_single_recalc(config)
        warm_timings_ms.append(float(warm_run["elapsed_ms"]))
        last_payload = warm_run["payload"]

    return {
        "ok": True,
        "repeat": repeat,
        "prep_ms": prep["elapsed_ms"],
        "cold_timings_ms": cold_timings_ms,
        "warm_timings_ms": warm_timings_ms,
        "cold_median_ms": statistics.median(cold_timings_ms),
        "warm_median_ms": statistics.median(warm_timings_ms),
        "cold_min_ms": min(cold_timings_ms),
        "cold_max_ms": max(cold_timings_ms),
        "warm_min_ms": min(warm_timings_ms),
        "warm_max_ms": max(warm_timings_ms),
        "payload": last_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark live dashboard recalc on real local data")
    parser.add_argument("--repeat", type=int, default=5, help="Number of benchmark runs (default: 5)")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable text")
    parser.add_argument("--single-run-json", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.repeat < 1:
        print("--repeat must be >= 1", file=sys.stderr)
        return 2

    repo_root = Path(__file__).resolve().parents[1]

    try:
        if args.single_run_json:
            config = DashboardConfig.from_env(repo_root)
            print(json.dumps(run_single_recalc(config)))
            return 0
        result = run_benchmark(args.repeat)
    except Exception as exc:  # noqa: BLE001
        if args.json or args.single_run_json:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"benchmark failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    print("benchmark: live recalc on local Codex/Claude/PI data")
    print(f"prep_ms: {result['prep_ms']:.3f}")
    for idx, timing in enumerate(result["cold_timings_ms"], start=1):
        print(f"cold run {idx}: {timing:.3f} ms")
    for idx, timing in enumerate(result["warm_timings_ms"], start=1):
        print(f"warm run {idx}: {timing:.3f} ms")
    print(f"cold_median_ms: {result['cold_median_ms']:.3f}")
    print(f"warm_median_ms: {result['warm_median_ms']:.3f}")
    print(f"cold_min_ms: {result['cold_min_ms']:.3f}")
    print(f"cold_max_ms: {result['cold_max_ms']:.3f}")
    print(f"warm_min_ms: {result['warm_min_ms']:.3f}")
    print(f"warm_max_ms: {result['warm_max_ms']:.3f}")
    print(f"METRIC live_recalc_ms={result['warm_median_ms']:.6f}")
    print(f"METRIC cold_recalc_ms={result['cold_median_ms']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
