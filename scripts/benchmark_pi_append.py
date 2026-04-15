#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import statistics
import tempfile
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dashboard_core.collectors import _PI_SESSION_RECORDS_CACHE, parse_pi_session_contribution_cached

SOURCE_PI_SESSION = (
    Path.home()
    / ".pi"
    / "agent"
    / "sessions"
    / "--Users-jwei-Acuvity-agentplane--"
    / "2026-04-12T21-05-22-642Z_64817a4a-0b93-40f4-ba1d-adc108b37f4b.jsonl"
)


def total_tokens_for(contribution: dict[str, object]) -> int:
    usage_rows = contribution.get("usage_rows")
    if not isinstance(usage_rows, list):
        return 0
    return sum(int(row.get("total_tokens") or 0) for row in usage_rows if isinstance(row, dict))


def append_usage_event(path: Path, run_index: int) -> int:
    added_total_tokens = 2
    event = {
        "type": "message",
        "timestamp": f"2026-04-15T16:30:{run_index:02d}Z",
        "message": {
            "role": "assistant",
            "model": "gpt-5.4",
            "usage": {
                "input": 1,
                "output": 1,
                "cacheRead": 0,
                "cacheWrite": 0,
                "totalTokens": added_total_tokens,
            },
        },
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")
    return added_total_tokens


def run_benchmark(repeat: int) -> dict[str, object]:
    if not SOURCE_PI_SESSION.exists():
        raise AssertionError(f"source PI session missing: {SOURCE_PI_SESSION}")

    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "pi-agent" / "sessions" / "--bench--" / SOURCE_PI_SESSION.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE_PI_SESSION, target)

        _PI_SESSION_RECORDS_CACHE.clear()
        initial = parse_pi_session_contribution_cached(target)
        if not initial.get("usage_rows"):
            raise AssertionError("initial parse produced no usage rows")

        initial_total_tokens = total_tokens_for(initial)
        timings_ms: list[float] = []
        latest = initial
        expected_total_tokens = initial_total_tokens

        for idx in range(repeat):
            expected_total_tokens += append_usage_event(target, idx)
            started = time.perf_counter()
            latest = parse_pi_session_contribution_cached(target)
            timings_ms.append((time.perf_counter() - started) * 1000.0)
            actual_total_tokens = total_tokens_for(latest)
            if actual_total_tokens != expected_total_tokens:
                raise AssertionError(
                    f"append parse total_tokens mismatch: expected {expected_total_tokens}, got {actual_total_tokens}"
                )

        return {
            "ok": True,
            "repeat": repeat,
            "initial_total_tokens": initial_total_tokens,
            "final_total_tokens": total_tokens_for(latest),
            "timings_ms": timings_ms,
            "median_ms": statistics.median(timings_ms),
            "min_ms": min(timings_ms),
            "max_ms": max(timings_ms),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark PI append-only cache invalidation latency")
    parser.add_argument("--repeat", type=int, default=3, help="Number of append/parse runs (default: 3)")
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

    print("benchmark: PI append-only cache invalidation")
    for idx, timing in enumerate(result["timings_ms"], start=1):
        print(f"append run {idx}: {timing:.3f} ms")
    print(f"median_ms: {result['median_ms']:.3f}")
    print(f"min_ms: {result['min_ms']:.3f}")
    print(f"max_ms: {result['max_ms']:.3f}")
    print(f"METRIC pi_append_parse_ms={result['median_ms']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
