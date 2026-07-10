from __future__ import annotations

import datetime as dt
import json
import queue
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any


class CodexAppServerError(RuntimeError):
    """Raised when Codex account information cannot be read safely."""


_EOF = object()


class _CodexAppServerClient:
    def __init__(self, codex_binary: str | Path) -> None:
        command = [str(codex_binary), "app-server", "--stdio"]
        try:
            self.process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            raise CodexAppServerError("Codex app-server is unavailable") from exc

        if self.process.stdin is None or self.process.stdout is None or self.process.stderr is None:
            self.close()
            raise CodexAppServerError("Codex app-server did not expose stdio")

        self._messages: queue.Queue[dict[str, Any] | object] = queue.Queue()
        self._pending: dict[str | int, dict[str, Any]] = {}
        self._stderr_tail: deque[str] = deque(maxlen=8)
        self._stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _read_stdout(self) -> None:
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                try:
                    message = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if isinstance(message, dict):
                    self._messages.put(message)
        finally:
            self._messages.put(_EOF)

    def _read_stderr(self) -> None:
        assert self.process.stderr is not None
        for line in self.process.stderr:
            text = line.strip()
            if text:
                self._stderr_tail.append(text)

    def _send(self, message: dict[str, Any]) -> None:
        if self.process.stdin is None or self.process.poll() is not None:
            raise CodexAppServerError("Codex app-server exited unexpectedly")
        try:
            self.process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise CodexAppServerError("Codex app-server closed its input") from exc

    def notify(self, method: str) -> None:
        self._send({"method": method})

    def request(
        self,
        request_id: str | int,
        method: str,
        params: dict[str, Any] | None,
        *,
        deadline: float,
    ) -> dict[str, Any]:
        message: dict[str, Any] = {"id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)
        response = self._wait_for_response(request_id, deadline=deadline)

        error = response.get("error")
        if error is not None:
            error_message = error.get("message") if isinstance(error, dict) else None
            detail = str(error_message or "request failed").strip()
            raise CodexAppServerError(f"Codex app-server {method} failed: {detail}")

        result = response.get("result")
        if not isinstance(result, dict):
            raise CodexAppServerError(f"Codex app-server {method} returned an invalid response")
        return result

    def _wait_for_response(self, request_id: str | int, *, deadline: float) -> dict[str, Any]:
        pending = self._pending.pop(request_id, None)
        if pending is not None:
            return pending

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexAppServerError("Codex app-server request timed out")
            try:
                message = self._messages.get(timeout=remaining)
            except queue.Empty as exc:
                raise CodexAppServerError("Codex app-server request timed out") from exc

            if message is _EOF:
                raise CodexAppServerError("Codex app-server exited before replying")
            if not isinstance(message, dict):
                continue

            message_id = message.get("id")
            if message_id == request_id:
                return message
            if isinstance(message_id, (str, int)) and "method" not in message:
                self._pending[message_id] = message

    def close(self) -> None:
        process = getattr(self, "process", None)
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        if process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=0.5)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    process.kill()
                    process.wait(timeout=0.5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        for thread_name in ("_stdout_thread", "_stderr_thread"):
            thread = getattr(self, thread_name, None)
            if thread is not None:
                thread.join(timeout=0.2)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass

    def __enter__(self) -> "_CodexAppServerClient":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def _utc_datetime(now: dt.datetime | None) -> dt.datetime:
    reference = now or dt.datetime.now(dt.timezone.utc)
    if reference.tzinfo is None:
        return reference.replace(tzinfo=dt.timezone.utc)
    return reference.astimezone(dt.timezone.utc)


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _normalized_window(payload: Any) -> dict[str, int | None] | None:
    if not isinstance(payload, dict):
        return None
    used_percent = _safe_int(payload.get("usedPercent"))
    if used_percent is None:
        return None
    used_percent = max(0, min(100, used_percent))

    duration_minutes = _safe_int(payload.get("windowDurationMins"))
    if duration_minutes is not None and duration_minutes <= 0:
        duration_minutes = None
    resets_at = _safe_int(payload.get("resetsAt"))
    if resets_at is not None and resets_at <= 0:
        resets_at = None

    return {
        "used_percent": used_percent,
        "remaining_percent": 100 - used_percent,
        "window_duration_minutes": duration_minutes,
        "resets_at": resets_at,
    }


def _normalized_credits(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    balance = payload.get("balance")
    return {
        "has_credits": bool(payload.get("hasCredits")),
        "unlimited": bool(payload.get("unlimited")),
        "balance": str(balance).strip() if balance is not None and str(balance).strip() else None,
    }


def _normalized_individual_limit(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    remaining_percent = _safe_int(payload.get("remainingPercent"))
    resets_at = _safe_int(payload.get("resetsAt"))
    if remaining_percent is None or resets_at is None:
        return None
    return {
        "remaining_percent": max(0, min(100, remaining_percent)),
        "resets_at": resets_at,
        "used": str(payload.get("used") or "").strip(),
        "limit": str(payload.get("limit") or "").strip(),
    }


def _normalized_limit(payload: dict[str, Any], fallback_id: str) -> dict[str, Any] | None:
    primary = _normalized_window(payload.get("primary"))
    secondary = _normalized_window(payload.get("secondary"))
    individual_limit = _normalized_individual_limit(payload.get("individualLimit"))
    if primary is None and secondary is None and individual_limit is None:
        return None

    limit_id = str(payload.get("limitId") or fallback_id).strip() or fallback_id
    limit_name = str(payload.get("limitName") or "").strip() or None
    reached_type = str(payload.get("rateLimitReachedType") or "").strip() or None
    plan_type = str(payload.get("planType") or "").strip().lower() or None
    return {
        "id": limit_id,
        "name": limit_name,
        "plan": plan_type,
        "primary": primary,
        "secondary": secondary,
        "credits": _normalized_credits(payload.get("credits")),
        "individual_limit": individual_limit,
        "rate_limit_reached_type": reached_type,
    }


def normalize_chatgpt_subscription_usage(
    account_payload: dict[str, Any],
    rate_limits_payload: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    account = account_payload.get("account")
    if not isinstance(account, dict) or str(account.get("type") or "").strip().lower() != "chatgpt":
        account_type = str(account.get("type") or "").strip() if isinstance(account, dict) else ""
        return {
            "version": 1,
            "state": "not_applicable",
            "fetched_at": _utc_datetime(now).isoformat(),
            "account_type": account_type or None,
        }

    legacy_snapshot = rate_limits_payload.get("rateLimits")
    if not isinstance(legacy_snapshot, dict):
        legacy_snapshot = {}
    by_limit_id = rate_limits_payload.get("rateLimitsByLimitId")
    if not isinstance(by_limit_id, dict):
        by_limit_id = {}

    canonical_id = str(legacy_snapshot.get("limitId") or "codex").strip() or "codex"
    canonical_by_id = by_limit_id.get(canonical_id)
    if isinstance(canonical_by_id, dict):
        merged_canonical = dict(legacy_snapshot)
        merged_canonical.update({key: value for key, value in canonical_by_id.items() if value is not None})
    else:
        merged_canonical = legacy_snapshot

    limits: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    canonical = _normalized_limit(merged_canonical, canonical_id)
    if canonical is not None:
        limits.append(canonical)
        seen_ids.add(str(canonical["id"]))

    for fallback_id in sorted(str(key) for key in by_limit_id):
        payload = by_limit_id.get(fallback_id)
        if not isinstance(payload, dict):
            continue
        normalized = _normalized_limit(payload, fallback_id)
        if normalized is None or str(normalized["id"]) in seen_ids:
            continue
        limits.append(normalized)
        seen_ids.add(str(normalized["id"]))

    account_plan = str(account.get("planType") or "").strip().lower()
    fallback_plan = next((str(limit.get("plan") or "") for limit in limits if limit.get("plan")), "")
    reset_credits = rate_limits_payload.get("rateLimitResetCredits")
    reset_credit_count = _safe_int(reset_credits.get("availableCount")) if isinstance(reset_credits, dict) else None

    return {
        "version": 1,
        "state": "ok" if limits else "unavailable",
        "fetched_at": _utc_datetime(now).isoformat(),
        "account_type": "chatgpt",
        "plan": account_plan or fallback_plan or "unknown",
        "limits": limits,
        "reset_credits_available": max(0, reset_credit_count) if reset_credit_count is not None else None,
    }


def fetch_chatgpt_subscription_usage(
    *,
    codex_binary: str | Path = "codex",
    timeout_seconds: float = 3.0,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    timeout = max(0.1, float(timeout_seconds))
    deadline = time.monotonic() + timeout

    with _CodexAppServerClient(codex_binary) as client:
        client.request(
            "initialize",
            "initialize",
            {
                "clientInfo": {
                    "name": "ai-token-usage-dashboard",
                    "title": "AI Token Usage Dashboard",
                    "version": "1.0",
                }
            },
            deadline=deadline,
        )
        client.notify("initialized")
        account_payload = client.request(
            "account",
            "account/read",
            {"refreshToken": False},
            deadline=deadline,
        )

        account = account_payload.get("account")
        if not isinstance(account, dict) or str(account.get("type") or "").strip().lower() != "chatgpt":
            return normalize_chatgpt_subscription_usage(account_payload, {}, now=now)

        rate_limits_payload = client.request(
            "rate-limits",
            "account/rateLimits/read",
            None,
            deadline=deadline,
        )
        return normalize_chatgpt_subscription_usage(account_payload, rate_limits_payload, now=now)
