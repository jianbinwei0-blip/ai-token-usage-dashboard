"""Read shared local settings without creating directories or changing files.

Contract v1: environment > saved settings > defaults. See ``SharedSettings`` and
``load_settings`` for the effective values. Only this module owns the saved-file
schema; the native build copies it verbatim into its Python bridge resources.
"""
from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import os
from pathlib import Path
import sys
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

SETTINGS_VERSION = 1
MAX_SETTINGS_BYTES = 65_536
APP_DIRECTORY = "ai-token-usage-dashboard"
_PATH_ENV = {
    "runtime_dir": "AI_USAGE_DASHBOARD_DIR",
    "pricing_file": "AI_USAGE_PRICING_FILE",
    "history_file": "AI_USAGE_HISTORY_FILE",
    "codex_sessions_root": "AI_USAGE_CODEX_SESSIONS_ROOT",
    "claude_projects_root": "AI_USAGE_CLAUDE_PROJECTS_ROOT",
    "pi_agent_root": "AI_USAGE_PI_AGENT_ROOT",
    "dsh_home": "AI_USAGE_DSH_HOME",
}
_SAVED_KEYS = {"version", "dashboard_url", *_PATH_ENV}


class SettingsError(ValueError):
    """Invalid or unreadable settings. Messages never echo saved values."""


@dataclass(frozen=True)
class SharedSettings:
    version: int
    settings_file: Path
    settings_loaded: bool
    runtime_dir: Path
    dashboard_url: str
    pricing_file: Path | None
    history_file: Path
    codex_sessions_root: Path
    claude_projects_root: Path
    pi_agent_root: Path
    dsh_home: Path
    server_host: str
    server_port: int
    sources: Mapping[str, str]


def _absolute_path(value: object, field: str, home: Path) -> Path:
    if not isinstance(value, str) or not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise SettingsError(f"{field} must be a nonempty absolute path or ~/path.")
    if value == "~":
        path = home
    elif value.startswith("~/"):
        path = home / value[2:]
    else:
        path = Path(value)
    if not path.is_absolute():
        raise SettingsError(f"{field} must be an absolute path or ~/path.")
    return path


def default_settings_path(*, environ: Mapping[str, str] | None = None,
                          home: Path | None = None, platform: str | None = None) -> Path:
    env = os.environ if environ is None else environ
    home = home or Path.home()
    platform = platform or sys.platform
    if platform == "darwin":
        directory = home / "Library" / "Application Support"
    elif platform == "win32":
        directory = _absolute_path(env.get("APPDATA", str(home / "AppData" / "Roaming")), "APPDATA", home)
    else:
        directory = _absolute_path(env.get("XDG_CONFIG_HOME", str(home / ".config")), "XDG_CONFIG_HOME", home)
    return directory / APP_DIRECTORY / "settings.json"


def default_history_path(*, environ: Mapping[str, str] | None = None,
                         home: Path | None = None, platform: str | None = None) -> Path:
    env = os.environ if environ is None else environ
    home = home or Path.home()
    platform = platform or sys.platform
    directory = (home / "Library" / "Application Support" if platform == "darwin"
                 else _absolute_path(env.get("XDG_DATA_HOME", str(home / ".local" / "share")), "XDG_DATA_HOME", home))
    return directory / APP_DIRECTORY / "history.v1.json"


def validate_dashboard_url(value: object) -> str:
    """Allow only credential-free HTTP(S) loopback URLs, without query/fragment."""
    if (not isinstance(value, str) or not value or "\\" in value
            or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
            or "?" in value or "#" in value):
        raise SettingsError("dashboard_url must be a local HTTP(S) URL without credentials, query or fragment.")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
        if (parsed.scheme not in {"http", "https"} or not host
                or parsed.username is not None or parsed.password is not None
                or parsed.netloc.endswith(":")):
            raise ValueError
        if host != "localhost" and not ipaddress.ip_address(host).is_loopback:
            raise ValueError
        if "%" in host or (port is not None and not 1 <= port <= 65535):
            raise ValueError
    except ValueError:
        raise SettingsError("dashboard_url must use HTTP(S), localhost or a loopback IP, and a valid port.") from None
    authority = f"[{host}]" if ":" in host else host
    if port is not None:
        authority += f":{port}"
    return urlunsplit((parsed.scheme, authority, parsed.path, "", ""))


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise SettingsError("Settings must not contain duplicate keys.")
        result[key] = value
    return result


def _read_saved(path: Path, *, explicit: bool, home: Path) -> tuple[dict, bool]:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_SETTINGS_BYTES + 1)
    except FileNotFoundError:
        if explicit:
            raise SettingsError("AI_USAGE_SETTINGS_FILE does not exist; correct the path or unset the override.") from None
        return {}, False
    except OSError:
        raise SettingsError("Settings file is unreadable; check its path and permissions.") from None
    if len(raw) > MAX_SETTINGS_BYTES:
        raise SettingsError("Settings file exceeds the 64 KiB limit.")
    try:
        saved = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError, RecursionError) as error:
        if isinstance(error, SettingsError):
            raise
        raise SettingsError("Settings file is not valid UTF-8 JSON; restore or repair it without deleting usage history.") from None
    if not isinstance(saved, dict):
        raise SettingsError("Settings must be a JSON object.")
    if type(saved.get("version")) is not int or saved["version"] != SETTINGS_VERSION:
        raise SettingsError(f"Unsupported settings version; this runtime requires version {SETTINGS_VERSION}.")
    if set(saved) - _SAVED_KEYS:
        raise SettingsError("Settings contain unsupported keys; check the version 1 settings contract.")
    # Validate the whole file, including fields masked by an environment override.
    for key in _PATH_ENV:
        if key in saved and not (key == "pricing_file" and saved[key] is None):
            _absolute_path(saved[key], key, home)
    if "dashboard_url" in saved:
        validate_dashboard_url(saved["dashboard_url"])
    return saved, True


def _server_port(value: str) -> int:
    if not value.isascii() or not value.isdecimal() or len(value) > 5 or not 1 <= int(value) <= 65535:
        raise SettingsError("AI_USAGE_SERVER_PORT must be an integer from 1 to 65535.")
    return int(value)


def load_settings(repo_root: Path | None = None, *, environ: Mapping[str, str] | None = None,
                  home: Path | None = None, platform: str | None = None) -> SharedSettings:
    """Resolve v1 settings. No collection, mkdir, settings writes or migrations.

    ``repo_root`` is the caller's default runtime, not an override of saved data.
    Without it, retain native discovery of the daemon runtime / Documents fallback.
    An explicitly selected settings file must exist; an absent default is normal.
    ``AI_USAGE_PRICING_FILE=''`` or saved ``pricing_file: null`` selects built-ins.
    """
    env = os.environ if environ is None else environ
    home = home or Path.home()
    default_file = default_settings_path(environ=env, home=home, platform=platform)
    explicit_file = "AI_USAGE_SETTINGS_FILE" in env
    settings_file = (_absolute_path(env["AI_USAGE_SETTINGS_FILE"], "AI_USAGE_SETTINGS_FILE", home)
                     if explicit_file else default_file)
    saved, loaded = _read_saved(settings_file, explicit=explicit_file, home=home)
    daemon = home / ".local" / "share" / "ai-token-usage-dashboard-daemon"
    default_runtime = repo_root if repo_root is not None else (
        daemon if (daemon / "scripts" / "render_tmux_status.py").is_file()
        else home / "Documents" / APP_DIRECTORY
    )
    default_prices = [default_file.with_name("pricing.json"), home / ".config" / APP_DIRECTORY / "pricing.json"]
    defaults: dict[str, Path | None] = {
        "runtime_dir": default_runtime.absolute(),
        "pricing_file": None,
        "history_file": default_history_path(environ=env, home=home, platform=platform),
        "codex_sessions_root": home / ".codex" / "sessions",
        "claude_projects_root": home / ".claude" / "projects",
        "pi_agent_root": home / ".pi" / "agent",
        "dsh_home": home / ".dsh",
    }
    sources: dict[str, str] = {}
    paths: dict[str, Path | None] = {}
    for key, env_name in _PATH_ENV.items():
        if env_name in env:
            value = env[env_name]
            paths[key] = None if key == "pricing_file" and value == "" else _absolute_path(value, env_name, home)
            sources[key] = "environment"
        elif key in saved:
            paths[key] = None if saved[key] is None else _absolute_path(saved[key], key, home)
            sources[key] = "saved"
        else:
            paths[key] = (next((p for p in default_prices if p.is_file()), None)
                          if key == "pricing_file" else defaults[key])
            sources[key] = "default"

    url = validate_dashboard_url(env.get("AI_USAGE_DASHBOARD_URL", saved.get("dashboard_url", "http://127.0.0.1:8765")))
    sources["dashboard_url"] = ("environment" if "AI_USAGE_DASHBOARD_URL" in env
                                else "saved" if "dashboard_url" in saved else "default")
    parsed = urlsplit(url)
    host = env.get("AI_USAGE_SERVER_HOST", parsed.hostname or "127.0.0.1")
    if not host or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in host):
        raise SettingsError("AI_USAGE_SERVER_HOST must be a nonempty host name or IP address.")
    port = (_server_port(env["AI_USAGE_SERVER_PORT"]) if "AI_USAGE_SERVER_PORT" in env
            else parsed.port or (443 if parsed.scheme == "https" else 80))
    if "AI_USAGE_DASHBOARD_URL" not in env and ("AI_USAGE_SERVER_HOST" in env or "AI_USAGE_SERVER_PORT" in env):
        # A wildcard is a bind address, not a browser destination. Never turn a
        # remote bind override into permission to open an arbitrary remote URL.
        browser_host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(host, host)
        authority = f"[{browser_host}]" if ":" in browser_host else browser_host
        try:
            candidate = validate_dashboard_url(urlunsplit((parsed.scheme, f"{authority}:{port}", parsed.path, "", "")))
        except SettingsError:
            authority = f"[{parsed.hostname}]" if ":" in (parsed.hostname or "") else parsed.hostname
            candidate = validate_dashboard_url(urlunsplit((parsed.scheme, f"{authority}:{port}", parsed.path, "", "")))
        url = candidate
        sources["dashboard_url"] = "environment"
    return SharedSettings(
        version=SETTINGS_VERSION, settings_file=settings_file, settings_loaded=loaded,
        dashboard_url=url, server_host=host, server_port=port, sources=sources,
        **paths,
    )
