"""Shared refresh ownership and atomic, owner-readable snapshots."""
from __future__ import annotations

import contextlib
import os
import tempfile
import threading
from pathlib import Path

import fcntl

_REFRESH_LOCK = threading.RLock()


@contextlib.contextmanager
def accounting_lock(path: Path):
    # flock alone does not serialize separate descriptors in every threaded caller.
    with _REFRESH_LOCK:
        lock = path.with_suffix(path.suffix + '.lock')
        lock.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, 'a') as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', dir=path.parent, encoding='utf-8', delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
