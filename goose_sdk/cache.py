"""Persistent last-known-good cache for warm starts across restarts."""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Optional, Protocol


class PersistentCache(Protocol):
    """Stores last-known-good SDK state so a process that restarts during a
    config-service outage can serve real values immediately instead of booting
    with an empty cache.

    The SDK reads it once on ``connect()`` (before any network call) and writes a
    debounced snapshot as values change. Implementations must tolerate concurrent
    calls; the SDK serializes its own, but a caller may share an instance.
    """

    def load(self) -> Optional[dict[str, Any]]:
        """Return the last saved snapshot, or ``None`` when nothing is stored."""

    def save(self, snapshot: dict[str, Any]) -> None:
        """Persist a snapshot, replacing any previous one."""


class FileCache:
    """A :class:`PersistentCache` backed by a single JSON file.

    Writes are atomic (temp file + ``os.replace``) so a crash mid-write never
    leaves a corrupt cache. A missing file loads as ``None``.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def load(self) -> Optional[dict[str, Any]]:
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                text = handle.read()
        except FileNotFoundError:
            return None
        if not text.strip():
            return None
        data = json.loads(text)
        return data if isinstance(data, dict) else None

    def save(self, snapshot: dict[str, Any]) -> None:
        directory = os.path.dirname(self._path) or "."
        fd, tmp_path = tempfile.mkstemp(prefix=".goose-cache-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, indent=2)
            os.replace(tmp_path, self._path)
        except BaseException:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
