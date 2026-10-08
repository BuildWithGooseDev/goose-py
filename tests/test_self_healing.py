"""Tests for the self-healing behavior of the Goose Python SDK."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest

from goose_sdk import ConnectionState, FileCache, GooseSDK
from goose_sdk.client import _Backoff, _parse_retry_after


def _client(**overrides) -> GooseSDK:
    base = dict(
        sdk_client_id="gsc_x",
        server_url="http://localhost:8080",
        flagsets=["default"],
        auto_connect=False,
    )
    base.update(overrides)
    return GooseSDK(**base)


# --- backoff ---------------------------------------------------------------


def test_backoff_bounds():
    backoff = _Backoff(base=1.0, cap=10.0)
    max_seen = 0.0
    for _ in range(200):
        delay = backoff.duration()
        assert 1.0 <= delay <= 10.0
        max_seen = max(max_seen, delay)
    assert max_seen >= 5.0  # climbs to the cap region
    backoff.reset()
    assert backoff.duration() <= 2.0


def test_parse_retry_after():
    assert _parse_retry_after("5") == 5.0
    assert _parse_retry_after("0") == 0.0
    assert _parse_retry_after("-3") == 0.0
    assert _parse_retry_after("") == 0.0
    assert _parse_retry_after("garbage") == 0.0
    assert _parse_retry_after(None) == 0.0


# --- validation ------------------------------------------------------------


def test_coerce_and_validate():
    c = _client()
    assert c._coerce_and_validate("true", "bool") == (True, True)
    assert c._coerce_and_validate("abc", "number")[1] is False
    assert c._coerce_and_validate("5", "number") == (5, True)
    assert c._coerce_and_validate(None, "bool")[1] is False
    assert c._coerce_and_validate("grid", "list_of_values") == ("grid", True)


# --- persistent cache ------------------------------------------------------


def test_file_cache_round_trip(tmp_path):
    path = str(tmp_path / "cache.json")
    cache = FileCache(path)
    assert cache.load() is None

    snapshot = {
        "flags": {"default": {"a": True, "b": 3, "c": "x"}},
        "flag_data_types": {"default": {"a": "bool", "b": "number", "c": "string"}},
        "rollouts": {"default": {"b": {"percentage": 25, "salt": "s", "value": 9}}},
        "poll_cursors": {"default": 1234},
        "configs": {"frontend": {"document": {"configs": {}}, "revision": 2}},
    }
    cache.save(snapshot)
    assert cache.load() == snapshot


# --- health / state --------------------------------------------------------


def test_state_transitions():
    c = _client()
    assert c.state() is ConnectionState.CONNECTING

    seen: list[ConnectionState] = []
    unsub = c.on_state_change(seen.append)

    c._record_success()
    assert c.state() is ConnectionState.LIVE
    c._record_failure(RuntimeError("boom"))
    assert c.state() is ConnectionState.DEGRADED

    unsub()
    c._record_success()  # not delivered after unsubscribe
    assert seen == [ConnectionState.LIVE, ConnectionState.DEGRADED]


def test_is_stale():
    c = _client()
    assert c.is_stale() is True  # never synced
    c._record_success()
    assert c.is_stale() is False


# --- fault-tolerant reads --------------------------------------------------


def test_malformed_delta_retains_last_good():
    errors: list[BaseException] = []
    c = _client(on_error=errors.append)

    c._commit_flag("default", "limit", "number", 10, {"percentage": None, "salt": None, "value": None})
    assert c.get_flag("limit") == 10

    # A malformed delta (string for a number flag) must be rejected.
    c._apply_delta({"flagKey": "limit", "flagValue": "not-a-number"}, fallback_flagset="default")
    assert c.get_flag("limit") == 10
    assert errors, "expected on_error for malformed value"

    # A valid delta still applies.
    c._apply_delta({"flagKey": "limit", "flagValue": 20}, fallback_flagset="default")
    assert c.get_flag("limit") == 20


# --- warm start ------------------------------------------------------------


class _ToggleServer:
    """Stub config service whose /config + /poll can be flipped to fail."""

    def __init__(self) -> None:
        self.fail = False
        state = self

        class Handler(BaseHTTPRequestHandler):
            def _send(self, payload, status=200):
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802
                path = urlparse(self.path).path
                if state.fail:
                    self._send({}, status=500)
                elif path == "/api/v1/config":
                    self._send({"flags": [{"flag_key": "feature_x", "flag_data_type": "bool", "flag_value": True, "updated_at": "2026-01-01T00:00:00Z"}]})
                elif path == "/api/v1/sdk/resolve":
                    self._send({"organization_id": "org1"})
                else:
                    self._send({}, status=404)

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                if length:
                    self.rfile.read(length)
                self._send({} if not state.fail else {}, status=500 if state.fail else 200)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def test_warm_start_degraded(tmp_path):
    path = str(tmp_path / "cache.json")
    server = _ToggleServer()
    try:
        # Client A populates + persists the cache from a healthy server.
        a = GooseSDK(
            sdk_client_id="gsc_x",
            server_url=server.base_url,
            flagsets=["default"],
            cache=FileCache(path),
            poll_interval_seconds=60.0,
        )
        assert a.state() is ConnectionState.LIVE
        assert a.get_flag("feature_x") is True
        a._flush_cache()  # deterministic persist
        a.close()

        # Server goes down; a restarting client must warm-start and not raise.
        server.fail = True
        b = GooseSDK(
            sdk_client_id="gsc_x",
            server_url=server.base_url,
            flagsets=["default"],
            cache=FileCache(path),
            poll_interval_seconds=60.0,
        )
        try:
            assert b.state() is ConnectionState.DEGRADED
            assert b.get_flag("feature_x") is True  # served from warm-started cache
        finally:
            b.close()
    finally:
        server.close()


def test_require_initial_connect_raises_when_down():
    server = _ToggleServer()
    server.fail = True
    try:
        with pytest.raises(Exception):
            GooseSDK(
                sdk_client_id="gsc_x",
                server_url=server.base_url,
                flagsets=["default"],
                require_initial_connect=True,
                poll_interval_seconds=60.0,
            )
    finally:
        server.close()
