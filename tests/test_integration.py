"""Integration tests for the Goose Python SDK against a stdlib stub server.

These exercise the real HTTP request path (snapshot load, polling deltas, config
delivery, webhook registration + the embedded listener) without any live
config-service — a threaded http.server stub stands in for it.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest

from goose_sdk import ConnectionType, GooseSDK


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _StubState:
    """Mutable server state the tests drive responses from."""

    def __init__(self) -> None:
        self.flags: list[dict] = []
        self.configs: dict[str, dict] = {}
        self.webhook_registrations: list[dict] = []


@pytest.fixture()
def stub():
    state = _StubState()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, payload, status=200):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b""
            return json.loads(raw or b"{}")

        def do_GET(self):  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/v1/config":
                self._send({"flags": state.flags})
            elif path == "/api/v1/configs":
                self._send({"configs": state.configs})
            elif path == "/api/v1/sdk/resolve":
                self._send({"organization_id": "org-123"})
            else:
                self._send({}, status=404)

        def do_POST(self):  # noqa: N802
            path = urlparse(self.path).path
            body = self._read_json()
            if path == "/api/v1/poll":
                self._send({"flags": state.flags})
            elif path == "/api/v1/configs/poll":
                self._send({"configs": state.configs})
            elif path == "/api/v1/webhook":
                state.webhook_registrations.append(body)
                self._send({})
            else:
                self._send({}, status=404)

        def log_message(self, *args):  # silence stub access logs
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base_url, state
    finally:
        server.shutdown()
        server.server_close()


def _flag(key, data_type, value, **extra):
    flag = {
        "flag_key": key,
        "flag_data_type": data_type,
        "flag_value": value,
        "updated_at": "2026-06-14T00:00:00Z",
    }
    flag.update(extra)
    return flag


# ---------------------------------------------------------------------------
# Flags: snapshot + polling deltas
# ---------------------------------------------------------------------------
def test_snapshot_loads_typed_values(stub):
    base_url, state = stub
    state.flags = [
        _flag("edge_ui", "bool", "true"),
        _flag("max_retries", "number", "3"),
        _flag("theme", "list_of_values", "grid"),
    ]
    client = GooseSDK("gsc_x", base_url, ["checkout"], auto_connect=False)
    client.refresh()

    assert client.get_flag("edge_ui") is True
    assert client.get_flag("max_retries") == 3
    assert client.get_flag("theme") == "grid"
    assert client.get_flag("missing", default="fallback") == "fallback"


def test_polling_picks_up_a_delta(stub):
    base_url, state = stub
    state.flags = [_flag("edge_ui", "bool", "false")]
    seen: list = []

    class Adapter:
        def create_or_update(self, flagset, flag_key, value):
            seen.append((flagset, flag_key, value))

    client = GooseSDK(
        "gsc_x", base_url, ["checkout"], storage_adapter=Adapter(), auto_connect=False
    )
    client.refresh()
    assert client.get_flag("edge_ui") is False

    state.flags = [_flag("edge_ui", "bool", "true")]
    client.refresh()  # initialized + polling => polls once
    assert client.get_flag("edge_ui") is True
    assert ("checkout", "edge_ui", True) in seen


def test_connect_lifecycle_with_background_polling(stub):
    base_url, state = stub
    state.flags = [_flag("edge_ui", "bool", "false")]
    client = GooseSDK(
        "gsc_x",
        base_url,
        ["checkout"],
        connection_type=ConnectionType.POLLING,
        poll_interval_seconds=1,
        auto_connect=True,
    )
    try:
        assert client.organization_id == "org-123"
        assert client.get_flag("edge_ui") is False

        state.flags = [_flag("edge_ui", "bool", "true")]
        deadline = time.time() + 5
        while time.time() < deadline and client.get_flag("edge_ui") is not True:
            time.sleep(0.1)
        assert client.get_flag("edge_ui") is True
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Canary rollouts
# ---------------------------------------------------------------------------
def test_canary_rollout_buckets_deterministically(stub):
    base_url, state = stub
    # user-123 buckets to 73 for (salt "s1", flag "new_checkout").
    state.flags = [
        _flag(
            "new_checkout",
            "bool",
            "false",
            rollout_percentage=80,
            rollout_salt="s1",
            rollout_value="true",
        )
    ]
    client = GooseSDK("gsc_x", base_url, ["checkout"], auto_connect=False)
    client.refresh()

    # 73 < 80 -> in the canary cohort -> candidate value.
    assert client.get_flag("new_checkout", targeting_key="user-123") is True

    # Lower the ramp below the user's bucket -> baseline.
    state.flags[0]["rollout_percentage"] = 50
    client.refresh()
    assert client.get_flag("new_checkout", targeting_key="user-123") is False


def test_canary_without_targeting_key_returns_baseline(stub):
    base_url, state = stub
    state.flags = [
        _flag(
            "new_checkout",
            "bool",
            "false",
            rollout_percentage=100,
            rollout_salt="s1",
            rollout_value="true",
        )
    ]
    client = GooseSDK("gsc_x", base_url, ["checkout"], auto_connect=False)
    client.refresh()
    # No targeting key -> never leak the feature -> baseline.
    assert client.get_flag("new_checkout") is False


# ---------------------------------------------------------------------------
# Configs
# ---------------------------------------------------------------------------
def test_configs_snapshot_and_change_listeners(stub):
    base_url, state = stub
    state.flags = [_flag("edge_ui", "bool", "false")]
    state.configs = {
        "frontend": {
            "document": {
                "configs": {
                    "layout": {"value": "grid", "apply_strategy": "immediate"}
                }
            },
            "revision_number": 1,
        }
    }

    client = GooseSDK(
        "gsc_x",
        base_url,
        ["checkout"],
        sdk_client_secret="sek",
        namespace_name="production",
        configs=["frontend"],
        auto_connect=False,
    )

    events = []
    client.on("frontend", events.append)
    client.refresh()  # seeds configs without firing listeners

    assert client.get_config_value("frontend", "layout") == "grid"
    assert "frontend" in client.configs_snapshot()
    assert events == []  # initial seed is silent

    # Change the document and poll.
    state.configs["frontend"]["document"]["configs"]["layout"]["value"] = "list"
    state.configs["frontend"]["revision_number"] = 2
    client.refresh()

    assert len(events) == 1
    assert events[0].name == "frontend"
    assert events[0].apply_strategy == "immediate"
    assert events[0].new_value["configs"]["layout"]["value"] == "list"
    assert client.get_config_value("frontend", "layout") == "list"


def test_config_requires_restart_fires_drain(stub):
    base_url, state = stub
    state.flags = []
    state.configs = {
        "database": {
            "document": {
                "configs": {
                    "url": {"value": "a", "apply_strategy": "requires_restart"}
                }
            },
            "revision_number": 1,
        }
    }
    client = GooseSDK(
        "gsc_x",
        base_url,
        ["checkout"],
        sdk_client_secret="sek",
        namespace_name="production",
        configs=["database"],
        auto_connect=False,
    )
    drains = []
    client.on_restart_required(drains.append)
    client.refresh()

    state.configs["database"]["document"]["configs"]["url"]["value"] = "b"
    client.refresh()

    assert len(drains) == 1
    assert drains[0].apply_strategy == "requires_restart"


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------
def test_webhook_registration_and_embedded_listener(stub):
    base_url, state = stub
    state.flags = [_flag("edge_ui", "bool", "false")]
    listener_port = _free_port()

    client = GooseSDK(
        "gsc_x",
        base_url,
        ["HookTest"],
        sdk_client_secret="sek",
        connection_type=ConnectionType.WEBHOOK,
        webhook_target_url=f"http://127.0.0.1:{listener_port}/webhook",
        webhook_secret="shh",
        webhook_listener_host="127.0.0.1",
        webhook_listener_port=listener_port,
        auto_connect=True,
    )
    try:
        # connect() registers a webhook per flagset.
        assert len(state.webhook_registrations) == 1
        assert state.webhook_registrations[0]["flagset"] == "HookTest"
        assert state.webhook_registrations[0]["client_secret"] == "sek"

        # The server posts a delta to the embedded listener.
        status = _post_webhook(
            client.webhook_listener_url,
            {"flagSet": "HookTest", "flagKey": "edge_ui", "flagValue": "true", "eventId": "e1"},
            secret="shh",
        )
        assert status == 200
        assert client.get_flag("edge_ui", flagset="HookTest") is True
        assert client.webhook_events_received == 1

        # A redelivery of the same eventId is deduped (counted, not reapplied twice).
        _post_webhook(
            client.webhook_listener_url,
            {"flagSet": "HookTest", "flagKey": "edge_ui", "flagValue": "false", "eventId": "e1"},
            secret="shh",
        )
        assert client.get_flag("edge_ui", flagset="HookTest") is True  # unchanged
        assert client.webhook_events_received == 2

        # A wrong secret is rejected.
        bad = _post_webhook(
            client.webhook_listener_url,
            {"flagSet": "HookTest", "flagKey": "edge_ui", "flagValue": "false", "eventId": "e2"},
            secret="wrong",
        )
        assert bad == 401
    finally:
        client.close()


def test_process_webhook_event_rejects_bad_secret(stub):
    base_url, _ = stub
    client = GooseSDK(
        "gsc_x",
        base_url,
        ["HookTest"],
        sdk_client_secret="sek",
        webhook_secret="shh",
        auto_connect=False,
    )
    with pytest.raises(PermissionError):
        client.process_webhook_event({"flagKey": "x"}, received_secret="wrong")


def _post_webhook(url: str, payload: dict, secret: str) -> int:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Webhook-Secret": secret},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code
