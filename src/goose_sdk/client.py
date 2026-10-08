from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import os
import secrets
import signal
import socket
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Coroutine, Optional, Protocol, Sequence, TypeVar
from urllib.parse import urlparse

import aiohttp

from .cache import FileCache, PersistentCache
from ._segments import matches_segment, parse_segment_targeting

# How long an SSE stream must stay connected to be judged healthy (resets
# reconnect backoff and clears the SSE-down marker), and the consecutive
# reconnect-failure count (SSE-only) after which fallback polling starts.
_SSE_STABLE_INTERVAL = 30.0
_SSE_FALLBACK_THRESHOLD = 3


class _Backoff:
    """Exponential backoff with equal jitter for retry loops.

    Half the exponential window is fixed and half is random, so delays never
    collapse toward zero (which would hammer a recovering server) yet still
    spread a fleet of clients out. Capped, and reset to base on success.
    """

    def __init__(self, base: float, cap: float) -> None:
        self._base = base if base > 0 else 1.0
        self._cap = cap if cap >= self._base else self._base
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def duration(self) -> float:
        window = self._base * (2 ** self._attempt)
        if window >= self._cap:
            window = self._cap
        else:
            self._attempt += 1
        half = window / 2
        return min(max(half + random.uniform(0, half), self._base), self._cap)


def _parse_retry_after(value: Optional[str]) -> float:
    """Interpret a Retry-After header (delta-seconds or HTTP-date). Returns 0
    when absent, malformed, or in the past."""
    if not value:
        return 0.0
    value = value.strip()
    try:
        seconds = int(value)
        return float(seconds) if seconds > 0 else 0.0
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return 0.0
    if parsed is None:
        return 0.0
    delta = parsed.timestamp() - time.time()
    return delta if delta > 0 else 0.0


def _bucket_user(salt: str, flag_key: str, targeting_key: str) -> int:
    """Deterministically map a user to a bucket in [0, 99].

    Uses SHA-256 over ``salt:flag_key:targeting_key`` for cross-language
    portability so any SDK (or a future server-side check) buckets identically.
    The salt is stable per flag, making ramps sticky and monotonic.
    """
    digest = hashlib.sha256(f"{salt}:{flag_key}:{targeting_key}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 100


def _extract_rollout(raw_flag: Mapping[str, Any]) -> dict[str, Any]:
    """Pull canary rollout config from a flag/delta payload.

    Accepts both snake_case (config/poll responses) and camelCase (SSE/webhook
    deltas). A missing percentage means the flag has no rollout configured.
    """
    percentage = raw_flag.get("rollout_percentage", raw_flag.get("rolloutPercentage"))
    salt = raw_flag.get("rollout_salt", raw_flag.get("rolloutSalt"))
    value = raw_flag.get("rollout_value", raw_flag.get("rolloutValue"))
    parsed: Optional[int] = None
    if percentage is not None:
        try:
            parsed = int(percentage)
        except (TypeError, ValueError):
            parsed = None
    return {"percentage": parsed, "salt": salt, "value": value}


def parse_app_version(raw: str) -> Optional[list[int]]:
    """Parse a lenient dotted version into its numeric components.

    Accepts ``"3"``, ``"2.4"``, ``"2.4.1"``, ``"v2.4.1-rc.1"``: a leading ``v``
    is dropped and any pre-release or build suffix is ignored, since app-version
    gating is coarse by design. Returns ``None`` when a component is not a
    number. Mirrors ``parseAppVersion`` in ``core_service/configs.go``.
    """
    trimmed = raw.strip()
    if trimmed.startswith("v"):
        trimmed = trimmed[1:]
    for index, char in enumerate(trimmed):
        if char in "-+":
            trimmed = trimmed[:index]
            break
    trimmed = trimmed.strip()
    if not trimmed:
        return None
    components: list[int] = []
    for part in trimmed.split("."):
        try:
            number = int(part.strip())
        except ValueError:
            return None
        if number < 0:
            return None
        components.append(number)
    return components


def compare_app_versions(a: list[int], b: list[int]) -> int:
    """Order two parsed versions, padding the shorter so "2.4" equals "2.4.0"."""
    for index in range(max(len(a), len(b))):
        left = a[index] if index < len(a) else 0
        right = b[index] if index < len(b) else 0
        if left != right:
            return -1 if left < right else 1
    return 0


def config_entry_applies(entry: dict[str, Any], app_version: Optional[list[int]]) -> bool:
    """Whether an entry's app-version range (both bounds inclusive) includes ``app_version``.

    Every unknown fails open — a ``None`` ``app_version``, an absent bound, or an
    unparseable one leaves the entry applying — so bad metadata degrades to the
    ungated behaviour rather than silently hiding a value.
    """
    if app_version is None:
        return True
    lower_raw = entry.get("min_app_version")
    if isinstance(lower_raw, str):
        lower = parse_app_version(lower_raw)
        if lower is not None and compare_app_versions(app_version, lower) < 0:
            return False
    upper_raw = entry.get("max_app_version")
    if isinstance(upper_raw, str):
        upper = parse_app_version(upper_raw)
        if upper is not None and compare_app_versions(app_version, upper) > 0:
            return False
    return True


def resolve_config_entry(
    entry: dict[str, Any], app_version: Optional[list[int]]
) -> tuple[bool, Any]:
    """What an entry resolves to for ``app_version``.

    Returns ``(found, value)``: the entry's ``value`` when it applies, otherwise
    its ``default``. ``found`` is ``False`` when neither key is present, leaving
    the caller to use its own fallback.
    """
    key = "value" if config_entry_applies(entry, app_version) else "default"
    if key not in entry:
        return False, None
    return True, entry[key]


def unsatisfied_required_entries(
    document: Any, app_version: Optional[list[int]]
) -> list[str]:
    """Entries in ``document`` marked ``required`` that resolve to nothing.

    Typically an entry gated out of this build's version range with no default
    to fall back on. Reported through ``on_error`` so the mistake surfaces at
    startup rather than at the first read.
    """
    if not isinstance(document, dict):
        return []
    entries = document.get("configs")
    if not isinstance(entries, dict):
        return []
    unsatisfied: list[str] = []
    for key, entry in entries.items():
        if not isinstance(entry, dict) or entry.get("required") is not True:
            continue
        found, value = resolve_config_entry(entry, app_version)
        if not found or value is None:
            unsatisfied.append(key)
    return sorted(unsatisfied)


def redact_sensitive_config_entries(document: Any) -> Any:
    """A copy of ``document`` without the values of entries marked ``sensitive``.

    Config values arrive with ``${secret}`` references already resolved
    server-side, so persisting a document verbatim would write plaintext secrets
    to the cache file; marking an entry sensitive keeps it in memory only. Such
    an entry reads as absent after a warm start, until the first successful
    fetch repopulates it.
    """
    if not isinstance(document, dict):
        return document
    entries = document.get("configs")
    if not isinstance(entries, dict):
        return document
    redacted_entries: Optional[dict[str, Any]] = None
    for key, entry in entries.items():
        if not isinstance(entry, dict) or entry.get("sensitive") is not True:
            continue
        if redacted_entries is None:
            redacted_entries = dict(entries)
        redacted_entries[key] = {
            k: v for k, v in entry.items() if k not in ("value", "default")
        }
    if redacted_entries is None:
        return document
    return {**document, "configs": redacted_entries}


class GooseSDKError(Exception):
    """Base SDK error."""


class GooseSDKHTTPError(GooseSDKError):
    """Raised for non-2xx HTTP responses."""

    def __init__(
        self,
        status_code: int,
        message: str,
        response_text: str = "",
        retry_after: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        # Server Retry-After hint in seconds (e.g. on a 429), or 0 when absent.
        self.retry_after = retry_after


class ConnectionType(str, Enum):
    POLLING = "polling"
    SSE = "sse"
    WEBHOOK = "webhook"


class ConnectionState(str, Enum):
    """Whether the client is currently receiving updates."""

    CONNECTING = "connecting"
    LIVE = "live"
    DEGRADED = "degraded"
    CLOSED = "closed"


class StorageAdapter(Protocol):
    def create_or_update(self, flagset: str, flag_key: str, value: Any) -> None:
        """Persist an upsert for a flag value."""


@dataclass
class ConfigChangeEvent:
    """A change to a watched config document delivered to ``on(name, ...)`` listeners.

    ``name`` is the watched config name (the document name). ``old_value`` and
    ``new_value`` are the full config documents (the ``{"configs": {...}}`` dict)
    before and after the change; ``old_value`` is ``None`` for a newly seen
    config. ``apply_strategy`` is ``"requires_restart"`` when any changed inner
    entry in the document requires a restart, else ``"immediate"``.
    """

    name: str
    old_value: Any
    new_value: Any
    apply_strategy: str


@dataclass
class _HTTPResponse:
    status_code: int
    text: str
    json_payload: Any


T = TypeVar("T")


class GooseSDK(Mapping[str, Any]):
    """
    Goose server-side SDK client.

    Index behavior:
    - Single flagset: client["flag_key"] -> latest value
    - Multiple flagsets: client["flagset_name"]["flag_key"] -> latest value
    """

    def __init__(
        self,
        sdk_client_id: str,
        server_url: str,
        flagsets: str | Sequence[str],
        connection_type: str | ConnectionType | Sequence[str | ConnectionType] = ConnectionType.POLLING,
        *,
        thumbprint: Optional[str] = None,
        sdk_client_secret: Optional[str] = None,
        namespace_name: Optional[str] = None,
        configs: Optional[Sequence[str]] = None,
        app_version: Optional[str] = None,
        restart_on_required_change: bool = False,
        flagset_namespaces: Optional[Mapping[str, str]] = None,
        default_targeting_key: Optional[str] = None,
        storage_adapter: Optional[StorageAdapter] = None,
        cache: Optional[PersistentCache] = None,
        require_initial_connect: bool = False,
        on_error: Optional[Callable[[BaseException], None]] = None,
        poll_interval_seconds: float = 5.0,
        request_timeout_seconds: float = 10.0,
        sse_read_timeout_seconds: float = 65.0,
        webhook_target_url: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        webhook_name_prefix: str = "goose-python-sdk",
        webhook_listener_enabled: bool = True,
        webhook_listener_host: str = "0.0.0.0",
        webhook_listener_port: Optional[int] = None,
        webhook_listener_path: Optional[str] = None,
        auto_connect: bool = True,
    ) -> None:
        if not sdk_client_id.strip() or not server_url.strip():
            raise ValueError("sdk_client_id and server_url are required")

        normalized_flagsets = self._normalize_flagsets(flagsets)
        if not normalized_flagsets:
            raise ValueError("at least one flagset is required")

        self.sdk_client_id = sdk_client_id.strip()
        # The client secret is only sent on secret-bearing endpoints (app configs +
        # webhook registration). Flag reads authenticate by client_id alone, so a
        # flag-only client may omit it. The requirement is enforced below.
        self.sdk_client_secret = (sdk_client_secret or "").strip()
        self.thumbprint = self._resolve_thumbprint(thumbprint)
        self.server_url = server_url.rstrip("/")
        self.flagsets = normalized_flagsets
        self.connection_types = self._normalize_connection_types(connection_type)
        self.storage_adapter = storage_adapter
        self._cache = cache
        self._require_initial_connect = bool(require_initial_connect)
        self.poll_interval_seconds = max(1.0, poll_interval_seconds)
        self.request_timeout_seconds = max(1.0, request_timeout_seconds)
        self.sse_read_timeout_seconds = max(5.0, sse_read_timeout_seconds)
        self.webhook_target_url = webhook_target_url
        self.webhook_secret = webhook_secret or secrets.token_urlsafe(24)
        self.webhook_name_prefix = webhook_name_prefix.strip() or "goose-python-sdk"
        self.webhook_listener_enabled = webhook_listener_enabled

        self.namespace_name = self._normalize_namespace(namespace_name)
        self.flagset_namespaces = self._normalize_flagset_namespaces(flagset_namespaces)

        # Watched config names are resolved against the client-level namespace_name.
        self._config_names: list[str] = self._normalize_config_names(configs)
        self.restart_on_required_change = bool(restart_on_required_change)
        if self._config_names and self.namespace_name is None:
            raise ValueError("namespace_name is required when watching configs")
        # Stable per-user identifier used to bucket canary rollouts when a caller
        # does not pass an explicit targeting_key to get_flag().
        self.default_targeting_key = (
            default_targeting_key.strip() if isinstance(default_targeting_key, str) else None
        ) or None

        parsed_webhook_target = (
            urlparse(self.webhook_target_url) if self.webhook_target_url is not None else None
        )
        if ConnectionType.WEBHOOK in self.connection_types:
            if not self.webhook_target_url:
                raise ValueError("webhook_target_url is required when connection_type includes webhook")
            if parsed_webhook_target is None or not parsed_webhook_target.scheme or not parsed_webhook_target.netloc:
                raise ValueError("webhook_target_url must be an absolute URL")

        # App configs and webhook registration are secret-bearing endpoints.
        if (self._config_names or ConnectionType.WEBHOOK in self.connection_types) and not self.sdk_client_secret:
            raise ValueError("sdk_client_secret is required when watching configs or using webhooks")

        inferred_webhook_port = 8091 if webhook_listener_port is None else int(webhook_listener_port)

        inferred_webhook_path = webhook_listener_path
        if inferred_webhook_path is None and parsed_webhook_target is not None:
            inferred_webhook_path = parsed_webhook_target.path
        if not inferred_webhook_path:
            inferred_webhook_path = "/webhook"
        if not inferred_webhook_path.startswith("/"):
            inferred_webhook_path = f"/{inferred_webhook_path}"

        self.webhook_listener_host = webhook_listener_host
        self.webhook_listener_port = inferred_webhook_port
        self.webhook_listener_path = inferred_webhook_path

        self._log = logging.getLogger("goose_sdk")
        # This application's own version, used to gate config entries carrying
        # min_app_version / max_app_version. An unparseable value fails open:
        # gating is skipped entirely rather than silently resolving every gated
        # entry to its default.
        self.app_version = (app_version or "").strip()
        self._app_version = parse_app_version(self.app_version) if self.app_version else None
        if self.app_version and self._app_version is None:
            self._log.warning(
                "ignoring unparseable app_version '%s'; config app-version gating is disabled",
                self.app_version,
            )
        # "<config name>\x00<entry key>" of entries already warned about as deprecated.
        self._warned_deprecated: set[str] = set()
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._webhook_server: Optional[ThreadingHTTPServer] = None
        self._webhook_server_thread: Optional[threading.Thread] = None
        self._webhook_events_received = 0

        # Self-healing state (guarded by _lock unless otherwise noted).
        self._state = ConnectionState.CONNECTING
        self._last_sync_at: Optional[float] = None
        self._consecutive_failures = 0
        self._state_listeners: list[Callable[[ConnectionState], None]] = []
        self._error_listeners: list[Callable[[BaseException], None]] = []
        if on_error is not None:
            self._error_listeners.append(on_error)
        # SSE→polling fallback: flagsets whose SSE stream is down, plus the
        # controller for the temporary polling loop that keeps them fresh.
        self._sse_down_flagsets: set[str] = set()
        self._fallback_stop = threading.Event()
        self._fallback_thread: Optional[threading.Thread] = None
        # Signals the debounced cache flusher that persisted state changed.
        self._cache_dirty = threading.Event()

        self._connected = False
        self._initialized = False
        # Informational only: config_service resolves the org from client_id on
        # every request, so the SDK sends self.sdk_client_id as the client_id and
        # never needs the org id for data calls. Populated best-effort at connect.
        self._resolved_organization_id: Optional[str] = None

        self._flags: dict[str, dict[str, Any]] = {flagset: {} for flagset in self.flagsets}
        self._flag_data_types: dict[str, dict[str, str]] = {flagset: {} for flagset in self.flagsets}
        # Canary rollout config per flag: {flagset: {flag_key: {"percentage": int|None, "salt": str|None}}}.
        # A non-None percentage switches get_flag() to client-side bucketing.
        self._rollouts: dict[str, dict[str, dict[str, Any]]] = {flagset: {} for flagset in self.flagsets}
        # Per-flagset, per-flag segment overrides, kept beside rollouts because
        # both are evaluated locally on the read path.
        self._segments: dict[str, dict[str, Any]] = {flagset: {} for flagset in self.flagsets}
        self._poll_cursors: dict[str, int] = {flagset: 0 for flagset in self.flagsets}

        # Watched configs: name -> {"document": <parsed dict>, "revision": <int>}.
        # Each watched name is a named JSON document in self.namespace_name with its
        # own revision. Delivered via the polling loop (not via flagsets).
        self._configs: dict[str, dict[str, Any]] = {}
        self._config_listeners: dict[str, list[Callable[[ConfigChangeEvent], None]]] = {}
        self._restart_listeners: list[Callable[[ConfigChangeEvent], None]] = []

        # Recently applied delta eventIds. Webhooks are delivered at-least-once
        # (a redelivery after a mid-dispatch crash repeats the same eventId), so
        # we apply each delta at most once. Bounded LRU to cap memory.
        self._seen_event_ids: "OrderedDict[str, None]" = OrderedDict()
        self._max_seen_event_ids = 2048

        if auto_connect:
            self.connect()

    @property
    def organization_id(self) -> Optional[str]:
        return self._resolved_organization_id

    @property
    def webhook_listener_url(self) -> str:
        return f"http://{self.webhook_listener_host}:{self.webhook_listener_port}{self.webhook_listener_path}"

    @property
    def webhook_events_received(self) -> int:
        return self._webhook_events_received

    def connect(self) -> None:
        """Fetch the initial snapshot and start the configured loops.

        By default connecting is graceful: if the server is unreachable the
        client comes up in ``ConnectionState.DEGRADED``, serves warm-started
        cache values (or defaults), and heals in the background. Pass
        ``require_initial_connect=True`` to restore fail-fast startup.
        """
        if self._connected:
            return

        self._stop_event.clear()

        # Warm start: hydrate last-known-good from the cache before any network
        # call, so reads are answerable immediately even if the server is down.
        self._load_from_cache()

        try:
            self._resolved_organization_id = self._resolve_organization_id()
        except Exception as error:  # noqa: BLE001
            self._log.debug("sdk org resolve failed: %s", error)
            self._resolved_organization_id = ""

        try:
            self.refresh()
            self._record_success()
        except Exception as error:  # noqa: BLE001
            if self._require_initial_connect:
                raise
            self._log.warning(
                "initial refresh failed; starting in degraded mode and healing in background: %s",
                error,
            )
            self._record_failure(error)

        if ConnectionType.WEBHOOK in self.connection_types:
            if self.webhook_listener_enabled:
                try:
                    self._start_webhook_listener()
                except Exception as error:  # noqa: BLE001
                    if self._require_initial_connect:
                        raise
                    self._log.warning("webhook listener failed to start: %s", error)
                    self._emit_error(error)
            try:
                self.register_webhooks()
            except Exception as error:  # noqa: BLE001
                if self._require_initial_connect:
                    raise
                self._log.warning("webhook registration failed: %s", error)
                self._emit_error(error)

        if ConnectionType.POLLING in self.connection_types:
            poller = threading.Thread(target=self._polling_loop, name="goose-sdk-poller", daemon=True)
            poller.start()
            self._threads.append(poller)

        if ConnectionType.SSE in self.connection_types:
            for flagset in self.flagsets:
                worker = threading.Thread(target=self._sse_loop, args=(flagset,), name=f"goose-sdk-sse-{flagset}", daemon=True)
                worker.start()
                self._threads.append(worker)

        if self._cache is not None:
            flusher = threading.Thread(target=self._cache_flush_loop, name="goose-sdk-cache-flusher", daemon=True)
            flusher.start()
            self._threads.append(flusher)

        self._connected = True

    def close(self) -> None:
        with self._lock:
            previous = self._state
            self._state = ConnectionState.CLOSED
            listeners = list(self._state_listeners)
        if previous != ConnectionState.CLOSED:
            self._notify_state(listeners, ConnectionState.CLOSED)

        self._stop_event.set()
        self._fallback_stop.set()
        self._cache_dirty.set()  # wake the flusher for its final flush
        self._stop_webhook_listener()
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads = []
        self._connected = False

    def refresh(self) -> None:
        if self._initialized and ConnectionType.POLLING in self.connection_types:
            for flagset in self.flagsets:
                self._poll_once(flagset)
            if self._config_names:
                self._poll_configs_once()
            return

        for flagset in self.flagsets:
            snapshot = self._fetch_config_snapshot(flagset)

            snapshot_flags = snapshot.get("flags", []) if isinstance(snapshot, dict) else []
            self._apply_snapshot(flagset, snapshot_flags)
            snapshot_cursor = self._max_updated_timestamp(snapshot_flags) or int(time.time())
            self._poll_cursors[flagset] = max(self._poll_cursors.get(flagset, 0), snapshot_cursor)

        if self._config_names:
            self._fetch_config_snapshot_once()

        self._initialized = True

    def register_webhooks(self) -> None:
        for flagset in self.flagsets:
            payload: dict[str, Any] = {
                "client_id": self.sdk_client_id,
                "client_secret": self.sdk_client_secret,
                "webhook_name": f"{self.webhook_name_prefix}-{flagset}",
                "flagset": flagset,
                "target_url": self.webhook_target_url,
                "secret": self.webhook_secret,
            }
            namespace = self._namespace_for_flagset(flagset)
            if namespace:
                payload["namespace_name"] = namespace

            try:
                self._request("POST", "/api/v1/webhook", json_body=payload)
            except GooseSDKHTTPError as error:
                if error.status_code == 409:
                    continue
                if error.status_code == 400 and (
                    "already exists" in error.response_text.lower() or error.response_text.strip() == ""
                ):
                    continue
                raise

    def process_webhook_event(self, payload: dict[str, Any], received_secret: Optional[str] = None) -> None:
        if received_secret is not None and not secrets.compare_digest(received_secret, self.webhook_secret):
            raise PermissionError("invalid webhook secret")
        self._apply_delta(payload)
        self._webhook_events_received += 1

    def _start_webhook_listener(self) -> None:
        if self._webhook_server is not None:
            return

        sdk_client = self

        class EmbeddedWebhookHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                request_path = urlparse(self.path).path
                if request_path != sdk_client.webhook_listener_path:
                    self.send_response(HTTPStatus.NOT_FOUND)
                    self.end_headers()
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)

                try:
                    payload = json.loads(raw_body.decode("utf-8"))
                except json.JSONDecodeError:
                    self.send_response(HTTPStatus.BAD_REQUEST)
                    self.end_headers()
                    self.wfile.write(b"invalid json payload")
                    return

                try:
                    sdk_client.process_webhook_event(
                        payload,
                        received_secret=self.headers.get("X-Webhook-Secret"),
                    )
                except PermissionError:
                    self.send_response(HTTPStatus.UNAUTHORIZED)
                    self.end_headers()
                    self.wfile.write(b"invalid webhook secret")
                    return
                except Exception as error:  # noqa: BLE001
                    sdk_client._log.warning("embedded webhook processing failed: %s", error)
                    self.send_response(HTTPStatus.BAD_REQUEST)
                    self.end_headers()
                    self.wfile.write(str(error).encode("utf-8"))
                    return

                self.send_response(HTTPStatus.OK)
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
                sdk_client._log.debug("embedded webhook: " + fmt, *args)

        try:
            server = ThreadingHTTPServer(
                (self.webhook_listener_host, self.webhook_listener_port),
                EmbeddedWebhookHandler,
            )
        except OSError as exc:
            raise GooseSDKError(
                f"failed to start embedded webhook listener on "
                f"{self.webhook_listener_host}:{self.webhook_listener_port}: {exc}"
            ) from exc

        thread = threading.Thread(
            target=server.serve_forever,
            name="goose-sdk-webhook-listener",
            daemon=True,
        )
        thread.start()

        self._webhook_server = server
        self._webhook_server_thread = thread
        self._log.info("embedded webhook listener started at %s", self.webhook_listener_url)

    def _stop_webhook_listener(self) -> None:
        if self._webhook_server is None:
            return

        self._webhook_server.shutdown()
        self._webhook_server.server_close()
        if self._webhook_server_thread is not None:
            self._webhook_server_thread.join(timeout=2.0)

        self._webhook_server = None
        self._webhook_server_thread = None

    def get_flag(
        self,
        flag_key: str,
        flagset: Optional[str] = None,
        default: Any = None,
        *,
        targeting_key: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Return the flag's value.

        When the flag has a canary rollout configured, the value is evaluated
        client-side by deterministically bucketing ``targeting_key`` (falling back
        to ``default_targeting_key``): in-bucket users receive the canary's
        candidate value, everyone else receives the baseline (stored) value. This
        works for every data type — a boolean canary serves True/False, a number
        canary serves the candidate number, and so on. Flags without a rollout
        return their stored value unchanged.
        """
        with self._lock:
            if flagset is None:
                if len(self.flagsets) != 1:
                    raise ValueError("flagset is required when multiple flagsets are configured")
                flagset = self.flagsets[0]

            value = self._flags.get(flagset, {}).get(flag_key, default)
            rollout = self._rollouts.get(flagset, {}).get(flag_key)
            data_type = self._flag_data_types.get(flagset, {}).get(flag_key, "")
            targeting = self._segments.get(flagset, {}).get(flag_key)

        # A read must never surface a fault to the caller. If rollout evaluation
        # hits unexpected data, fall back to the last-known-good (stored) value.
        try:
            # Segment targeting resolves before the canary: being in the
            # audience is a statement about who you are, while a canary is
            # about what fraction of traffic sees something. A subject in the
            # segment gets the targeted value outright rather than re-diced.
            if targeting is not None and matches_segment(targeting["definition"], context or {}):
                return self._coerce_value(targeting["value"], data_type)

            if rollout is not None and rollout.get("percentage") is not None:
                return self._evaluate_rollout(
                    flag_key, rollout, targeting_key, baseline=value, data_type=data_type
                )
            return value
        except Exception as error:  # noqa: BLE001
            self._log.warning(
                "flag resolution recovered for %s; returning last-known-good: %s", flag_key, error
            )
            self._emit_error(error)
            return value

    def _evaluate_rollout(
        self,
        flag_key: str,
        rollout: dict[str, Any],
        targeting_key: Optional[str],
        *,
        baseline: Any,
        data_type: str,
    ) -> Any:
        key = targeting_key if targeting_key is not None else self.default_targeting_key
        if not key:
            self._log.warning(
                "flag %s has a canary rollout but no targeting_key was provided; "
                "returning the baseline value",
                flag_key,
            )
            return baseline
        bucket = _bucket_user(rollout.get("salt") or "", flag_key, str(key))
        if bucket < int(rollout["percentage"]):
            candidate = rollout.get("value")
            if candidate is None:
                return baseline
            return self._coerce_value(candidate, data_type)
        return baseline

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {flagset: dict(values) for flagset, values in self._flags.items()}

    def get_config(self, name: str, default: Any = None) -> Any:
        """Return a watched config's current document, or ``default`` if unset.

        A config is a named JSON document of the shape
        ``{"configs": {"<entry>": {"value": ..., "apply_strategy": ...}}}``. The
        whole document is returned (native JSON, no coercion). Reading a name that
        is not being watched logs a warning and returns ``default``.
        """
        if name not in self._config_names:
            self._log.warning("get_config('%s') called for an unwatched config name", name)
            return default
        with self._lock:
            entry = self._configs.get(name)
            if entry is None:
                return default
            return entry.get("document", default)

    def get_config_value(self, name: str, key: str, default: Any = None) -> Any:
        """Return a single entry's value from within a watched config document.

        A config document holds ``{"configs": {"<key>": {"value": ...,
        "apply_strategy": ...}}}``; this returns the ``value`` for ``key`` inside
        the config named ``name`` (e.g. ``get_config_value("frontend",
        "dashboard_layout")``). Returns ``default`` when the config is not watched,
        not yet loaded, the key is absent, or the document is malformed.

        This is where per-entry metadata is applied. When the client was built
        with an ``app_version`` and the entry's min/max_app_version range excludes
        it, the entry's ``default`` key is returned instead of its ``value`` (and
        the ``default`` argument when it has neither). A ``deprecated`` entry logs
        a warning the first time it is read. Reading an entry straight out of
        ``get_config``'s document bypasses all of this.
        """
        document = self.get_config(name, default=None)
        if not isinstance(document, dict):
            return default
        entries = document.get("configs")
        if not isinstance(entries, dict):
            return default
        entry = entries.get(key)
        if not isinstance(entry, dict):
            return default
        self._warn_deprecated_once(name, key, entry)
        found, value = resolve_config_entry(entry, self._app_version)
        return value if found else default

    def _warn_deprecated_once(self, name: str, key: str, entry: dict[str, Any]) -> None:
        """Warn on the first read of an entry marked ``deprecated``.

        The warning names the entry's ``replaced_by`` successor when one is set.
        Later reads stay quiet so a hot path does not flood the log.
        """
        if entry.get("deprecated") is not True:
            return
        cache_key = f"{name}\x00{key}"
        with self._lock:
            if cache_key in self._warned_deprecated:
                return
            self._warned_deprecated.add(cache_key)
        replaced_by = str(entry.get("replaced_by") or "").strip()
        if replaced_by:
            self._log.warning(
                "config entry '%s.%s' is deprecated; use '%s' instead", name, key, replaced_by
            )
        else:
            self._log.warning("config entry '%s.%s' is deprecated", name, key)

    def configs_snapshot(self) -> dict[str, Any]:
        """Return a ``{name: document}`` copy of the watched configs' documents."""
        with self._lock:
            return {
                name: entry.get("document")
                for name, entry in self._configs.items()
            }

    def on(self, name: str, callback: Callable[[ConfigChangeEvent], None]) -> None:
        """Register a callback fired when the watched config ``name`` changes."""
        with self._lock:
            self._config_listeners.setdefault(name, []).append(callback)

    def on_restart_required(self, callback: Callable[[ConfigChangeEvent], None]) -> None:
        """Register a drain callback run when a ``requires_restart`` config changes."""
        with self._lock:
            self._restart_listeners.append(callback)

    # --- Health / self-healing ---

    def state(self) -> ConnectionState:
        """Return the current connection state."""
        with self._lock:
            return self._state

    def last_sync(self) -> Optional[float]:
        """Return the epoch time of the last successful sync, or ``None``."""
        with self._lock:
            return self._last_sync_at

    def is_stale(self) -> bool:
        """Report whether served values may be stale (no successful sync within
        ``max(3×poll_interval, sse_read_timeout)``). Never-synced counts as stale."""
        with self._lock:
            last = self._last_sync_at
        if last is None:
            return True
        window = max(3 * self.poll_interval_seconds, self.sse_read_timeout_seconds)
        return (time.time() - last) > window

    def on_state_change(self, callback: Callable[[ConnectionState], None]) -> Callable[[], None]:
        """Subscribe to connection-state transitions. Returns an unsubscribe function."""
        with self._lock:
            self._state_listeners.append(callback)

        def unsubscribe() -> None:
            with self._lock:
                try:
                    self._state_listeners.remove(callback)
                except ValueError:
                    pass

        return unsubscribe

    def on_error(self, callback: Callable[[BaseException], None]) -> Callable[[], None]:
        """Subscribe to background errors (never affect reads). Returns an unsubscribe function."""
        with self._lock:
            self._error_listeners.append(callback)

        def unsubscribe() -> None:
            with self._lock:
                try:
                    self._error_listeners.remove(callback)
                except ValueError:
                    pass

        return unsubscribe

    def _record_success(self) -> None:
        with self._lock:
            self._last_sync_at = time.time()
            self._consecutive_failures = 0
            listeners, changed, new_state = self._set_state_locked(ConnectionState.LIVE)
        if changed:
            self._notify_state(listeners, new_state)

    def _record_failure(self, error: BaseException) -> None:
        with self._lock:
            self._consecutive_failures += 1
            listeners, changed, new_state = self._set_state_locked(ConnectionState.DEGRADED)
        if changed:
            self._notify_state(listeners, new_state)
        self._emit_error(error)

    def _set_state_locked(
        self, next_state: ConnectionState
    ) -> tuple[list[Callable[[ConnectionState], None]], bool, ConnectionState]:
        # Caller holds _lock. Closed is terminal.
        if self._state == ConnectionState.CLOSED:
            return [], False, self._state
        previous = self._state
        self._state = next_state
        return list(self._state_listeners), previous != next_state, next_state

    def _notify_state(
        self, listeners: list[Callable[[ConnectionState], None]], state: ConnectionState
    ) -> None:
        for listener in listeners:
            try:
                listener(state)
            except Exception as error:  # noqa: BLE001
                self._log.warning("on_state_change listener failed: %s", error)

    def _emit_error(self, error: Optional[BaseException]) -> None:
        if error is None:
            return
        with self._lock:
            listeners = list(self._error_listeners)
        for listener in listeners:
            try:
                listener(error)
            except Exception as exc:  # noqa: BLE001
                self._log.warning("on_error listener failed: %s", exc)

    def _honor_retry_after(self, error: BaseException) -> None:
        """Block for a 429's Retry-After hint so we don't immediately re-hit a
        rate-limited server."""
        if isinstance(error, GooseSDKHTTPError) and error.status_code == 429 and error.retry_after > 0:
            self._stop_event.wait(error.retry_after)

    # --- Persistent cache (warm start + debounced save) ---

    def _load_from_cache(self) -> None:
        if self._cache is None:
            return
        try:
            snapshot = self._cache.load()
        except Exception as error:  # noqa: BLE001
            self._log.warning("cache load failed: %s", error)
            self._emit_error(error)
            return
        if not snapshot:
            return

        flags = snapshot.get("flags") or {}
        data_types = snapshot.get("flag_data_types") or {}
        rollouts = snapshot.get("rollouts") or {}
        cursors = snapshot.get("poll_cursors") or {}
        configs = snapshot.get("configs") or {}

        with self._lock:
            for flagset in self.flagsets:
                for key, value in (flags.get(flagset) or {}).items():
                    self._flags.setdefault(flagset, {})[key] = value
                for key, value in (data_types.get(flagset) or {}).items():
                    self._flag_data_types.setdefault(flagset, {})[key] = value
                for key, value in (rollouts.get(flagset) or {}).items():
                    self._rollouts.setdefault(flagset, {})[key] = value
                cursor = cursors.get(flagset)
                if isinstance(cursor, int) and cursor > self._poll_cursors.get(flagset, 0):
                    self._poll_cursors[flagset] = cursor
            for name in self._config_names:
                entry = configs.get(name)
                if isinstance(entry, dict) and isinstance(entry.get("document"), dict):
                    self._configs[name] = {
                        "document": entry["document"],
                        "revision": int(entry.get("revision", 0) or 0),
                    }
        self._log.info("warm-started from persistent cache")

    def _mark_dirty(self) -> None:
        if self._cache is not None:
            self._cache_dirty.set()

    def _cache_flush_loop(self) -> None:
        while not self._stop_event.is_set():
            self._cache_dirty.wait(timeout=1.0)
            if self._stop_event.is_set():
                break
            if self._cache_dirty.is_set():
                self._cache_dirty.clear()
                # Coalesce a brief burst of changes into one write.
                self._stop_event.wait(1.0)
                self._flush_cache()
        self._flush_cache()  # final flush on shutdown

    def _flush_cache(self) -> None:
        if self._cache is None:
            return
        snapshot = self._build_snapshot()
        try:
            self._cache.save(snapshot)
        except Exception as error:  # noqa: BLE001
            self._log.warning("cache save failed: %s", error)
            self._emit_error(error)

    def _build_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "flags": {fs: dict(values) for fs, values in self._flags.items()},
                "flag_data_types": {fs: dict(values) for fs, values in self._flag_data_types.items()},
                "rollouts": {
                    fs: {key: dict(rollout) for key, rollout in values.items()}
                    for fs, values in self._rollouts.items()
                },
                "poll_cursors": dict(self._poll_cursors),
                "configs": {
                    name: {
                        "document": redact_sensitive_config_entries(entry.get("document")),
                        "revision": entry.get("revision", 0),
                    }
                    for name, entry in self._configs.items()
                },
            }

    # --- SSE→polling fallback ---

    def _mark_sse_down(self, flagset: str) -> None:
        if ConnectionType.POLLING in self.connection_types:
            return  # steady polling already keeps flags fresh
        with self._lock:
            self._sse_down_flagsets.add(flagset)
            if self._fallback_thread is None or not self._fallback_thread.is_alive():
                self._fallback_stop.clear()
                thread = threading.Thread(
                    target=self._fallback_polling_loop, name="goose-sdk-fallback-poller", daemon=True
                )
                self._fallback_thread = thread
                self._threads.append(thread)
                thread.start()
                self._log.warning("sse unavailable; starting fallback polling (flagset=%s)", flagset)

    def _mark_sse_up(self, flagset: str) -> None:
        with self._lock:
            self._sse_down_flagsets.discard(flagset)
            if not self._sse_down_flagsets and self._fallback_thread is not None:
                self._fallback_stop.set()
                self._fallback_thread = None
                self._log.info("sse recovered; stopping fallback polling")

    def _fallback_polling_loop(self) -> None:
        while not self._stop_event.is_set() and not self._fallback_stop.is_set():
            for flagset in self.flagsets:
                try:
                    self._poll_once(flagset)
                    self._record_success()
                except Exception as error:  # noqa: BLE001
                    self._record_failure(error)
            if self._config_names:
                try:
                    self._poll_configs_once()
                except Exception as error:  # noqa: BLE001
                    self._record_failure(error)
            if self._stop_event.wait(self.poll_interval_seconds) or self._fallback_stop.is_set():
                break

    def _coerce_and_validate(self, raw_value: Any, data_type: str) -> tuple[Any, bool]:
        """Coerce ``raw_value`` to its declared type and report whether the result
        is usable. A ``None`` value, or one that fails to match its type (e.g. a
        non-numeric string for a number flag), is rejected."""
        if raw_value is None:
            return None, False
        value = self._coerce_value(raw_value, data_type)
        normalized = data_type.strip().lower()
        if normalized == "bool":
            return value, isinstance(value, bool)
        if normalized in ("number", "int", "integer", "float", "double"):
            return value, isinstance(value, (int, float)) and not isinstance(value, bool)
        return value, True

    def _commit_flag(
        self,
        flagset: str,
        flag_key: str,
        data_type: str,
        raw_value: Any,
        rollout: dict[str, Any],
        targeting: Optional[dict[str, Any]] = None,
    ) -> None:
        """Validate an incoming value and, only if usable, store its data type,
        rollout, and value. A malformed value is rejected (logged + reported to
        on_error) so the last-known-good value is retained."""
        value, ok = self._coerce_and_validate(raw_value, data_type)
        if not ok:
            self._log.warning(
                "rejecting malformed flag value; retaining last-known-good (flagset=%s flag=%s dtype=%s)",
                flagset,
                flag_key,
                data_type,
            )
            self._emit_error(
                GooseSDKError(
                    f"malformed value for flag '{flag_key}' in flagset '{flagset}' (data type '{data_type}')"
                )
            )
            return
        with self._lock:
            self._flag_data_types.setdefault(flagset, {})[flag_key] = data_type
            self._flags.setdefault(flagset, {})[flag_key] = value
            self._rollouts.setdefault(flagset, {})[flag_key] = rollout
            segments = self._segments.setdefault(flagset, {})
            if targeting is not None:
                segments[flag_key] = targeting
            else:
                # A payload with no segment means targeting was removed;
                # clearing mirrors how a delta with no percentage clears a canary.
                segments.pop(flag_key, None)
        self._persist(flagset, flag_key, value)
        self._mark_dirty()

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            if len(self.flagsets) == 1:
                return self._flags[self.flagsets[0]][key]
            if key not in self._flags:
                raise KeyError(key)
            return dict(self._flags[key])

    def __iter__(self):
        with self._lock:
            if len(self.flagsets) == 1:
                return iter(dict(self._flags[self.flagsets[0]]))
            return iter(list(self._flags.keys()))

    def __len__(self) -> int:
        with self._lock:
            if len(self.flagsets) == 1:
                return len(self._flags[self.flagsets[0]])
            return len(self._flags)

    def _resolve_organization_id(self) -> str:
        """Best-effort: ask config_service for the org id behind this client_id.

        The org id is informational (exposed via the organization_id property);
        data calls always send self.sdk_client_id and let config_service resolve
        the org server-side. Failures are non-fatal.
        """
        try:
            resolve_resp = self._request(
                "GET",
                "/api/v1/sdk/resolve",
                query_params={"client_id": self.sdk_client_id},
            )
        except GooseSDKHTTPError as error:
            if error.status_code in (404, 405):
                self._log.debug("SDK org resolve endpoint unavailable")
                return ""
            raise

        parsed = resolve_resp.json_payload
        payload = parsed if isinstance(parsed, dict) else {}
        return str(payload.get("organization_id", "")).strip()

    def _fetch_config_snapshot(self, flagset: str) -> dict[str, Any]:
        # Flag read: client_id only (no secret).
        query_params: dict[str, Any] = {
            "client_id": self.sdk_client_id,
            "flagset": flagset,
        }
        namespace = self._namespace_for_flagset(flagset)
        if namespace:
            query_params["namespace_name"] = namespace

        response = self._request("GET", "/api/v1/config", query_params=query_params)

        parsed = response.json_payload
        if isinstance(parsed, dict):
            return parsed
        if response.text.strip() == "":
            return {}
        raise GooseSDKError("failed to decode config snapshot")

    def _poll_once(self, flagset: str) -> None:
        cursor = self._poll_cursors.get(flagset, 0)
        payload: dict[str, Any] = {
            "client_id": self.sdk_client_id,
            "flagset": flagset,
            "timestamp": cursor,
        }
        namespace = self._namespace_for_flagset(flagset)
        if namespace:
            payload["namespace_name"] = namespace

        response = self._request("POST", "/api/v1/poll", json_body=payload)
        result = response.json_payload if isinstance(response.json_payload, dict) else {}

        flags = result.get("flags", [])
        max_timestamp = cursor
        for raw_flag in flags:
            flag_key = str(raw_flag.get("flag_key", "")).strip()
            if not flag_key:
                continue

            updated_at = str(raw_flag.get("updated_at", "")).strip()
            parsed = self._to_unix_seconds(updated_at)
            if parsed is not None:
                max_timestamp = max(max_timestamp, parsed)

            data_type = str(raw_flag.get("flag_data_type", "")).strip()
            self._commit_flag(
                flagset, flag_key, data_type, raw_flag.get("flag_value"),
                _extract_rollout(raw_flag), parse_segment_targeting(raw_flag),
            )

        self._poll_cursors[flagset] = max_timestamp

    def _fetch_config_snapshot_once(self) -> None:
        """Seed watched config documents from GET /api/v1/configs without firing listeners."""
        # App config read: secret-bearing (documents may carry resolved secrets).
        query_params: dict[str, Any] = {
            "client_id": self.sdk_client_id,
            "client_secret": self.sdk_client_secret,
            "namespace_name": self.namespace_name,
            "names": self._config_names,
        }

        response = self._request("GET", "/api/v1/configs", query_params=query_params)
        result = response.json_payload if isinstance(response.json_payload, dict) else {}

        entries = result.get("configs", {})
        self._apply_configs(entries if isinstance(entries, dict) else {}, initial=True)

    def _poll_configs_once(self) -> None:
        # App config read: secret-bearing (documents may carry resolved secrets).
        payload: dict[str, Any] = {
            "client_id": self.sdk_client_id,
            "client_secret": self.sdk_client_secret,
            "namespace_name": self.namespace_name,
            "names": self._config_names,
        }

        response = self._request("POST", "/api/v1/configs/poll", json_body=payload)
        result = response.json_payload if isinstance(response.json_payload, dict) else {}

        entries = result.get("configs", {})
        self._apply_configs(entries if isinstance(entries, dict) else {})

    def _apply_configs(
        self,
        entries: dict[str, Any],
        *,
        initial: bool = False,
    ) -> None:
        """Apply a batch of named config documents.

        ``entries`` is ``{name: {"document": <dict>, "revision_number": <int>}}``.
        For each watched name present, the new document is compared against the
        cached one; a difference (or a newly seen name) produces a
        ``ConfigChangeEvent`` whose ``apply_strategy`` is ``requires_restart`` when
        any changed inner entry requires a restart. On ``initial`` the cache is
        seeded without dispatching listeners.
        """
        changes: list[ConfigChangeEvent] = []
        unsatisfied_required: list[tuple[str, list[str]]] = []
        batch_requires_restart = False

        with self._lock:
            for name in self._config_names:
                incoming = entries.get(name)
                if not isinstance(incoming, dict):
                    # Names absent from the response keep their current document.
                    continue

                new_doc = incoming.get("document")
                if not isinstance(new_doc, dict):
                    continue
                revision_number = int(incoming.get("revision_number", 0) or 0)

                current = self._configs.get(name)
                old_doc = current.get("document") if current is not None else None

                if current is None or old_doc != new_doc:
                    # Reported on first sight and on each change, not on every
                    # poll, so an unsatisfied entry does not flood on_error
                    # once per interval.
                    missing = unsatisfied_required_entries(new_doc, self._app_version)
                    if missing:
                        unsatisfied_required.append((name, missing))

                    changed_entries = self._changed_config_entries(
                        old_doc, new_doc, self._app_version
                    )
                    requires_restart = any(
                        strategy == "requires_restart" for _, strategy in changed_entries
                    )
                    if requires_restart:
                        batch_requires_restart = True
                    changes.append(
                        ConfigChangeEvent(
                            name=name,
                            old_value=old_doc,
                            new_value=new_doc,
                            apply_strategy="requires_restart" if requires_restart else "immediate",
                        )
                    )

                self._configs[name] = {"document": new_doc, "revision": revision_number}

            if changes:
                self._mark_dirty()

            listeners: dict[str, list[Callable[[ConfigChangeEvent], None]]] = {}
            restart_listeners: list[Callable[[ConfigChangeEvent], None]] = []
            if not initial:
                listeners = {
                    change.name: list(self._config_listeners.get(change.name, []))
                    for change in changes
                }
                restart_listeners = list(self._restart_listeners)

        for name, missing in unsatisfied_required:
            error = GooseSDKError(
                f"config '{name}': required entries resolve to nothing for app version "
                f"'{self.app_version}': {', '.join(missing)}"
            )
            self._log.error("required config entry is unsatisfied: %s", error)
            self._emit_error(error)

        if initial:
            return

        for change in changes:
            for callback in listeners.get(change.name, []):
                try:
                    callback(change)
                except Exception as error:  # noqa: BLE001
                    self._log.warning("config listener for '%s' failed: %s", change.name, error)

        if batch_requires_restart:
            # Drains receive the first requires_restart change in the batch so
            # callbacks can inspect which config triggered the restart.
            restart_change = next(
                c for c in changes if c.apply_strategy == "requires_restart"
            )
            for callback in restart_listeners:
                try:
                    callback(restart_change)
                except Exception as error:  # noqa: BLE001
                    self._log.warning("restart-required drain callback failed: %s", error)

            if self.restart_on_required_change:
                self._log.info(
                    "requires_restart config changed; raising SIGINT for orchestrator restart"
                )
                signal.raise_signal(signal.SIGINT)

    @staticmethod
    def _changed_config_entries(
        old_doc: Optional[dict[str, Any]],
        new_doc: dict[str, Any],
        app_version: Optional[list[int]] = None,
    ) -> list[tuple[str, str]]:
        """Return inner entries from ``new_doc`` that changed vs ``old_doc``.

        Each result is ``(entry_key, apply_strategy)`` for an inner entry whose
        ``value`` or ``apply_strategy`` differs from the old document (or is newly
        added). A ``None`` ``old_doc`` treats every inner entry as changed.

        Entries gated out by ``app_version`` are skipped: a change to an entry
        this build never reads must not drag the process through a
        ``requires_restart``. A ``None`` ``app_version`` leaves every entry
        applying, which is the pre-app_version behaviour.
        """
        new_entries = new_doc.get("configs")
        if not isinstance(new_entries, dict):
            return []

        old_entries = old_doc.get("configs") if isinstance(old_doc, dict) else None
        if not isinstance(old_entries, dict):
            old_entries = {}

        changed: list[tuple[str, str]] = []
        for key, new_entry in new_entries.items():
            new_entry = new_entry if isinstance(new_entry, dict) else {}
            if not config_entry_applies(new_entry, app_version):
                continue
            strategy = str(new_entry.get("apply_strategy", "")).strip()
            old_entry = old_entries.get(key)
            old_entry = old_entry if isinstance(old_entry, dict) else None
            if (
                old_entry is None
                or old_entry.get("value") != new_entry.get("value")
                or str(old_entry.get("apply_strategy", "")).strip() != strategy
            ):
                changed.append((key, strategy))
        return changed

    def _polling_loop(self) -> None:
        while not self._stop_event.is_set():
            for flagset in self.flagsets:
                try:
                    self._poll_once(flagset)
                    self._record_success()
                except Exception as error:  # noqa: BLE001
                    self._log.warning("poll failed for flagset %s: %s", flagset, error)
                    self._record_failure(error)
                    self._honor_retry_after(error)
            if self._config_names:
                try:
                    self._poll_configs_once()
                except Exception as error:  # noqa: BLE001
                    self._log.warning("config poll failed: %s", error)
                    self._record_failure(error)
                    self._honor_retry_after(error)
            self._stop_event.wait(self.poll_interval_seconds)

    def _sse_loop(self, flagset: str) -> None:
        # Flag stream: client_id only (no secret).
        payload: dict[str, Any] = {
            "clientId": self.sdk_client_id,
            "flagSet": flagset,
        }
        namespace = self._namespace_for_flagset(flagset)
        if namespace:
            payload["namespaceName"] = namespace

        backoff = _Backoff(base=1.0, cap=30.0)
        failures = 0
        while not self._stop_event.is_set():
            start = time.monotonic()
            error: Optional[BaseException] = None
            try:
                self._run_async(self._consume_sse_stream_async(payload, flagset))
            except Exception as exc:  # noqa: BLE001
                error = exc

            if self._stop_event.is_set():
                break

            # A stream that stayed up for a healthy interval counts as recovered:
            # reset backoff and stop any fallback polling.
            if time.monotonic() - start >= _SSE_STABLE_INTERVAL:
                backoff.reset()
                failures = 0
                self._mark_sse_up(flagset)

            if error is not None:
                self._record_failure(error)
                self._log.warning("sse loop disconnected for flagset %s: %s", flagset, error)

            failures += 1
            if failures >= _SSE_FALLBACK_THRESHOLD:
                self._mark_sse_down(flagset)

            delay = backoff.duration()
            retry_after = getattr(error, "retry_after", 0.0) or 0.0
            self._stop_event.wait(max(delay, retry_after))

    async def _consume_sse_stream_async(self, payload: dict[str, Any], fallback_flagset: str) -> None:
        url = f"{self.server_url}/api/v1/sse"
        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "X-Goose-Client-Thumbprint": self.thumbprint,
        }
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.request_timeout_seconds,
            sock_connect=self.request_timeout_seconds,
            sock_read=self.sse_read_timeout_seconds,
        )

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url=url, headers=headers, json=payload) as response:
                await self._raise_for_status(response, "POST", "/api/v1/sse")

                # Connected: the client is receiving updates again.
                self._record_success()

                block: list[str] = []
                while not self._stop_event.is_set():
                    raw_line = await response.content.readline()
                    if not raw_line:
                        break

                    line = raw_line.decode("utf-8", errors="replace").rstrip("\n").rstrip("\r")
                    if line == "":
                        self._handle_sse_block(block, fallback_flagset)
                        block = []
                        continue

                    block.append(line)

                if block:
                    self._handle_sse_block(block, fallback_flagset)

    def _handle_sse_block(self, block: list[str], fallback_flagset: str) -> None:
        if not block:
            return

        data_lines: list[str] = []
        for line in block:
            if line.startswith("data:"):
                data_lines.append(line[5:].strip())

        if not data_lines:
            return

        raw_data = "\n".join(data_lines)
        try:
            payload = json.loads(raw_data)
        except json.JSONDecodeError:
            self._log.warning("skipping invalid sse payload: %s", raw_data)
            return

        self._apply_delta(payload, fallback_flagset=fallback_flagset)

    def _apply_snapshot(self, flagset: str, flags: Sequence[dict[str, Any]]) -> None:
        for raw_flag in flags:
            flag_key = str(raw_flag.get("flag_key", "")).strip()
            if not flag_key:
                continue

            data_type = str(raw_flag.get("flag_data_type", "")).strip()
            self._commit_flag(
                flagset, flag_key, data_type, raw_flag.get("flag_value"),
                _extract_rollout(raw_flag), parse_segment_targeting(raw_flag),
            )

    def _apply_delta(self, payload: dict[str, Any], *, fallback_flagset: Optional[str] = None) -> None:
        event_id = str(payload.get("eventId") or payload.get("event_id") or "").strip()
        if event_id and self._is_duplicate_event(event_id):
            return

        flagset = str(payload.get("flagSet") or payload.get("flag_set") or fallback_flagset or "").strip()
        flag_key = str(payload.get("flagKey") or payload.get("flag_key") or "").strip()
        if not flagset or not flag_key:
            return

        with self._lock:
            data_type = self._flag_data_types.get(flagset, {}).get(flag_key, "")

        raw_value = payload.get("flagValue", payload.get("flag_value"))
        # Deltas carry the flag's current rollout state; a delta with no
        # percentage means canary was disabled, so this also clears it. A
        # malformed value is rejected, retaining the last-known-good value.
        self._commit_flag(
            flagset, flag_key, data_type, raw_value,
            _extract_rollout(payload), parse_segment_targeting(payload),
        )
        self._record_success()

    def _is_duplicate_event(self, event_id: str) -> bool:
        """Return True if this delta eventId was already applied recently.

        Deltas funnel through here from both SSE and the webhook listener; because
        webhook delivery is at-least-once, the same eventId can arrive more than
        once (a redelivery repeats the original message, so this also prevents a
        late redelivery from clobbering a newer value).
        """
        with self._lock:
            if event_id in self._seen_event_ids:
                self._seen_event_ids.move_to_end(event_id)
                return True
            self._seen_event_ids[event_id] = None
            while len(self._seen_event_ids) > self._max_seen_event_ids:
                self._seen_event_ids.popitem(last=False)
            return False

    def _persist(self, flagset: str, flag_key: str, value: Any) -> None:
        if self.storage_adapter is None:
            return

        try:
            self.storage_adapter.create_or_update(flagset, flag_key, value)
        except Exception as error:  # noqa: BLE001
            self._log.warning("storage_adapter.create_or_update failed: %s", error)

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        query_params: Optional[dict[str, Any]] = None,
        timeout: Any = None,
    ) -> _HTTPResponse:
        return self._run_async(
            self._request_async(
                method=method,
                path=path,
                json_body=json_body,
                query_params=query_params,
                timeout=timeout,
            )
        )

    async def _request_async(
        self,
        *,
        method: str,
        path: str,
        json_body: Optional[dict[str, Any]] = None,
        query_params: Optional[dict[str, Any]] = None,
        timeout: Any = None,
    ) -> _HTTPResponse:
        url = f"{self.server_url}{path}"
        # Credentials travel per-request in the query/body (client_id always;
        # client_secret only on secret-bearing endpoints), never in headers.
        headers = {
            "Accept": "application/json",
            "X-Goose-Client-Thumbprint": self.thumbprint,
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        async_timeout = self._build_http_timeout(timeout)

        async with aiohttp.ClientSession(timeout=async_timeout) as session:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                params=query_params,
            ) as response:
                text = await response.text()
                if response.status >= 400:
                    message = self._extract_error_message(method, path, response.status, text)
                    raise GooseSDKHTTPError(
                        response.status,
                        message,
                        response_text=text,
                        retry_after=_parse_retry_after(response.headers.get("Retry-After")),
                    )

                json_payload: Any = None
                if text.strip():
                    try:
                        json_payload = json.loads(text)
                    except json.JSONDecodeError:
                        json_payload = None

                return _HTTPResponse(status_code=response.status, text=text, json_payload=json_payload)

    async def _raise_for_status(self, response: aiohttp.ClientResponse, method: str, path: str) -> None:
        if response.status < 400:
            return

        text = await response.text()
        message = self._extract_error_message(method, path, response.status, text)
        raise GooseSDKHTTPError(
            response.status,
            message,
            response_text=text,
            retry_after=_parse_retry_after(response.headers.get("Retry-After")),
        )

    def _build_http_timeout(self, timeout: Any) -> aiohttp.ClientTimeout:
        if timeout is None:
            return aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        if isinstance(timeout, tuple) and len(timeout) == 2:
            connect_timeout = max(float(timeout[0]), 0.1)
            read_timeout = max(float(timeout[1]), 0.1)
            return aiohttp.ClientTimeout(
                total=None,
                connect=connect_timeout,
                sock_connect=connect_timeout,
                sock_read=read_timeout,
            )

        return aiohttp.ClientTimeout(total=max(float(timeout), 0.1))

    def _extract_error_message(self, method: str, path: str, status_code: int, response_text: str) -> str:
        message = f"{method} {path} failed ({status_code})"

        if response_text.strip():
            try:
                payload = json.loads(response_text)
            except json.JSONDecodeError:
                return response_text

            if isinstance(payload, dict) and isinstance(payload.get("error"), str):
                return payload["error"]

        return message

    def _run_async(self, coroutine: Coroutine[Any, Any, T]) -> T:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coroutine)

        result: dict[str, T] = {}
        error: dict[str, BaseException] = {}

        def runner() -> None:
            try:
                result["value"] = asyncio.run(coroutine)
            except BaseException as exc:  # noqa: BLE001
                error["error"] = exc

        thread = threading.Thread(target=runner, name="goose-sdk-async-runner", daemon=True)
        thread.start()
        thread.join()

        if "error" in error:
            raise error["error"]

        return result["value"]

    def _namespace_for_flagset(self, flagset: str) -> Optional[str]:
        return self.flagset_namespaces.get(flagset)

    def _normalize_flagset_namespaces(self, mapping: Optional[Mapping[str, str]]) -> dict[str, str]:
        normalized: dict[str, str] = {}

        if mapping:
            for raw_flagset, raw_namespace in mapping.items():
                flagset = str(raw_flagset).strip()
                namespace = self._normalize_namespace(raw_namespace)
                if not flagset or namespace is None:
                    continue
                if flagset not in self.flagsets:
                    raise ValueError(f"flagset_namespaces includes unknown flagset '{flagset}'")
                normalized[flagset] = namespace

        if self.namespace_name is not None:
            for flagset in self.flagsets:
                normalized.setdefault(flagset, self.namespace_name)

        return normalized

    def _resolve_thumbprint(self, explicit: Optional[str]) -> str:
        normalized = str(explicit).strip() if explicit is not None else ""
        if normalized:
            return normalized

        hostname = ""
        try:
            hostname = socket.gethostname()
        except OSError:
            hostname = ""

        executable = os.path.basename(sys.argv[0]) if sys.argv else ""
        seed = f"python|{self.sdk_client_id}|{hostname}|{executable}"
        if seed.strip("|") == "":
            seed = f"python|{self.sdk_client_id}|{time.time_ns()}"
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        return f"gth_{digest}"

    @staticmethod
    def _normalize_namespace(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @staticmethod
    def _normalize_flagsets(flagsets: str | Sequence[str]) -> list[str]:
        if isinstance(flagsets, str):
            candidate = [flagsets]
        else:
            candidate = list(flagsets)

        normalized: list[str] = []
        for item in candidate:
            value = str(item).strip()
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _normalize_config_names(configs: Optional[Sequence[str]]) -> list[str]:
        if not configs:
            return []

        normalized: list[str] = []
        for item in configs:
            value = str(item).strip()
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _normalize_connection_types(
        connection_type: str | ConnectionType | Sequence[str | ConnectionType],
    ) -> set[ConnectionType]:
        if isinstance(connection_type, (str, ConnectionType)):
            items = [connection_type]
        else:
            items = list(connection_type)

        resolved: set[ConnectionType] = set()
        for item in items:
            if isinstance(item, ConnectionType):
                resolved.add(item)
                continue

            normalized = str(item).strip().lower()
            if normalized == "on_demand":
                normalized = ConnectionType.POLLING.value

            resolved.add(ConnectionType(normalized))

        return resolved or {ConnectionType.POLLING}

    @staticmethod
    def _coerce_value(value: Any, data_type: str) -> Any:
        normalized_type = data_type.strip().lower()

        if normalized_type == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() == "true"

        if normalized_type in ("number", "int", "integer", "float", "double"):
            return GooseSDK._coerce_number(value)

        return value

    @staticmethod
    def _coerce_number(value: Any) -> Any:
        """Coerce a stored number flag to int when integral, else float.

        bool is excluded (it is an int subclass in Python). Unparseable values
        are returned unchanged so a malformed payload never crashes evaluation.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else value

        text = str(value).strip()
        if not text:
            return value
        try:
            number = float(text)
        except ValueError:
            return value
        return int(number) if number.is_integer() else number

    @staticmethod
    def _to_unix_seconds(raw: str) -> Optional[int]:
        if not raw:
            return None

        value = raw.strip()

        # RFC3339 / ISO-8601
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            pass

        # Go's time.String() style: "2006-01-02 15:04:05.999999 +0000 UTC"
        for layout in ("%Y-%m-%d %H:%M:%S.%f %z %Z", "%Y-%m-%d %H:%M:%S %z %Z"):
            try:
                return int(datetime.strptime(value, layout).timestamp())
            except ValueError:
                continue

        return None

    def _max_updated_timestamp(self, flags: Sequence[dict[str, Any]]) -> Optional[int]:
        max_timestamp: Optional[int] = None
        for raw_flag in flags:
            updated_at = str(raw_flag.get("updated_at", "")).strip()
            parsed = self._to_unix_seconds(updated_at)
            if parsed is None:
                continue
            if max_timestamp is None or parsed > max_timestamp:
                max_timestamp = parsed
        return max_timestamp


def create_client(*args: Any, **kwargs: Any) -> GooseSDK:
    """Factory helper for GooseSDK."""

    return GooseSDK(*args, **kwargs)
