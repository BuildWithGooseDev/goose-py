import os
from pathlib import Path
import sys
import threading
import time
from typing import Any

try:
    from goose_sdk import ConfigChangeEvent, ConnectionType, GooseSDK, GooseSDKHTTPError
except ModuleNotFoundError:
    local_src = Path(__file__).resolve().parents[1] / "src"
    sys.path.insert(0, str(local_src))
    from goose_sdk import ConfigChangeEvent, ConnectionType, GooseSDK, GooseSDKHTTPError


def load_local_env_file() -> None:
    env_path = Path(__file__).resolve().with_name(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def require_sdk_credentials() -> tuple[str, str, str]:
    server_url = os.getenv("GOOSE_SERVER_URL", "http://localhost:8080")
    sdk_client_id = os.getenv("GOOSE_SDK_CLIENT_ID", "")
    # The client secret is only needed for app configs and webhook registration.
    # Flag reads (polling / SSE) authenticate with the client_id alone, so a
    # flag-only client can omit it entirely. This example exercises webhooks and
    # configs, so it still requires the secret.
    sdk_client_secret = os.getenv("GOOSE_SDK_CLIENT_SECRET", "")

    if not sdk_client_id:
        raise SystemExit("Set GOOSE_SDK_CLIENT_ID before running the example.")
    if not sdk_client_secret:
        raise SystemExit(
            "Set GOOSE_SDK_CLIENT_SECRET before running the example: the webhook and "
            "config clients need it (flag polling / SSE do not)."
        )

    return server_url, sdk_client_id, sdk_client_secret


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Set {name} in sdks/python/example/.env")
    return value


class LiveDeltaLogger:
    def __init__(self, label: str) -> None:
        self.label = label
        self.enabled = False
        self._lock = threading.Lock()

    def enable(self) -> None:
        self.enabled = True

    def create_or_update(self, flagset: str, flag_key: str, value: Any) -> None:
        if not self.enabled:
            return
        with self._lock:
            print(f"[{self.label}] delta {flagset}.{flag_key}={value}")


def main() -> None:
    load_local_env_file()
    server_url, sdk_client_id, sdk_client_secret = require_sdk_credentials()

    poll_flagset = os.getenv("GOOSE_FLAGSET_POLLTEST", "PollTest").strip() or "PollTest"
    hook_flagset = os.getenv("GOOSE_FLAGSET_HOOKTEST", "HookTest").strip() or "HookTest"
    sse_flagset = os.getenv("GOOSE_FLAGSET_SSETEST", "SSETest").strip() or "SSETest"
    poll_namespace = os.getenv("GOOSE_NAMESPACE_POLLTEST", "").strip() or None
    hook_namespace = os.getenv("GOOSE_NAMESPACE_HOOKTEST", "").strip() or None
    sse_namespace = os.getenv("GOOSE_NAMESPACE_SSETEST", "").strip() or None

    # Config NAMES are document names within a single namespace, e.g. "frontend"
    # and "database". Each watched name resolves to its own JSON document.
    config_namespace = os.getenv("GOOSE_CONFIG_NAMESPACE", "").strip() or None
    config_names = [
        name.strip()
        for name in os.getenv("GOOSE_CONFIG_NAMES", "frontend,database").split(",")
        if name.strip()
    ]

    # Optional: this app's own version. When set, config entries carrying
    # min_app_version/max_app_version are gated against it — an entry out of
    # range resolves to its "default" instead of its "value", and a change to it
    # never triggers a restart. Leave it unset and every entry applies.
    app_version = os.getenv("GOOSE_APP_VERSION", "").strip() or None

    webhook_target_url = required_env("GOOSE_WEBHOOK_TARGET_URL")
    webhook_secret = os.getenv("GOOSE_WEBHOOK_SECRET", "goose-local-webhook-secret")
    webhook_listener_host = os.getenv("GOOSE_WEBHOOK_LISTENER_HOST", "0.0.0.0")
    webhook_listener_port = int(os.getenv("GOOSE_WEBHOOK_LISTENER_PORT", "8091"))
    webhook_listener_path = os.getenv("GOOSE_WEBHOOK_LISTENER_PATH", "/webhook")
    polling_interval_seconds = float(os.getenv("GOOSE_POLL_INTERVAL_SECONDS", "3"))

    sse_logger = LiveDeltaLogger("SSE")
    webhook_logger = LiveDeltaLogger("WEBHOOK")

    # Flag-only clients: polling and SSE authenticate with the client_id alone —
    # no secret is sent or required.
    polling_client = GooseSDK(
        sdk_client_id=sdk_client_id,
        server_url=server_url,
        flagsets=[poll_flagset],
        connection_type=ConnectionType.POLLING,
        namespace_name=poll_namespace,
        poll_interval_seconds=polling_interval_seconds,
        auto_connect=False,
    )
    sse_client = GooseSDK(
        sdk_client_id=sdk_client_id,
        server_url=server_url,
        flagsets=[sse_flagset],
        connection_type=ConnectionType.SSE,
        namespace_name=sse_namespace,
        storage_adapter=sse_logger,
        auto_connect=False,
    )
    # Webhook registration is secret-bearing, so this client passes the secret.
    webhook_client = GooseSDK(
        sdk_client_id=sdk_client_id,
        sdk_client_secret=sdk_client_secret,
        server_url=server_url,
        flagsets=[hook_flagset],
        connection_type=ConnectionType.WEBHOOK,
        namespace_name=hook_namespace,
        storage_adapter=webhook_logger,
        webhook_target_url=webhook_target_url,
        webhook_secret=webhook_secret,
        webhook_listener_host=webhook_listener_host,
        webhook_listener_port=webhook_listener_port,
        webhook_listener_path=webhook_listener_path,
        auto_connect=False,
    )

    named_clients = [
        ("polling", poll_flagset, polling_client),
        ("sse", sse_flagset, sse_client),
        ("webhook", hook_flagset, webhook_client),
    ]

    # Each config is a named JSON document within a single namespace, watched by
    # name and delivered on the polling loop. get_config(name) returns that whole
    # document. This client is optional: set GOOSE_CONFIG_NAMESPACE (and optionally
    # override GOOSE_CONFIG_NAMES, comma-separated) to enable it.
    config_client = None
    if config_names and config_namespace:
        # App config documents can carry server-resolved ${secret} values, so the
        # config endpoints are secret-bearing: this client passes the secret.
        config_client = GooseSDK(
            sdk_client_id=sdk_client_id,
            sdk_client_secret=sdk_client_secret,
            server_url=server_url,
            flagsets=[poll_flagset],
            connection_type=ConnectionType.POLLING,
            namespace_name=config_namespace,
            configs=config_names,
            app_version=app_version,
            # Opt in to raising SIGINT when a requires_restart config changes so an
            # orchestrator restarts the process. Drains run regardless.
            restart_on_required_change=False,
            poll_interval_seconds=polling_interval_seconds,
            auto_connect=False,
        )

        def on_config_change(event: ConfigChangeEvent) -> None:
            # old_value / new_value are the full config documents (or None for a
            # config seen for the first time).
            print(
                f"[CONFIG] '{event.name}' document changed "
                f"({event.apply_strategy}): {event.old_value!r} -> {event.new_value!r}"
            )

        def on_restart_required(event: ConfigChangeEvent) -> None:
            print(
                f"[CONFIG] draining for restart; '{event.name}' has an entry "
                "with apply_strategy=requires_restart"
            )

        for name in config_names:
            config_client.on(name, on_config_change)
        config_client.on_restart_required(on_restart_required)

    try:
        for name, flagset, client in named_clients:
            try:
                client.connect()
            except GooseSDKHTTPError as error:
                raise SystemExit(
                    f"Failed connecting {name} client for flagset '{flagset}' "
                    f"(HTTP {error.status_code}). Ensure this flagset exists in the "
                    "SDK client's organization and that required endpoints are enabled."
                ) from error

        if config_client is not None:
            config_client.connect()

        print("POLLING initial:", polling_client.snapshot())
        print("SSE initial:", sse_client.snapshot())
        print("WEBHOOK initial:", webhook_client.snapshot())
        if config_client is not None:
            print("CONFIGS initial:", config_client.configs_snapshot())

        # Canary releasing: for a flag with a rollout percentage configured, pass a
        # stable per-user identifier so each user is deterministically bucketed.
        # Flags without a rollout ignore targeting_key and return their value.
        print(
            "canary_demo for user-123:",
            polling_client.get_flag("canary_demo", targeting_key="user-123"),
        )

        sse_logger.enable()
        webhook_logger.enable()

        print("Live watch started. Polling snapshot logs every 10s. Press Ctrl+C to stop.")
        while True:
            print("POLLING check:", polling_client.snapshot())
            if config_client is not None:
                print("CONFIGS check:", config_client.configs_snapshot())
            time.sleep(10)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        for _, _, client in named_clients:
            client.close()
        if config_client is not None:
            config_client.close()


if __name__ == "__main__":
    main()
