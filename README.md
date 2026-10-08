# Goose Python SDK

Server-side SDK for Goose config retrieval and change tracking.

## Install (local repo)

From repo root:

```bash
pip install -e ./sdks/python
```

For packaging and release tooling:

```bash
pip install -e ./sdks/python[dev]
```

## Constructor

```python
from goose_sdk import GooseSDK

client = GooseSDK(
    sdk_client_id="gsc_...",
    sdk_client_secret="...",
    server_url="http://localhost:8080",
    flagsets=["default"],
    connection_type="polling",  # polling | sse | webhook
    namespace_name="prod",      # optional
    default_targeting_key=None, # optional; per-user id for canary bucketing
    poll_interval_seconds=3,
)
```

Supported `connection_type` values:
- `polling`
- `sse`
- `webhook`

`on_demand` is accepted as a backward-compatible alias for `polling`.

You can also pass multiple connection types, e.g. `connection_type=["polling", "webhook"]`.

Namespace options:
- `namespace_name="shared-ns"` applies one namespace to all configured flagsets.
- `flagset_namespaces={"flagsA": "ns-a", "flagsB": "ns-b"}` scopes per flagset.

## Indexing behavior

- Single flagset: `client["my_flag"]`
- Multiple flagsets: `client["my_flagset"]["my_flag"]`

## Canary releasing (gradual rollout)

A boolean flag can be rolled out to a percentage of users. The server ships the
rollout config; the SDK buckets each user **locally** so the percentage is
deterministic and sticky per user (a user enabled at 10% stays enabled at 50%).

Pass a stable per-user identifier as `targeting_key`:

```python
if client.get_flag("new_checkout", targeting_key=user.id):
    ...  # this user is in the canary cohort
```

- Flags **without** a rollout ignore `targeting_key` and return their stored value.
- Set `default_targeting_key="..."` on the constructor to avoid passing it on
  every call.
- If a canaried flag is evaluated with no targeting key, it returns `False` and
  logs a warning (the feature is never leaked to unidentified users).
- Bucketing is `sha256(salt:flag_key:targeting_key)[:8] % 100 < percentage`,
  matching the server so any SDK buckets identically.

## Storage adapter hook

Pass any object implementing:

```python
def create_or_update(flagset: str, flag_key: str, value) -> None:
    ...
```

The SDK calls `create_or_update`:
- during initial config load
- whenever deltas are received via polling/SSE/webhook

## Configs

A **config** is a named JSON **document** inside the client-level `namespace_name`,
and a namespace holds many of them. The app watches a set of config **names** (e.g.
`"frontend"`, `"database"`); each watched name has its own document and its own
revision. Configs are delivered on the **polling** loop and surfaced through
per-name change listeners.

Each config document has the shape:

```json
{
  "configs": {
    "<entryKey>": { "value": <any>, "apply_strategy": "immediate" | "requires_restart" }
  }
}
```

The watched **name** is the document name; the inner `configs` keys are entries
within that document.

Construct a client that watches configs (a `namespace_name` is required):

```python
from goose_sdk import GooseSDK, ConfigChangeEvent

client = GooseSDK(
    sdk_client_id="gsc_...",
    sdk_client_secret="...",
    server_url="http://localhost:8080",
    flagsets=["default"],            # configs can be watched alongside flags
    namespace_name="production",     # required when watching configs
    configs=["frontend", "database"],  # config NAMES (documents) to watch
    restart_on_required_change=True, # opt in to SIGINT on requires_restart changes
)

# Per-config listeners fire when that named config's document changes
def on_frontend(event: ConfigChangeEvent) -> None:
    # old_value / new_value are the full documents (or None for a first sighting)
    print(f"{event.name}: {event.old_value} -> {event.new_value} ({event.apply_strategy})")

client.on("frontend", on_frontend)

# Drain callbacks run when a requires_restart change is applied
def drain(event: ConfigChangeEvent) -> None:
    print(f"draining for restart due to {event.name}")

client.on_restart_required(drain)

# Read a watched config's whole document (native JSON, no coercion)
frontend = client.get_config("frontend", default={})

# Or pull a single entry's value straight out of a config document
theme = client.get_config_value("frontend", "theme", default="classic")

all_configs = client.configs_snapshot()  # {name: document}
```

### Entry metadata

Beyond `value` and `apply_strategy`, an entry may carry optional metadata. The
server stores and delivers it untouched — none of it is a server-side gate — and
this SDK acts on it at read time, keyed off the `app_version` keyword:

| Key | Type | What the SDK does with it |
|---|---|---|
| `default` | any | Served instead of `value` when the entry is gated out by app version |
| `min_app_version` | string | Lowest app version the entry applies to (**inclusive**) |
| `max_app_version` | string | Highest app version the entry applies to (**inclusive**) |
| `deprecated` | bool | Warns once, the first time the entry is read |
| `replaced_by` | string | Successor entry named in that deprecation warning |
| `sensitive` | bool | Keeps the value out of the on-disk `PersistentCache` |
| `required` | bool | Reports an error when the entry resolves to nothing |

Versions are lenient dotted strings — `"3"`, `"2.4"`, `"2.4.1"`, `"v2.4.1-rc.1"`
all parse, a leading `v` is dropped, and any pre-release/build suffix is ignored.
They must be **quoted**: unquoted `2.4` is a number, and both the editor and the
server reject it.

Everything fails open. A client built without an app version applies every
entry, an unparseable version or bound disables that gate, and a `default` is
only consulted for an entry that is actually gated out — so existing documents
and existing clients behave exactly as they did before.

```python
client = GooseSDK(
    sdk_client_id="gsc_…",
    sdk_client_secret="…",
    server_url="https://goose.example.com",
    flagsets="checkout",
    namespace_name="production",
    configs=["frontend"],
    app_version="2.1.0",   # optional; omit to apply every entry
)

# For an entry with min_app_version "2.4.0", this 2.1.0 build resolves to the
# entry's "default" rather than its "value".
layout = client.get_config_value("frontend", "dashboard_layout", default="grid")
```

Gating applies to restarts too: a `requires_restart` change to an entry this
build is gated out of will not restart the process. `get_config()` returns the
raw document, so reading `entry["value"]` out of it yourself bypasses all of the
above — `get_config_value()` is where resolution happens.

### Apply strategy

`apply_strategy` is a property of each inner entry, and a config change's overall
strategy is derived from the entries that actually changed:

- `immediate`: when no changed inner entry requires a restart, listeners registered
  via `on(name, ...)` fire and the event's `apply_strategy` is `immediate`.
- `requires_restart`: when **any** changed inner entry in the document uses
  `apply_strategy="requires_restart"`, the event's `apply_strategy` becomes
  `requires_restart`. Per-config listeners still fire; then **all** drain callbacks
  registered via `on_restart_required(...)` run. Drains always run. If the client
  was constructed with `restart_on_required_change=True`, the SDK additionally
  raises `SIGINT` to the current process (via `signal.raise_signal`) once per poll
  batch after the drains, so an orchestrator restarts the process. `SIGINT` is
  opt-in; drains are not.

The initial snapshot is loaded on connect **without** firing listeners.

### Secrets

Config documents can reference **secrets** — named, write-only values managed in
the dashboard's Secrets tab, scoped per namespace and encrypted at rest with a
per-organization RSA keypair. Inside any string value, `${secret_name}` is
replaced with the secret's value **server-side** when configs are delivered to
the SDK:

```json
{
  "configs": {
    "database_url": {
      "value": "postgres://app:${db_password}@db.internal:5432/app",
      "apply_strategy": "requires_restart"
    }
  }
}
```

```python
# The SDK receives the document already resolved — no keys, no extra setup.
url = client.get_config_value("database", "database_url")
# -> "postgres://app:hunter2@db.internal:5432/app"
```

Notes:

- The SDK needs **no configuration** for secrets; resolution happens before
  delivery. Dashboard and management-API reads always show the raw
  `${secret_name}` placeholder, never the value.
- **Changing a secret behaves like a config change**: the resolved document the
  SDK receives on its next poll differs, so `on(name, ...)` listeners fire with
  the new value substituted — including drain callbacks / opt-in `SIGINT` when
  the affected entry uses `apply_strategy="requires_restart"`.
- A `${name}` with no matching secret in the namespace is left as-is, so missing
  references are visible in the delivered value.

## Webhook mode

When using `connection_type="webhook"`, pass:
- `webhook_target_url`
- optional `webhook_secret` (auto-generated if omitted)
- optional `webhook_listener_host` (default: `0.0.0.0`)
- optional `webhook_listener_port` (default: `8091`)
- optional `webhook_listener_path` (default: `/webhook`)

The SDK starts an embedded webhook HTTP listener and applies deltas internally.
No custom HTTP handler wiring is required in user code.

You can inspect listener metadata via:
- `client.webhook_listener_url`
- `client.webhook_events_received` (counts every webhook POST received, including duplicates)

### At-least-once delivery / deduplication

The server delivers webhooks **at-least-once**: after a config-service replica
crashes mid-dispatch, another replica reclaims and resends the same delta. Each
delta carries a unique `eventId`, and the SDK applies each `eventId` at most once.
This makes delta handling idempotent and prevents a late redelivery from
overwriting a newer value, so user code does not need to dedupe. If you process
webhook payloads yourself (instead of using the embedded listener), key your own
idempotency on the `eventId` field.

## Resilience & self-healing

The SDK keeps serving flags through config-service outages, network blips, and
process restarts — and recovers on its own.

**Reads never raise.** `get_flag` always returns a value from a fallback ladder —
the current value, else the last-known-good value, else your `default` — and
never raises. Malformed values pushed from the server (e.g. a non-numeric value
for a number flag) are rejected and logged rather than overwriting a good value.

**Warm start.** Pass a `cache` to persist last-known-good state (flags, rollouts,
poll cursors, watched configs). It is read back on `connect()` **before** any
network call, so a process that restarts during an outage boots with real values
instead of an empty cache. `FileCache` is a built-in atomic file-backed store:

```python
from goose_sdk import GooseSDK, FileCache

client = GooseSDK(..., cache=FileCache("/var/lib/myapp/goose-cache.json"))
```

**Graceful degraded start.** By default a failed initial fetch starts the client
in `ConnectionState.DEGRADED` and heals in the background instead of raising from
the constructor. Pass `require_initial_connect=True` for fail-fast startup.

**Disciplined reconnects.** SSE reconnects and post-error retries use exponential
backoff with jitter (capped), and honor a `429`'s `Retry-After`. The SSE idle read
deadline (`sse_read_timeout_seconds`) tears down a silently half-open stream so it
reconnects; in SSE-only mode a sustained stream outage falls back to polling.

**Observability.** Inspect and react to health:

```python
client.state()      # ConnectionState.CONNECTING / LIVE / DEGRADED / CLOSED
client.last_sync()  # epoch seconds of the last successful sync, or None
client.is_stale()   # True if no successful sync within the staleness window

client.on_state_change(lambda s: metrics.gauge("goose.state", s.value))
client.on_error(lambda err: log.warning("goose background error: %s", err))
```

## Notes

- The SDK resolves organization automatically using SDK credentials via `/api/v1/sdk/resolve`.
- Snapshot loading uses `GET /api/v1/config` query params.
- Delta polling uses `POST /api/v1/poll`. Server responses may be served from a short-lived cache, so a brand-new change can take a few seconds to appear on the poll path.
- Config snapshots use `GET /api/v1/configs` and config polling uses `POST /api/v1/configs/poll`; both return the current document and `revision_number` for each watched name, and the SDK fires per-name listeners when a document changes.
- SSE streams emit periodic keepalive comments; the SDK ignores them and uses them to keep the long-lived connection alive through proxies/load balancers.
- Initial snapshot is always loaded into memory on connect.
- Use `client.snapshot()` to retrieve a full copy of in-memory values.
- Call `client.close()` on shutdown for polling/SSE modes.

## Test

Install the dev extras (which include `pytest`) and run the suite. It exercises
the pure logic (coercion, canary bucketing, config diffing, validation) plus an
in-process stub of the config service for flag polling, configs, and the embedded
webhook listener — no live services required:

```bash
pip install -e ./sdks/python[dev]
pytest sdks/python
```

## Build distribution artifacts

From repo root:

```bash
python -m build ./sdks/python
```

Or from `sdks/python`:

```bash
python -m build
```

This generates:
- `sdks/python/dist/*.whl` (wheel)
- `sdks/python/dist/*.tar.gz` (sdist)
