# Goose Python SDK Examples

## Setup

From repo root:

```bash
pip install -e ./sdks/python
```

The consolidated example also supports a local import fallback from `../src` if you skip install.

Set environment variables:

```bash
export GOOSE_SERVER_URL=http://localhost:8080
export GOOSE_POLL_INTERVAL_SECONDS=3
export GOOSE_SDK_CLIENT_ID=gsc_xxx
export GOOSE_SDK_CLIENT_SECRET=xxx
export GOOSE_FLAGSET_POLLTEST=PollTest
export GOOSE_FLAGSET_HOOKTEST=HookTest
export GOOSE_FLAGSET_SSETEST=SSETest
export GOOSE_NAMESPACE_POLLTEST=
export GOOSE_NAMESPACE_HOOKTEST=
export GOOSE_NAMESPACE_SSETEST=
```

Or copy the local template and edit it:

```bash
cp sdks/python/example/.env.example sdks/python/example/.env
```

`example.py` auto-loads `sdks/python/example/.env` if present.

Set `GOOSE_APP_VERSION` (e.g. `2.4.0`) to switch on config **app-version
gating**: an entry whose `min_app_version`/`max_app_version` range excludes
that version resolves to its `default` instead of its `value`, and a change
to it never triggers a restart. Leave it unset and every entry applies.

For webhook mode only:

```bash
export GOOSE_WEBHOOK_TARGET_URL=https://<public-endpoint>/webhook
export GOOSE_WEBHOOK_SECRET=<stable-shared-secret>
export GOOSE_WEBHOOK_LISTENER_HOST=0.0.0.0
export GOOSE_WEBHOOK_LISTENER_PORT=8091
export GOOSE_WEBHOOK_LISTENER_PATH=/webhook
```

## Run consolidated example

```bash
python sdks/python/example/example.py
```

Behavior:
- Creates 3 SDK clients:
  - polling client for `GOOSE_FLAGSET_POLLTEST`
  - webhook client for `GOOSE_FLAGSET_HOOKTEST`
  - SSE client for `GOOSE_FLAGSET_SSETEST`
- Polling cadence is controlled by `GOOSE_POLL_INTERVAL_SECONDS`
- Logs initial snapshots for all 3 clients
- Enters an infinite loop:
  - every 10s logs the polling client snapshot
  - SSE and webhook deltas are logged immediately when updates arrive
- Stop with `Ctrl+C`
