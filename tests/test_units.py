"""Unit tests for the pure logic of the Goose Python SDK (no network)."""

from __future__ import annotations

import pytest

from goose_sdk.client import (
    ConnectionType,
    GooseSDK,
    _bucket_user,
    _extract_rollout,
    compare_app_versions,
    config_entry_applies,
    parse_app_version,
    redact_sensitive_config_entries,
    resolve_config_entry,
    unsatisfied_required_entries,
)


# ---------------------------------------------------------------------------
# Canary bucketing — must match the Go/JS SDKs exactly so a user buckets
# identically across languages. Values come from the shared formula
# sha256(salt:flag_key:targeting_key)[:8] big-endian mod 100.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "salt,flag_key,key,expected",
    [
        ("s1", "new_checkout", "user-123", 73),
        ("s1", "new_checkout", "user-456", 68),
        ("", "edge_ui", "user-123", 39),
    ],
)
def test_bucket_user_cross_language(salt, flag_key, key, expected):
    assert _bucket_user(salt, flag_key, key) == expected


def test_bucket_user_is_deterministic_and_in_range():
    for i in range(200):
        bucket = _bucket_user("salt", "flag", f"user-{i}")
        assert 0 <= bucket < 100
        assert bucket == _bucket_user("salt", "flag", f"user-{i}")


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------
def test_coerce_bool():
    assert GooseSDK._coerce_value("true", "bool") is True
    assert GooseSDK._coerce_value("TRUE", "bool") is True
    assert GooseSDK._coerce_value("false", "bool") is False
    assert GooseSDK._coerce_value(True, "bool") is True


def test_coerce_number_int_and_float():
    assert GooseSDK._coerce_value("5", "number") == 5
    assert isinstance(GooseSDK._coerce_value("5", "number"), int)
    assert GooseSDK._coerce_value("5.5", "number") == 5.5
    assert GooseSDK._coerce_value(3.0, "int") == 3
    # bool is an int subclass but must not be coerced to a number.
    assert GooseSDK._coerce_value(True, "number") is True


def test_coerce_string_passthrough():
    assert GooseSDK._coerce_value("grid", "list_of_values") == "grid"
    assert GooseSDK._coerce_value("https://x", "string") == "https://x"


def test_coerce_unparseable_number_returns_value():
    assert GooseSDK._coerce_value("not-a-number", "number") == "not-a-number"


# ---------------------------------------------------------------------------
# Rollout extraction (snake_case and camelCase)
# ---------------------------------------------------------------------------
def test_extract_rollout_snake_case():
    rollout = _extract_rollout(
        {"rollout_percentage": 25, "rollout_salt": "abc", "rollout_value": "on"}
    )
    assert rollout == {"percentage": 25, "salt": "abc", "value": "on"}


def test_extract_rollout_camel_case():
    rollout = _extract_rollout({"rolloutPercentage": "10", "rolloutSalt": "xyz"})
    assert rollout["percentage"] == 10
    assert rollout["salt"] == "xyz"


def test_extract_rollout_missing_is_none():
    assert _extract_rollout({"flag_key": "x"})["percentage"] is None


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------
def test_to_unix_seconds_iso8601():
    assert GooseSDK._to_unix_seconds("2026-06-14T00:00:00+00:00") == 1781395200
    assert GooseSDK._to_unix_seconds("2026-06-14T00:00:00Z") == 1781395200


def test_to_unix_seconds_go_string_form():
    assert (
        GooseSDK._to_unix_seconds("2026-06-14 00:00:00 +0000 UTC") == 1781395200
    )


def test_to_unix_seconds_unparseable():
    assert GooseSDK._to_unix_seconds("") is None
    assert GooseSDK._to_unix_seconds("nonsense") is None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def test_normalize_flagsets_dedupes_and_trims():
    assert GooseSDK._normalize_flagsets(["a", " a ", "b", ""]) == ["a", "b"]
    assert GooseSDK._normalize_flagsets("solo") == ["solo"]


def test_normalize_connection_types_aliases_on_demand():
    assert GooseSDK._normalize_connection_types("on_demand") == {ConnectionType.POLLING}
    assert GooseSDK._normalize_connection_types(["polling", "sse"]) == {
        ConnectionType.POLLING,
        ConnectionType.SSE,
    }


def test_normalize_connection_types_rejects_unknown():
    with pytest.raises(ValueError):
        GooseSDK._normalize_connection_types("carrier-pigeon")


# ---------------------------------------------------------------------------
# Config diffing
# ---------------------------------------------------------------------------
def test_changed_config_entries_detects_value_change():
    old = {"configs": {"a": {"value": 1, "apply_strategy": "immediate"}}}
    new = {"configs": {"a": {"value": 2, "apply_strategy": "immediate"}}}
    assert GooseSDK._changed_config_entries(old, new) == [("a", "immediate")]


def test_changed_config_entries_detects_strategy_change():
    old = {"configs": {"a": {"value": 1, "apply_strategy": "immediate"}}}
    new = {"configs": {"a": {"value": 1, "apply_strategy": "requires_restart"}}}
    assert GooseSDK._changed_config_entries(old, new) == [("a", "requires_restart")]


def test_changed_config_entries_none_old_treats_all_changed():
    new = {"configs": {"a": {"value": 1}, "b": {"value": 2}}}
    changed = dict(GooseSDK._changed_config_entries(None, new))
    assert set(changed) == {"a", "b"}


def test_changed_config_entries_no_change():
    doc = {"configs": {"a": {"value": 1, "apply_strategy": "immediate"}}}
    assert GooseSDK._changed_config_entries(doc, doc) == []


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------
def _opts(**overrides):
    base = dict(
        sdk_client_id="gsc_x",
        server_url="http://localhost:8080",
        flagsets=["default"],
        auto_connect=False,
    )
    base.update(overrides)
    return base


def test_requires_client_id_and_url():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(sdk_client_id=" "))
    with pytest.raises(ValueError):
        GooseSDK(**_opts(server_url=" "))


def test_requires_a_flagset():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(flagsets=[]))


def test_configs_require_namespace():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(sdk_client_secret="sek", configs=["frontend"]))


def test_configs_require_secret():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(namespace_name="prod", configs=["frontend"]))


def test_webhook_requires_target_url():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(sdk_client_secret="sek", connection_type="webhook"))


def test_webhook_requires_absolute_url():
    with pytest.raises(ValueError):
        GooseSDK(
            **_opts(
                sdk_client_secret="sek",
                connection_type="webhook",
                webhook_target_url="/relative/webhook",
            )
        )


def test_webhook_requires_secret():
    with pytest.raises(ValueError):
        GooseSDK(
            **_opts(
                connection_type="webhook",
                webhook_target_url="http://host:8091/webhook",
            )
        )


def test_flagset_namespaces_rejects_unknown_flagset():
    with pytest.raises(ValueError):
        GooseSDK(**_opts(flagset_namespaces={"other": "ns"}))


def test_flag_only_client_constructs_without_secret():
    client = GooseSDK(**_opts())
    assert client.flagsets == ["default"]
    assert client.sdk_client_secret == ""


# ---------------------------------------------------------------------------
# Config entry metadata — app-version gating, defaults, deprecation, sensitive
# values. parse_app_version must agree with the Go/JS SDKs and the two Go
# services so the same metadata gates identically everywhere.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2.4.1", [2, 4, 1]),
        ("v2.4.1", [2, 4, 1]),
        ("2.4", [2, 4]),
        ("3", [3]),
        ("2.4.1-rc.1", [2, 4, 1]),
        ("2.4.1+build.7", [2, 4, 1]),
        ("1.2.3.4", [1, 2, 3, 4]),
        ("", None),
        ("latest", None),
        ("2.x", None),
        ("-1.0", None),
    ],
)
def test_parse_app_version_cross_language(raw, expected):
    assert parse_app_version(raw) == expected


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ([2, 4], [2, 4, 0], 0),
        ([2, 4, 1], [2, 4], 1),
        ([2, 3, 9], [2, 4], -1),
        ([10], [9, 9, 9], 1),
    ],
)
def test_compare_app_versions_pads_shorter(a, b, expected):
    assert compare_app_versions(a, b) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.3.9", False),
        ("2.4.0", True),  # lower bound is inclusive
        ("2.7.1", True),
        ("3.0.0", True),  # upper bound is inclusive
        ("3.0.1", False),
    ],
)
def test_config_entry_applies_bounds_are_inclusive(version, expected):
    entry = {"min_app_version": "2.4.0", "max_app_version": "3.0.0"}
    assert config_entry_applies(entry, parse_app_version(version)) is expected


def test_config_entry_applies_fails_open():
    """Bad or missing metadata must degrade to the ungated behaviour."""
    v240 = parse_app_version("2.4.0")
    assert config_entry_applies({"min_app_version": "9.0.0"}, None) is True
    assert config_entry_applies({}, v240) is True
    assert config_entry_applies({"min_app_version": "not-a-version"}, v240) is True
    assert config_entry_applies({"min_app_version": None}, v240) is True


def test_resolve_config_entry_falls_back_to_default():
    entry = {"value": "grid-v2", "default": "grid", "min_app_version": "2.4.0"}
    assert resolve_config_entry(entry, parse_app_version("2.6.0")) == (True, "grid-v2")
    assert resolve_config_entry(entry, parse_app_version("2.1.0")) == (True, "grid")
    # Gated out with no default: the caller's own fallback wins.
    bare = {"value": "x", "max_app_version": "1.0.0"}
    assert resolve_config_entry(bare, parse_app_version("2.1.0")) == (False, None)


def test_changed_config_entries_skips_gated_out_entries():
    """A requires_restart change this build never reads must not restart it."""
    old_doc = {
        "configs": {
            "future": {"value": "a", "apply_strategy": "requires_restart", "min_app_version": "9.0.0"},
            "here": {"value": "a", "apply_strategy": "immediate"},
        }
    }
    new_doc = {
        "configs": {
            "future": {"value": "b", "apply_strategy": "requires_restart", "min_app_version": "9.0.0"},
            "here": {"value": "b", "apply_strategy": "immediate"},
        }
    }
    gated = GooseSDK._changed_config_entries(old_doc, new_doc, parse_app_version("2.4.0"))
    assert gated == [("here", "immediate")]
    # Without an app version the pre-gating behaviour is preserved.
    assert len(GooseSDK._changed_config_entries(old_doc, new_doc, None)) == 2


def test_unsatisfied_required_entries():
    doc = {
        "configs": {
            "ok": {"value": "x", "required": True},
            "gated_default": {"value": "x", "default": "y", "required": True, "min_app_version": "9.0.0"},
            "gated_bare": {"value": "x", "required": True, "min_app_version": "9.0.0"},
            "explicit_null": {"value": None, "required": True},
            "not_required": {"min_app_version": "9.0.0"},
        }
    }
    assert unsatisfied_required_entries(doc, parse_app_version("2.4.0")) == [
        "explicit_null",
        "gated_bare",
    ]


def test_redact_sensitive_config_entries():
    """Values arrive with ${secret} already resolved, so they must not be cached."""
    doc = {
        "configs": {
            "db_url": {
                "value": "postgres://app:hunter2@db",
                "default": "postgres://localhost",
                "sensitive": True,
                "apply_strategy": "requires_restart",
            },
            "layout": {"value": "grid", "apply_strategy": "immediate"},
        }
    }
    redacted = redact_sensitive_config_entries(doc)["configs"]
    assert "value" not in redacted["db_url"]
    assert "default" not in redacted["db_url"]
    assert redacted["db_url"]["apply_strategy"] == "requires_restart"
    assert redacted["layout"]["value"] == "grid"
    # The live document must not be mutated by building a snapshot of it.
    assert doc["configs"]["db_url"]["value"] == "postgres://app:hunter2@db"
    plain = {"configs": {"a": {"value": 1}}}
    assert redact_sensitive_config_entries(plain) == plain


def _config_client(**overrides):
    client = GooseSDK(
        **_opts(
            sdk_client_secret="sek",
            namespace_name="production",
            configs=["frontend"],
            **overrides,
        )
    )
    client._configs["frontend"] = {
        "document": {
            "configs": {
                "layout": {"value": "grid-v2", "default": "grid", "min_app_version": "2.4.0"},
                "no_default": {"value": "on", "min_app_version": "2.4.0"},
                "ungated": {"value": "always"},
            }
        },
        "revision": 1,
    }
    return client


def test_get_config_value_applies_app_version_gating():
    client = _config_client(app_version="2.1.0")
    assert client.get_config_value("frontend", "layout", "caller-fallback") == "grid"
    assert client.get_config_value("frontend", "no_default", "caller-fallback") == "caller-fallback"
    assert client.get_config_value("frontend", "ungated") == "always"


def test_get_config_value_without_app_version_ignores_bounds():
    """A client with no app_version behaves as it did before gating existed."""
    client = _config_client()
    assert client.get_config_value("frontend", "layout") == "grid-v2"


def test_unparseable_app_version_disables_gating():
    client = _config_client(app_version="nightly")
    assert client._app_version is None
    assert client.get_config_value("frontend", "layout") == "grid-v2"


def test_deprecated_entry_warns_once(caplog):
    client = _config_client()
    client._configs["frontend"]["document"]["configs"]["old_key"] = {
        "value": "x",
        "deprecated": True,
        "replaced_by": "new_key",
    }
    with caplog.at_level("WARNING", logger="goose_sdk"):
        client.get_config_value("frontend", "old_key")
        client.get_config_value("frontend", "old_key")
    warnings = [r for r in caplog.records if "deprecated" in r.getMessage()]
    assert len(warnings) == 1
    assert "new_key" in warnings[0].getMessage()


# ---------------------------------------------------------------------------
# Segment targeting. The vector assertions read the same
# tests/fixtures/segment_vectors.json the server and the other SDKs read, so
# an audience cannot mean one thing here and another in a browser.
# ---------------------------------------------------------------------------
import json
import pathlib

from goose_sdk._segments import (
    attribute_string,
    condition_matches,
    matches_segment,
    parse_segment_targeting,
)

_VECTORS = json.loads(
    (pathlib.Path(__file__).parent.parent.parent.parent
     / "tests" / "fixtures" / "segment_vectors.json").read_text()
)


def test_segment_match_vectors():
    assert _VECTORS["matches"], "no match vectors"
    for case in _VECTORS["matches"]:
        segment = _VECTORS["segments"][case["segment"]]
        context = _VECTORS["contexts"][case["context"]]
        assert matches_segment(segment, context) is case["matches"], (
            f"{case['segment']} against {case['context']}"
        )


def test_attribute_string_vectors():
    for case in _VECTORS["attribute_strings"]:
        assert attribute_string(case["value"]) == case["string"]


def test_empty_segment_matches_nobody():
    """An unfinished audience must not become a full rollout."""
    context = {"plan": "enterprise"}
    assert matches_segment({"rules": []}, context) is False
    assert matches_segment({"rules": [{"conditions": []}]}, context) is False


@pytest.mark.parametrize("operator", _VECTORS["operators"])
def test_missing_attribute_fails_every_operator_except_not_exists(operator):
    """The subtle rule: a negative condition against a missing attribute is
    false, not true. Otherwise `plan != enterprise` quietly includes every
    subject whose context omitted `plan` — during a rollout, most of them."""
    condition = {"attribute": "plan", "operator": operator, "values": ["enterprise"]}
    assert condition_matches(condition, {}) is (operator == "not_exists")


def test_null_attribute_is_treated_as_absent():
    context = {"plan": None}
    assert condition_matches({"attribute": "plan", "operator": "exists", "values": []}, context) is False
    assert condition_matches({"attribute": "plan", "operator": "not_exists", "values": []}, context) is True


def test_numeric_comparison_is_not_lexicographic():
    condition = {"attribute": "seats", "operator": "lt", "values": ["10"]}
    assert condition_matches(condition, {"seats": 9}) is True
    # A non-numeric attribute does not match, rather than comparing as text.
    assert condition_matches(condition, {"seats": "many"}) is False


def test_version_comparison_is_not_lexicographic():
    condition = {"attribute": "v", "operator": "version_gt", "values": ["2.9.0"]}
    assert condition_matches(condition, {"v": "2.10.0"}) is True
    assert condition_matches(condition, {"v": "nonsense"}) is False


def test_list_attribute_matches_any_element():
    context = {"roles": ["billing", "admin"]}
    assert condition_matches(
        {"attribute": "roles", "operator": "in", "values": ["admin", "owner"]}, context) is True
    assert condition_matches(
        {"attribute": "roles", "operator": "not_in", "values": ["admin"]}, context) is False


def test_unknown_operator_never_matches():
    """A definition from a newer dashboard must not include everyone."""
    assert condition_matches(
        {"attribute": "plan", "operator": "regex_match", "values": [".*"]},
        {"plan": "enterprise"}) is False


def test_parse_segment_targeting():
    targeting = parse_segment_targeting({
        "segment_key": "enterprise",
        "segment_value": True,
        "segment_definition": '{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}',
    })
    assert targeting is not None
    assert targeting["key"] == "enterprise"
    assert targeting["value"] is True

    assert parse_segment_targeting({"flag_key": "plain"}) is None
    # Corrupt rules mean the flag is served untargeted, not that the read fails.
    assert parse_segment_targeting({"segment_key": "s", "segment_definition": "{not json"}) is None


def test_get_flag_serves_segment_value_to_matching_context():
    client = GooseSDK(**_opts())
    client._commit_flag(
        "default", "new_checkout", "bool", False, {},
        parse_segment_targeting({
            "segment_key": "enterprise",
            "segment_value": True,
            "segment_definition": '{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}',
        }),
    )

    assert client.get_flag("new_checkout", context={"plan": "enterprise"}) is True
    assert client.get_flag("new_checkout", context={"plan": "free"}) is False
    # A caller that has not adopted targeting keeps working.
    assert client.get_flag("new_checkout") is False


def test_segment_targeting_wins_over_canary():
    """Being in the audience is about who you are; a canary is about what
    fraction of traffic sees something. A segment hit wins outright."""
    client = GooseSDK(**_opts(default_targeting_key="user-123"))
    client._commit_flag(
        "default", "f", "bool", False,
        {"percentage": 0, "salt": "s1", "value": True},
        parse_segment_targeting({
            "segment_key": "s", "segment_value": True,
            "segment_definition": '{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}',
        }),
    )
    assert client.get_flag("f", context={"plan": "enterprise"}) is True


def test_segment_targeting_is_cleared_when_removed():
    client = GooseSDK(**_opts())
    targeting = parse_segment_targeting({
        "segment_key": "s", "segment_value": True,
        "segment_definition": '{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}',
    })
    client._commit_flag("default", "f", "bool", False, {}, targeting)
    assert client.get_flag("f", context={"plan": "enterprise"}) is True

    client._commit_flag("default", "f", "bool", False, {}, None)
    assert client.get_flag("f", context={"plan": "enterprise"}) is False
