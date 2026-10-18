"""Segment evaluation — a port of ``utils/segments.go`` on the server.

A segment answers "is this subject in this audience", from attributes the
caller supplies. Evaluated here rather than server-side because only the caller
has the attributes, and a flag read must not cost a round trip.

Checked against ``tests/fixtures/segment_vectors.json``, the same file the
server and the other SDKs read. There is deliberately no regex operator: a
pattern that backtracks badly would be a denial of service, and in three
different regex engines it would not even fail the same way.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

# Condition operators.
OP_EQUALS = "equals"
OP_NOT_EQUALS = "not_equals"
OP_CONTAINS = "contains"
OP_STARTS_WITH = "starts_with"
OP_ENDS_WITH = "ends_with"
OP_IN = "in"
OP_NOT_IN = "not_in"

OP_GT = "gt"
OP_GTE = "gte"
OP_LT = "lt"
OP_LTE = "lte"

OP_VERSION_GT = "version_gt"
OP_VERSION_GTE = "version_gte"
OP_VERSION_LT = "version_lt"
OP_VERSION_LTE = "version_lte"

OP_EXISTS = "exists"
OP_NOT_EXISTS = "not_exists"


def attribute_string(value: Any) -> str:
    """Canonical string form of an attribute value.

    Pinned explicitly because default number formatting differs between Go,
    Python and JavaScript, and a segment that matches in one SDK but not
    another is exactly the failure the shared vectors exist to prevent. An
    integral float renders ``"42"``, never ``"42.0"``.
    """
    if value is None:
        return ""
    # bool before int: in Python bool is a subclass of int, and True would
    # otherwise render as "1".
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        return repr(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, int):
        return str(value)
    return json.dumps(value, separators=(",", ":"))


def _attribute_float(value: Any) -> Optional[float]:
    """Parse an attribute as a number, or ``None`` when it is not numeric."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def parse_version(raw: str) -> Optional[list[int]]:
    """Lenient dotted version parse, matching the config app-version gating."""
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


def compare_versions(a: list[int], b: list[int]) -> int:
    """Order two parsed versions, padding the shorter with zeroes."""
    for index in range(max(len(a), len(b))):
        left = a[index] if index < len(a) else 0
        right = b[index] if index < len(b) else 0
        if left != right:
            return -1 if left < right else 1
    return 0


def _any_attribute_string(raw: Any, predicate: Callable[[str], bool]) -> bool:
    """Apply a string predicate; a list matches when any element does."""
    if isinstance(raw, (list, tuple)):
        return any(predicate(attribute_string(item)) for item in raw)
    return predicate(attribute_string(raw))


def condition_matches(condition: dict[str, Any], context: dict[str, Any]) -> bool:
    """Evaluate one condition.

    A missing attribute makes every operator false except ``not_exists``. In
    particular ``not_equals`` is false, not true: the vacuous reading would
    make a negative condition silently match every subject whose context
    omitted the attribute, which during a rollout is usually most of them.
    """
    attribute = condition.get("attribute", "")
    operator = condition.get("operator", "")
    values = condition.get("values") or []

    raw = context.get(attribute)
    present = attribute in context and raw is not None

    if operator == OP_EXISTS:
        return present
    if operator == OP_NOT_EXISTS:
        return not present
    if not present:
        return False

    operand = values[0] if values else ""

    if operator == OP_EQUALS:
        return _any_attribute_string(raw, lambda s: s == operand)
    if operator == OP_NOT_EQUALS:
        return not _any_attribute_string(raw, lambda s: s == operand)
    if operator == OP_CONTAINS:
        return _any_attribute_string(raw, lambda s: operand in s)
    if operator == OP_STARTS_WITH:
        return _any_attribute_string(raw, lambda s: s.startswith(operand))
    if operator == OP_ENDS_WITH:
        return _any_attribute_string(raw, lambda s: s.endswith(operand))
    if operator == OP_IN:
        return _any_attribute_string(raw, lambda s: s in values)
    if operator == OP_NOT_IN:
        return not _any_attribute_string(raw, lambda s: s in values)

    if operator in (OP_GT, OP_GTE, OP_LT, OP_LTE):
        left = _attribute_float(raw)
        if left is None:
            return False
        try:
            right = float(operand.strip())
        except (ValueError, AttributeError):
            return False
        return {
            OP_GT: left > right,
            OP_GTE: left >= right,
            OP_LT: left < right,
            OP_LTE: left <= right,
        }[operator]

    if operator in (OP_VERSION_GT, OP_VERSION_GTE, OP_VERSION_LT, OP_VERSION_LTE):
        left_version = parse_version(attribute_string(raw))
        right_version = parse_version(operand)
        if left_version is None or right_version is None:
            return False
        comparison = compare_versions(left_version, right_version)
        return {
            OP_VERSION_GT: comparison > 0,
            OP_VERSION_GTE: comparison >= 0,
            OP_VERSION_LT: comparison < 0,
            OP_VERSION_LTE: comparison <= 0,
        }[operator]

    # An operator this SDK does not know must never match: a definition from a
    # newer dashboard must not accidentally include everyone.
    return False


def matches_segment(definition: dict[str, Any], context: dict[str, Any]) -> bool:
    """Whether the context satisfies the segment.

    Rules are OR'd and conditions within a rule are AND'd. An empty definition
    matches nobody — an unfinished audience must not become a full rollout.
    """
    rules = definition.get("rules") or []
    if not rules:
        return False
    for rule in rules:
        conditions = rule.get("conditions") or []
        if not conditions:
            continue
        if all(condition_matches(c, context) for c in conditions):
            return True
    return False


def parse_segment_targeting(raw: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Read the segment override off a flag payload.

    Returns ``None`` when the flag is not targeted, or when the rules are
    unparseable — in which case the flag is served untargeted rather than not
    at all, since losing an override is far less harmful than failing the read.
    """
    key = str(raw.get("segment_key") or raw.get("segmentKey") or "").strip()
    if not key:
        return None
    encoded = raw.get("segment_definition") or raw.get("segmentDefinition") or ""
    if not str(encoded).strip():
        return None
    try:
        definition = json.loads(encoded) if isinstance(encoded, str) else encoded
    except (ValueError, TypeError):
        return None
    if not isinstance(definition, dict):
        return None
    return {
        "key": key,
        "value": raw.get("segment_value", raw.get("segmentValue")),
        "definition": definition,
    }
