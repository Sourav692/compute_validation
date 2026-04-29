from __future__ import annotations

import re
from typing import Any

_MISSING = object()


def get_path(obj: Any, dotted: str) -> Any:
    """Resolve a dotted path against nested dicts/lists. Returns _MISSING if absent."""
    if dotted is None:
        return obj
    cur: Any = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return _MISSING
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return _MISSING
        else:
            return _MISSING
    return cur


def matches_filter(resource: dict[str, Any], filt: dict[str, Any]) -> bool:
    for path, expected in filt.items():
        actual = get_path(resource, path)
        if actual is _MISSING or actual != expected:
            return False
    return True


def evaluate(op: str, actual: Any, expected: Any) -> bool:
    """Return True if the resource PASSES the check."""
    # `exists`/`not_exists` treat both _MISSING (field absent) and None
    # (field present but null) as "not set". System tables surface unset
    # config as NULL, which becomes None in our dicts; without this rule a
    # NULL `policy_id` would falsely pass `policy_attached: exists`.
    if op == "exists":
        return actual is not _MISSING and actual is not None
    if op == "not_exists":
        return actual is _MISSING or actual is None
    if actual is _MISSING:
        # field absent fails comparison ops (treated as violation, not skip)
        return False
    # None on comparison ops falls through to the comparison; the engine
    # catches the resulting TypeError and marks the row as skipped, which
    # matches the intent that "no value to compare" is not a violation
    # (e.g. NULL max_autoscale_workers on a single-node cluster).
    if op == "truthy":
        return bool(actual)
    if op == "falsy":
        return not bool(actual)
    if op == "eq":
        return actual == expected
    if op == "ne":
        return actual != expected
    if op == "gt":
        return actual > expected
    if op == "gte":
        return actual >= expected
    if op == "lt":
        return actual < expected
    if op == "lte":
        return actual <= expected
    if op == "in":
        return actual in (expected or [])
    if op == "not_in":
        return actual not in (expected or [])
    if op == "regex":
        return bool(re.search(str(expected), str(actual)))
    if op == "contains":
        if isinstance(actual, (list, tuple, set, str)):
            return expected in actual
        if isinstance(actual, dict):
            return expected in actual
        return False
    raise ValueError(f"Unsupported op: {op}")


def actual_repr(actual: Any) -> str:
    if actual is _MISSING:
        return "<missing>"
    return repr(actual)
