from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from .config import Check, ValidationConfig
from .rules import actual_repr, evaluate, get_path, matches_filter


log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    run_id: str
    run_ts: str
    workspace_host: str
    resource_type: str
    resource_id: str
    resource_name: str
    check_name: str
    description: str
    severity: str
    field: str
    op: str
    expected: str
    actual: str
    passed: bool
    skipped: bool
    skip_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_checks(
    config: ValidationConfig,
    resources_by_type: dict[str, list[dict[str, Any]]],
    workspace_host: str,
) -> list[CheckResult]:
    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc).isoformat()
    log.info("Starting check evaluation (run_id=%s)", run_id)
    results: list[CheckResult] = []

    for check in config.checks:
        resources = resources_by_type.get(check.resource_type, [])
        log.info(
            "  evaluating %s.%s [%s] across %d resources",
            check.resource_type, check.name, check.severity, len(resources),
        )
        check_passed = check_failed = check_skipped = 0
        for resource in resources:
            result = _evaluate_one(check, resource, run_id, run_ts, workspace_host)
            results.append(result)
            if result.skipped:
                check_skipped += 1
            elif result.passed:
                check_passed += 1
            else:
                check_failed += 1
                log.warning(
                    "    FAIL: %s/%s — %s %s %s (actual=%s)",
                    check.resource_type,
                    result.resource_name or result.resource_id,
                    result.field, result.op, result.expected, result.actual,
                )
        log.info(
            "    result: %d passed, %d failed, %d skipped",
            check_passed, check_failed, check_skipped,
        )

    return results


def _evaluate_one(
    check: Check,
    resource: dict[str, Any],
    run_id: str,
    run_ts: str,
    workspace_host: str,
) -> CheckResult:
    rid = str(resource.get("_id") or "")
    rname = str(resource.get("_name") or "")

    base = dict(
        run_id=run_id,
        run_ts=run_ts,
        workspace_host=workspace_host,
        resource_type=check.resource_type,
        resource_id=rid,
        resource_name=rname,
        check_name=check.name,
        description=check.description,
        severity=check.severity,
        field=check.field or "",
        op=check.op,
        expected=repr(check.value) if check.value is not None else "",
    )

    if check.filter and not matches_filter(resource, check.filter):
        return CheckResult(
            **base,
            actual="",
            passed=True,
            skipped=True,
            skip_reason="filter not matched",
        )

    actual = get_path(resource, check.field) if check.field else resource
    try:
        passed = evaluate(check.op, actual, check.value)
    except Exception as exc:
        return CheckResult(
            **base,
            actual=actual_repr(actual),
            passed=False,
            skipped=True,
            skip_reason=f"evaluation error: {exc}",
        )

    return CheckResult(
        **base,
        actual=actual_repr(actual),
        passed=passed,
        skipped=False,
        skip_reason="",
    )
