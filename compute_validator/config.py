from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


SUPPORTED_RESOURCES = ("clusters", "sql_warehouses", "instance_pools")
VALID_SEVERITIES = ("INFO", "WARN", "CRITICAL")
SEVERITY_RANK = {s: i for i, s in enumerate(VALID_SEVERITIES)}
VALID_OPS = (
    "eq", "ne", "gt", "gte", "lt", "lte",
    "in", "not_in", "regex", "contains",
    "exists", "not_exists", "truthy", "falsy",
)


@dataclass
class Check:
    resource_type: str
    name: str
    field: str | None
    op: str
    value: Any = None
    severity: str = "WARN"
    description: str = ""
    filter: dict[str, Any] = field(default_factory=dict)


VALID_WRITE_MODES = ("all", "violations_only")
VALID_WRITE_STRATEGIES = ("append", "overwrite")


@dataclass
class StorageConfig:
    catalog: str
    schema: str
    table: str
    warehouse_id: str
    write_mode: str = "all"
    write_strategy: str = "append"

    def table_for(self, resource_type: str) -> str:
        """Per-resource-type Delta table: `{catalog}.{schema}.{table}_{resource_type}`."""
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}_{resource_type}`"


VALID_SOURCES = ("sdk", "system_tables")


@dataclass
class FetchConfig:
    source: str = "sdk"
    current_workspace_only: bool = True


@dataclass
class EmailConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password_env: str = "SMTP_PASSWORD"
    from_addr: str = ""
    to_addrs: list[str] = field(default_factory=list)


@dataclass
class AlertConfig:
    min_severity: str = "WARN"
    slack_webhook_url: str = ""
    email: EmailConfig = field(default_factory=EmailConfig)


@dataclass
class ValidationConfig:
    storage: StorageConfig
    alerting: AlertConfig
    fetch: FetchConfig
    checks: list[Check]


def load_config(path: str | Path) -> ValidationConfig:
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"Config root must be a mapping, got {type(raw).__name__}")

    storage_raw = raw.get("storage") or {}
    for key in ("catalog", "schema", "table", "warehouse_id"):
        if not storage_raw.get(key):
            raise ValueError(f"storage.{key} is required in config")
    write_mode = storage_raw.get("write_mode", "all")
    if write_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"storage.write_mode must be one of {VALID_WRITE_MODES}, got {write_mode!r}"
        )
    write_strategy = storage_raw.get("write_strategy", "append")
    if write_strategy not in VALID_WRITE_STRATEGIES:
        raise ValueError(
            f"storage.write_strategy must be one of {VALID_WRITE_STRATEGIES}, got {write_strategy!r}"
        )
    storage = StorageConfig(
        catalog=storage_raw["catalog"],
        schema=storage_raw["schema"],
        table=storage_raw["table"],
        warehouse_id=storage_raw["warehouse_id"],
        write_mode=write_mode,
        write_strategy=write_strategy,
    )

    alert_raw = raw.get("alerting") or {}
    email_raw = alert_raw.get("email") or {}
    email = EmailConfig(
        enabled=bool(email_raw.get("enabled", False)),
        smtp_host=email_raw.get("smtp_host", ""),
        smtp_port=int(email_raw.get("smtp_port", 587)),
        smtp_user=email_raw.get("smtp_user", ""),
        smtp_password_env=email_raw.get("smtp_password_env", "SMTP_PASSWORD"),
        from_addr=email_raw.get("from_addr", ""),
        to_addrs=list(email_raw.get("to_addrs") or []),
    )
    min_sev = alert_raw.get("min_severity", "WARN")
    if min_sev not in VALID_SEVERITIES:
        raise ValueError(f"alerting.min_severity must be one of {VALID_SEVERITIES}")
    alerting = AlertConfig(
        min_severity=min_sev,
        slack_webhook_url=alert_raw.get("slack_webhook_url", "") or "",
        email=email,
    )

    fetch_raw = raw.get("fetch") or {}
    source = fetch_raw.get("source", "sdk")
    if source not in VALID_SOURCES:
        raise ValueError(f"fetch.source must be one of {VALID_SOURCES}, got {source!r}")
    fetch = FetchConfig(
        source=source,
        current_workspace_only=bool(fetch_raw.get("current_workspace_only", True)),
    )

    checks: list[Check] = []
    for resource_type in SUPPORTED_RESOURCES:
        for idx, raw_check in enumerate(raw.get(resource_type) or []):
            checks.append(_parse_check(resource_type, idx, raw_check))

    return ValidationConfig(
        storage=storage, alerting=alerting, fetch=fetch, checks=checks
    )


def _parse_check(resource_type: str, idx: int, raw: dict[str, Any]) -> Check:
    if not isinstance(raw, dict):
        raise ValueError(f"{resource_type}[{idx}] must be a mapping")
    name = raw.get("name") or f"{resource_type}_check_{idx}"
    op = raw.get("op")
    if op not in VALID_OPS:
        raise ValueError(f"{resource_type}.{name}: op '{op}' must be one of {VALID_OPS}")

    field_path = raw.get("field")
    if op not in ("exists", "not_exists", "truthy", "falsy") and field_path is None:
        # presence ops can target the resource itself; others need a field
        raise ValueError(f"{resource_type}.{name}: 'field' is required for op '{op}'")

    severity = raw.get("severity", "WARN")
    if severity not in VALID_SEVERITIES:
        raise ValueError(f"{resource_type}.{name}: severity must be one of {VALID_SEVERITIES}")

    return Check(
        resource_type=resource_type,
        name=name,
        field=field_path,
        op=op,
        value=raw.get("value"),
        severity=severity,
        description=raw.get("description", ""),
        filter=dict(raw.get("filter") or {}),
    )
