from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Iterable

from databricks.sdk import WorkspaceClient

from .config import StorageConfig
from .engine import CheckResult
from .sql_exec import execute as _execute_stmt


log = logging.getLogger(__name__)


def _execute(w: WorkspaceClient, warehouse_id: str, sql: str) -> None:
    _execute_stmt(w, warehouse_id, sql)


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id            STRING,
  run_ts            TIMESTAMP,
  workspace_host    STRING,
  resource_type     STRING,
  resource_id       STRING,
  resource_name     STRING,
  check_name        STRING,
  description       STRING,
  severity          STRING,
  field             STRING,
  op                STRING,
  expected          STRING,
  actual            STRING,
  passed            BOOLEAN,
  skipped           BOOLEAN,
  skip_reason       STRING
)
USING DELTA
""".strip()


def ensure_tables(
    w: WorkspaceClient, storage: StorageConfig, resource_types: Iterable[str]
) -> None:
    log.info("  ensuring catalog `%s`", storage.catalog)
    _execute(w, storage.warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{storage.catalog}`")
    log.info("  ensuring schema `%s`.`%s`", storage.catalog, storage.schema)
    _execute(
        w,
        storage.warehouse_id,
        f"CREATE SCHEMA IF NOT EXISTS `{storage.catalog}`.`{storage.schema}`",
    )
    for rt in resource_types:
        table = storage.table_for(rt)
        log.info("  ensuring table %s", table)
        _execute(w, storage.warehouse_id, _TABLE_DDL.format(table=table))


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


_INSERT_COLUMNS = (
    "run_id", "run_ts", "workspace_host", "resource_type", "resource_id",
    "resource_name", "check_name", "description", "severity", "field",
    "op", "expected", "actual", "passed", "skipped", "skip_reason",
)


def write_results(
    w: WorkspaceClient,
    storage: StorageConfig,
    results: list[CheckResult],
) -> None:
    """Split results by resource_type and write to per-type Delta tables."""
    if not results:
        log.info("  no results to write")
        return

    by_type: dict[str, list[CheckResult]] = defaultdict(list)
    for r in results:
        by_type[r.resource_type].append(r)

    for resource_type, rows in by_type.items():
        _write_one_table(w, storage, resource_type, rows)


def _write_one_table(
    w: WorkspaceClient,
    storage: StorageConfig,
    resource_type: str,
    rows: list[CheckResult],
) -> None:
    table = storage.table_for(resource_type)
    batch_size = 200000
    total_batches = (len(rows) + batch_size - 1) // batch_size
    for batch_idx, start in enumerate(range(0, len(rows), batch_size), start=1):
        chunk = rows[start:start + batch_size]
        rows_sql = []
        for r in chunk:
            d = r.to_dict()
            row = "(" + ", ".join(
                _sql_literal(d[c]) if c != "run_ts" else f"TIMESTAMP {_sql_literal(d[c])}"
                for c in _INSERT_COLUMNS
            ) + ")"
            rows_sql.append(row)

        sql = (
            f"INSERT INTO {table} ("
            + ", ".join(_INSERT_COLUMNS) + ") VALUES "
            + ", ".join(rows_sql)
        )
        log.info("  %s: writing batch %d/%d (%d rows)",
                 resource_type, batch_idx, total_batches, len(chunk))
        _execute(w, storage.warehouse_id, sql)
    log.info("  wrote %d rows to %s", len(rows), table)
