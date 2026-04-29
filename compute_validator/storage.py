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
    # Spark SQL uses backslash escaping for string literals — the standard
    # SQL `''` doubling is parsed as adjacent literals concatenated with an
    # empty string, eating the apostrophe (so "Sourav Banerjee''s" becomes
    # "Sourav Banerjees"). Use \' instead, and escape backslashes first so
    # we don't double-escape ones we just inserted.
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + s + "'"


_INSERT_COLUMNS = (
    "run_id", "run_ts", "workspace_host", "resource_type", "resource_id",
    "resource_name", "check_name", "description", "severity", "field",
    "op", "expected", "actual", "passed", "skipped", "skip_reason",
)


def _get_spark():
    """Return an active SparkSession when running inside Databricks, else None.

    Inside a Lakeflow Job (notebook task, spark_python_task, or serverless
    python_wheel_task with databricks-connect), this resolves to a usable
    session. Local `python main.py` runs return None and fall back to the
    Statement Execution INSERT path.
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception as exc:
        log.info("  Spark not available — using SQL Statement Execution path (%s)",
                 type(exc).__name__)
        return None


def write_results(
    w: WorkspaceClient,
    storage: StorageConfig,
    results: list[CheckResult],
    resource_types: Iterable[str],
) -> None:
    """Split results by resource_type and write to per-type Delta tables.

    Two write paths:
      - Spark DataFrame write (1 op per table) when running inside Databricks.
      - SQL INSERT batches via Statement Execution API for local dev.

    `resource_types` is the full set of types this bundle manages. In
    `overwrite` strategy we truncate every one of them (even types with zero
    rows this run) so a fixed/removed resource disappears from the table.
    """
    by_type: dict[str, list[CheckResult]] = defaultdict(list)
    for r in results:
        by_type[r.resource_type].append(r)

    is_overwrite = storage.write_strategy == "overwrite"
    types_to_visit = list(resource_types) if is_overwrite else list(by_type.keys())

    spark = _get_spark()

    for resource_type in types_to_visit:
        rows = by_type.get(resource_type, [])
        table = storage.table_for(resource_type)

        if not rows:
            if is_overwrite:
                log.info("  %s: no rows this run — truncating %s to clear stale data",
                         resource_type, table)
                _execute(w, storage.warehouse_id, f"TRUNCATE TABLE {table}")
            else:
                log.info("  %s: no rows to write", resource_type)
            continue

        if spark is not None:
            _write_via_spark(spark, storage, resource_type, rows)
        else:
            if is_overwrite:
                log.info("  %s: truncating %s before write (write_strategy=overwrite)",
                         resource_type, table)
                _execute(w, storage.warehouse_id, f"TRUNCATE TABLE {table}")
            _write_one_table(w, storage, resource_type, rows)


def _write_via_spark(
    spark, storage: StorageConfig, resource_type: str, rows: list[CheckResult]
) -> None:
    """Write all rows for one resource type in a single Spark DataFrame op."""
    from pyspark.sql import functions as F  # type: ignore

    # storage.table_for returns the backtick-quoted form; saveAsTable needs
    # the unquoted three-part identifier.
    qualified = f"{storage.catalog}.{storage.schema}.{storage.table}_{resource_type}"
    mode = "overwrite" if storage.write_strategy == "overwrite" else "append"

    log.info("  %s: writing %d rows via Spark to %s (mode=%s)",
             resource_type, len(rows), qualified, mode)

    df = spark.createDataFrame([r.to_dict() for r in rows])
    # Engine emits run_ts as ISO string; cast to TIMESTAMP to match the table schema.
    df = df.withColumn("run_ts", F.to_timestamp("run_ts"))

    (df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "false")
        .saveAsTable(qualified))

    log.info("  %s: Spark write complete", resource_type)


def _write_one_table(
    w: WorkspaceClient,
    storage: StorageConfig,
    resource_type: str,
    rows: list[CheckResult],
) -> None:
    table = storage.table_for(resource_type)
    # Each VALUES row is ~150-300 bytes of SQL; the Statement Execution API
    # has practical limits on statement size, so cap batches conservatively.
    batch_size = 1000
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
