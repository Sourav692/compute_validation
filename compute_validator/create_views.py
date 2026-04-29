from __future__ import annotations

import argparse
import logging
import sys

from .client import get_client
from .config import SUPPORTED_RESOURCES, StorageConfig, load_config
from .sql_exec import execute as execute_sql
from .storage import ensure_tables


log = logging.getLogger(__name__)


# View semantics:
#   - One row per resource (cluster / warehouse / pool).
#   - Scoped to the latest run via the `latest` CTE — so the view always
#     reflects current state regardless of write_strategy=append|overwrite.
#   - failing_checks is an array of structs with the per-check details the
#     user asked for: check_name, severity, field, op, expected, actual, passed.
#   - failing_checks_count gives the integer count for quick filtering.
_VIEW_DDL = """
CREATE OR REPLACE VIEW {view} AS
WITH latest AS (
  SELECT run_id
  FROM {table}
  ORDER BY run_ts DESC
  LIMIT 1
)
SELECT
  workspace_host,
  resource_type,
  resource_id,
  resource_name,
  MAX(run_ts) AS last_run_ts,
  MAX(run_id) AS last_run_id,
  SUM(CASE WHEN NOT passed AND NOT skipped THEN 1 ELSE 0 END) AS failing_checks_count,
  collect_list(
    named_struct(
      'check_name', check_name,
      'severity', severity,
      'field', field,
      'op', op,
      'expected', expected,
      'actual', actual,
      'passed', passed
    )
  ) FILTER (WHERE NOT passed AND NOT skipped) AS failing_checks
FROM {table}
WHERE run_id IN (SELECT run_id FROM latest)
GROUP BY workspace_host, resource_type, resource_id, resource_name
""".strip()


def _view_name(storage: StorageConfig, resource_type: str) -> str:
    return f"`{storage.catalog}`.`{storage.schema}`.`{storage.table}_{resource_type}_summary`"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="compute-validate-create-views",
        description=(
            "Create per-resource-type summary views on top of the validation "
            "result tables. Each view collapses multiple check rows into one "
            "row per resource, with failing checks aggregated as an array of "
            "structs (check_name, severity, field, op, expected, actual, passed)."
        ),
    )
    parser.add_argument(
        "--config",
        default="config/expectations.yaml",
        help="Path to expectations YAML (default: config/expectations.yaml)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile (defaults to SDK auth resolution).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default INFO.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(args.config)
    w = get_client(args.profile)

    log.info("=" * 70)
    log.info("Compute Validation — creating summary views")
    log.info("=" * 70)
    log.info("Connected to %s", (w.config.host or "").rstrip("/"))

    # Make sure backing tables exist; ensures view DDL doesn't fail on a
    # cold-start workspace where the validate task hasn't run yet.
    log.info("Ensuring backing tables exist")
    ensure_tables(w, config.storage, SUPPORTED_RESOURCES)

    log.info("Creating %d summary views", len(SUPPORTED_RESOURCES))
    for resource_type in SUPPORTED_RESOURCES:
        table = config.storage.table_for(resource_type)
        view = _view_name(config.storage, resource_type)
        sql = _VIEW_DDL.format(view=view, table=table)
        log.info("  creating %s", view)
        execute_sql(w, config.storage.warehouse_id, sql)

    log.info("Done — %d views ready", len(SUPPORTED_RESOURCES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
