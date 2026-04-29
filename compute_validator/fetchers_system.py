from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from .sql_exec import fetch_rows


log = logging.getLogger(__name__)


# Scalar columns from system.compute.clusters. The heavy columns (tags map,
# init_scripts array, *_attributes struct) are excluded — pulling them with
# SELECT * for a large account exceeds the 25 MiB inline-result limit. If you
# need them, project them as scalars in SQL (e.g. tags['cost-center']).
_CLUSTER_COLUMNS = (
    "account_id", "workspace_id", "cluster_id", "cluster_name", "owned_by",
    "create_time", "delete_time", "driver_node_type", "worker_node_type",
    "worker_count", "min_autoscale_workers", "max_autoscale_workers",
    "auto_termination_minutes", "enable_elastic_disk", "cluster_source",
    "driver_instance_pool_id", "worker_instance_pool_id", "dbr_version",
    "change_time", "change_date", "data_security_mode", "policy_id",
)

_WAREHOUSE_COLUMNS = (
    "warehouse_id", "workspace_id", "account_id", "warehouse_name",
    "warehouse_type", "warehouse_channel", "warehouse_size",
    "min_clusters", "max_clusters", "auto_stop_minutes",
    "change_time", "delete_time",
)


_CLUSTERS_SQL = """
WITH ranked AS (
  SELECT
    {cols},
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS _rn
  FROM system.compute.clusters
  WHERE delete_time IS NULL
    {workspace_filter}
)
SELECT {cols}
FROM ranked
WHERE _rn = 1
  AND (cluster_source IS NULL OR cluster_source IN ('UI', 'API'))
""".strip()


_WAREHOUSES_SQL = """
WITH ranked AS (
  SELECT
    {cols},
    ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS _rn
  FROM system.compute.warehouses
  WHERE delete_time IS NULL
    {workspace_filter}
)
SELECT {cols}
FROM ranked
WHERE _rn = 1
""".strip()


def _normalize(value: Any) -> Any:
    """Best-effort decode of struct/array columns returned as JSON strings."""
    if isinstance(value, str):
        s = value.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except (ValueError, TypeError):
                return value
    return value


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _normalize(v) for k, v in row.items() if not k.startswith("_")}


def _resolve_workspace_filter(w: WorkspaceClient, current_workspace_only: bool) -> str:
    if not current_workspace_only:
        return ""
    try:
        wsid = str(w.get_workspace_id())
        log.info("  scoping to workspace_id=%s", wsid)
        return f"AND workspace_id = '{wsid}'"
    except Exception as exc:
        log.warning("  could not resolve current workspace_id (will scan all workspaces): %s", exc)
    return ""


def fetch_clusters_system(
    w: WorkspaceClient, warehouse_id: str, current_workspace_only: bool = True
) -> list[dict[str, Any]]:
    log.info("Fetching clusters from system.compute.clusters...")
    workspace_filter = _resolve_workspace_filter(w, current_workspace_only)
    sql = _CLUSTERS_SQL.format(
        cols=", ".join(_CLUSTER_COLUMNS),
        workspace_filter=workspace_filter,
    )
    rows = fetch_rows(w, warehouse_id, sql)
    out = []
    for r in rows:
        d = _normalize_row(r)
        d["_id"] = d.get("cluster_id")
        d["_name"] = d.get("cluster_name")
        out.append(d)
    log.info("  -> %d clusters", len(out))
    return out


def fetch_sql_warehouses_system(
    w: WorkspaceClient, warehouse_id: str, current_workspace_only: bool = True
) -> list[dict[str, Any]]:
    log.info("Fetching warehouses from system.compute.warehouses...")
    workspace_filter = _resolve_workspace_filter(w, current_workspace_only)
    sql = _WAREHOUSES_SQL.format(
        cols=", ".join(_WAREHOUSE_COLUMNS),
        workspace_filter=workspace_filter,
    )
    rows = fetch_rows(w, warehouse_id, sql)
    out = []
    for r in rows:
        d = _normalize_row(r)
        d["_id"] = d.get("warehouse_id")
        d["_name"] = d.get("warehouse_name")
        out.append(d)
    log.info("  -> %d warehouses", len(out))
    return out
