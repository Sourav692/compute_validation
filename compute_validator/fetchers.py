from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

from databricks.sdk import WorkspaceClient


log = logging.getLogger(__name__)


def _to_dict(obj: Any) -> dict[str, Any]:
    """Convert an SDK dataclass to a plain dict, handling nested objects/enums."""
    if obj is None:
        return {}
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    if isinstance(obj, dict):
        return obj
    # Last resort: best-effort attribute scrape
    return {k: v for k, v in vars(obj).items() if not k.startswith("_")}


def fetch_clusters(w: WorkspaceClient) -> list[dict[str, Any]]:
    log.info("Fetching all-purpose clusters...")
    out = []
    total_seen = 0
    for c in w.clusters.list():
        total_seen += 1
        d = _to_dict(c)
        # only validate all-purpose clusters; job clusters are ephemeral
        if d.get("cluster_source") in (None, "UI", "API"):
            d["_id"] = d.get("cluster_id")
            d["_name"] = d.get("cluster_name")
            out.append(d)
            log.debug("  cluster: %s (%s)", d["_name"], d["_id"])
    log.info("  -> %d all-purpose clusters (filtered from %d total)", len(out), total_seen)
    return out


def fetch_sql_warehouses(w: WorkspaceClient) -> list[dict[str, Any]]:
    log.info("Fetching SQL warehouses...")
    out = []
    for wh in w.warehouses.list():
        d = _to_dict(wh)
        d["_id"] = d.get("id")
        d["_name"] = d.get("name")
        out.append(d)
        log.debug("  warehouse: %s (%s)", d["_name"], d["_id"])
    log.info("  -> %d SQL warehouses", len(out))
    return out


def fetch_instance_pools(w: WorkspaceClient) -> list[dict[str, Any]]:
    log.info("Fetching instance pools...")
    out = []
    for p in w.instance_pools.list():
        d = _to_dict(p)
        d["_id"] = d.get("instance_pool_id")
        d["_name"] = d.get("instance_pool_name")
        out.append(d)
        log.debug("  pool: %s (%s)", d["_name"], d["_id"])
    log.info("  -> %d instance pools", len(out))
    return out


FETCHERS: dict[str, Callable[[WorkspaceClient], list[dict[str, Any]]]] = {
    "clusters": fetch_clusters,
    "sql_warehouses": fetch_sql_warehouses,
    "instance_pools": fetch_instance_pools,
}


def fetch_resources(w: WorkspaceClient, resource_types: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for rt in resource_types:
        if rt in FETCHERS:
            out[rt] = FETCHERS[rt](w)
    return out
