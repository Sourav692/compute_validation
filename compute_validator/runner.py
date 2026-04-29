from __future__ import annotations

import logging
from pathlib import Path

from .alerts import send_alerts
from .client import get_client
from .config import SUPPORTED_RESOURCES, FetchConfig, StorageConfig, load_config
from .engine import run_checks
from .fetchers import fetch_resources as fetch_resources_sdk
from .fetchers_system import fetch_clusters_system, fetch_sql_warehouses_system
from .fetchers import fetch_instance_pools
from .storage import ensure_tables, write_results


log = logging.getLogger(__name__)


def run(config_path: str | Path, profile: str | None = None, dry_run: bool = False) -> int:
    """Execute the validation pipeline.

    Returns the count of violations found (excluding skipped checks).
    """
    log.info("=" * 70)
    log.info("Compute Validation — starting")
    log.info("=" * 70)
    log.info("Step 1/5: Loading config from %s", config_path)
    config = load_config(config_path)
    log.info("  -> loaded %d checks across %d resource types",
             len(config.checks), len({c.resource_type for c in config.checks}))

    log.info("Step 2/5: Connecting to Databricks (profile=%s)", profile or "DEFAULT")
    w = get_client(profile)
    workspace_host = (w.config.host or "").rstrip("/")
    log.info("  -> connected to %s", workspace_host)

    log.info("Step 3/5: Fetching compute resources (source=%s)", config.fetch.source)
    resources = _fetch_all(w, config.fetch, config.storage)
    total_resources = sum(len(v) for v in resources.values())
    log.info("  -> fetched %d resources total", total_resources)

    log.info("Step 4/5: Running %d checks", len(config.checks))
    results = run_checks(config, resources, workspace_host)
    passed = sum(1 for r in results if r.passed and not r.skipped)
    skipped = sum(1 for r in results if r.skipped)
    violations = [r for r in results if not r.passed and not r.skipped]
    log.info(
        "  -> %d evaluations: %d passed, %d violations, %d skipped",
        len(results), passed, len(violations), skipped,
    )

    if dry_run:
        log.info("Step 5/5: DRY RUN — skipping Delta write and alerts")
        if violations:
            log.warning("Violations summary:")
            for v in violations:
                log.warning(
                    "  [%s] %s/%s — %s: %s %s %s (actual=%s)",
                    v.severity, v.resource_type, v.resource_name or v.resource_id,
                    v.check_name, v.field, v.op, v.expected, v.actual,
                )
        else:
            log.info("  no violations found")
        log.info("Done.")
        return len(violations)

    log.info("Step 5/5: Persisting results and dispatching alerts")
    log.info("  ensuring per-resource-type tables exist (prefix=%s_*)", config.storage.table)
    ensure_tables(w, config.storage, SUPPORTED_RESOURCES)

    if config.storage.write_mode == "violations_only":
        rows_to_write = violations
        log.info("  write_mode=violations_only — writing %d rows (skipping %d passed/skipped)",
                 len(rows_to_write), len(results) - len(rows_to_write))
    else:
        rows_to_write = results
        log.info("  write_mode=all — writing %d rows", len(rows_to_write))
    write_results(w, config.storage, rows_to_write)

    log.info("  evaluating alerts (min_severity=%s)", config.alerting.min_severity)
    send_alerts(config.alerting, results, workspace_host)

    log.info("Done — %d violations", len(violations))
    return len(violations)


def _fetch_all(w, fetch_cfg: FetchConfig, storage: StorageConfig) -> dict:
    if fetch_cfg.source == "system_tables":
        if not storage.warehouse_id:
            raise ValueError(
                "fetch.source=system_tables requires storage.warehouse_id to query system tables"
            )
        return {
            "clusters": fetch_clusters_system(
                w, storage.warehouse_id, fetch_cfg.current_workspace_only
            ),
            "sql_warehouses": fetch_sql_warehouses_system(
                w, storage.warehouse_id, fetch_cfg.current_workspace_only
            ),
            # No system table for instance pools — fall back to SDK.
            "instance_pools": fetch_instance_pools(w),
        }
    return fetch_resources_sdk(w, SUPPORTED_RESOURCES)
