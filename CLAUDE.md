# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A CLI utility that validates Databricks compute (all-purpose clusters, SQL warehouses, instance pools) against declarative YAML expectations. Results are written to a Delta table; severity-gated violations trigger Slack and/or email alerts. The framework is plain Python — no notebook dependency.

## Commands

```bash
pip install -r requirements.txt

# Run validation (writes to Delta + alerts)
python main.py --config config/expectations.yaml

# Preview without persisting or alerting
python main.py --dry-run

# Use a non-default Databricks CLI profile
python main.py --profile <profile-name>

# CI gate — exit 1 if any violation
python main.py --fail-on-violation
```

There is no test suite, lint config, or build step yet.

## Architecture

The pipeline is a single linear flow orchestrated by `compute_validator/runner.py::run`:

1. **Config load** (`config.py`) — parses `expectations.yaml` into typed dataclasses (`ValidationConfig`, `StorageConfig`, `AlertConfig`, `Check`). Validates ops and severities at parse time. The YAML has three peer sections — `clusters`, `sql_warehouses`, `instance_pools` — each a list of checks. `SUPPORTED_RESOURCES` is the source of truth for which sections are recognized.
2. **Client** (`client.py`) — single `WorkspaceClient` from `databricks-sdk`, defaulting to standard SDK auth resolution (env vars / `DEFAULT` profile in `~/.databrickscfg`).
3. **Fetch** (`fetchers.py`) — one function per resource type, registered in the `FETCHERS` dict. Each converts SDK objects to plain dicts via `as_dict()` and stamps `_id`/`_name` for downstream uniformity. Adding a new resource type means: add a fetcher, register it in `FETCHERS`, add `SUPPORTED_RESOURCES`, allow the section in the YAML.
4. **Evaluate** (`engine.py` + `rules.py`) — `engine.run_checks` produces a flat list of `CheckResult` rows (one per `(check, resource)` pair). `rules.evaluate` implements the operators; `rules.get_path` walks dotted field paths against nested dicts/lists and returns a sentinel `_MISSING` for absent fields. `filter` short-circuits to `skipped=True` when a resource doesn't match.
5. **Persist** (`storage.py`) — uses the SDK's **Statement Execution API** (not the SQL connector, not Spark) so it works from any Python runtime. `ensure_table` creates catalog/schema/Delta table; `write_results` batches INSERTs (200 rows/statement) with hand-rolled SQL literal escaping. `_execute` polls until the statement reaches a terminal state.
6. **Alert** (`alerts.py`) — filters violations to those at or above `alerting.min_severity` using `SEVERITY_RANK` from `config.py`, then dispatches to Slack webhook and/or SMTP email. Both are independent and best-effort (failures logged, not raised).

### Key design choices to preserve

- **Resources are plain dicts after fetch.** All downstream code (rules, engine) operates on dicts so adding fields or new resource types doesn't require dataclass changes.
- **`CheckResult` is the wire format.** Its field order matches the Delta table DDL in `storage.py`. If you add a column, update both `CheckResult` and `_TABLE_DDL` and the `columns` tuple in `write_results`.
- **No Spark dependency.** Persistence goes through `w.statement_execution`, which means `storage.warehouse_id` is required in the YAML — there's no local-mode fallback.
- **Severity values are fixed**: `INFO | WARN | CRITICAL`, ordered by `SEVERITY_RANK`. Adding a level means updating `VALID_SEVERITIES` in `config.py`.
- **Operators are fixed in `rules.VALID_OPS`.** Presence ops (`exists`, `not_exists`, `truthy`, `falsy`) don't require a `value`; everything else does. `config._parse_check` enforces the `field`-required rule for non-presence ops.

### Adding a check operator

Add the op to `VALID_OPS` in `config.py` and to the `evaluate` function in `rules.py`. Nothing else changes — the engine and storage layers are operator-agnostic.

### Adding a resource type

1. Write a fetcher in `fetchers.py` that returns `list[dict]` with `_id` and `_name` keys, register in `FETCHERS`.
2. Add the type name to `SUPPORTED_RESOURCES` in `config.py`.
3. Document the new YAML section.
