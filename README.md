# Databricks Compute Validation

Utility for validating Databricks compute resources (all-purpose clusters, SQL
warehouses, instance pools) against user-defined expectations declared in YAML.
Results are persisted to a Delta table and violations trigger Slack and/or
email alerts.

The framework is plain Python — it runs from the CLI, a Databricks Job, or any
scheduler. It does **not** require Databricks notebooks (`.ipynb`).

## Layout

```
compute_validation/
├── config/
│   └── expectations.yaml        # rules, storage target, alerting config
├── compute_validator/
│   ├── client.py                # WorkspaceClient (default profile)
│   ├── config.py                # YAML loader / dataclasses
│   ├── fetchers.py              # SDK calls per resource type
│   ├── rules.py                 # operator evaluation
│   ├── engine.py                # check execution
│   ├── storage.py               # Delta table writer (Statement Execution API)
│   ├── alerts.py                # Slack webhook + SMTP email
│   └── runner.py                # orchestration
├── main.py                      # CLI entry point
└── requirements.txt
```

## Setup

```bash
pip install -r requirements.txt
```

Authenticate to Databricks via the standard CLI profile (uses `DEFAULT` from
`~/.databrickscfg` unless overridden):

```bash
databricks configure --profile DEFAULT
```

## Configure expectations

Edit `config/expectations.yaml`. At minimum set:

- `storage.catalog`, `storage.schema`, `storage.table` — destination Delta table
- `storage.warehouse_id` — SQL warehouse used for DDL/DML
- Optional `alerting.slack_webhook_url` and/or `alerting.email`

Each check supports operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`,
`not_in`, `regex`, `contains`, `exists`, `not_exists`, `truthy`, `falsy`.
Use `filter` to scope a check to resources matching specific attributes (e.g.
only shared-access clusters).

Field paths are dotted: e.g. `autoscale.max_workers`.

### Data source: SDK vs system tables

Set `fetch.source` in the YAML:

| Source           | What it does                                                           | When to use                                          |
|------------------|------------------------------------------------------------------------|------------------------------------------------------|
| `sdk` (default)  | Live `WorkspaceClient.list()` calls per resource type.                 | Small/medium workspaces, freshest config view.       |
| `system_tables`  | One SQL query each against `system.compute.clusters` and `system.compute.warehouses`. | Large workspaces where SDK pagination is slow.       |

System table mode reads the latest non-deleted row per cluster/warehouse via a
`ROW_NUMBER() OVER (PARTITION BY id ORDER BY change_time DESC)` window. It uses
the SQL warehouse from `storage.warehouse_id` and scopes to the current
workspace via `current_workspace_id()` (override with `fetch.current_workspace_only: false`).

**Field names differ between modes** because system table column names don't
match SDK fields (e.g. `auto_termination_minutes` vs `autotermination_minutes`,
`dbr_version` vs `spark_version`, `warehouse_type` vs `enable_serverless_compute`).
A ready-to-use example for system tables is in
`config/expectations.system_tables.yaml`. Instance pools have no system table
and always come through the SDK.

## Run

```bash
# Evaluate, write results, send alerts on violations
python main.py --config config/expectations.yaml

# Dry run — print violations only
python main.py --dry-run

# Use a non-default profile
python main.py --profile prod-workspace

# Make CI fail when violations exist
python main.py --fail-on-violation
```

## Result schema

```sql
CREATE TABLE <catalog>.<schema>.<table> (
  run_id          STRING,
  run_ts          TIMESTAMP,
  workspace_host  STRING,
  resource_type   STRING,   -- clusters | sql_warehouses | instance_pools
  resource_id     STRING,
  resource_name   STRING,
  check_name      STRING,
  description     STRING,
  severity        STRING,   -- INFO | WARN | CRITICAL
  field           STRING,
  op              STRING,
  expected        STRING,
  actual          STRING,
  passed          BOOLEAN,
  skipped         BOOLEAN,
  skip_reason     STRING
) USING DELTA PARTITIONED BY (resource_type);
```

## Summary views (one row per resource)

Each per-type results table also gets a companion **summary view** built by the
`compute-validate-create-views` console script (a separate task in the
Lakeflow Job, runs after `validate_compute` with `run_if: ALL_DONE`):

```
{table}_clusters_summary
{table}_sql_warehouses_summary
{table}_instance_pools_summary
```

Each view collapses multiple check rows for the same resource into one row,
with failing checks aggregated as an array of structs:

```sql
SELECT
  workspace_host,
  resource_type,
  resource_id,
  resource_name,
  last_run_ts,
  last_run_id,
  failing_checks_count,
  failing_checks   -- array<struct<
                   --   check_name STRING, severity STRING, field STRING,
                   --   op STRING, expected STRING, actual STRING, passed BOOLEAN >>
FROM main.compute_validation.validation_results_clusters_summary
```

Views always reflect the **latest run** (a `WITH latest AS (... ORDER BY run_ts
DESC LIMIT 1)` filter), so they work cleanly under both `write_strategy: append`
and `overwrite`. Run locally with:

```bash
.venv/bin/compute-validate-create-views --config config/expectations.system_tables.yaml
```

## Write semantics

Two independent knobs in `storage:`:

| Setting           | Values                       | Effect |
|-------------------|------------------------------|--------|
| `write_mode`      | `all` / `violations_only`    | What to write — every result row, or only failed checks. |
| `write_strategy`  | `append` / `overwrite`       | How to write — append to history, or `TRUNCATE` each per-type table before insert. |

Use `overwrite` when you want the table to reflect the **current** state — a
compute that's been remediated drops out of the table on the next run. Use
`append` to keep an audit trail of every run for trend analysis.

The two combine, e.g. `write_mode: violations_only` + `write_strategy: overwrite`
gives you a compact "open findings" table.

## Alerts

A violation is any check where `passed=false` and `skipped=false` whose severity
meets or exceeds `alerting.min_severity`. Slack and email are independent — set
either or both.

For email, the SMTP password is read from the environment variable named in
`alerting.email.smtp_password_env` (default: `SMTP_PASSWORD`).

## Scheduling

Wire this as a Databricks Job (Python script task) or a cron entry. The CLI
returns a non-zero exit code with `--fail-on-violation`, suitable for CI gates.

## Deploy as a Lakeflow Job (Databricks Asset Bundle)

This repo ships a DAB so the validator runs daily in your workspace on
serverless compute, and emails on every violation.

```bash
# One-time: install the Databricks CLI ≥ 0.218 and authenticate
databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com

# Validate the bundle (--profile required if multiple profiles share the host)
databricks bundle validate --target dev --profile DEFAULT

# Build the wheel + sync the bundle to the workspace
databricks bundle deploy --target dev --profile DEFAULT       # PAUSED schedule
databricks bundle deploy --target prod --profile DEFAULT      # UNPAUSED schedule

# Run on demand
databricks bundle run compute_validation --target prod --profile DEFAULT
```

**What gets deployed:**
- `compute_validator-0.1.0-*.whl` (built from `pyproject.toml` via `python -m build`)
- The repo source (so `config/expectations.system_tables.yaml` is reachable from the job)
- A Lakeflow Job named `[<target>] Compute Validation` running once per day
  on serverless compute, calling the `compute-validate` console script with
  `--config config/expectations.system_tables.yaml --fail-on-violation`

**How alerts work in the bundle:**
The Lakeflow Job has `email_notifications.on_failure: [sourav.banerjee@databricks.com]`.
The CLI is invoked with `--fail-on-violation`, so any policy violation makes
the task exit non-zero, the job is marked failed, and Databricks emails the
recipient. The same path covers genuine task errors (auth issues, missing
warehouse, etc.).

To change the recipient, override the `notification_email` variable:

```bash
databricks bundle deploy --target prod --var="notification_email=team@example.com"
```

To split *violation* alerts from *infra* alerts, drop `--fail-on-violation`
in `resources/compute_validation.job.yml` and use the in-app Slack/email
config inside `config/expectations.system_tables.yaml` (the `alerting` block).
