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

## Alerts

A violation is any check where `passed=false` and `skipped=false` whose severity
meets or exceeds `alerting.min_severity`. Slack and email are independent — set
either or both.

For email, the SMTP password is read from the environment variable named in
`alerting.email.smtp_password_env` (default: `SMTP_PASSWORD`).

## Scheduling

Wire this as a Databricks Job (Python script task) or a cron entry. The CLI
returns a non-zero exit code with `--fail-on-violation`, suitable for CI gates.
