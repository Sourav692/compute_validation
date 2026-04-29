# Compute Validation — Enterprise Hardening Plan

## Context

Today the framework is a working MVP: a single-config CLI that fetches compute via SDK or system tables, runs YAML-defined checks, writes per-resource-type Delta tables, and dispatches Slack/email alerts. To take this from "works on my workspace" to "deployed across a Field/Platform org", several gaps need closing — most around **trust**, **scale**, **operability**, and **discoverability**. This plan groups improvements by tier so we can sequence the work and ship value early.

The current code is at:
- `compute_validator/runner.py` (orchestration)
- `compute_validator/config.py` (YAML schema)
- `compute_validator/fetchers.py`, `fetchers_system.py` (data sources)
- `compute_validator/engine.py`, `rules.py` (check evaluation)
- `compute_validator/storage.py` (Delta writer via Statement Execution API)
- `compute_validator/alerts.py` (Slack + SMTP)

---

## Tier 1 — Adoption blockers (do first)

These are the items that, if missing, make the tool hard to onboard or unsafe to run unattended in prod.

### 1.1 Multi-workspace / account-level runs
Most enterprises have 5–50+ workspaces. Today we connect to one. Add an `accounts.yaml` (or `workspaces:` block) listing target workspaces, iterate, and stamp `workspace_host` + `workspace_id` on every result. With `fetch.source: system_tables` this becomes "1 query per resource type total" instead of "N queries per workspace" — the system tables already span all workspaces in the account.
- **Modify**: `runner.py` (loop over workspaces), `client.py` (per-workspace client factory), `engine.py` (already stamps `workspace_host`).

### 1.2 Exemptions / waivers
Compliance always has exceptions. Add a top-level `exemptions:` block keyed by `(resource_id, check_name)` with `reason` and `expires_on`. Engine emits `skipped=true, skip_reason="exempt: …"` for matches. Track expired exemptions as a separate critical finding.
- **Modify**: `config.py` (parse), `engine.py` (apply before evaluation).

### 1.3 Secrets via Databricks Secret Scopes
Today SMTP password reads from env var; Slack URL is plaintext in YAML. Switch to `secret_ref: { scope: ..., key: ... }` format and resolve via `w.secrets.get_secret()`. Falls back to env var for local dev.
- **Modify**: `config.py`, `alerts.py`.

### 1.4 Pre-flight checks
Today failures show up mid-run. Add `runner._preflight()` that verifies: warehouse exists and user can use it, system tables are queryable (when source=system_tables), target catalog/schema exist or are creatable, secret scopes resolve. Fail fast with a clear list of what's missing.
- **Add**: `compute_validator/preflight.py`.

### 1.5 Required tags / cluster policy checks
The single most-requested compliance check in practice. Add first-class support:
- `tags.cost_center exists` style (already works via dotted paths if we expose `tags` as a dict — system table SELECT currently drops it for size; allow opt-in projection of specific tag keys).
- Cluster policy attachment (`policy_id exists`) — already works in system tables mode.
- Optional new resource type `cluster_policies` to validate the policies themselves (their `definition` JSON enforces fleet-wide compute defaults).
- **Modify**: `fetchers_system.py` to project `tags['cost_center']` etc. into top-level columns; consider new `fetchers.py::fetch_cluster_policies`.

### 1.6 Init / scaffolding command
`python main.py init --warehouse-id ...` writes a starter YAML pre-populated with the warehouse ID and a sensible default ruleset. Onboarding goes from "read README + edit YAML" to "one command".
- **Modify**: `main.py`.

### 1.7 Field discovery
`python main.py describe-fields --resource clusters` prints the column names available to check against, separately for SDK and system table modes. Removes the "what can I check?" guessing.
- **Modify**: `main.py`, `fetchers*.py` (export column lists).

### 1.8 Test suite
Currently zero tests. Bare minimum:
- Unit tests for `rules.evaluate` (every operator + missing-field semantics) and `rules.get_path` (dotted paths over dicts/lists).
- Unit tests for `config.load_config` (every validation error path).
- A fixture-based engine test that feeds a fake `resources_by_type` dict and asserts `CheckResult` rows.
- Smoke integration test gated by env var `COMPUTE_VALIDATION_INTEGRATION=1` that runs against a sandbox profile in dry-run mode.
- **Add**: `tests/` with `pytest` + `pytest-mock`. Wire into `requirements-dev.txt`.

### 1.9 Databricks Asset Bundle (DAB)
Most enterprise teams deploy via DABs. Ship a `databricks.yml` and `resources/jobs.yml` that defines a daily Job running `main.py` with the right entrypoint, parameters, and notification destinations. One-command deployment via `databricks bundle deploy`.
- **Add**: `databricks.yml`, `resources/compute_validation_job.yml`.

---

## Tier 2 — Operability and signal quality

### 2.1 Drift / regression-only alerting
Today alerts fire on every violation every run. With dozens of resources × dozens of checks, that's noise. Add `alerting.mode: all | new_only`. `new_only` joins against the last run's results table and alerts only on `(resource_id, check_name)` pairs that newly failed. Alert fatigue is the #1 reason these tools get muted.
- **Modify**: `alerts.py`, requires storage read.

### 2.2 Per-severity routing
Different severities → different channels: `INFO → Slack #data-platform`, `WARN → Slack #data-platform-alerts`, `CRITICAL → PagerDuty`. Today everything goes one place.
- **Modify**: `config.py` (per-severity destinations), `alerts.py` (route accordingly), add PagerDuty/MS Teams adapters.

### 2.3 AI/BI dashboard for results
Ship a published dashboard: violations over time, top offending resources, by severity, by check, by workspace. Compliance teams want to see this without writing SQL. Use the `databricks-aibi-dashboards` skill or hand-author `*.lvdash.json`.
- **Add**: `dashboards/compute_validation.lvdash.json`.

### 2.4 Native Databricks SQL alerts as alternative
Alongside Slack/email, support creating a Databricks SQL Alert on the result table. Enterprise security teams sometimes prefer this since it's already governed.
- **Modify**: alternative path in `alerts.py` or new `alerts_native.py`.

### 2.5 Result table hygiene
Add `OPTIMIZE` and `VACUUM` after writes; configurable retention; ZORDER on `(workspace_id, resource_id, check_name)`. Without this the table fragments over time.
- **Modify**: `storage.py`.

### 2.6 Concurrent SDK fetches
When `source: sdk`, fetch clusters / warehouses / pools in parallel via `concurrent.futures.ThreadPoolExecutor`. Cuts wall-clock by ~3x.
- **Modify**: `fetchers.py::fetch_resources`.

### 2.7 Structured JSON logs
Behind `--log-format json` flag. Makes the tool friendly to logs platforms (Splunk, Datadog, log analytics on the workspace).
- **Modify**: `main.py`.

### 2.8 Retry with backoff
Statement Execution and SDK calls can transiently fail. Wrap the API entry points with `tenacity`-style retry on 429/5xx.
- **Modify**: `sql_exec.py`, `fetchers.py`.

---

## Tier 3 — Polish & nice-to-haves

### 3.1 Pre-commit / CI for the YAML
A GitHub Action that runs `python -c "from compute_validator.config import load_config; load_config('config/expectations.yaml')"` on PRs. Catches typos before deploy.

### 3.2 Cookbook of common checks
A `docs/cookbook.md` with copy-paste recipes: "require `cost_center` tag", "block legacy DBR", "limit to specific node types", "auto-stop ≤ 30 min". Lower the blank-page barrier.

### 3.3 Slack message richness
Use Slack Block Kit instead of plain text — group violations by severity, include a button linking to the AI/BI dashboard, threading per resource type. HTML email body.

### 3.4 Auto-remediation (guarded)
For low-risk fixes, support `auto_remediate: true` per check (e.g., flip a missing `auto_termination_minutes`). Gated by `--allow-remediate` CLI flag. Log every change to a separate audit table.

### 3.5 Parameterization / environments
Support `${ENV.PROD_WAREHOUSE_ID}` interpolation and `--env prod|dev` for environment-specific thresholds (e.g., max_workers cap differs by env). Avoids forking the YAML per env.

### 3.6 Packaging
`pyproject.toml`, console-script entrypoint (`compute-validate ...`), publish as a wheel to internal PyPI. Removes the "git clone and run main.py" friction.

---

## Files most likely to change (by tier)

| Tier | Files |
|------|-------|
| 1.1, 1.2, 1.4, 1.6 | `runner.py`, `main.py`, new `preflight.py` |
| 1.3, 1.5, 1.7 | `config.py`, `fetchers*.py`, `alerts.py` |
| 1.8 | new `tests/` |
| 1.9 | new `databricks.yml`, `resources/*.yml` |
| 2.1–2.4 | `alerts.py`, new `alerts_native.py`, dashboards/ |
| 2.5–2.8 | `storage.py`, `fetchers.py`, `sql_exec.py`, `main.py` |

## Verification

- **Tier 1 unit tests**: `pytest tests/ -v` — must pass for rules, config, exemptions, preflight.
- **Tier 1 integration smoke**: `python main.py --config config/expectations.system_tables.yaml --dry-run` against the dev workspace; assert non-zero passes, no exceptions.
- **Tier 1.1 multi-workspace**: deploy DAB to two workspaces sharing one results catalog; confirm `workspace_id` distinguishes rows.
- **Tier 2.1 drift**: run twice consecutively with no config change; second run should produce zero alerts even though violations persist.
- **Tier 2.5 hygiene**: query `DESCRIBE HISTORY` on result tables; confirm OPTIMIZE/VACUUM ran.
