from __future__ import annotations

import json
import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Iterable

import requests

from .config import AlertConfig, EmailConfig
from .config import SEVERITY_RANK
from .engine import CheckResult


log = logging.getLogger(__name__)


def _violations(results: Iterable[CheckResult], min_severity: str) -> list[CheckResult]:
    threshold = SEVERITY_RANK[min_severity]
    return [
        r for r in results
        if not r.passed and not r.skipped and SEVERITY_RANK[r.severity] >= threshold
    ]


def send_alerts(config: AlertConfig, results: list[CheckResult], workspace_host: str) -> None:
    bad = _violations(results, config.min_severity)
    if not bad:
        log.info("  no violations at or above %s — no alerts sent", config.min_severity)
        return

    log.info("  found %d violations at or above %s", len(bad), config.min_severity)
    if config.slack_webhook_url:
        log.info("  posting Slack alert...")
        try:
            _post_slack(config.slack_webhook_url, bad, workspace_host)
            log.info("  Slack alert sent")
        except Exception as exc:
            log.error("  Slack alert failed: %s", exc)
    else:
        log.info("  Slack webhook not configured — skipping")

    if config.email.enabled:
        log.info("  sending email alert to %s...", ", ".join(config.email.to_addrs))
        try:
            _send_email(config.email, bad, workspace_host)
            log.info("  email alert sent")
        except Exception as exc:
            log.error("  email alert failed: %s", exc)
    else:
        log.info("  email alerts disabled — skipping")


def _format_summary(violations: list[CheckResult], workspace_host: str) -> str:
    by_sev: dict[str, int] = {}
    for r in violations:
        by_sev[r.severity] = by_sev.get(r.severity, 0) + 1
    counts = ", ".join(f"{sev}: {n}" for sev, n in sorted(by_sev.items()))
    lines = [f"Compute validation violations on {workspace_host} — {counts}", ""]
    for r in violations[:50]:
        lines.append(
            f"[{r.severity}] {r.resource_type}/{r.resource_name or r.resource_id} — "
            f"{r.check_name}: {r.field} {r.op} {r.expected} (actual={r.actual})"
        )
    if len(violations) > 50:
        lines.append(f"... and {len(violations) - 50} more")
    return "\n".join(lines)


def _post_slack(webhook_url: str, violations: list[CheckResult], workspace_host: str) -> None:
    text = _format_summary(violations, workspace_host)
    resp = requests.post(
        webhook_url,
        data=json.dumps({"text": text}),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()


def _send_email(cfg: EmailConfig, violations: list[CheckResult], workspace_host: str) -> None:
    if not cfg.smtp_host or not cfg.from_addr or not cfg.to_addrs:
        raise ValueError("email config requires smtp_host, from_addr, to_addrs")

    msg = EmailMessage()
    msg["Subject"] = f"[Compute Validation] {len(violations)} violations on {workspace_host}"
    msg["From"] = cfg.from_addr
    msg["To"] = ", ".join(cfg.to_addrs)
    msg.set_content(_format_summary(violations, workspace_host))

    password = os.environ.get(cfg.smtp_password_env, "")
    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=30) as smtp:
        smtp.starttls()
        if cfg.smtp_user and password:
            smtp.login(cfg.smtp_user, password)
        smtp.send_message(msg)
