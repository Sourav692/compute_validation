from __future__ import annotations

import argparse
import logging
import sys

from .runner import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="compute-validate",
        description=(
            "Validate Databricks compute (clusters, SQL warehouses, instance pools) "
            "against YAML expectations."
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
        help="Databricks CLI profile (defaults to SDK auth resolution: env vars / DEFAULT).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate checks but do not write to Delta or send alerts.",
    )
    parser.add_argument(
        "--fail-on-violation",
        action="store_true",
        help="Exit with non-zero status if any violation is found.",
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

    violations = run(args.config, profile=args.profile, dry_run=args.dry_run)

    if args.fail_on_violation and violations > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
