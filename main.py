"""Local CLI entry point. Delegates to the packaged console script.

Run as `python main.py ...` for local development. In production / Databricks
Jobs the wheel is invoked via the `compute-validate` console script defined
in pyproject.toml.
"""
from __future__ import annotations

import sys

from compute_validator.cli import main


if __name__ == "__main__":
    sys.exit(main())
