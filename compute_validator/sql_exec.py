from __future__ import annotations

import logging
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


log = logging.getLogger(__name__)


def execute(w: WorkspaceClient, warehouse_id: str, sql: str, wait_seconds: int = 300) -> Any:
    """Run a SQL statement on the given warehouse and return the final response.

    Polls until terminal state. Raises on failure or timeout.
    """
    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    statement_id = resp.statement_id
    state = resp.status.state if resp.status else None
    deadline = time.time() + wait_seconds
    while state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            raise TimeoutError(f"Statement {statement_id} timed out after {wait_seconds}s")
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        err = resp.status.error.message if (resp.status and resp.status.error) else "unknown error"
        raise RuntimeError(f"Statement {statement_id} ended in state {state}: {err}")
    return resp


def fetch_rows(w: WorkspaceClient, warehouse_id: str, sql: str) -> list[dict[str, Any]]:
    """Execute SELECT and return list of dicts keyed by column name.

    Iterates result chunks if the response is paginated.
    """
    resp = execute(w, warehouse_id, sql)
    manifest = resp.manifest
    if not manifest or not manifest.schema:
        return []
    columns = [c.name for c in (manifest.schema.columns or [])]

    rows: list[dict[str, Any]] = []
    result = resp.result
    chunk_index = 0
    while result is not None:
        for raw_row in (result.data_array or []):
            rows.append({columns[i]: raw_row[i] for i in range(len(columns))})
        next_chunk = result.next_chunk_index
        if next_chunk is None:
            break
        chunk_index = next_chunk
        log.debug("    fetching result chunk %d", chunk_index)
        result = w.statement_execution.get_statement_result_chunk_n(
            resp.statement_id, chunk_index
        )
    return rows
