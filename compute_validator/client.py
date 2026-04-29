from __future__ import annotations

from databricks.sdk import WorkspaceClient


def get_client(profile: str | None = None) -> WorkspaceClient:
    """Build a WorkspaceClient using the default Databricks CLI profile.

    If `profile` is provided, that named profile is used instead. Falls back to
    standard Databricks SDK auth resolution (env vars, .databrickscfg DEFAULT).
    """
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()
