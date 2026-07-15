"""Shared runtime configuration for the mstr_robotics MCP servers.

Loads the MicroStrategy credentials from USER_CONFIG once at import time and
exposes connection/URL helpers bound to that configuration. The MCP server
tool modules import from here instead of reading the config themselves.
"""

import contextlib
import json
import sys

from dotenv import load_dotenv
from mstrio.connection import Connection

from mstr_robotics._paths import ENV_FILE, USER_CONFIG

with open(USER_CONFIG, "r") as openfile:
    user_d = json.load(openfile)

load_dotenv(str(ENV_FILE))

MSTR_USERNAME = user_d["conn_params"]["username"]
MSTR_PASSWORD = user_d["conn_params"]["password"]
MSTR_BASE_URL = user_d["conn_params"]["base_url"]
MSTR_PROJECT_ID = user_d["conn_params"]["project_id"]


# One shared connection per project, opened lazily and reused for the whole
# server session (Claude Desktop keeps the server process alive).
_conn_cache: dict[str, Connection] = {}


def _open_conn(project_id: str | None, login_mode: int) -> Connection:
    conn = Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_id=project_id,
        login_mode=login_mode,
    )
    conn.headers["Content-type"] = "application/json"
    return conn


def _session_alive(conn: Connection) -> bool:
    """Check the cached session; try to renew it if the token went stale."""
    try:
        if conn.status():
            return True
        conn.renew()
        return conn.status()
    except Exception:
        return False


def get_conn(project_id: str | None = None, login_mode: int = 1) -> Connection:
    """Return a shared, session-managed MicroStrategy connection.

    The first call per project logs in; afterwards the same Connection is
    reused. Before handing it out the session is validated with a cheap
    status() call and renewed — or fully re-opened — when the i-server has
    timed it out, so tools never see an expired session.

    stdout is redirected to stderr during MSTR access so mstrio banners never
    pollute the MCP stdio JSON protocol (pure JSON on stdout).
    """
    pid = project_id or MSTR_PROJECT_ID or ""
    with contextlib.redirect_stdout(sys.stderr):
        conn = _conn_cache.get(pid)
        if conn is not None and _session_alive(conn):
            return conn
        conn = _open_conn(pid or None, login_mode)
        _conn_cache[pid] = conn
        return conn


def get_report_url(report_id: str, project_id: str | None = None) -> str:
    """Build a MicroStrategy Library web URL to open a report in the browser."""
    base = MSTR_BASE_URL.rstrip("/")
    if base.endswith("/api"):
        base = base[:-4]  # strip trailing /api → .../MicroStrategyLibrary
    return f"{base}/app/{project_id or MSTR_PROJECT_ID}/{report_id}"
