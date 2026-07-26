"""Shared runtime configuration for the mstr_robotics MCP servers.

Loads the MicroStrategy credentials from USER_CONFIG once at import time and
exposes connection/URL helpers bound to that configuration. The MCP server
tool modules import from here instead of reading the config themselves.
"""

import contextlib
import sys

import yaml
from dotenv import load_dotenv
from mstrio import config as mstrio_config
from mstrio.connection import Connection

from mstr_robotics._helper import StrFunc
from mstr_robotics._paths import ENV_FILE, USER_CONFIG

i_str_func = StrFunc()

# mstrio emits its "Connection established / Project object named / _Cube object
# named" banners through a logging StreamHandler bound to stdout at import time,
# which contextlib.redirect_stdout(sys.stderr) cannot intercept. On a stdio MCP
# server those lines land on stdout and corrupt the JSON-RPC stream ("Unexpected
# token 'C', Connection... is not valid JSON"). Silence them at the source; this
# module is imported before any MSTR access so it applies process-wide.
mstrio_config.verbose = False
mstrio_config.progress_bar = False

try:
    with open(USER_CONFIG, "r") as openfile:
        user_d = yaml.safe_load(openfile)
except FileNotFoundError as exc:
    raise RuntimeError(
        f"MicroStrategy configuration not found at {USER_CONFIG}.\n"
        "Run the setup notebook (mstr_robotics/notebooks/00_setup.ipynb), or copy the template by hand:\n"
        f"    cp {USER_CONFIG.parent / 'user_d.example.yml'} {USER_CONFIG}\n"
        "then fill in base_url, username, password and project_id."
    ) from exc

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
    """Build a classic MicroStrategy Web (servlet/mstrWeb) URL for a report.

    mstrWeb keys on the project *name* rather than the GUID, so the shared
    connection is used to resolve both the project name and the i-server host
    from the base URL.
    """
    conn = get_conn(project_id)
    web_base = i_str_func.web_base_url(conn.base_url)
    server = i_str_func.get_server_base_url(conn.base_url)
    project_name = i_str_func.get_project_name_base_url(conn.project_name)
    return (
        f"{web_base}Server={server}&Project={project_name}"
        f"&evt=4001&src=mstrWeb.4001&reportViewMode=1"
        f"&reportID={report_id}&currentViewMedia=2"
    )
