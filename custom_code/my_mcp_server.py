"""My MCP server — the full mstr_robotics MCP server, plus room for own tools.

Reuses the FastMCP instance from ``mstr_robotics.mcp_servers.mstr_osi_mcp``, so
all eight shipped tools are available unchanged:

    find_dashboard_for_question   resolve_object_by_path
    get_object_definitions        query_bi_report
    export_report_tabular         get_visualization_data
    run_and_answer_bi_question    query_wikidata_sparql

Own tools are registered below with ``@mcp.tool()`` and show up next to them.

Run it:
    python -m custom_code.my_mcp_server        # from the project root
    python custom_code/my_mcp_server.py        # works too, see sys.path fix

Claude Desktop launches this file directly (stdio transport) — see
``config/claude_desktop_config.json`` in this repo for the entry to paste into
%APPDATA%\\Claude\\claude_desktop_config.json.

Two things matter for a stdio server and are handled here:

1. ``MSTR_REPO_ROOT`` — mstr_robotics resolves ``config/user_d.yml`` by walking
   up from the *current working directory*. Claude Desktop starts the process
   with its own cwd, so the project root is pinned explicitly before importing
   anything from the package.
2. **stdout must stay pure JSON-RPC.** Never ``print()`` here; use ``logging``
   (configured onto stderr below) or the ``ctx`` logging helpers inside a tool.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# --- 1. pin the project root BEFORE importing mstr_robotics -----------------
# This file lives at <project>/custom_code/my_mcp_server.py.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("MSTR_REPO_ROOT", str(PROJECT_ROOT))

# Allows `python custom_code/my_mcp_server.py` in addition to `-m`.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- 2. logging to stderr; stdout belongs to the MCP protocol ---------------
logging.basicConfig(
    level=os.environ.get("MSTR_MCP_LOGLEVEL", "INFO"),
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("my_mcp_server")

# --- 3. import the ready-made server ----------------------------------------
# Importing this module opens no connection, but it does load the RAG cubes and
# build the keyword index (a few seconds) — that is why Claude Desktop may take
# a moment before the tools appear.
from mstr_robotics.mcp_servers.mstr_osi_mcp import mcp  # noqa: E402

log.info("mstr_robotics MCP tools loaded (repo root: %s)", PROJECT_ROOT)


# --- 4. own tools ------------------------------------------------------------
# Delete or extend. Every function registered here is offered to Claude next to
# the eight shipped tools. The docstring is what Claude reads to decide when to
# call it, so describe *when to use it*, not how it is implemented.

@mcp.tool()
def mstr_environment_info() -> dict[str, str]:
    """Report which MicroStrategy environment and project this server talks to.

    Use this to confirm the target environment before running any tool that
    reads or writes BI objects, or when an answer looks like it came from the
    wrong project.
    """
    from mstr_robotics.mcp_servers import _server_config as cfg

    conn = cfg.get_conn()
    return {
        "base_url": cfg.MSTR_BASE_URL,
        "username": cfg.MSTR_USERNAME,
        "project_id": cfg.MSTR_PROJECT_ID,
        "project_name": conn.project_name or "",
        "repo_root": str(PROJECT_ROOT),
    }


if __name__ == "__main__":
    # stdio is the transport Claude Desktop speaks: requests on stdin,
    # responses on stdout, everything else on stderr.
    mcp.run(transport="stdio")
