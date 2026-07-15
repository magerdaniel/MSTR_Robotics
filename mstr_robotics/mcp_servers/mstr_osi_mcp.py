"""
mstr_robotics MCP Server

Entry point only: creates the FastMCP instance and registers the tools,
grouped by purpose. The implementations live in one module per group:

  find BI data       -> _osi_find_bi_data.py
  fetch BI data      -> _fetch_static_data.py, _run_mstr_report_wizzard.py
  specific analysis  -> _osi_specific_analysis.py
  object analysis    -> _osi_object_analysis.py

Importing _run_mstr_report_wizzard loads the RAG cubes and keyword index once
at server start, so expect a few seconds of startup time.

Shared configuration (credentials, default project, connection helpers)
lives in _server_config.py.
"""

from mcp.server.fastmcp import FastMCP

from mstr_robotics.mcp_servers._fetch_static_data import (
    export_report_tabular,
    get_visualization_data,
)
from mstr_robotics.mcp_servers._osi_find_bi_data import find_dashboard_for_question
from mstr_robotics.mcp_servers._osi_object_analysis import (
    get_object_definitions,
    resolve_object_by_path,
)
from mstr_robotics.mcp_servers._run_mstr_report_wizzard import query_bi_report
from mstr_robotics.mcp_servers._osi_specific_analysis import (
    query_wikidata_sparql,
    run_and_answer_bi_question,
)

mcp = FastMCP("mstr_robotics")

# ── find BI data ──────────────────────────────────────────────────────────
mcp.tool()(find_dashboard_for_question)

# ── fetch BI data ──────────────────────────────────────────────────────────

mcp.tool()(export_report_tabular)
mcp.tool()(get_visualization_data)
mcp.tool()(query_bi_report)


# ── specific analysis ─────────────────────────────────────────────────────
mcp.tool()(run_and_answer_bi_question)
mcp.tool()(query_wikidata_sparql)

# ── object analysis ───────────────────────────────────────────────────────
mcp.tool()(get_object_definitions)
mcp.tool()(resolve_object_by_path)

if __name__ == "__main__":
    mcp.run(transport="stdio")
