"""
MicroStrategy MCP Server
Connects to MicroStrategy and exports report 89FFB2AE475653785E693DBA32A5E6F3
"""
import json
from mcp.server.fastmcp import FastMCP
from mstrio.connection import Connection
from mstr_robotics.report import rep as MstrRep
from mstr_robotics.read_out_prj_obj import read_gen
from mstr_robotics._connectors import mstr_api
from mstrio.api import reports
_mstr_api = mstr_api()

# ---------------------------------------------------------------------------
# Configuration – override via environment variables
# ---------------------------------------------------------------------------

with open('C:\\coding\\Python_environments\\mstr_robotics\\config\\user_d.json', 'r') as openfile:
    user_d = json.load(openfile)

#set user credentials and open a connection to the i-server
MSTR_USERNAME = user_d["conn_params"]["username"]
MSTR_PASSWORD = user_d["conn_params"]["password"]
MSTR_BASE_URL = user_d["conn_params"]["base_url"]


MSTR_PROJECT_ID = "B7CA92F04B9FAE8D941C3E9B7E0CD754"
#MSTR_PROJECT_ID = "B7CA92F04B9FAE8D941C3E9B7E0CD754"

#REPORT_ID = "89FFB2AE475653785E693DBA32A5E6F3"

mcp = FastMCP("mstr_robotics")

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _report_url(report_id: str, project_id: str | None = None) -> str:
    """Build a MicroStrategy Library web URL to open the report in the browser."""
    base = MSTR_BASE_URL.rstrip("/")
    if base.endswith("/api"):
        base = base[:-4]  # strip trailing /api → .../MicroStrategyLibrary
    pid = project_id or MSTR_PROJECT_ID
    return f"{base}/app/{pid}/{report_id}"


def _get_conn(project_id: str | None = None) -> Connection:
    pid = project_id or MSTR_PROJECT_ID or None
    conn = Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_id=pid,
        login_mode=1,
    )
    conn.headers["Content-type"] = "application/json"
    return conn

@mcp.tool()
def export_report_tabular(
    report_id: str,
    project_id: str = ""
) -> str:
    """Export a MicroStrategy report and return the data as CSV text.

    Args:
        report_id:  MicroStrategy report ID (required).
        project_id: MicroStrategy project ID. Leave empty to use the
                    MSTR_PROJECT_ID environment variable.
    """
    try:
        conn = _get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    try:
        i_rep = MstrRep()
        instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
        df = i_rep.rep_to_dataframe(conn=conn, report_id=report_id, instance_id=instance_id)
    except Exception as e:
        return f"ERROR – could not execute or read report {report_id}: {e}"

    if df is None or df.empty:
        return f"Report {report_id} returned no data."


    rows, cols = df.shape
    csv_text = df.to_csv(index=False)
    url = _report_url(report_id, project_id or None)
    return f"Report {report_id} — {rows} rows × {cols} columns\nVerify in MSTR Library: {url}\n\n{csv_text}"


@mcp.tool()
def get_object_definitions(
    guid_list: list[str],
    project_id: str = "",
) -> str:
    """Fetch MicroStrategy object definitions for a list of GUIDs.

    For each GUID the tool resolves type and subtype via the metadata search
    API and then fetches the full object definition.

    Args:
        guid_list:  List of MicroStrategy object GUIDs.
        project_id: MicroStrategy project ID. Leave empty to use the default.
    """
    try:
        conn = _get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    i_read_gen = read_gen()
    try:
        obj_def_l = i_read_gen.get_proj_obj_def_by_id_l(conn=conn, obj_id_l=guid_list)
    except Exception as e:
        return f"ERROR – could not fetch object definitions: {e}"

    result = {
        "definitions": obj_def_l,
        "errors": i_read_gen.obj_read_error_d_l,
        "not_mapped": i_read_gen.obj_not_mapped_d_l,
        "summary": {
            "requested": len(guid_list),
            "fetched": len(obj_def_l),
            "errors": len(i_read_gen.obj_read_error_d_l),
            "not_mapped": len(i_read_gen.obj_not_mapped_d_l),
        },
    }
    return json.dumps(result, indent=2, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="stdio")
