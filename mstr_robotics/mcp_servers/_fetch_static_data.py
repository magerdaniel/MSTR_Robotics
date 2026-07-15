"""Tool group *fetch BI data* for the mstr_osi_mcp server.

Tools that extract data from static (already existing) reports and dashboards:
  - export_report_tabular  — run a report and return its data as CSV
  - get_visualization_data — read a single dossier visualization
"""

import contextlib
import json
import sys

from mstr_robotics._connectors import MstrApi
from mstr_robotics.dossier import DossReadOutDet
from mstr_robotics.mcp_servers._server_config import get_conn, get_report_url
from mstr_robotics.report import Rep as MstrRep

_mstr_api = MstrApi()


def export_report_tabular(report_id: str, project_id: str = "") -> str:
    """Export a MicroStrategy report and return the data as CSV text.

    Prompted reports cannot be exported directly — the tool detects them and
    points to query_bi_report instead.

    Args:
        report_id:  MicroStrategy report ID (required).
        project_id: MicroStrategy project ID. Leave empty to use the
                    MSTR_PROJECT_ID environment variable.
    """
    # stdout → stderr so mstrio banners never pollute the MCP stdio protocol
    with contextlib.redirect_stdout(sys.stderr):
        try:
            conn = get_conn(project_id or None)
        except Exception as e:
            return f"ERROR – could not connect to MicroStrategy: {e}"

        i_rep = MstrRep()
        try:
            prompt_l = i_rep.get_rep_prp_l(conn=conn, report_id=report_id)
        except Exception:
            prompt_l = []
        if prompt_l:
            return (
                f"Report {report_id} requires {len(prompt_l)} prompt answer(s) and cannot be "
                f"exported directly. Use query_bi_report to run a prompted request instead."
            )

        try:
            instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
            df = i_rep.report_df(conn=conn, report_id=report_id, instance_id=instance_id)
        except Exception as e:
            return f"ERROR – could not execute or read report {report_id}: {e}"

        if df is None or df.empty:
            return f"Report {report_id} returned no data."

        rows, cols = df.shape
        csv_text = df.to_csv(index=False)
        url = get_report_url(report_id, project_id or None)
        return f"Report {report_id} — {rows} rows × {cols} columns\nVerify in MSTR Library: {url}\n\n{csv_text}"


def get_visualization_data(
    dossier_id: str,
    chapter_key: str,
    visualization_key: str,
    project_id: str = "",
) -> str:
    """Extract the definition and data of a single visualization from a MicroStrategy dossier.

    Opens a dossier instance, fetches the visualization's full definition
    (rows, columns, metrics, attributes) together with its current data, and
    returns everything as JSON.

    Args:
        dossier_id:        MicroStrategy dossier (dashboard) GUID.
        chapter_key:       Key of the chapter that contains the visualization
                           (e.g. 'K36').  Use get_object_definitions or
                           export_report_tabular to discover chapter keys first.
        visualization_key: Key of the visualization inside the chapter
                           (e.g. 'W54').
        project_id:        MicroStrategy project ID. Leave empty to use the
                           default project.
    """
    with contextlib.redirect_stdout(sys.stderr):
        try:
            conn = get_conn(project_id or None)
        except Exception as e:
            return f"ERROR – could not connect to MicroStrategy: {e}"

        i_det = DossReadOutDet()
        try:
            instance_id = _mstr_api.create_dossier_instance(conn, dossier_id)
        except Exception as e:
            return f"ERROR – could not create dossier instance for {dossier_id}: {e}"

        try:
            data = i_det.get_definition_and_results_of_visualization(
                conn=conn,
                dossier_id=dossier_id,
                instance_id=instance_id,
                chapter_key=chapter_key,
                vis_key=visualization_key,
            )
        except Exception as e:
            return f"ERROR – could not fetch visualization {visualization_key}: {e}"

        return json.dumps(data, indent=2, ensure_ascii=False, default=str)
