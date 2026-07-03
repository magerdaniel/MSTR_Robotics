"""
mstr_robotics MCP Server

"""

import contextlib
import json
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mstrio.connection import Connection
from ruamel.yaml import YAML

from mstr_robotics._connectors import MstrApi
from mstr_robotics.dossier import DossReadOutDet
from mstr_robotics.read_out_prj_obj import ReadGen
from mstr_robotics.report import Rep as MstrRep

_mstr_api = MstrApi()

WIKIDATA_USER_AGENT = "Mstrrobotics/1.0 (mstr_robotics MCP tool; https://www.wikidata.org/wiki/User:Mstrrobotics)"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

# ---------------------------------------------------------------------------
# Configuration – override via environment variables
# ---------------------------------------------------------------------------

from mstr_robotics._paths import ENV_FILE, OSI_PRODUKTION, USER_CONFIG

with open(USER_CONFIG, "r") as openfile:
    user_d = json.load(openfile)

ENV_PATH = str(ENV_FILE)

# set user credentials and open a connection to the i-server
MSTR_USERNAME = user_d["conn_params"]["username"]
MSTR_PASSWORD = user_d["conn_params"]["password"]
MSTR_BASE_URL = user_d["conn_params"]["base_url"]

load_dotenv(ENV_PATH)

MSTR_PROJECT_ID = user_d["conn_params"]["default_project_id"]

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
    with contextlib.redirect_stdout(sys.stderr):
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
def export_report_tabular(report_id: str, project_id: str = "") -> str:
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

    i_read_gen = ReadGen()
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


@mcp.tool()
def resolve_object_by_path(
    path_str: str,
    project_id: str = "",
    top_folder_id: str = "D3C7D461F69C4610AA6BAA5EF51F4125",
) -> str:
    """Resolve a semicolon-separated MSTR folder path to an object's ID, type and subtype.

    Traverses the folder hierarchy segment by segment, starting from the
    Shared Reports root, and returns the identity of the final object so it
    can be passed to other tools (e.g. export_report_tabular,
    get_visualization_data).

    Args:
        path_str:           Folder path with segments separated by ' ; '.
                            Example:
                            "Shared Reports ; MSTR_Robotics ; Ontologies ; Regional Marketing Ofensive 2024"
        project_id:         MicroStrategy project ID. Leave empty to use the default.
        top_folder_id:      GUID of the root folder. Defaults to Shared Reports.
    """
    try:
        conn = _get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    try:
        result = ReadGen().get_obj_id_by_path(
            conn=conn,
            path_str=path_str,
            top_folder_id=top_folder_id,
        )
    except Exception as e:
        return f"ERROR – path resolution failed: {e}"

    if result is None:
        return json.dumps({"error": f"Object not found for path: {path_str}"})

    return json.dumps(result, indent=2, ensure_ascii=False, default=str)


def _bld_item_sel(item_filt_l):

    items_str = "".join(item_filt_l)  # e.g. "wd:Q58444 wd:Q832086 wd:Q2263"
    sparql_query = f"""
    SELECT ?item ?itemLabel ?propLabel ?value ?valueLabel WHERE {{
    VALUES ?item {{ {items_str} }}
    ?item ?p ?value .
    ?prop wikibase:directClaim ?p .
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }}
    """.strip()
    return sparql_query


@mcp.tool()
def query_wikidata_sparql(item_filt_l: str) -> str:
    """Execute a SPARQL query against the Wikidata Query Service and return results.

    Wikidata blocks datacenter IPs without a valid User-Agent header (HTTP 403).
    This tool sets the required header automatically using the registered
    Mstrrobotics bot account — no authentication is needed, the endpoint is public.

    Returns a JSON object with row_count and a flat list of result rows.

    Args:
        sparql_query: A valid SPARQL SELECT query, e.g.:
            SELECT ?item ?itemLabel ?prop ?propLabel ?place ?placeLabel WHERE {
              VALUES ?item { wd:Q58444 wd:Q832086 }
              VALUES ?prop { wdt:P19 wdt:P740 wdt:P937 }
              ?item ?prop ?place .
              ?place wdt:P131* wd:Q1384 .
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
            }
    """

    try:
        sparql_query = _bld_item_sel(item_filt_l)
        resp = requests.get(
            WIKIDATA_SPARQL_URL,
            params={"query": sparql_query, "format": "json"},
            headers={
                "User-Agent": WIKIDATA_USER_AGENT,
                "Accept": "application/sparql-results+json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        return f"ERROR – Wikidata HTTP {e.response.status_code}: {e}"
    except Exception as e:
        return f"ERROR – Wikidata SPARQL query failed: {e}"

    bindings = data.get("results", {}).get("bindings", [])
    rows = [{k: v.get("value", "") for k, v in b.items()} for b in bindings]
    return json.dumps({"row_count": len(rows), "results": rows}, indent=2, ensure_ascii=False)


@mcp.tool()
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
    try:
        conn = _get_conn(project_id or None)
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


# ---------------------------------------------------------------------------
# OSI dashboard selector
# ---------------------------------------------------------------------------

OSI_FOLDER = str(OSI_PRODUKTION)

ai_sys_dashbaord_prp = "Your goal is to show the audience the dashboard context. "
ai_sys_dashbaord_prp += "Using the provided files, you will extract and present relevant information. "
ai_sys_dashbaord_prp += "Overall goal is, that everything what you say, can be proved by the human colleague."


def _call_llm(sys_cont: str, msg_t: str, temperature: float = 0.1) -> str:
    """
    Call Perplexity via the OpenAI-compatible client and return the raw
    text reply.  Accesses response.choices[0].message.content directly so
    it is not affected by the json.loads(response.json()) issue in
    call_perplexity when newer SDK / Pydantic versions are in use.
    """
    from openai import OpenAI

    client = OpenAI(
        api_key=os.environ.get("PERPLEXITY_API_KEY"),
        base_url="https://api.perplexity.ai",
    )
    response = client.chat.completions.create(
        model="sonar-pro",
        messages=[
            {"role": "system", "content": sys_cont},
            {"role": "user", "content": msg_t},
        ],
        temperature=temperature,
    )
    return response.choices[0].message.content


def _load_osi_files(osi_files: list[str]) -> str:
    """
    Parse a list of OSI YAML file paths and return:
      - osi_raw    : full concatenated content of all files (passed to the LLM)
    """
    YAML()
    raw_parts = []

    for path_str in osi_files:
        p = Path(path_str)
        try:
            content = p.read_text(encoding="utf-8")
        except Exception:
            continue

        raw_parts.append(f"=== {p.name} ===\n{content}")

    osi_raw = "\n\n".join(raw_parts)
    return osi_raw


def _llm_select(user_message: str, osi_raw: str) -> list[dict]:
    """
    Ask Perplexity which candidates best match *user_message*.
    Passes the full OSI content alongside the numbered candidate list so the
    LLM has complete context for its decision.
    Returns a ranked subset (at most 3), best first.
    """

    sys_cont = (
        " You are a BI assistant. Your job is to find the best matching dashboard "
        " or report for a user question. "
        " You will receive the user question, a numbered candidate list, and the full OSI_raw files"
        f"\n\nFull OSI files:\n{osi_raw}"
        " As output I expect a JSON file, with a list of matching dashboards or reports, containing name and ID"
        " as well as the full OSI context for each match."
    )
    msg_t = f"User question: {user_message}\n\n"
    raw = _call_llm(sys_cont=sys_cont, msg_t=msg_t, temperature=0.0)
    return raw


def _load_rag_files(osi_file: str, obj_id: str) -> str:
    """
    Read the OSI YAML and load every path listed under the matching
    dashboard/semantic_model's rag_files field.
    Returns the concatenated file contents.
    """
    ry = YAML()
    try:
        with open(osi_file, encoding="utf-8") as f:
            doc = ry.load(f)
    except Exception:
        return ""

    node = None
    for db in doc.get("dashboards") or []:
        if str(db.get("name", "")) == obj_id:
            node = db
            break
    if node is None:
        for sm in doc.get("semantic_model") or []:
            if str(sm.get("name", "")) == obj_id:
                node = sm
                break
    if node is None:
        return ""

    rag_files = node.get("rag_files") or []
    parts = []
    for rf in rag_files:
        fpath = rf if isinstance(rf, str) else rf.get("path", "")
        try:
            with open(fpath, encoding="utf-8") as f:
                parts.append(f"=== {fpath} ===\n{f.read()}")
        except Exception as e:
            parts.append(f"=== {fpath} (error: {e}) ===")
    return "\n\n".join(parts)


@mcp.tool()
def find_dashboard_for_question(
    user_message: str,
    osi_folder: str = OSI_FOLDER,
) -> str:
    """
    Search all OSI files in *osi_folder* for the best matching dashboard or
    report for the user's BI question.

    Returns a JSON list of candidates.
    - Exactly 1 match  → call run_and_answer_bi_question() immediately.
    - Multiple matches → present them to the user and ask which one to use,
                         then call run_and_answer_bi_question() with their choice.

    Args:
        user_message: The user's natural-language BI question.
        osi_folder:   Folder that contains the OSI .yml files.
    """
    osi_files = [str(p) for p in Path(osi_folder).glob("*.y*ml")]
    if not osi_files:
        return json.dumps({"error": f"No OSI YAML files found in: {osi_folder}"})

    osi_raw = _load_osi_files(osi_files)
    if not osi_raw:
        return json.dumps({"error": f"No dashboards or reports found in OSI files: {osi_folder}"})

    matches = _llm_select(user_message, osi_raw)
    if not matches:
        return json.dumps(
            {
                "message": "No matching dashboard or report found for this question.",
            }
        )

    return json.dumps(matches)


@mcp.tool()
def run_and_answer_bi_question(
    user_message: str,
    object_id: str,
    object_type: str,
    osi_file_path: str,
    project_id: str = MSTR_PROJECT_ID,
) -> str:
    """
    Execute the confirmed dashboard or report, load its OSI context and any
    RAG files, then ask the LLM to answer the user's question.

    Args:
        user_message:   The original user question.
        object_id:      MicroStrategy GUID of the dashboard or report.
        object_type:    'dashboard' or 'report'.
        osi_file_path:  Full path to the OSI .yml file describing the object.
        project_id:     MicroStrategy project ID (optional, uses default if empty).
    """
    # ── 1. MSTR connection ────────────────────────────────────────────────
    try:
        conn = _get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    # ── 2. Execute report or dashboard ────────────────────────────────────
    if object_type == "report":
        data_text = export_report_tabular(report_id=object_id, project_id=project_id)
    else:
        try:
            i_det = DossReadOutDet()
            hier_l = i_det.run_read_out_doss_hier_det(conn, [object_id])
            df = pd.DataFrame(hier_l) if hier_l else None
            data_text = (
                df.to_csv(index=False)
                if df is not None and not df.empty
                else f"Dashboard {object_id} returned no hierarchy data."
            )
        except Exception as e:
            data_text = f"ERROR executing dashboard {object_id}: {e}"

    # ── 4. Load RAG files ─────────────────────────────────────────────────
    rag_content = _load_rag_files(osi_file=osi_file_path, obj_id=object_id)

    return json.dumps(
        {
            "data_text": data_text,
            "object_id": object_id,
            "object_type": object_type,
            "rag_content": rag_content,
        },
        ensure_ascii=False,
        indent=2,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="stdio")
    """
    #item_filt_l="wd:Q58444 wd:Q832086 wd:Q2263"
    oo=query_wikidata_sparql(str(item_filt_l))
    print(oo)

    print("JDJD")
    item_filt_l="wd:Q58444 wd:Q832086 wd:Q2263"
    sparql_query=_bld_item_sel(item_filt_l)
    print(sparql_query)
    sparql_query1='''SELECT ?item ?itemLabel ?propLabel ?value ?valueLabel WHERE {
                    VALUES ?item {
                        wd:Q58444 wd:Q832086 wd:Q2263
                    }
                    ?item ?p ?value .
                    ?prop wikibase:directClaim ?p .
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
                    }'''

    oo=query_wikidata_sparql(str(item_filt_l))
    print(oo)

    print("JDJD")
    osi_folder=OSI_FOLDER
    user_message="Hi, please use the mcp_server mstr_robotics and search for a dashboard that can help me in ther regional Markeeting offensive"
    ttt=find_dashboard_for_question(user_message=user_message,
                                 osi_folder=osi_folder)
    print("ttt")
    print(ttt)
    """
