"""Tool group *specific analysis* for the mstr_osi_mcp server.

Tools that answer a concrete analytical question:
  - run_and_answer_bi_question — execute the confirmed object with OSI/RAG context
  - query_wikidata_sparql      — enrich an analysis with Wikidata facts
"""

import json

import pandas as pd
import requests
from ruamel.yaml import YAML

from mstr_robotics.dossier import DossReadOutDet
from mstr_robotics.mcp_servers._server_config import MSTR_PROJECT_ID, get_conn
from mstr_robotics.mcp_servers._fetch_static_data import export_report_tabular

WIKIDATA_USER_AGENT = "Mstrrobotics/1.0 (mstr_robotics MCP tool; https://www.wikidata.org/wiki/User:Mstrrobotics)"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"


def _bld_item_sel(item_filt_l: str) -> str:
    """Build a SPARQL query that fetches all direct claims of the given items."""
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


def query_wikidata_sparql(item_filt_l: str) -> str:
    """Fetch all direct claims for a list of Wikidata items via the Query Service.

    Builds a SPARQL SELECT query from the given item list and executes it
    against the public Wikidata Query Service.

    Wikidata blocks datacenter IPs without a valid User-Agent header (HTTP 403).
    This tool sets the required header automatically using the registered
    Mstrrobotics bot account — no authentication is needed, the endpoint is public.

    Returns a JSON object with row_count and a flat list of result rows.

    Args:
        item_filt_l: Space-separated Wikidata item IDs with the wd: prefix,
            e.g. "wd:Q58444 wd:Q832086 wd:Q2263".
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
        conn = get_conn(project_id or None)
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

    # ── 3. Load RAG files ─────────────────────────────────────────────────
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
