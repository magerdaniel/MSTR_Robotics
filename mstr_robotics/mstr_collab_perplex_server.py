"""
MicroStrategy Collab Perplexity MCP Server

Exposes a tool that accepts a natural-language BI question, runs the full
keyword-extraction → Perplexity RAG → prompt-answering → report-execution
pipeline, and returns the result as CSV plus a browser link.
"""
import sys

# Redirect stdout → stderr during init so MCP stdio protocol (pure JSON on
# stdout) is not polluted by connection banners or debug prints.
_real_stdout = sys.stdout
sys.stdout = sys.stderr

import json
import os
import pandas as pd
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mstrio.connection import Connection

from mstr_robotics.navigation import answer_prompts, mstr_objects
from mstr_robotics.report import rep as MstrRep, prompts
from mstr_robotics.user_rag import keyword_processor, perplexity

# ---------------------------------------------------------------------------
# Static configuration
# ---------------------------------------------------------------------------

from mstr_robotics._paths import USER_CONFIG, ENV_FILE, MCP_DATA as _MCP_DATA

CONFIG_PATH = str(USER_CONFIG)
ENV_PATH    = str(ENV_FILE)
MCP_DATA    = str(_MCP_DATA)

PROJECT_ID      = "B7CA92F04B9FAE8D941C3E9B7E0CD754"
TEMPLATE_REP_ID = "25D40AD444B6D51B333021ADFB219501"
AI_REP_NAME     = "dyn_prompt_page_botstat"
AI_REP_FOLDER   = "2F2302AE4D1C2DDDFA9CDCB46802B185"

# ---------------------------------------------------------------------------
# One-time initialisation
# ---------------------------------------------------------------------------

with open(CONFIG_PATH, "r") as fh:
    _user_d = json.load(fh)

load_dotenv(ENV_PATH)

# Load MCP data CSVs
_attribute_form_elements_df = pd.read_csv(os.path.join(MCP_DATA, "attribute_form_elements.csv"))
_attribute_elements_df      = pd.read_csv(os.path.join(MCP_DATA, "attribute_elements.csv"))
_att_form_def_df            = pd.read_csv(os.path.join(MCP_DATA, "att_form_def.csv"))
_obj_prp_rel_df             = pd.read_csv(os.path.join(MCP_DATA, "obj_prp_rel.csv"))
_dos_rep_prp_rel_df         = pd.read_csv(os.path.join(MCP_DATA, "dos_rep_prp_rel.csv"))
_dashboard_definitions_df   = pd.read_csv(os.path.join(MCP_DATA, "dashboard_definitions.csv"))
_dashboard_chapter_filter_df  = pd.read_csv(os.path.join(MCP_DATA, "dashboard_chapter_filter.csv"))
_dashboard_selector_filter_df = pd.read_csv(os.path.join(MCP_DATA, "dashboard_selector_filter.csv"))

# RAG / AI helpers
_keyword_proc = keyword_processor()
_perplexity   = perplexity()
_mstr_objects = mstr_objects()
_answer_prpts = answer_prompts(
    attribute_form_elements_df=_attribute_form_elements_df,
    attribute_elements_df=_attribute_elements_df,
    obj_prp_rel_df=_obj_prp_rel_df,
    att_form_def_df=_att_form_def_df,
    dos_rep_prp_rel_df=_dos_rep_prp_rel_df,
    dashboard_definitions_df=_dashboard_definitions_df,
    dashboard_chapter_filter_df=_dashboard_chapter_filter_df,
    dashboard_selector_filter_df=_dashboard_selector_filter_df,
)

# Keyword index – attributes and metrics
_bi_obj_df = _obj_prp_rel_df[["object_name", "obj_type", "object_id"]][
    _obj_prp_rel_df["obj_type"].isin(["attribute", "metric"])
]
_element_df_d_l = [
    {
        "df": _attribute_form_elements_df,
        "key_col": "element_val",
        "key_type": "element_val",
        "rag_cols": ["attribute_name", "form_name", "element_val"],
    },
    {
        "df": _attribute_elements_df,
        "key_col": "element_val",
        "key_type": "element_val",
        "rag_cols": ["attribute_name", "element_val"],
    },
]
_obj_df_d_l = [
    {
        "df": _bi_obj_df,
        "key_col": "object_name",
        "key_type": "object_name",
        "rag_cols": ["object_name", "obj_type"],
    }
]

# Pre-load all known keywords into the flash-text processor
_all_keywords = (
    list(_attribute_form_elements_df["element_val"].dropna().unique())
    + list(_attribute_elements_df["element_val"].dropna().unique())
    + list(_bi_obj_df["object_name"].dropna().unique())
)
_keyword_proc.load_keyword_processor(_all_keywords)

MSTR_USERNAME = _user_d["conn_params"]["username"]
MSTR_PASSWORD = _user_d["conn_params"]["password"]
MSTR_BASE_URL = _user_d["conn_params"]["base_url"]

mcp = FastMCP("mstr_collab_perplex", host="127.0.0.1", port=8001)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn() -> Connection:
    conn = Connection(
        base_url=MSTR_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_id=PROJECT_ID,
        login_mode=1,
    )
    conn.headers["Content-type"] = "application/json"
    return conn


def _report_url(report_id: str) -> str:
    base = MSTR_BASE_URL.rstrip("/")
    if base.endswith("/api"):
        base = base[:-4]
    return f"{base}/app/{PROJECT_ID}/{report_id}"


# ---------------------------------------------------------------------------
# MCP tool
# ---------------------------------------------------------------------------

@mcp.tool()
def query_bi_report(question: str) -> str:
    """Answer a natural-language BI question against MicroStrategy.

    The tool:
    1. Extracts keywords from the question using flash-text.
    2. Builds a RAG context from the MCP data (attributes, metrics, elements).
    3. Calls the Perplexity LLM (sonar-pro) to parse the question into a
       structured BI request (attributes, metrics, filters).
    4. Translates the request into MicroStrategy prompt answers.
    5. Saves a new report instance and executes it.
    6. Returns the result as CSV plus a browser link.

    Args:
        question: Natural-language BI request, e.g.
            "Show me Revenue and Profit for Year and Category,
             filter Year In 2022, 2023 and Category starts with B"
    """
    # --- Step 1: keyword extraction + RAG context ---------------------------
    key_word_l = _keyword_proc.extract_keywords(msg_t=question)

    att_elem_str = _mstr_objects.get_att_elem_str(_element_df_d_l, key_word_l=key_word_l)
    bi_obj_str   = _mstr_objects.get_att_elem_str(_obj_df_d_l, key_word_l=key_word_l)

    sys_cont = _perplexity.rag_sys_cont(
        key_word_l=key_word_l,
        att_elem_str=att_elem_str,
        bi_obj_str=bi_obj_str,
    )

    # --- Step 2: LLM call + parse -------------------------------------------
    message_check_d = {"msg_nr": "1", "msg_t": question}
    message_check_d = _perplexity.call_perplexity(
        msg_t=question,
        sys_cont=sys_cont,
        message_check_d=message_check_d,
        temperature=0.1,
    )

    if message_check_d.get("valid_d_fg") != 1:
        err = message_check_d.get("err", "unknown error")
        return f"ERROR – Perplexity call failed: {err}"

    bi_request_d = _perplexity.parse_and_structure([message_check_d])

    # If the LLM has a clarifying question, surface it
    if bi_request_d.get("question"):
        return f"Clarification needed: {bi_request_d['question']}"

    # --- Step 3: build MicroStrategy prompt answers -------------------------
    try:
        prompt_answ = _answer_prpts.AI_mstr_prp_page_ans(
            vector_store=_keyword_proc,
            bi_request_d=bi_request_d,
            rep_dos_id=TEMPLATE_REP_ID,
        )
    except Exception as e:
        return f"ERROR – building prompt answers: {e}"

    # --- Step 4: save + execute report --------------------------------------
    try:
        conn = _get_conn()
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    try:
        rep_resp = _answer_prpts.save_AI_rep(
            conn=conn,
            report_id=TEMPLATE_REP_ID,
            prompt_answ=prompt_answ,
            ai_rep_name=AI_REP_NAME,
            ai_rep_folder_id=AI_REP_FOLDER,
        )
        new_rep_id = rep_resp.json()["id"]
    except Exception as e:
        return f"ERROR – saving report instance: {e}"

    try:
        i_rep = MstrRep()
        instance_id = i_rep.open_Instance(conn=conn, report_id=new_rep_id)
        df = i_rep.report_df(conn=conn, report_id=new_rep_id, instance_id=instance_id)
    except Exception as e:
        return f"ERROR – executing report {new_rep_id}: {e}"

    if df is None or df.empty:
        return f"Report executed but returned no data. Report ID: {new_rep_id}"

    # --- Step 5: return result ----------------------------------------------
    rows, cols = df.shape
    url      = _report_url(new_rep_id)
    csv_text = df.to_csv(index=False)

    summary = (
        f"BI Request parsed:\n"
        f"  Attributes : {bi_request_d.get('attributes', [])}\n"
        f"  Metrics    : {bi_request_d.get('metrics', [])}\n"
        f"  Filters    : {json.dumps(bi_request_d.get('filter', {}), default=str)}\n\n"
        f"Report '{AI_REP_NAME}' — {rows} rows × {cols} columns\n"
        f"Open in MSTR: {url}\n\n"
        f"{csv_text}"
    )
    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Restore stdout so the MCP stdio JSON protocol can use it
    sys.stdout = _real_stdout

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="sse",
        help="MCP transport: 'stdio' for Claude Desktop subprocess mode, "
             "'sse' for HTTP/REST mode (default: sse)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    if args.transport == "sse":
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")
