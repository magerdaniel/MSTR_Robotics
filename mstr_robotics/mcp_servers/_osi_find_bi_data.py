"""Tool group *find BI data* for the mstr_osi_mcp server.

Finds the dashboard or report relevant for a user's BI question:
  - find_dashboard_for_question — LLM search over the OSI files

Data extraction from the found objects lives in _fetch_static_data.py.
"""

import json
import os
from pathlib import Path

from mstr_robotics._paths import OSI_DIR

OSI_FOLDER = str(OSI_DIR)


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
    """Concatenate the raw content of all OSI YAML files (passed to the LLM)."""
    raw_parts = []

    for path_str in osi_files:
        p = Path(path_str)
        try:
            content = p.read_text(encoding="utf-8")
        except Exception:
            continue

        raw_parts.append(f"=== {p.name} ===\n{content}")

    return "\n\n".join(raw_parts)


def _llm_select(user_message: str, osi_raw: str) -> str:
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
    return _call_llm(sys_cont=sys_cont, msg_t=msg_t, temperature=0.0)


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
