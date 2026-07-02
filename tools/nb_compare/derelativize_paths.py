"""One-shot refactor: replace hardcoded absolute paths in notebooks with
imports from mstr_robotics._paths (repo-relative resolution).

Only paths that point INTO the repo or its known siblings (OSI_Files,
python_io) are rewritten. Genuinely machine/user-specific paths
(C:\\Users\\danie\\..., D:\\shared_drive\\<personal data>) are left untouched.

Usage:
    python derelativize_paths.py <repo_root> [--apply]

Without --apply it reports matches only (dry run).
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys

NAMES = ["REPO_ROOT", "USER_CONFIG", "OSI_FILES", "OSI_SCHEMA",
         "OSI_DASHBOARD_CONTEXT", "MCP_DATA", "PYTHON_IO"]
IMPORT_LINE = "from mstr_robotics._paths import " + ", ".join(NAMES) + "\n"

# (literal as it appears in cell source text) -> replacement expression
REPLACEMENTS = [
    # --- repo config (both the C: copy and the D:\shared_drive copy) ---
    (r"'C:\\coding\\Python_environments\\mstr_robotics\\config\\user_d.json'", "USER_CONFIG"),
    (r'"C:\\coding\\Python_environments\\mstr_robotics\\config\\user_d.json"', "USER_CONFIG"),
    (r"'D:\\shared_drive\\Python\\mstr_robotics\\mstr_robotics\\user_d.json'", "USER_CONFIG"),
    (r'"D:\\shared_drive\\Python\\mstr_robotics\\mstr_robotics\\user_d.json"', "USER_CONFIG"),
    # --- sibling OSI_Files repo ---
    (r"'C:\\coding\\Python_environments\\OSI_Files\\osi-schema-with-dashboards.json'", "OSI_SCHEMA"),
    (r'["C:\\coding\\Python_environments\\OSI_Files\\Dashboard_Context\\*"]', '[str(OSI_DASHBOARD_CONTEXT / "*")]'),
    (r'"C:\\coding\\Python_environments\\OSI_Files\\mstr\\db_enums\\db_data_type_form_type.yaml"',
     'str(OSI_FILES / "mstr" / "db_enums" / "db_data_type_form_type.yaml")'),
    (r'"C:\\coding\\Python_environments\\OSI_Files"', "str(OSI_FILES)"),
    (r'r"C:\coding\Python_environments\OSI_Files\osi-schema-with-dashboards.json"', "str(OSI_SCHEMA)"),
    (r'rf"C:\coding\Python_environments\OSI_Files\osi_dashboard_{doss_id}.yml"',
     'str(OSI_FILES / f"osi_dashboard_{doss_id}.yml")'),
    # --- repo self-references (raw strings) ---
    (r'r"C:\coding\Python_environments\mstr_robotics\import_files\update_zipcode_ontology.sql"',
     'str(REPO_ROOT / "import_files" / "update_zipcode_ontology.sql")'),
    (r"r'C:\coding\Python_environments\mstr_robotics\notebooks_dev'",
     'str(REPO_ROOT / "notebooks_dev")'),
    # --- shared external output area (python_io) ---
    (r'"C:\\coding\\python_io\\output_files\\MCP_data"', "str(MCP_DATA)"),
    (r"r'C:\coding\python_io\output_files\SML\models\model.md'",
     'str(PYTHON_IO / "output_files" / "SML" / "models" / "model.md")'),
]

NAME_RE = re.compile(r"\b(" + "|".join(NAMES) + r")\b")


def process_notebook(path: str, apply: bool) -> int:
    with open(path, "r", encoding="utf-8") as fh:
        nb = json.load(fh)
    total = 0
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        new = src
        for literal, repl in REPLACEMENTS:
            if literal in new:
                cnt = new.count(literal)
                total += cnt
                print(f"  {os.path.basename(path)}: {cnt}x  {literal[:60]} -> {repl}")
                new = new.replace(literal, repl)
        if new != src:
            # ensure the import is present in this cell
            if "from mstr_robotics._paths import" not in new and NAME_RE.search(new):
                new = IMPORT_LINE + new
            cell["source"] = new.splitlines(keepends=True)
    if total and apply:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(nb, fh, ensure_ascii=False, indent=1)
            fh.write("\n")
    return total


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    repo = sys.argv[1]
    apply = "--apply" in sys.argv[2:]
    grand = 0
    for folder in ("notebooks", "notebooks_dev"):
        for nb in sorted(glob.glob(os.path.join(repo, folder, "*.ipynb"))):
            grand += process_notebook(nb, apply)
    print(f"\n{'APPLIED' if apply else 'DRY RUN'} -- {grand} replacement(s) total in {repo}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
