"""Execute a single notebook in-process and report the shape of every DataFrame.

Run ONCE PER ENVIRONMENT, by that environment's own venv python, e.g.:

    <repo>/.venv/Scripts/python.exe run_shapes.py \
        --notebook semantic_endpoints.ipynb \
        --repo C:/coding/Python_environments/mstr_robotics \
        --out report.json

The notebook's code cells are concatenated and exec()'d with the working
directory set to <repo>/notebooks, so the notebooks' relative config paths
(e.g. ..\\config\\user_d.yml) resolve to THIS environment's config. After the
last cell runs, every pandas.DataFrame left in the namespace is discovered by
runtime type (names are inconsistent across notebooks) and its (rows, cols)
recorded.

Only stdlib + nbformat + pandas are required (already installed in both venvs).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback

import nbformat
import pandas as pd


def _strip_magics(source: str) -> str:
    """Drop Jupyter magic / shell lines so plain exec() does not choke on them.

    The target notebooks use none of these, but this keeps the collector robust
    if a `%magic` or `!shell` line ever appears. Skipped lines are warned about.
    """
    kept = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%") or stripped.startswith("!"):
            sys.stderr.write(f"[run_shapes] skipping non-Python line: {line}\n")
            continue
        kept.append(line)
    return "\n".join(kept)


def _build_code(nb) -> str:
    parts = []
    for cell in nb.cells:
        if cell.cell_type == "code":
            parts.append(_strip_magics(cell.source))
    return "\n\n".join(parts)


def collect_dataframes(notebook_path: str, notebooks_dir: str) -> dict:
    """Execute the notebook and return {var_name: {rows, cols, columns}}."""
    nb = nbformat.read(notebook_path, as_version=4)
    code = _build_code(nb)

    # Run as if it were the top-level notebook: cwd = notebooks/, __name__ main,
    # and harmless stubs for interactive-only names.
    ns = {
        "__name__": "__main__",
        "get_ipython": lambda *a, **k: None,
        "display": lambda *a, **k: None,
    }
    prev_cwd = os.getcwd()
    os.chdir(notebooks_dir)
    try:
        exec(compile(code, notebook_path, "exec"), ns)
    finally:
        os.chdir(prev_cwd)

    frames = {}
    for name, value in ns.items():
        if isinstance(value, pd.DataFrame):
            frames[name] = {
                "rows": int(value.shape[0]),
                "cols": int(value.shape[1]),
                "columns": [str(c) for c in value.columns],
            }
    return frames


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--notebook", required=True, help="notebook file name (under <repo>/notebooks)")
    ap.add_argument("--repo", required=True, help="environment repo root")
    ap.add_argument("--out", required=True, help="path to write the JSON report")
    args = ap.parse_args()

    notebooks_dir = os.path.join(args.repo, "notebooks")
    notebook_path = os.path.join(notebooks_dir, args.notebook)
    if not os.path.isfile(notebook_path):
        sys.stderr.write(f"[run_shapes] notebook not found: {notebook_path}\n")
        return 2

    report = {
        "notebook": args.notebook,
        "repo": args.repo,
        "python": sys.executable,
        "ok": True,
        "error": None,
        "dataframes": {},
    }
    try:
        report["dataframes"] = collect_dataframes(notebook_path, notebooks_dir)
    except Exception:
        report["ok"] = False
        report["error"] = traceback.format_exc()

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    if not report["ok"]:
        sys.stderr.write(f"[run_shapes] execution failed for {args.notebook}:\n{report['error']}\n")
        return 1
    sys.stderr.write(
        f"[run_shapes] {args.notebook}: {len(report['dataframes'])} DataFrame(s) found\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
