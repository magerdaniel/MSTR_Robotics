"""Compare the DataFrames a notebook produces across two environments.

You pass the notebooks to INCLUDE (opt-in safelist) -- anything you do not name
is never executed, so destructive notebooks stay untouched:

    .venv/Scripts/python tools/nb_compare/compare_envs.py \
        semantic_endpoints.ipynb jup_schema_exporter.ipynb

For each notebook, the collector (run_shapes.py) is launched once per
environment under THAT environment's own venv python (see envs.yml). The
results are merged into one flat table:

    environment, notebook, dataframe, rows, columns

written to CSV (--out, default shape_report.csv) and printed, followed by a
per-(notebook, dataframe) identical / differs summary.

WARNING: this runs each named notebook for real against BOTH live MSTR systems.
Only pass read-only notebooks. Keep write-side-effect notebooks off the list
(jup_migrate, load_rag_cubes, jup_osi_file_generator) unless you intend it.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
RUN_SHAPES = os.path.join(HERE, "run_shapes.py")
DEFAULT_ENVS = os.path.join(HERE, "envs.yml")


def load_envs(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    envs = [data[k] for k in sorted(data)]
    for env in envs:
        for key in ("name", "root", "python"):
            if key not in env:
                raise SystemExit(f"env config {path}: missing '{key}' in {env}")
    return envs


def run_one(env: dict, notebook: str) -> dict:
    """Run a notebook under one environment, return its report dict."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        out_path = tmp.name
    try:
        proc = subprocess.run(
            [env["python"], RUN_SHAPES,
             "--notebook", notebook,
             "--repo", env["root"],
             "--out", out_path],
            capture_output=True, text=True, encoding="utf-8",
            env={**os.environ, "PYTHONUTF8": "1"},
        )
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        # run_shapes.py exits WITHOUT writing a report when the notebook is
        # missing (rc 2) or crashes before the write, leaving an empty temp
        # file. Treat any unreadable/empty report as a failure rather than
        # letting json.load raise and abort the whole comparison.
        try:
            with open(out_path, "r", encoding="utf-8") as fh:
                report = json.load(fh)
        except (OSError, json.JSONDecodeError):
            report = {
                "notebook": notebook,
                "repo": env["root"],
                "ok": False,
                "error": (proc.stderr or "").strip()
                or f"run_shapes.py wrote no report (returncode {proc.returncode})",
                "dataframes": {},
            }
        report["returncode"] = proc.returncode
        return report
    finally:
        try:
            os.remove(out_path)
        except OSError:
            pass


def main(notebooks: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--envs", default=DEFAULT_ENVS, help="environments config (default: envs.yml)")
    ap.add_argument("--out", default="shape_report.csv", help="CSV output path")
    args = ap.parse_args()

    envs = load_envs(args.envs)

    rows = []          # flat table rows
    failures = []      # (env, notebook) that did not execute cleanly
    # results[(notebook, dataframe)][env_name] = (rows, cols)
    results: dict = {}

    for notebook in notebooks:
        print("*******************************")
        print(notebook)
        print("*******************************")
        for env in envs:
            report = run_one(env, notebook)
            if not report.get("ok"):
                failures.append((env["name"], notebook))
                continue
            for df_name, info in report["dataframes"].items():
                rows.append({
                    "environment": env["name"],
                    "notebook": notebook,
                    "dataframe": df_name,
                    "rows": info["rows"],
                    "columns": info["cols"],
                })
                results.setdefault((notebook, df_name), {})[env["name"]] = (
                    info["rows"], info["cols"])

    # Write CSV
    with open(args.out, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["environment", "notebook", "dataframe", "rows", "columns"])
        writer.writeheader()
        writer.writerows(rows)

    # Print the flat table
    print(f"\n{'environment':<12} {'notebook':<32} {'dataframe':<28} {'rows':>8} {'columns':>8}")
    print("-" * 92)
    for r in rows:
        print(f"{r['environment']:<12} {r['notebook']:<32} {r['dataframe']:<28} "
              f"{r['rows']:>8} {r['columns']:>8}")

    # Per-(notebook, dataframe) identical / differs summary
    env_names = [e["name"] for e in envs]
    print(f"\nComparison ({env_names[0]} vs {env_names[1] if len(env_names) > 1 else '?'}):")
    print("-" * 92)
    mismatches = 0
    for (notebook, df_name) in sorted(results):
        per_env = results[(notebook, df_name)]
        values = set(per_env.get(name) for name in env_names)
        present_in_all = all(name in per_env for name in env_names)
        identical = present_in_all and len(values) == 1
        if identical:
            status = "OK   identical"
        elif not present_in_all:
            missing = [n for n in env_names if n not in per_env]
            status = f"MISS missing in {', '.join(missing)}"
            mismatches += 1
        else:
            detail = ", ".join(f"{n}={per_env.get(n)}" for n in env_names)
            status = f"DIFF {detail}"
            mismatches += 1
        print(f"  [{status}]  {notebook} :: {df_name}")

    print("-" * 92)
    print(f"CSV written to: {args.out}")
    if failures:
        print(f"\n{len(failures)} notebook execution(s) FAILED (see stderr above):")
        for env_name, notebook in failures:
            print(f"  - {env_name}: {notebook}")
    if mismatches:
        print(f"\n{mismatches} DataFrame(s) differ between environments.")
    elif not failures:
        print("\nAll DataFrames identical across environments.")

    return 1 if (mismatches or failures) else 0


if __name__ == "__main__":
    # Notebooks to INCLUDE (opt-in safelist). Read-only notebooks only --
    # each one runs for real against BOTH live MSTR systems. Keep
    # write-side-effect notebooks (jup_migrate, load_rag_cubes,
    # jup_osi_file_generator) off this list unless you intend it.
    NOTEBOOKS = [

          "jup_chat_answer_prompt_page.ipynb",
          "jup_migrate.ipynb",
          "jup_prj_obj_exporter.ipynb",
          "jup_REGAM.ipynb",
          "jup_schema_monitor.ipynb",
          "load_rag_cubes.ipynb",
          "mstr_admin.ipynb"
    ]
    print("EEEE")
    #raise SystemExit(main(NOTEBOOKS))
    main(NOTEBOOKS)
    print("RRWRWE")
