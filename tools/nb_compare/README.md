# nb_compare — cross-environment DataFrame shape comparison

Prove that two environments produce **identical** results by listing the shape of
every pandas DataFrame each notebook produces, in each environment.

## Environments

Defined in [`envs.yml`](envs.yml):

| Env  | Repo root                                       | Python                          |
|------|-------------------------------------------------|---------------------------------|
| env1 | `C:\coding\Python_environments\mstr_robotics`      | `.venv\Scripts\python.exe`      |
| env2 | `C:\coding\Python_environments\mstr_robotics_comp` | `.venv\Scripts\python.exe`      |

Each notebook is executed by **that environment's own venv python**, with the working
directory set to `<root>\notebooks`, so each notebook reads its own
`..\config\user_d.json`. The environment is selected purely by which repo/venv runs
the notebook — the notebooks themselves are never modified.

## Usage

You **opt in** to the notebooks to include — anything you do not name is never executed:

```powershell
.venv\Scripts\python tools\nb_compare\compare_envs.py semantic_endpoints.ipynb jup_schema_exporter.ipynb
```

`compare_envs.py` (the orchestrator) can run under either env's venv; it only spawns
subprocesses and merges their output.

### Output

A flat CSV + console table (`--out`, default `shape_report.csv`):

```
environment, notebook,                 dataframe,      rows, columns
env1,        semantic_endpoints.ipynb, metric_d_l_df,  142,  6
env2,        semantic_endpoints.ipynb, metric_d_l_df,  142,  6
...
```

followed by a per-`(notebook, dataframe)` identical / differs summary. Exit code is
non-zero if any DataFrame differs or any notebook fails to execute.

## Pieces

- `run_shapes.py` — runs ONE notebook in-process under one env's python and reports
  `{dataframe: {rows, cols, columns}}` for every DataFrame in the namespace (discovered
  by runtime type, since names are inconsistent across notebooks). Lives here only; env2's
  python runs this same file with `--repo` pointing at env2.
- `compare_envs.py` — orchestrator + table/CSV + comparison summary.
- `envs.yml` — the two environments.

## ⚠️ Safety

Each named notebook is executed **for real against BOTH live MSTR systems**. Only pass
**read-only** notebooks. Keep write-side-effect notebooks off the list unless you truly
intend to run them:

- `jup_migrate.ipynb` (migrates objects)
- `load_rag_cubes.ipynb` (loads cubes)
- `jup_osi_file_generator.ipynb` (writes files)

## Limitations

- DataFrames are inspected in their **final** namespace state after all cells run (a
  variable reassigned in a loop is seen only in its last value).
- Cell-level Jupyter magics (`%`, `!`) are skipped (the target notebooks use none).
- Row counts come from live systems; the tool **reports** differences, it does not judge
  which environment is correct.
