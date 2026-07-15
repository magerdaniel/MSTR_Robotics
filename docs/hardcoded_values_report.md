# Hardcoded Values Report

Automated scan of **every `*.py` and `*.ipynb`** in the repo (excluding
`.venv`, `.git`, `.ipynb_checkpoints`, `__pycache__`) for hardcoded secrets,
credentials, connection strings, absolute paths, URLs, emails and MSTR object
GUIDs. Generated 2026-07-03.

Secret-like values below are **masked** (first 4 chars + length) — this report
never contains a usable credential. 87 files had at least one match.

## Legend — where a file lives

- **PACKAGE** = `mstr_robotics/` (shipped code)
- **NOTEBOOKS** = `notebooks/` (the 11 supported notebooks)
- **BACKUP/DEV** = `notebooks_BAK/`, `notebooks_dev/`, `alt/`, `Dansfiles/` — out of scope for v0.5, **but real secrets here are still real leaks**

---

## 1. CRITICAL — credentials / secrets (act on these)

These are live-looking secrets committed in plain text. All are in
**backup/dev** notebooks (not the supported set), but the git history still
carries them, so they must be **rotated at the provider**, exactly like the
Azure key was.

| Kind | File | Location | Masked value |
|---|---|---|---|
| OpenAI API key (`sk-…`) | `notebooks_BAK/Prompt Bot.ipynb` | cell 2 | `sk-p…(56)` ×2 |
| OpenAI API key (`sk-…`) | `notebooks_BAK/jup_JSON_exporter.ipynb` | cell 2 | `sk-p…(56)` |
| Perplexity API key (`pplx-…`) | `notebooks_BAK/Colab_perplexBak1.ipynb` | cell 2 | `pplx…(53)` |
| JWT / bearer token | `notebooks_dev/load_Qdrant.ipynb` | cell 1 | `eyJh…(100)` |
| JWT / bearer token | `notebooks_dev/Untitled.ipynb` | cell 0 | `eyJh…(84)` |
| MSTR login password (literal) | `notebooks_dev/jup_PxCD.ipynb` | cell 0 | `Vict…(11)` |
| MSTR login password (literal) | `notebooks_dev/Power BI or DIE.ipynb` | cell 1 | `Vict…(11)` |
| MSTR login password (literal) | `notebooks_dev/Spelling.ipynb` | cell 1 | `Vick…(8)` |
| password/secret literal | `notebooks_BAK/jup_JSON_exporter.ipynb` | cell 2 | `Vick…(8)` |
| password/secret literal | `notebooks_BAK/zzz_jup_prj_compare_old.ipynb` | cell 8 | `KSvQ…(32)` |
| password/secret literal | `alt/zzz_redis_operations_manni.ipynb` | cell 4 | `pzSq…(32)` |

**Action:** rotate the OpenAI key, the Perplexity key, the Qdrant token(s), and
change the MSTR account password(s). Because these live in **git history**, the
same caveat as the Azure key applies — removing them from the current files is
not enough; regenerate/rotate at the source. Consider a history purge
(`git filter-repo`) if these dirs are ever pushed.

### Already handled
- **Azure Storage key** in `notebooks/jup_migrate.ipynb` (old commits) — purged
  from history on 2026-07-03 and rotated. The **current** `jup_migrate.ipynb`
  builds the connection string from an f-string
  (`AccountKey={AccountKey}` sourced from `config/dans_migrations.yml`), so it
  holds **no literal key** — the one `connection_string` match there is that
  safe template.

---

## 2. MEDIUM — machine-specific absolute paths

Hardcoded `C:\Users\danie\…` / `c:\coding\…` paths break on any other machine
and leak the local username. Prefer `mstr_robotics._paths` (already used by
most notebooks) or config.

| File | Area | Location | Path |
|---|---|---|---|
| `notebooks/jup_migrate.ipynb` | NOTEBOOKS | cell 4 | `C:\Users\danie\OneDrive\…\MigrationDemoUserGuppe.xlsx` |
| `notebooks/jup_load_ontologies.ipynb` | NOTEBOOKS | cell 4 | `C:\Users\danie\Downloads\URI_Zip_Code.xlsx` |
| `mstr_robotics/_paths.py` | PACKAGE | code | `C:\coding\python_io` (fallback default — intended, but machine-specific) |

Other absolute paths flagged in `notebooks/jup_osi_file_generator.ipynb` and
`notebooks/mstr_admin.ipynb` are inside **cell outputs** (printed results), not
code — harmless, but they will disappear if you clear notebook outputs before
committing (recommended anyway; see §5).

---

## 3. MEDIUM — hardcoded URLs / endpoints in package code

Externalize to config if these ever change per environment.

| File | URL | Note |
|---|---|---|
| `mstr_robotics/user_rag.py` | `https://api.perplexity.ai` | Perplexity API base |
| `mstr_robotics/mcp_servers/mstr_osi_mcp.py` | `https://api.perplexity.ai` | Perplexity API base |
| `mstr_robotics/mcp_servers/mstr_osi_mcp.py` | `https://query.wikidata.org/sparql` | Wikidata SPARQL endpoint |
| `mstr_robotics/mcp_servers/mstr_osi_mcp.py` | `https://www.wikidata.org/wiki/User:Mstrrobotics` | Wikidata user page |

(URLs in notebook **outputs**, e.g. `localhost` health-check results in
`mstr_admin.ipynb`, are not code and are ignored.)

---

## 4. INFORMATIONAL — MSTR object GUIDs & project IDs

> **Resolved 2026-07-03:** the hardcoded project / folder / report / prompt /
> cube IDs in the code cells of `notebooks/` and `notebooks_dev/` were moved to
> `config/jupyter_objects_d.json` (one entry per notebook, grouped by
> `reports` / `prompts` / `cubes` / `folders` / `misc`) and the notebooks now
> read them via `jupyter_objects_d["<notebook name>"]`. GUIDs remaining in this
> section's tables are in cell **outputs** or legacy dirs (`alt/`,
> `notebooks_BAK/`, `Dansfiles/`), which were out of scope.

**6,946 distinct 32-char GUIDs** across the repo — the bulk are legitimate
(REST payloads, cube/report IDs in outputs, element IDs). They are not secrets,
but the **hardcoded project / folder / report IDs pinned at the top of
notebooks** tie the notebooks to one specific MSTR environment:

| File | # GUIDs | Typical hardcoded IDs (in code cells) |
|---|---|---|
| `notebooks/mstr_admin.ipynb` | 227 | mostly privilege/object dumps in outputs |
| `notebooks/jup_osi_file_generator.ipynb` | 50 | dashboard IDs |
| `notebooks/semantic_endpoints.ipynb` | 30 | object IDs |
| `notebooks/jup_schema_monitor.ipynb` | 27 | project_id, cube IDs |
| `notebooks/jup_schema_exporter.ipynb` | 19 | project_id, cube IDs |
| `notebooks/jup_Colab_perplex.ipynb` | 17 | cube/report IDs |
| `notebooks/load_rag_cubes.ipynb` | 13 | cube IDs |
| `notebooks/jup_REGAM.ipynb` | 9 | `project_id`, `pa_project_id`, report IDs |
| `notebooks/jup_migrate.ipynb` | 7 | source/target project IDs |
| `notebooks/jup_prj_obj_exporter.ipynb` | 4 | project/cube IDs |

Package code also embeds a few GUIDs (`cube_load.py` 6, `mcp_servers/mstr_osi_mcp.py` 3,
`mcp_servers/mstr_collab_perplex_server.py` 3, plus singletons in `_helper.py`,
`mstr_classes.py`, `read_out_prj_obj.py`, `report.py`). These are worth
checking — an object GUID baked into *shipped* code is usually an example that
should be a parameter or config value.

**Recommendation:** move the per-notebook project/folder/report IDs into
`config/` (you already have `dans_migrations.yml`, `First_comp_run.yml`, etc.),
so switching environments is a config edit, not a code edit.

---

## 5. Emails & misc

- `notebooks_dev/fabric_warehouse_loader.py`: `workspace@onelake.dfs.fabric.microsoft.com`,
  `YourWorkspace@onelake.dfs.fabric.microsoft.com` (Fabric OneLake endpoints —
  the second is clearly a placeholder). Dev-only.

## Notes on method / caveats

- Scan is regex-based → some **false positives** (e.g. long base64 blobs in
  outputs) and possible **false negatives** (a secret split across variables, or
  an unusual key format). Treat "no finding" as "nothing obvious", not a guarantee.
- Notebook **outputs** were scanned too; clearing outputs before committing
  (`jupyter nbconvert --clear-output`) would remove most path/GUID/URL noise and
  any secret accidentally printed by a cell.
- `config/*.yml` is gitignored and was **not** part of this code scan — that's
  the correct home for the values in §2–§4.
