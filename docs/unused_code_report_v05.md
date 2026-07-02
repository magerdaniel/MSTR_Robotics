# Unused-Code Report — v0.5

Generated 2026-07-02 on branch `v0.5-cleanup`. **No code was deleted based on
this report** (per decision: report only, decide later).

"Authoritative consumers" = the 11 notebooks in `notebooks\` plus the package
itself. `notebooks_BAK\`, `notebooks_dev\`, `alt\`, `Dansfiles\` are treated as
backups/experiments.

## 1. Module level

### Imported by nothing, anywhere
| Module | Evidence |
|---|---|
| `cube_load.py` | No import of it in the package, notebooks, tools or examples. It is a standalone script and even executes `handle_cube_load(...)` at module level when imported. Its dependencies (none beyond stdlib + a live connection object) are self-contained. **Strongest deletion candidate.** |

### Standalone entry points (not dead, but not on any notebook path)
These are started as programs, not imported by notebooks. Their server
dependencies (`fastapi`, `uvicorn`, `mcp`, `streamlit`) are **not installed in
the current .venv**, so none of them can currently run from this environment —
they are declared as the `servers` optional dependency group in pyproject.toml.

| Module | Role |
|---|---|
| `api_server.py` | FastAPI HTTP server (uses `user_run_compare`) |
| `streamlit_app.py` | Streamlit comparison UI (uses `redis_db`, `json_compare`) |
| `mstr_osi_mcp.py` | MCP server exposing MSTR/OSI tools |
| `mstr_collab_perplex_server.py` | MCP server: NL question → Perplexity → report |

### Used only by dev/backup code (`notebooks_dev\`)
| Module | Only consumer |
|---|---|
| `bq_connector.py` | `notebooks_dev\jup_bq_uplooad_csv.ipynb` (google-cloud-bigquery also not installed in .venv → `bigquery` optional group) |
| `select_mig_objects.py` + `_mod_prj_obj.py` | `notebooks_dev\create_migration_packages.ipynb` |

### Transitively reachable from the notebooks — NOT unused
| Module | Import chain from a notebook |
|---|---|
| `_lu_data.py` | notebooks → `mstr_classes` → `_lu_data` |
| `json_compare.py` | notebooks → `prepare_ai_data` / `redis_db` → `json_compare` |
| `_pa_etl.py`, `_export.py` | `jup_REGAM.ipynb` → `regam` → `_pa_etl`, `_export` |
| `user_run_compare.py` | only via `api_server.py` (server-side; unused if the API server is retired) |

## 2. Method/class level (vulture 60% confidence, cross-checked against notebooks)

Vulture found 151 candidates; 47 were false positives because notebooks, tools
or examples reference them. The 104 below are referenced **neither in the
package nor in `notebooks\`/`tools\`/`examples\`**.

### Caveats — do NOT treat these groups as dead
- `mstr_osi_mcp.py` / `mstr_collab_perplex_server.py` functions
  (`get_object_definitions`, `resolve_object_by_path`, `query_wikidata_sparql`,
  `get_visualization_data`, `find_dashboard_for_question`,
  `run_and_answer_bi_question`, `query_bi_report`): registered as **MCP tools
  via decorators** — invoked by the MCP runtime, not by imports.
- `api_server.py` (`connect_redis`, `logout`): **FastAPI route handlers** —
  invoked over HTTP.
- `streamlit_app.py` attribute hits (`prefix_1_widget`, `auto_load_comparison`,
  …): **Streamlit session state** — read/written by the framework at runtime.
- `user_rag.py` `ChatBot`/`VectorDbFaiss`: referenced by scripts in
  `Dansfiles\` (out-of-scope personal scripts); dead from the notebooks' view.

### Explicitly experimental (author-marked `zzz`/`ZZZ` prefix) — REMOVED
All `zzz`/`ZZZ`-prefixed members were verified unreferenced and deleted after
this report was first written (same for ~59 commented-out `# print(...)` debug
lines and the disabled code block wrapped in a string literal inside
`CompareMstrObjects`):
- `_connectors.py`: `zzz_fetch_cube_elements`, `ZZZ_get_cube_data`, `ZZZ_save_rep_sat_inst_as`
- `_pa_etl.py`: `zzz_fetch_pa_data_prp`, `ZZZ_fetch_pa_data`
- `read_out_prj_obj.py`: `zzz_obj_prp_search_dpn`, `zzz_trans_cbe_el_prp`
- `navigation.py`: `zzz_fetch_mstr_keys`
- `report.py`: `zzz_loop_att_exp_prp`, `zzz_get_form_type`, `zzzzloop_prp_ans_bld`
- `json_compare.py`: string-disabled block with `bld_redis_key`, `zzz_fetch_comp_objdef_d_l`, `zzz_fetch_obj_id_from_sh_folder`
- (removed in Phase 3: `prepare_ai_data.ZzzRedisMstrJson`)

### Unreferenced methods/classes in notebook-relevant modules (review before deleting)
- `_connectors.py`: `get_report_sql`, `get_report_raw`
- `_export.py`: `write_list_to_JSON_to_file`, `load_JSON_files`, class `GetObjJson` (+ `extract_obj_JSON`)
- `_helper.py`: `_get_last_chars`, `bld_mstr_obj_md_guid`, `get_vals_from_dict_l`, `get_key_from_dict`, `sort_dict_by_key_in_l`
- `_mod_prj_obj.py`: `run_short_cut_build`
- `dossier.py`: class `DossierGlobal`, `doss_hier_to_df`, `read_out_doss_datasets`
- `mstr_classes.py`: `get_obj_type_from_l` (empty stub), `get_obj_name`, `_bld_objType_l`, attribute `md_searches` in `MstrGlobal.__init__`
- `navigation.py`: `merge_prompts`
- `osi_exporter/export_dashboard.py`: `read_out_obj`
- `prepare_ai_data.py`: class `SortMstrJson` (its `sort_json_lists_by_keys` is
  itself marked DEPRECATED), class `CleanMstrIds`
- `read_out_prj_obj.py`: `_read_hier_in_prp` (comment says "currently not
  used"), `read_out_hier_df`, `set_run_prop_d`, `add_prp_def_to_cube`
- `redis_db.py`: `fetch_key_list`, `bulk_upload_json_files`, `build_subtype_map`
- `regam.py`: `read_from_report`, `run_read_out_job_vis`, module var `i_file_io`
- `report.py`: `bld_df_of_mstr`, `report_has_data`, `bld_rep_library_url`,
  `get_default_prp_answ`, `open_instance` (lower-case twin of the used
  `open_Instance`!), `get_RAG_cube_col_mstr_id`, `cube_upload_1_table`,
  `close_open_prp`
- `select_mig_objects.py`: classes `OpenConn`, `GetChangeLog` (several methods), `BldMigContent.from_folder` — see module-level note above
- `user_rag.py`: `KeywordProcessor.check_keyword`, `MstrOpenAi.check_trans_chatGPT`, `Perplexity.merge_AI_ans_d`
- `user_run_compare.py`: `update_searches`

## 3. Known latent defects (pre-existing, documented not fixed)

| Location | Issue |
|---|---|
| ~~`json_compare.py` `bld_redis_key` NameError~~ | Resolved: the offending code turned out to live inside a string-literal-disabled block in `CompareMstrObjects` (dead text, not executable) and has been deleted. |
| `examples\example_fetch_definitions.py` | Imports `mstr_robotics.fetch_obj_definitions`, a module that does not exist. The example is broken/stale (docs/fetch_obj_definitions_README.md refers to the same missing module). |
| `report.py` | Both `open_Instance` (used) and `open_instance` (unused) exist — near-duplicate implementations; consolidate in v0.6 when method names are normalized. |
| `regam.py` `run_read_out_job_vis` | Was referencing undefined `i_dossiers`; repaired in Phase 3 to `i_mstr_api.create_dossier_instance` (the method lives on `MstrApi`). The method is itself unreferenced (see section 2). |

## 4. Suggested v0.6 actions
1. Delete `cube_load.py` (or move to `tools\`). (`zzz`/`ZZZ` members: done.)
2. Decide the fate of the four server entry points (keep = add a `servers`
   extra install + smoke test; retire = delete together with
   `user_run_compare.py`).
3. Normalize method/argument names (`open_Instance` → `open_instance`, `sKey` →
   `s_key`, …) — the `N802/N803/N806` ruff ignores in pyproject.toml mark this
   debt.
4. Remove the reviewed unreferenced methods from section 2.
