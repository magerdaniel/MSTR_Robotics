# v0.5 Rename Map

All renames applied in the v0.5 cleanup (branch `v0.5-cleanup`). Scope: the
`mstr_robotics` package, the 11 notebooks in `notebooks\`, `tools\`, and
`examples\`. Not touched: `notebooks_BAK\`, `notebooks_dev\`, `alt\`,
`Dansfiles\` (they still reference the old names).

## Modules

| Old | New | Reason |
|---|---|---|
| `mstr_robotics/osi_expoter/` | `mstr_robotics/osi_exporter/` | typo; also gained an `__init__.py` |
| `streamLit.py` | `streamlit_app.py` | camelCase filename; avoids confusion with the `streamlit` library |
| `prepare_AI_data.py` | `prepare_ai_data.py` | PEP8 module naming |
| `user_RAG.py` | `user_rag.py` | PEP8 module naming |

## Classes (snake_case → PascalCase)

| Module | Old | New |
|---|---|---|
| `dossier.py` | `dossier_global` | `DossierGlobal` |
| `dossier.py` | `doss_read_out_det` | `DossReadOutDet` |
| `dossier.py` | `doss_read_out` | `DossReadOut` |
| `mstr_classes.py` | `mstr_global` | `MstrGlobal` |
| `mstr_classes.py` | `md_searches` | `MdSearches` |
| `json_compare.py` | `compare_mstr_objects` | `CompareMstrObjects` |
| `json_compare.py` | `json_checksum_handler` | `JsonChecksumHandler` |
| `mstr_pandas.py` | `df_helper` | `DfHelper` |
| `user_rag.py` | `keyword_processor` | `KeywordProcessor` |
| `user_rag.py` | `vectorDB_faisst` | `VectorDbFaiss` (typo "faisst" fixed) |
| `user_rag.py` | `mstr_openAI` | `MstrOpenAi` |
| `user_rag.py` | `chat_bot` | `ChatBot` |
| `user_rag.py` | `perplexity` | `Perplexity` |
| `navigation.py` | `answer_prompts` | `AnswerPrompts` |
| `navigation.py` | `mstr_objects` | `MstrObjects` |
| `_helper.py` | `str_func` | `StrFunc` |
| `_helper.py` | `msic` | `Misc` (typo "msic" fixed) |
| `select_mig_objects.py` | `open_conn` | `OpenConn` |
| `select_mig_objects.py` | `get_change_log` | `GetChangeLog` |
| `select_mig_objects.py` | `bld_mig_content` | `BldMigContent` |
| `_mod_prj_obj.py` | `bld_short_cuts` | `BldShortCuts` |
| `regam.py` | `regam_jobs` | `RegamJobs` |
| `regam.py` | `regam` | `Regam` (module name `regam` unchanged) |
| `regam.py` | `test_exe` | `TestExe` |
| `read_out_prj_obj.py` | `read_out_hierarchy` | `ReadOutHierarchy` |
| `read_out_prj_obj.py` | `read_table_def` | `ReadTableDef` |
| `read_out_prj_obj.py` | `io_facts` | `IoFacts` |
| `read_out_prj_obj.py` | `io_attributes` | `IoAttributes` |
| `read_out_prj_obj.py` | `read_schema` | `ReadSchema` |
| `read_out_prj_obj.py` | `read_gen` | `ReadGen` |
| `read_out_prj_obj.py` | `read_prompts` | `ReadPrompts` |
| `read_out_prj_obj.py` | `read_report` | `ReadReport` |
| `read_out_prj_obj.py` | `read_cube` | `ReadCube` (the *method* `ReadCube.read_cube()` keeps its name) |
| `_connectors.py` | `mstr_api` | `MstrApi` |
| `redis_db.py` | `redis_bi_analysis` | `RedisBiAnalysis` |
| `redis_db.py` | `fetch_it_all` | `FetchItAll` |
| `redis_db.py` | `redis_mstr_json` | `RedisMstrJson` |
| `_export.py` | `file_io` | `FileIo` |
| `_export.py` | `get_obj_JSON` | `GetObjJson` |
| `report.py` | `rep` | `Rep` |
| `report.py` | `cube` | `Cube` |
| `report.py` | `prompts` | `Prompts` |
| `_pa_etl.py` | `parse_pa` | `ParsePa` |
| `_pa_etl.py` | `pa_parse_prp` | `PaParsePrp` |
| `_pa_etl.py` | `parse_att_exp_prp` | `ParseAttExpPrp` |
| `_pa_etl.py` | `run_prp_ans_bld` | `RunPrpAnsBld` |
| `_pa_etl.py` | `parse_exp_prp` | `ParseExpPrp` |
| `user_run_compare.py` | `run_compare` | `RunCompare` |
| `_lu_data.py` | `lu_mstr_md` | `LuMstrMd` |
| `prepare_ai_data.py` | `export_mstr_md` | `ExportMstrMd` |
| `prepare_ai_data.py` | `sort_mstr_json` | `SortMstrJson` |
| `prepare_ai_data.py` | `mstr_to_json` | `MstrToJson` |
| `prepare_ai_data.py` | `parse_json` | `ParseJson` |
| `prepare_ai_data.py` | `map_objects` | `MapObjects` |
| `prepare_ai_data.py` | `clean_mstr_ids` | `CleanMstrIds` |
| `prepare_ai_data.py` | `zzz_redis_mstr_json` | `ZzzRedisMstrJson` (dead code, removed in Phase 3) |

## Other identifier fixes

| Location | Old | New |
|---|---|---|
| `osi_exporter/export_dashboard.py` | parameter `serch_obj` | `search_obj` |

## Deliberately unchanged

- Variable and attribute names such as `self.rep`, `self.mstr_api`, `i_msic`
  remain snake_case (correct PEP8 for variables).
- Module names `regam.py`, `read_out_prj_obj.py` (established feature names).
- The word "prompts"/"cube" in REST URLs, JSON payloads and comments.
