# mstr-robotics

A Python toolkit for automating **MicroStrategy / Strategy One** via its REST API:
reading out project objects, comparing environments, driving migrations, running
regression tests, and exporting dashboards to **Open Semantic Interchange (OSI)**
YAML so BI content can be consumed by LLM agents.

> Built against a real MicroStrategy estate and shared as-is. The API surface is
> not stable yet — expect changes between 0.x releases.

## Highlights

**OSI export.** `osi_exporter` walks a MicroStrategy dossier and emits an OSI
semantic model — datasets, fields, metrics and relationships — plus a dashboard
document describing chapters, pages and visualizations. MicroStrategy's native
visualization types are mapped onto neutral OSI ones (`BarChart` → `bar_chart`,
`KpiWidget` → `kpi_card`, …), and `add_ai_context()` annotates any node with
free-text context for downstream agents.

**MCP servers.** `mstr_robotics.mcp_servers` exposes the toolkit over the Model
Context Protocol, so an assistant can find the right dashboard for a natural-language
question, resolve objects by folder path, execute a report, and answer from the
result together with its OSI context.

**Environment comparison.** `json_compare` diffs object definitions between two
MicroStrategy environments with configurable path filtering and checksums —
the basis for "what actually changed between dev and prod".

## Install

Requires Python 3.12+ and network access to a MicroStrategy Library REST endpoint.

```bash
git clone https://github.com/magerdaniel/mstr_robotics.git
cd mstr_robotics
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows;  source .venv/bin/activate on Unix
pip install -e .
```

Optional extras:

```bash
pip install -e ".[servers]"   # MCP servers
pip install -e ".[dev]"       # ruff, vulture, jupyter
```

## Configure

Every real config file is gitignored; each ships an `.example` twin. Drop the
`.example` from the name and fill in your own values:

```bash
cd config
cp user_d.example.yml            user_d.yml              # connection + projects
cp jupyter_objects_d.example.yml jupyter_objects_d.yml   # per-notebook object IDs
cp API_KEY.example.env           API_KEY.env             # LLM / Azure keys
cp mstr_redis_y.example.yml      mstr_redis_y.yml        # optional: Redis
cp dans_migrations.example.yml   dans_migrations.yml     # optional: migrations
```

| Example file | Needed for |
|---|---|
| `user_d.example.yml` | everything — connection and default project |
| `jupyter_objects_d.example.yml` | the notebooks; object IDs keyed by notebook name |
| `API_KEY.example.env` | `user_rag`, the MCP servers, `ms_azure` |
| `mstr_redis_y.example.yml` | `redis_db`, Redis-backed metadata analysis |
| `dans_migrations.example.yml` | `jup_migrate.ipynb`, Azure-staged migrations |

`config/user_d.yml` is also the marker file that `mstr_robotics._paths` uses to
locate the repo root, so it must exist even if you override paths.

The example values are from a reference environment — every GUID, host, user and
key must be replaced with your own before anything will connect. See
[.env.example](.env.example) for the full set of environment variables the
package reads (OpenAI, Perplexity and Azure).

### Paths

All data locations resolve relative to the repo by default and can be repointed
with environment variables:

| Variable | Default | Holds |
|---|---|---|
| `MSTR_REPO_ROOT` | auto-detected from `config/user_d.yml` | repo root |
| `MSTR_OSI_DIR` | `<repo>/data/osi` | generated OSI YAML |
| `MSTR_OSI_SCHEMA_DIR` | `MSTR_OSI_DIR` | `osi-schema-with-dashboards.json` |
| `MSTR_OUTPUT_DIR` | `<repo>/output` | exports, logs, MCP data |

## Module map

| Module | Purpose |
|---|---|
| `_connectors` | `MstrApi` — REST session handling |
| `read_out_prj_obj` | Read schema, facts, attributes, prompts, reports, cubes |
| `osi_exporter` | Build OSI semantic models and dashboard documents |
| `json_compare` | Diff object definitions across environments |
| `select_mig_objects` | Change-log driven migration package building |
| `regam` | Regression testing against Platform Analytics data |
| `cube_load` | Parallel cube publish with follow-up chains |
| `report`, `dossier`, `navigation` | Report/dossier execution and prompt answering |
| `user_rag` | FAISS vector store, OpenAI and Perplexity clients |
| `redis_db` | Redis-backed BI metadata analysis |
| `prepare_ai_data` | Normalize MSTR metadata JSON for AI consumption |
| `mcp_servers` | MCP tool groups over the above |

### Cube load chains

`cube_load` publishes cubes in parallel and can trigger a follow-up cube once one
finishes. Copy `mstr_robotics/cube_load_sample.json`, replace the placeholder
GUIDs, and run:

```bash
CUBE_LOAD_PLAN=/path/to/my_plan.json python -m mstr_robotics.cube_load
```

## Notebooks

`notebooks/` holds runnable examples that read their object IDs from
`config/user_d.yml` rather than hardcoding them. Outputs are stripped before commit.

## License

See [LICENSE](LICENSE).
