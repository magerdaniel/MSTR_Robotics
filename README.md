# MSTR – Robotics

**AI on top of BI** — a Python automation toolkit for MicroStrategy / Strategy One.

Object read-out, migration, environment comparison, regression testing, RAG cubes,
OSI dashboard export, and a custom MCP server that lets an AI assistant drive your
BI platform directly.

> **Maturity: beta, sometimes alpha** 🙂 Everything here is built and tested against
> **MicroStrategy Tutorial** and **Platform Analytics**. For real use cases you will
> want to adjust the notebooks — or skip them and call the libraries yourself.

---

## Goals of this package

- Get you running against Strategy Tutorial with as little setup as possible
- Share experience in coding on top of MicroStrategy
- Share the code itself

Delivered for free, in two parts:

1. The **Python package** (this repository)
2. The **MSTR Object Manager packages** for Tutorial & Platform Analytics, in
   [`OM_packages/`](OM_packages/)

---

## What's in it

### Custom MCP server

An MCP server is the bridge that connects an AI assistant to your own programs —
here, to Python code that talks to MicroStrategy. With a custom MCP server you can
automate individual workflows in natural language.

[`mstr_robotics/mcp_servers/mstr_osi_mcp.py`](mstr_robotics/mcp_servers/mstr_osi_mcp.py)
registers eight tools:

| Tool | Purpose |
|---|---|
| `find_dashboard_for_question` | Find an existing report/dashboard that can answer a business question |
| `resolve_object_by_path` | Turn a breadcrumb path into an object ID |
| `get_object_definitions` | Fetch full object definitions as JSON |
| `query_bi_report` | Answer report-wizard prompts from a natural-language question |
| `export_report_tabular` | Export a report to tabular data |
| `get_visualization_data` | Read the live data behind a single visualization |
| `run_and_answer_bi_question` | Run a BI question and enrich/expand the result |
| `query_wikidata_sparql` | Enrich BI data with external Wikidata properties |

Things this makes possible:

- **Compare two dashboards.** Point the AI at two screenshots; it extracts the
  breadcrumb paths, resolves both IDs, pulls both JSON definitions and diffs them —
  down to *"the Cost column is bound to a different metric in each dashboard"*.
  Worth knowing: developers can rename objects, so names alone don't prove equality.
- **Find the dashboard you need** instead of building a new one.
- **Answer report-wizard prompts** in natural language, then verify the generated
  report inside Strategy itself.
- **Enrich BI data** with external sources and build something on top of it.

Requires an MCP-capable AI client (e.g. Claude). Some tools additionally use a
Perplexity API key — see [Configuration](#6-configure-the-yaml-files).

### Jupyter notebooks

Notebooks are Command Manager 2.0. Behind a JupyterHub they give MSTR developers a
stable, safe entry point for metadata analysis. All notebooks live in
[`notebooks/`](notebooks/):

| Notebook | What it does |
|---|---|
| `00_setup` | **Run first.** Validates config and resolves your project paths |
| `jup_load_rag_cubes` | **Run second.** Builds the RAG cubes the other tooling reads |
| `jup_prj_obj_exporter` | Exports metadata to Redis for analysis |
| `jup_migrate` | Migration driven by an Excel package list |
| `jup_REGAM` | Regression testing from Platform Analytics |
| `jup_chat_answer_prompt_page` | Report-wizard prompt answering, notebook edition |
| `jup_schema_monitor` | Schema monitoring |
| `jup_osi_file_generator` | Exports dashboards to OSI YAML |
| `jup_mstr_admin` | Administrative helpers |

### Migrations

Define your migration package from the Platform Analytics **Change Logs** dashboard
and export it to Excel. `jup_migrate` then drives the automated deployment from that
spreadsheet — optionally staging packages through Azure Blob storage.

### Regression testing (REGAM)

Platform Analytics as the source of truth for what your users actually do:
execution times, result row counts, involved views / tables / columns, all jobs from
last Monday, top *x* reports per user group, security groups. From that,
`mstr_robotics` simulates user behaviour for regression tests — aiming at
meaningful coverage at an acceptable system workload.

- **Test definition** — pick the workload from PA
- **Test preparation** — extract, parse and enrich the raw data. PA was never
  designed for this, and the metadata changes constantly, so exception handling has
  to be forgiving.
- **Test execution** — in the current version, `mstr_robotics` creates report copies
  with stored prompt answers. To analyse results you can keep static copies of
  prompted reports for Integrity Manager, or compare execution times from EM/PA and
  database logs.

### Metadata export to Redis

Reading metadata through the REST APIs means restricted access, poor query
performance, and several APIs to stitch together. Exporting it into Redis once turns
it into something you can actually query — a powerful way to monitor and clean your
metadata.

**You need a Redis DB** — on prem or cloud. The free/open version is fine: install it
locally, or take it as a managed service (e.g. StackIT).

### RAG cubes

Same idea as the Redis export, but the data lands where **both AI and humans** can
read it. RAG cubes provide metadata as context to an AI, and stay verifiable by
people because they are ordinary cubes in your BI platform. Technically:
read the JSON definitions, transform them to tabular, load them into cubes.

---

## Installation

### Minimum requirements

- A supported MicroStrategy / Strategy One version, on prem or in cloud
- **Architect privileges**
- Python 3.12+ (developed on 3.12.7)
- A Python IDE

Strongly recommended: **Visual Studio Code**, and the possibility of vibe-coding —
if something here doesn't fit your enterprise standard, you can fix it by writing
(or vibing) code.

### 1. Set up a Python environment

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows;  source .venv/bin/activate on Unix
```

### 2. Install the package

From PyPI:

```powershell
pip install "MSTR-Robotics-magerdaniel[all,dev]"
```

Or from a clone, for editable development:

```powershell
git clone https://github.com/magerdaniel/MSTR_Robotics.git
cd MSTR_Robotics
pip install -e ".[all,dev]"
```

The base install is deliberately small — connection, object read-out, comparison,
migration and OSI export. Everything feature-specific is an opt-in extra, and
**plain `pip install .` installs none of them**:

| Extra | Adds | Needed for |
|---|---|---|
| `[rag]` | `openai`, `flashtext` | chat / RAG notebooks |
| `[redis]` | `redis` | metadata export to Redis |
| `[azure]` | `azure-storage-blob` | Azure Blob staging in migrations |
| `[servers]` | `mcp` (+ implies `rag`) | the MCP servers |
| `[all]` | all four of the above | everything |
| `[dev]` | `ruff`, `vulture`, `jupyter` | linting and a Jupyter kernel |

Pick any combination:

```powershell
pip install -e ".[rag,redis,servers]"      # no Azure
pip install -e ".[redis,azure]"            # no AI stack at all
pip install -e ".[rag,redis,azure,dev]"    # everything but the MCP servers
pip install -e ".[all,dev]"                # the full dev machine
```

Note that `[all]` does **not** include `[dev]`, so `[all]` alone leaves you without a
Jupyter kernel. Quoting the brackets is required in PowerShell and zsh.

### 3. Install Jupyter

Covered by `[dev]` above. Otherwise: `pip install jupyter`.

### 4. Copy the `utils` folders into your project

A non-editable install bundles all the reference material under
`.venv\Lib\site-packages\mstr_robotics\utils\`. Copy those folders **up into your own
Python project folder** so the notebooks and config sit next to your work:

```
config/          notebooks/       examples/        OM_packages/
docs/            OSI_production/  osi_templates/
```

If you installed with `-e` from a clone, they are already at project level — skip
this step.

### 5. Deploy the Object Manager packages

The notebooks and libraries read from — and write into — cubes, reports and dossiers
that must exist in **your** MicroStrategy environment. Everything is pre-configured
for MSTR Tutorial. [`OM_packages/`](OM_packages/) contains the bundle:

| Package | Provides |
|---|---|
| `tutorial_objetcs.mmp` | Folder structure, MTDI cubes and MSTR application objects — **deploy this first** |
| `PA_Objects.mmp` | Only needed for REGAM regression testing; supplies the report used to replay user jobs from Platform Analytics |

Prerequisites: access to MSTR Tutorial and Platform Analytics via Workstation and
Library as a developer. A POC or development environment is recommended.

### 6. Configure the YAML files

Copy each `*.example.*` file in [`config/`](config/) to its real name and fill it in:

| Example file | Becomes | Holds |
|---|---|---|
| `user_d.example.yml` | `user_d.yml` | MSTR connection and credentials |
| `mstr_redis_y.example.yml` | `mstr_redis_y.yml` | Redis connection |
| `dans_migrations.example.yml` | `dans_migrations.yml` | Migration settings |
| `jupyter_objects_d.example.yml` | `jupyter_objects_d.yml` | Object GUIDs per notebook |
| `API_KEY.example.env` | `API_KEY.env` | Perplexity API key |

The real files are gitignored — keep it that way.

### 7. Run the notebooks in order

1. `00_setup`
2. `jup_load_rag_cubes`
3. … then whichever notebook fits your task

Full walkthrough: [`docs/SETUP.md`](docs/SETUP.md).

---

## Repository layout

| Path | Contents |
|---|---|
| [`mstr_robotics/`](mstr_robotics/) | The package: connection, objects, reports, migration, Redis, RAG, MCP servers, OSI exporter |
| [`notebooks/`](notebooks/) | Jupyter notebooks — the primary entry points |
| [`config/`](config/) | `*.example.*` configuration templates |
| [`docs/`](docs/) | Setup and reference documentation |
| [`examples/`](examples/) | Sample data used by the notebooks |
| [`OM_packages/`](OM_packages/) | MSTR Object Manager packages for Tutorial & PA |
| [`osi_templates/`](osi_templates/) | Read-only OSI schema — the exporter reads it, nothing writes it |
| [`OSI_production/`](OSI_production/) | Example OSI YAML export of a dashboard |

---

## Community

- **Free webinars every 2 to 4 weeks**
- Contributions are very welcome — especially senior Python review
- Ideas on how to push (Micro-)Strategy community packages forward are welcome too
- Open comments appreciated: comment or DM

Need more hands-on help? Workshops (1, 3 or 5 days) and Strategy Python automation
work are available — 1.500 € / day. Send a direct message if interested.

Questions or a failing OM package import: daniel@magdata.de
