# Setup

Three stages, in order. Stage 2 is one notebook; stage 3 is automatic.

1. [Install the Python package](#1-install-the-python-package)
2. [Deploy the Object Manager packages](#2-deploy-the-object-manager-packages)
3. [Run the setup notebook](#3-run-the-setup-notebook)

---

## 1. Install the Python package

Requires **Python 3.12+** and network access to a MicroStrategy Library REST endpoint.

```bash
git clone https://github.com/magerdaniel/MSTR_Robotics.git
cd MSTR_Robotics
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows;  source .venv/bin/activate on Unix
pip install -e .
```

The base install is deliberately small — connection, object read-out, comparison and
migration. Add only what you need:

| Command | Adds |
|---|---|
| `pip install -e ".[rag]"` | OpenAI / Perplexity — the chat and RAG notebooks |
| `pip install -e ".[redis]"` | Redis-backed metadata analysis |
| `pip install -e ".[azure]"` | Azure Blob staging for migration packages |
| `pip install -e ".[servers]"` | the MCP servers |
| `pip install -e ".[all]"` | everything above |
| `pip install -e ".[dev]"` | ruff, vulture, jupyter |

---

## 2. Deploy the Object Manager packages

The notebooks and the libraries read from cubes, reports and dossiers that must exist in **your**
MicroStrategy environment. Further they write into cubes. To simplify your start, everything is pre-configured for MSTR - Tutorial. You'll find the relevant mmp files for Tutorial and Platform Analytics in the file mstr_robotis_demo.zip under
[`OM_packages/`](../OM_packages/).

### Prerequisites

- Access to MSTR Tutorial and Platform Analytics over Workstation and Library as developer
- Python environment with the posibility to pip install
- A POC or development environment is recomended

### Packages

| Package | Provides |
|---|---|
| `tutorial_objetcs.mmp` | contains the folder structure, the MTDI-cubes and MSTR application objects.  — **deploy this first** |
| `PA_Objects.mmp` | is only needed for REGAM regession testing. Using this report, we can fetch user job for re-play out of PA |


### If a package fails to import

Send me a mail to daniel@magdata.de.

---

## 3. Run the setup notebook

```bash
jupyter lab notebooks/00_setup.ipynb
```

Run it top to bottom. It creates the output folders, copies the config templates,
validates what you filled in, verifies the connection, and then **resolves the object
GUIDs automatically**.

### If you want to use your 

If you want or need to modify the cubes, reports or objects, you can use this feature to check if they all fit. 
This saves a lot of time and nervs. `config/jupyter_objects_d.example.yml` are meaningless if the do not exist. Their *names* are fixed
by the packages, so the notebook searches your project by name and writes the correct
GUIDs into `config/jupyter_objects_d.yml` for you.

If a name is reported as **not found**, the package providing it has not been deployed.
If a name is **ambiguous**, your project has duplicates — set that ID by hand.

### Configuration reference

Every live config is gitignored; each ships an `.example` twin that
`00_setup.ipynb` copies for you.

| File | Needed for |
|---|---|
| `config/user_d.yml` | everything — connection and project GUIDs |
| `config/jupyter_objects_d.yml` | the notebooks; filled in automatically in step 5 |
| `config/API_KEY.env` | RAG notebooks and the MCP servers |
| `config/mstr_redis_y.yml` | Redis-backed metadata analysis |
| `config/dans_migrations.yml` | Azure-staged migrations |

`config/user_d.yml` doubles as the marker file `mstr_robotics._paths` uses to locate the
repo root, so it must exist even if you override paths via environment variables.

### Where files are written

| Variable | Default | Holds |
|---|---|---|
| `MSTR_REPO_ROOT` | auto-detected from `config/user_d.yml` | repo root |
| `MSTR_OSI_DIR` | `<repo>/data/osi` | generated OSI YAML |
| `MSTR_OSI_TEMPLATES_DIR` | `<repo>/osi_templates` | read-only OSI input templates, incl. `osi-schema-with-dashboards.json` |
| `MSTR_OUTPUT_DIR` | `<repo>/output` | exports, logs, MCP data |

---

## Troubleshooting

**`Cannot save file into a non-existent directory`** — the output folders were never
created. Run step 1 of `00_setup.ipynb`, or `python -c "from mstr_robotics import setup; print(setup.ensure_dirs())"`.

**`MicroStrategy configuration not found`** — `config/user_d.yml` is missing. Run step 2
of the setup notebook.

**`no object named '...'`** during GUID discovery — the Object Manager package providing
that object has not been deployed to the project you connected to.

**Config still points at the reference environment** — you copied the template but did
not edit it. `setup.check_configs()` reports this explicitly.
