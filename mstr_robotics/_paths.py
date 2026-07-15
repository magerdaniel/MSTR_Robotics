"""Central repo-relative path resolution.

Every path here is derived from the installed package location
(``Path(__file__).parent.parent`` == the repo root), so the same code resolves
correctly in whichever environment the package is installed -- e.g.
``mstr_robotics`` vs ``mstr_robotics_comp`` -- without any hardcoded absolute
paths. This is what lets ``tools/nb_compare`` run the same notebooks against two
environments and get comparable results.

Notebooks and modules should import the constants they need from here, e.g.::

    from mstr_robotics._paths import USER_CONFIG
    with open(USER_CONFIG, "r") as fh:
        user_d = json.load(fh)
"""

from __future__ import annotations

import os
from pathlib import Path

# Marker that identifies a repo root (every environment has its own copy).
_MARKER = Path("config") / "user_d.json"


def find_repo_root() -> Path:
    """Resolve the repo root of the *current* environment.

    The two comparison environments (mstr_robotics / mstr_robotics_comp) share a
    single installed ``mstr_robotics`` package, so ``Path(__file__)`` always
    points at whichever repo the package was installed from -- it cannot tell
    the environments apart. Instead we anchor on the runtime working directory:
    ``run_shapes.py`` executes each notebook with cwd set to ``<repo>/notebooks``
    (see tools/nb_compare), so walking up from cwd to the dir that contains
    ``config/user_d.json`` yields THIS environment's repo.

    Resolution order:
      1. ``MSTR_REPO_ROOT`` environment variable, if set.
      2. The nearest ancestor of the cwd that contains ``config/user_d.json``.
      3. Fall back to this file's install location (the package's own repo).
    """
    env = os.environ.get("MSTR_REPO_ROOT")
    if env:
        return Path(env).resolve()
    cwd = Path.cwd()
    for base in (cwd, *cwd.parents):
        if (base / _MARKER).exists():
            return base
    return Path(__file__).resolve().parent.parent


REPO_ROOT = find_repo_root()

# --- inside this repo --------------------------------------------------------
CONFIG_DIR = REPO_ROOT / "config"
USER_CONFIG = CONFIG_DIR / "user_d.json"
ENV_FILE = CONFIG_DIR / "API_KEY.env"
OSI_PRODUKTION = REPO_ROOT / "OSI_Produktion"

# --- sibling repo: <...>/Python_environments/OSI_Files ----------------------
OSI_FILES = REPO_ROOT.parent / "OSI_Files"
OSI_SCHEMA = OSI_FILES / "osi-schema-with-dashboards.json"
OSI_DASHBOARD_CONTEXT = OSI_FILES / "Dashboard_Context"

# --- shared external output area: C:\coding\python_io -----------------------
# REPO_ROOT.parents[1] == the drive-level "coding" folder that holds both
# Python_environments and python_io.
PYTHON_IO = REPO_ROOT.parents[1] / "python_io"
MCP_DATA = PYTHON_IO / "output_files" / "MCP_data"
