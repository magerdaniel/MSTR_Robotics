"""Central repo-relative path resolution.

Every path here is derived from the installed package location
(``Path(__file__).parent.parent`` == the repo root), so the same code resolves
correctly in whichever environment the package is installed -- e.g.
``mstr_robotics`` vs ``mstr_robotics_comp`` -- without any hardcoded absolute
paths. This is what lets ``tools/nb_compare`` run the same notebooks against two
environments and get comparable results.

Notebooks and modules should import the constants they need from here, e.g.::

    import yaml
    from mstr_robotics._paths import USER_CONFIG
    with open(USER_CONFIG, "r") as fh:
        user_d = yaml.safe_load(fh)
"""

from __future__ import annotations

import os
from pathlib import Path

# Marker that identifies a repo root (every environment has its own copy).
_MARKER = Path("config") / "user_d.yml"


def find_repo_root() -> Path:
    """Resolve the repo root of the *current* environment.

    The two comparison environments (mstr_robotics / mstr_robotics_comp) share a
    single installed ``mstr_robotics`` package, so ``Path(__file__)`` always
    points at whichever repo the package was installed from -- it cannot tell
    the environments apart. Instead we anchor on the runtime working directory:
    ``run_shapes.py`` executes each notebook with cwd set to ``<repo>/mstr_robotics/notebooks``
    (see tools/nb_compare), so walking up from cwd to the dir that contains
    ``config/user_d.yml`` yields THIS environment's repo.

    Resolution order:
      1. ``MSTR_REPO_ROOT`` environment variable, if set.
      2. The nearest ancestor of the cwd that contains ``config/user_d.yml``.
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


def _dir_from_env(var: str, default: Path) -> Path:
    """Return the directory named by environment variable *var*, else *default*.

    Every data location below is overridable this way so the package works both
    from a plain clone (defaults, all inside the repo) and from a larger local
    setup where the OSI files and the output area live elsewhere.
    """
    value = os.environ.get(var)
    return Path(value).expanduser().resolve() if value else default


# --- configuration -----------------------------------------------------------
CONFIG_DIR = REPO_ROOT / "config"
USER_CONFIG = CONFIG_DIR / "user_d.yml"
ENV_FILE = CONFIG_DIR / "API_KEY.env"

# --- OSI (Open Semantic Interchange) content ---------------------------------
# Generated OSI YAML lives here; MSTR_OSI_DIR repoints it at an external folder.
OSI_DIR = _dir_from_env("MSTR_OSI_DIR", REPO_ROOT / "data" / "osi")
OSI_SCHEMA = _dir_from_env("MSTR_OSI_SCHEMA_DIR", OSI_DIR) / "osi-schema-with-dashboards.json"
OSI_DASHBOARD_CONTEXT = OSI_DIR / "Dashboard_Context"

# --- output area -------------------------------------------------------------
OUTPUT_DIR = _dir_from_env("MSTR_OUTPUT_DIR", REPO_ROOT / "output")
MCP_DATA = OUTPUT_DIR / "MCP_data"

# The output area is generated content, not tracked in git, so it may be absent
# on a fresh clone or clean checkout. Create it eagerly on import: pandas'
# to_csv() (used throughout the notebooks) does not create missing parent dirs.
MCP_DATA.mkdir(parents=True, exist_ok=True)

# Deprecated aliases kept so existing notebooks keep importing. Prefer OSI_DIR
# and OUTPUT_DIR in new code; these will be removed in 0.6.
OSI_PRODUKTION = OSI_DIR
OSI_FILES = OSI_DIR
PYTHON_IO = OUTPUT_DIR
