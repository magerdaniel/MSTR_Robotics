"""Central project-relative path resolution.

Resolution walks up from the current working directory looking for
``config/user_d.yml`` -- the marker that says "this is a project that has run
setup". This is what lets a user ``pip install`` the package, copy the
reference material from ``mstr_robotics/utils/`` to the top level of their own
project, and have config/output/OSI paths resolve there -- instead of inside
the installed package's own location (e.g. site-packages), which is not
writable in any meaningful sense for the user.

It is also what lets ``tools/nb_compare`` (in the dans_playground dev repo)
run the same notebooks against two full repo checkouts that share one
installed ``mstr_robotics`` package: each checkout has its own
``config/user_d.yml``, so walking up from cwd tells the two apart even though
``Path(__file__)`` cannot.

Notebooks and modules should import the constants they need from here, e.g.::

    import yaml
    from mstr_robotics._paths import USER_CONFIG
    with open(USER_CONFIG, "r") as fh:
        user_d = yaml.safe_load(fh)
"""

from __future__ import annotations

import os
from pathlib import Path

# Markers that identify a project root (every environment has its own copy).
# ``user_d.yml`` exists once setup has run; ``user_d.example.yml`` exists as
# soon as the user copies mstr_robotics/utils/config/ in, before setup has run
# even once -- needed because the setup notebook itself runs from
# <project>/notebooks/, one level below the project root it needs to find.
_MARKERS = (
    Path("config") / "user_d.yml",
    Path("config") / "user_d.example.yml",
)


def find_repo_root() -> Path:
    """Resolve the root of the *current* project.

    Resolution order:
      1. ``MSTR_REPO_ROOT`` environment variable, if set.
      2. The nearest ancestor of the cwd that contains ``config/user_d.yml``
         or ``config/user_d.example.yml`` (a project that has copied
         mstr_robotics/utils/ in, whether or not setup has run yet).
      3. The current working directory, if neither marker is found anywhere
         above it.

    Step 3 deliberately does NOT fall back to this file's install location
    (e.g. site-packages) -- that would silently write config/output inside the
    venv instead of the user's own project.
    """
    env = os.environ.get("MSTR_REPO_ROOT")
    if env:
        return Path(env).resolve()
    cwd = Path.cwd()
    for base in (cwd, *cwd.parents):
        if any((base / marker).exists() for marker in _MARKERS):
            return base
    return cwd


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
