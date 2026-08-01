"""First-time setup helpers, driven by ``notebooks/00_setup.ipynb``.

Each function performs one setup step and returns a :class:`StepResult` instead
of raising, so the setup notebook can show every problem at once rather than
stopping at the first. Nothing here runs at import time -- importing this module
never touches the filesystem or the network.

Typical use::

    from mstr_robotics import setup

    print(setup.ensure_dirs())
    print(setup.init_configs())
    # ... fill in config/user_d.yml ...
    print(setup.check_configs())
    conn = setup.open_connection()
    print(setup.check_object_ids(conn))
"""

from __future__ import annotations

import contextlib
import io
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from mstr_robotics._paths import (
    CONFIG_DIR,
    MCP_DATA,
    OSI_DASHBOARD_CONTEXT,
    OSI_DIR,
    OUTPUT_DIR,
    REPO_ROOT,
    USER_CONFIG,
)

# Lower-cased substrings that mark a value as still being an untouched template.
# "your" covers YOUR/your-/your_/yourserver/yourlogis/yourPort; "you account"
# catches the account_key template, which reads "YOU ACCOUNT KEY".
_PLACEHOLDER_MARKERS = ("_guid", "your", "you account", "changeme", "placeholder")

# Directories the package writes into. Nothing else creates these, and pandas
# `to_csv` / `open(..., "w")` do not create parents -- see notebooks/00_setup.ipynb.
_WRITABLE_DIRS = (OSI_DIR, OSI_DASHBOARD_CONTEXT, OUTPUT_DIR, MCP_DATA)

# Configs every user must customise. The rest are feature-specific and may
# legitimately stay untouched, so they are reported as notes rather than failures.
_REQUIRED_CONFIGS = ("user_d.yml", "jupyter_objects_d.yml")
_OPTIONAL_PURPOSE = {
    "API_KEY.env": "RAG notebooks and the MCP servers",
    "mstr_redis_y.yml": "Redis-backed metadata analysis",
    "dans_migrations.yml": "Azure-staged migrations",
}

# A MicroStrategy object GUID as it appears in the configs: 32 hex characters.
# Used to pick the checkable values out of jupyter_objects_d.yml and skip names,
# types and the numeric Platform Analytics element IDs.
_GUID_RE = re.compile(r"^[0-9A-F]{32}$", re.IGNORECASE)

# ``GET /api/objects/{id}`` needs the object type alongside the ID, and the config
# does not record it. The key name picks the likely type first, then the rest are
# tried, so an ID that exists is usually confirmed on the first call.
# "fold" rather than "folder" so read_out_cbe_fold_id is hinted too.
_TYPE_BY_HINT = (("fold", 8), ("search", 39), ("prompt", 10), ("dossier", 55), ("project", 32))
_TYPE_SWEEP = (3, 8, 55, 39, 10, 32, 12, 13, 4)


@dataclass
class StepResult:
    """Outcome of one setup step: a pass/fail flag plus human-readable lines."""

    title: str
    ok: bool = True
    details: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def add(self, line: str) -> None:
        self.details.append(line)

    def fail(self, line: str) -> None:
        self.problems.append(line)
        self.ok = False

    def __str__(self) -> str:
        head = f"{'OK  ' if self.ok else 'FAIL'}  {self.title}"
        lines = [head]
        lines += [f"      {d}" for d in self.details]
        lines += [f"  ->  {p}" for p in self.problems]
        return "\n".join(lines)


# ── step 1: directories ───────────────────────────────────────────────────────
def ensure_dirs() -> StepResult:
    """Create the directories the package writes into.

    ``_paths`` only computes these locations; without this step the first
    ``to_csv`` or OSI dump fails with ``FileNotFoundError``. Both ``data/`` and
    ``output/`` are gitignored, so nothing created here becomes committable.
    """
    res = StepResult("Output directories")
    for d in _WRITABLE_DIRS:
        existed = d.exists()
        try:
            d.mkdir(parents=True, exist_ok=True)
            res.add(f"{'exists ' if existed else 'created'}  {_rel(d)}")
        except OSError as exc:
            res.fail(f"could not create {d}: {exc}")
    return res


# ── step 2: config files ──────────────────────────────────────────────────────
def _example_targets() -> list[tuple[Path, Path]]:
    """Map each ``config/*.example.*`` template to the live filename it seeds."""
    pairs = []
    for example in sorted(CONFIG_DIR.glob("*.example.*")):
        live = example.with_name(example.name.replace(".example", "", 1))
        pairs.append((example, live))
    return pairs


def init_configs() -> StepResult:
    """Copy each ``*.example.*`` template to its live filename, never overwriting."""
    res = StepResult("Config files")
    pairs = _example_targets()
    if not pairs:
        res.fail(f"no *.example.* templates found in {CONFIG_DIR}")
        return res

    for example, live in pairs:
        if live.exists():
            res.add(f"kept     {_rel(live)} (already present)")
            continue
        try:
            shutil.copyfile(example, live)
            res.add(f"created  {_rel(live)} from {example.name}")
        except OSError as exc:
            res.fail(f"could not create {live}: {exc}")
    return res


def _find_placeholders(node, trail: str = "") -> list[str]:
    """Recursively collect ``path = value`` for every value still templated."""
    found = []
    if isinstance(node, dict):
        for key, value in node.items():
            found += _find_placeholders(value, f"{trail}.{key}" if trail else str(key))
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            found += _find_placeholders(value, f"{trail}[{idx}]")
    elif isinstance(node, str):
        low = node.lower()
        if any(marker in low for marker in _PLACEHOLDER_MARKERS):
            found.append(f"{trail} = {node}")
    return found


def check_configs() -> StepResult:
    """Check every live config exists, parses, and has no template values left.

    All problems are collected, so one run tells you everything still to fix.

    Only ``user_d.yml`` and ``jupyter_objects_d.yml`` can fail the step -- the
    others are feature-specific, so an untouched Redis or Azure config is a note,
    not an error, for someone who only reads objects out.
    """
    res = StepResult("Config contents")

    def report(live: Path, msg: str) -> None:
        """Fail on required configs; note the optional ones with their purpose."""
        if live.name in _REQUIRED_CONFIGS:
            res.fail(msg)
        else:
            purpose = _OPTIONAL_PURPOSE.get(live.name)
            suffix = f" -- only needed for {purpose}" if purpose else ""
            res.add(f"note     {msg}{suffix}")

    for _example, live in _example_targets():
        if not live.exists():
            report(live, f"{_rel(live)} missing -- run init_configs()")
            continue

        if live.suffix == ".env":
            text = live.read_text(encoding="utf-8")
            hits = [ln.strip() for ln in text.splitlines() if any(m in ln.lower() for m in _PLACEHOLDER_MARKERS)]
            if hits:
                report(live, f"{_rel(live)} still has template values: {'; '.join(hits)}")
            else:
                res.add(f"ok       {_rel(live)}")
            continue

        try:
            live_d = yaml.safe_load(live.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            res.fail(f"{_rel(live)} does not parse: {exc}")
            continue

        placeholders = _find_placeholders(live_d)
        if placeholders:
            shown = "; ".join(placeholders[:4])
            more = f" (+{len(placeholders) - 4} more)" if len(placeholders) > 4 else ""
            report(live, f"{_rel(live)} still has {len(placeholders)} template value(s): {shown}{more}")
        else:
            res.add(f"ok       {_rel(live)}")

    return res


# ── step 3: connection ────────────────────────────────────────────────────────
def open_connection(project_id: str | None = None, login_mode: int = 1):
    """Open a MicroStrategy connection from ``config/user_d.yml``.

    Imported lazily so this module stays importable without ``mstrio`` present.
    """
    from mstrio.connection import Connection

    if not USER_CONFIG.exists():
        raise FileNotFoundError(
            f"{USER_CONFIG} not found. Run mstr_robotics.setup.init_configs() and fill in your credentials."
        )
    user_d = yaml.safe_load(USER_CONFIG.read_text(encoding="utf-8"))
    params = user_d["conn_params"]
    conn = Connection(
        base_url=params["base_url"],
        username=params["username"],
        password=params["password"],
        project_id=project_id or params["project_id"],
        login_mode=login_mode,
    )
    conn.headers["Content-type"] = "application/json"
    return conn


def check_connection() -> StepResult:
    """Confirm the configured credentials actually authenticate."""
    res = StepResult("MicroStrategy connection")
    try:
        conn = open_connection()
    except Exception as exc:  # noqa: BLE001 - surfaced to the notebook, not swallowed
        res.fail(f"{type(exc).__name__}: {exc}")
        return res

    try:
        res.add(f"connected to {conn.base_url}")
        res.add(f"project      {getattr(conn, 'project_name', None) or conn.project_id}")
    finally:
        # mstrio prints teardown banners to stdout; keep the step output clean.
        with contextlib.redirect_stdout(io.StringIO()):
            conn.close()
    return res


# ── step 4: check the object GUIDs exist ──────────────────────────────────────
def _guid_entries(node, trail: str = "") -> list[tuple[str, str, str, object]]:
    """Collect ``(dotted_path, key, guid, declared_type)`` for every GUID value.

    ``declared_type`` is the sibling ``type`` key where the config records one
    (``single_object_d``); it saves probing when present.
    """
    entries = []
    if isinstance(node, dict):
        declared = node.get("type")
        for key, value in node.items():
            path = f"{trail}.{key}" if trail else str(key)
            if isinstance(value, str) and _GUID_RE.match(value):
                entries.append((path, str(key), value, declared))
            else:
                entries += _guid_entries(value, path)
    elif isinstance(node, list):
        key = trail.rsplit(".", 1)[-1]
        for idx, value in enumerate(node):
            path = f"{trail}[{idx}]"
            if isinstance(value, str) and _GUID_RE.match(value):
                entries.append((path, key, value, None))
            else:
                entries += _guid_entries(value, path)
    return entries


def _candidate_types(key: str, declared: object = None) -> list[int]:
    """Object types to probe for ``key``, most likely first."""
    if declared is not None:
        with contextlib.suppress(TypeError, ValueError):
            first = int(declared)
            return [first] + [t for t in _TYPE_SWEEP if t != first]

    low = key.lower()
    for hint, obj_type in _TYPE_BY_HINT:
        if hint in low:
            return [obj_type] + [t for t in _TYPE_SWEEP if t != obj_type]
    return list(_TYPE_SWEEP)


def _search_projects(conn) -> list[tuple[str, str]]:
    """``(label, project_id)`` pairs an object may live in, most likely first.

    Some configured objects -- the usage reports and element prompts -- are read
    against Platform Analytics rather than the working project, so a GUID missing
    from one is not missing from the environment.
    """
    projects = [("", conn.project_id)]
    if USER_CONFIG.exists():
        with contextlib.suppress(yaml.YAMLError, OSError, AttributeError):
            user_d = yaml.safe_load(USER_CONFIG.read_text(encoding="utf-8")) or {}
            pa_id = (user_d.get("mstr_projects") or {}).get("pa_project_id")
            if pa_id and pa_id != conn.project_id:
                projects.append(("Platform Analytics", pa_id))
    return projects


@contextlib.contextmanager
def _quiet_mstrio():
    """Mute mstrio's per-request error output for the duration of the block.

    Probing object types means most calls are expected to miss, and mstrio reports
    every miss -- as a log record on the ``mstrio`` logger tree, and on stdout.
    Both are restored afterwards so nothing else in the session gets quieter.
    """
    import logging

    from mstrio import config

    mstrio_log = logging.getLogger("mstrio")
    was_level, was_verbose = mstrio_log.level, config.verbose
    mstrio_log.setLevel(logging.CRITICAL + 1)
    config.verbose = False
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            yield
    finally:
        mstrio_log.setLevel(was_level)
        config.verbose = was_verbose


def _lookup_guid(conn, guid: str, types: list[int], projects: list[tuple[str, str]]) -> tuple[dict, str] | None:
    """Return ``(metadata, project_label)``, or ``None`` if the GUID resolves nowhere.

    A wrong type is indistinguishable from a missing object, so every candidate
    type is tried in each project before the ID is reported as gone.
    """
    from mstrio.api import objects as api_objects

    for label, project_id in projects:
        for obj_type in types:
            try:
                resp = api_objects.get_object_info(
                    connection=conn,
                    id=guid,
                    object_type=obj_type,
                    # a project (type 32) lives in the non-project area
                    project_id=None if obj_type == 32 else project_id,
                )
            except Exception:  # noqa: BLE001 - a wrong type is an expected miss, keep probing
                continue
            if not getattr(resp, "ok", False):
                continue
            body = resp.json()
            if str(body.get("id", "")).upper() == guid.upper():
                return body, label
    return None


def check_object_ids(conn) -> StepResult:
    """Check that every GUID in ``jupyter_objects_d.yml`` exists in the environment.

    Each ID is looked up in the connected project and, failing that, in the
    Platform Analytics project from ``user_d.yml`` -- the usage reports and
    element prompts live there. Only the GUID is checked; object names are not
    consulted, since they drift from what the config records. This only reads:
    nothing is written and no ID is guessed at. An ID reported as missing means
    the Object Manager package providing it is not deployed, or the object was
    recreated and needs its new GUID pasted into the config.
    """
    res = StepResult("Object GUIDs")
    target = CONFIG_DIR / "jupyter_objects_d.yml"
    if not target.exists():
        res.fail(f"{_rel(target)} missing -- run init_configs()")
        return res

    try:
        doc = yaml.safe_load(target.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        res.fail(f"{_rel(target)} does not parse: {exc}")
        return res

    entries = _guid_entries(doc)
    if not entries:
        res.fail(f"no object GUIDs found in {_rel(target)} -- nothing to check")
        return res

    projects = _search_projects(conn)
    missing = 0
    with _quiet_mstrio():
        for path, key, guid, declared in entries:
            found = _lookup_guid(conn, guid, _candidate_types(key, declared), projects)
            if found is None:
                where = " or ".join(label or "the working project" for label, _ in projects)
                res.fail(f"{path} = {guid} does not exist in {where}")
                missing += 1
            else:
                info, label = found
                res.add(f"exists   {path} = {guid}  ({info.get('name')}){f'  [{label}]' if label else ''}")

    res.add(f"{len(entries) - missing} of {len(entries)} GUID(s) resolved")
    if missing:
        res.fail(
            f"{missing} GUID(s) missing -- deploy the Object Manager package that provides them, "
            "then put the GUID into config/jupyter_objects_d.yml"
        )
    return res


# ── helpers ───────────────────────────────────────────────────────────────────
def _rel(path: Path) -> str:
    """Path relative to the repo root when possible, for readable output."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
