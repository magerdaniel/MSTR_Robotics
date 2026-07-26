"""First-time setup helpers, driven by ``mstr_robotics/notebooks/00_setup.ipynb``.

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
    print(setup.discover_object_ids(conn))
"""

from __future__ import annotations

import contextlib
import io
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
# `to_csv` / `open(..., "w")` do not create parents -- see mstr_robotics/notebooks/00_setup.ipynb.
_WRITABLE_DIRS = (OSI_DIR, OSI_DASHBOARD_CONTEXT, OUTPUT_DIR, MCP_DATA)

# Configs every user must customise. The rest are feature-specific and may
# legitimately stay untouched, so they are reported as notes rather than failures.
_REQUIRED_CONFIGS = ("user_d.yml", "jupyter_objects_d.yml")
_OPTIONAL_PURPOSE = {
    "API_KEY.env": "RAG notebooks and the MCP servers",
    "mstr_redis_y.yml": "Redis-backed metadata analysis",
    "dans_migrations.yml": "Azure-staged migrations",
}


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

    for example, live in _example_targets():
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

        try:
            example_d = yaml.safe_load(example.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        if live_d == example_d and not placeholders:
            # The shipped examples carry a reference environment's real GUIDs, so an
            # untouched copy silently points at the wrong MicroStrategy project. This
            # is the only signal for jupyter_objects_d.yml, which has no placeholders.
            report(
                live, f"{_rel(live)} is unchanged from {example.name}, so it still points at the reference environment"
            )

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


# ── step 4: resolve object GUIDs ──────────────────────────────────────────────
def _name_id_pairs(node, trail: str = "") -> list[tuple[dict, str, str, str]]:
    """Find every ``<x>_name`` key that has a sibling ``<x>_id`` in the same dict.

    Returns ``(owning_dict, name_key, id_key, dotted_path)`` so the caller can
    write the resolved GUID straight back into the loaded document.
    """
    pairs = []
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{trail}.{key}" if trail else str(key)
            if isinstance(key, str) and key.endswith("_name") and isinstance(value, str):
                id_key = key[: -len("_name")] + "_id"
                if id_key in node:
                    pairs.append((node, key, id_key, path))
            pairs += _name_id_pairs(value, path)
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            pairs += _name_id_pairs(value, f"{trail}[{idx}]")
    return pairs


def _search_by_name(conn, name: str) -> list[dict]:
    """Exact-name search across the connected project.

    Mirrors the store-instance / fetch-results pattern already used by
    ``osi_exporter.export_dashboard.fetch_json_search``.
    """
    from mstrio.api import browsing

    instance = browsing.store_search_instance(
        connection=conn,
        project_id=conn.project_id,
        name=name,
        pattern=2,  # SearchPattern.EXACTLY
    )
    total = instance.json().get("totalItems", 0)
    if not total:
        return []
    return browsing.get_search_results(
        connection=conn,
        search_id=instance.json()["id"],
        project_id=conn.project_id,
        offset=0,
        limit=100,
    ).json()


def discover_object_ids(conn, dry_run: bool = False) -> StepResult:
    """Fill the ``*_id`` GUIDs in ``jupyter_objects_d.yml`` by searching on ``*_name``.

    Deploying the Object Manager packages creates objects whose GUIDs differ per
    environment, but their *names* are fixed by the packages. This resolves each
    name against the connected project and writes the GUID back, preserving
    comments via ruamel.yaml. Names that match zero or several objects are
    reported and left untouched rather than guessed at.
    """
    from ruamel.yaml import YAML

    res = StepResult("Object GUID discovery" + (" (dry run)" if dry_run else ""))
    target = CONFIG_DIR / "jupyter_objects_d.yml"
    if not target.exists():
        res.fail(f"{_rel(target)} missing -- run init_configs()")
        return res

    ry = YAML()
    ry.preserve_quotes = True
    with target.open(encoding="utf-8") as fh:
        doc = ry.load(fh)

    pairs = _name_id_pairs(doc)
    if not pairs:
        res.fail("no <x>_name / <x>_id pairs found -- nothing to resolve")
        return res

    resolved = 0
    for owner, name_key, id_key, path in pairs:
        obj_name = owner[name_key]
        try:
            matches = _search_by_name(conn, obj_name)
        except Exception as exc:  # noqa: BLE001 - one bad name must not stop the rest
            res.fail(f"{path}: search failed for {obj_name!r}: {type(exc).__name__}: {exc}")
            continue

        if not matches:
            res.fail(f"{path}: no object named {obj_name!r} -- is the Object Manager package deployed?")
        elif len(matches) > 1:
            kinds = ", ".join(sorted({f"type {m.get('type')}/{m.get('subtype')}" for m in matches}))
            res.fail(f"{path}: {len(matches)} objects named {obj_name!r} ({kinds}) -- set {id_key} by hand")
        else:
            guid = matches[0]["id"]
            if owner[id_key] == guid:
                res.add(f"unchanged  {id_key} = {guid}")
            else:
                if not dry_run:
                    owner[id_key] = guid
                res.add(f"resolved   {id_key} = {guid}  ({obj_name})")
                resolved += 1

    if resolved and not dry_run:
        with target.open("w", encoding="utf-8") as fh:
            ry.dump(doc, fh)
        res.add(f"wrote {resolved} GUID(s) to {_rel(target)}")
    elif dry_run:
        res.add(f"{resolved} GUID(s) would change; nothing written")

    return res


# ── helpers ───────────────────────────────────────────────────────────────────
def _rel(path: Path) -> str:
    """Path relative to the repo root when possible, for readable output."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
