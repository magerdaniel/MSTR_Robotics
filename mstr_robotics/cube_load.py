import logging
import time
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

_BIT_LOADED     = 0x01   # EnumDSSCubeStates: cube is in memory
_BIT_REFRESHING = 0x08   # EnumDSSCubeStates: publish in progress


def _proj_header(project_id: str) -> dict:
    """Per-request project override. Empty -> use the connection's selected project."""
    return {"X-MSTR-ProjectID": project_id} if project_id else {}


def _publish_and_wait(conn, cube_id, project_id, max_time_s, poll_interval_s) -> str:
    """Publish a cube and poll until done. Returns 'done', 'failed', 'timeout' or 'publish_failed'.

    The project is passed per-request so one shared connection can drive cubes in
    different projects concurrently without mutating session-level state.
    """
    headers = _proj_header(project_id)
    try:
        conn.post(endpoint=f"/api/v2/cubes/{cube_id}", headers=headers)
    except Exception as exc:
        logger.warning("Publish failed for cube %s: %s", cube_id, exc)
        return "publish_failed"

    deadline = time.monotonic() + max_time_s if max_time_s else None
    while True:
        resp = conn.head(endpoint=f"/api/cubes/{cube_id}", headers=headers)
        raw = resp.headers.get("X-MSTR-CubeStatus")
        if raw is not None and not (int(raw) & _BIT_REFRESHING):
            return "done" if (int(raw) & _BIT_LOADED) else "failed"
        if deadline and time.monotonic() > deadline:
            return "timeout"
        time.sleep(poll_interval_s)


def _run_chain(conn, item, index, poll_interval_s) -> list[dict]:
    """Publish one cube, then its follow-up inline. Runs in a single worker thread."""
    results = []
    while item:
        project_id = str(item.get("project_id", ""))
        cube_id = str(item["cube_id"])
        follow_up = item.get("follow_up") or {}
        max_time_s = int(follow_up["max_time_s"]) if follow_up.get("max_time_s") else None

        logger.info("Publishing cube %s (project=%s)", cube_id, project_id or "default")
        status = _publish_and_wait(conn, cube_id, project_id, max_time_s, poll_interval_s)
        results.append({"cube_id": cube_id, "project_id": project_id, "status": status})
        logger.info("cube=%s status=%s", cube_id, status)

        # chain to the follow-up only if this cube finished cleanly
        next_id = follow_up.get("cube_id")
        item = index.get(next_id, {"cube_id": next_id, "project_id": project_id}) \
            if status == "done" and next_id else None
    return results


def handle_cube_load(conn, execution_list, max_cube_parallel=10, poll_interval_s=10) -> list[dict]:
    """Trigger and monitor MSTR cube publications in parallel over one connection.

    Each entry in execution_list supports:
        project_id (str)   — MSTR project GUID (per-request; defaults to conn's project)
        cube_id (str)      — cube to publish
        run_any_time (str) — "True" to run immediately; "False" to run only as a follow-up
        follow_up (dict)   — {cube_id: str, max_time_s: str}
                             cube to chain after this one finishes;
                             max_time_s is the poll timeout for the current cube

    Cubes with run_any_time=False are indexed for follow-up chaining only.
    A cube and its follow-ups run sequentially in one worker thread.

    Returns list of dicts: {cube_id, project_id, status}
    Status values: 'done', 'failed', 'timeout', 'publish_failed'
    """
    index = {str(item["cube_id"]): item for item in execution_list}
    immediate = [item for item in execution_list
                 if str(item.get("run_any_time", "True")).strip().lower() == "true"]

    if not immediate:
        logger.warning("No cubes with run_any_time=True — nothing to execute.")
        return []

    with ThreadPoolExecutor(max_workers=max_cube_parallel,
                            thread_name_prefix="cube_load") as executor:
        chains = [executor.submit(_run_chain, conn, item, index, poll_interval_s)
                  for item in immediate]
        return [result for future in chains for result in future.result()]
    

if __name__=="__main__":
    load_json_d_l=[
  {
    "project_id": "B7CA92F04B9FAE8D941C3E9B7E0CD754",
    "cube_id": "C751D2654E00039F8F2EA2886E3255B4",
    "run_any_time": "True"
  },
  {
    "project_id": "B7CA92F04B9FAE8D941C3E9B7E0CD754",
    "cube_id": "FCD44FCF44541D9632056DAC1814A1FC",
    "run_any_time": "True",
    "follow_up": { "max_time_s": "30" }
  },
  {
    "project_id": "B7CA92F04B9FAE8D941C3E9B7E0CD754",
    "cube_id": "604A34174813EB0FDBE256B49D0EEC76",
    "run_any_time": "True",
    "follow_up": { "cube_id": "0287179B4CB6C203D21CC7BEE0371409" }
  },
  {
    "project_id": "B7CA92F04B9FAE8D941C3E9B7E0CD754",
    "cube_id": "0287179B4CB6C203D21CC7BEE0371409",
    "run_any_time": "False",
    "follow_up": { "cube_id": "4701E4F54E94022FDFF369A81DB726C7" }
  },
  {
    "project_id": "B7CA92F04B9FAE8D941C3E9B7E0CD754",
    "cube_id": "4701E4F54E94022FDFF369A81DB726C7",
    "run_any_time": "False"
  }
]

handle_cube_load(load_json_d_l)