"""Tool group *object analysis* for the mstr_osi_mcp server.

Tools that inspect MicroStrategy metadata objects:
  - get_object_definitions — fetch object definitions for a list of GUIDs
  - resolve_object_by_path — resolve a folder path to an object's identity
"""

import json

from mstr_robotics.mcp_servers._server_config import get_conn
from mstr_robotics.read_out_prj_obj import ReadGen


def get_object_definitions(
    guid_list: list[str],
    project_id: str = "",
) -> str:
    """Fetch MicroStrategy object definitions for a list of GUIDs.

    For each GUID the tool resolves type and subtype via the metadata search
    API and then fetches the full object definition.

    Args:
        guid_list:  List of MicroStrategy object GUIDs.
        project_id: MicroStrategy project ID. Leave empty to use the default.
    """
    try:
        conn = get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    i_read_gen = ReadGen()
    try:
        obj_def_l = i_read_gen.get_proj_obj_def_by_id_l(conn=conn, obj_id_l=guid_list)
    except Exception as e:
        return f"ERROR – could not fetch object definitions: {e}"

    result = {
        "definitions": obj_def_l,
        "errors": i_read_gen.obj_read_error_d_l,
        "not_mapped": i_read_gen.obj_not_mapped_d_l,
        "summary": {
            "requested": len(guid_list),
            "fetched": len(obj_def_l),
            "errors": len(i_read_gen.obj_read_error_d_l),
            "not_mapped": len(i_read_gen.obj_not_mapped_d_l),
        },
    }
    return json.dumps(result, indent=2, ensure_ascii=False, default=str)


def resolve_object_by_path(
    path_str: str,
    project_id: str = "",
    top_folder_id: str = "D3C7D461F69C4610AA6BAA5EF51F4125",
) -> str:
    """Resolve a semicolon-separated MSTR folder path to an object's ID, type and subtype.

    Traverses the folder hierarchy segment by segment, starting from the
    Shared Reports root, and returns the identity of the final object so it
    can be passed to other tools (e.g. export_report_tabular,
    get_visualization_data).

    Args:
        path_str:           Folder path with segments separated by ' ; '.
                            Example:
                            "Shared Reports ; MSTR_Robotics ; Ontologies ; Regional Marketing Ofensive 2024"
        project_id:         MicroStrategy project ID. Leave empty to use the default.
        top_folder_id:      GUID of the root folder. Defaults to Shared Reports.
    """
    try:
        conn = get_conn(project_id or None)
    except Exception as e:
        return f"ERROR – could not connect to MicroStrategy: {e}"

    try:
        result = ReadGen().get_obj_id_by_path(
            conn=conn,
            path_str=path_str,
            top_folder_id=top_folder_id,
        )
    except Exception as e:
        return f"ERROR – path resolution failed: {e}"

    if result is None:
        return json.dumps({"error": f"Object not found for path: {path_str}"})

    return json.dumps(result, indent=2, ensure_ascii=False, default=str)
