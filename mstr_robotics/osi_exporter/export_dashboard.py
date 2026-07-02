import pandas as pd
import numpy as np
import json
from mstr_robotics._connectors import MstrApi
from mstrio.api import browsing
from mstr_robotics.read_out_prj_obj import ReadGen
from mstr_robotics.user_rag import Perplexity
from dotenv import load_dotenv
from ruamel.yaml import YAML as RuamelYAML
from ruamel.yaml.comments import CommentedMap

i_mstr_api = MstrApi()
u_perplexity = Perplexity()
env_file = "..\\config\\streamlit.env"

load_dotenv(env_file)
i_read_gen = ReadGen()


# ── MSTR → OSI type mappings ──────────────────────────────────────────────────
MSTR_VIZ_TYPE_MAP = {
    "Grid": "table", "BarChart": "bar_chart", "LineChart": "line_chart",
    "PieChart": "pie_chart", "BubbleChart": "scatter_plot", "ComboChart": "combo_chart",
    "HeatMap": "heatmap", "TreeMap": "treemap", "Selector": "filter_control",
    "GaugeChart": "gauge", "KpiWidget": "kpi_card", "GeoChart": "geo_map",
    "WaterfallChart": "waterfall", "FunnelChart": "funnel", "PanelStack": "container",
    "TextBox": "text_box", "Button": "button", "Image": "image",
}
MSTR_SEL_TYPE_MAP = {
    "attribute_element_list":  "element_selection",
    "metric_qualification":    "expression",
    "object_replacement":      "object_selection",
    "visualization_as_filter": "element_selection",
}


# ── utilities ─────────────────────────────────────────────────────────────────
def _mstr_ext(data_d):
    def _np_default(o):
        if isinstance(o, np.integer):  return int(o)
        if isinstance(o, np.floating): return float(o)
        if isinstance(o, np.bool_):    return bool(o)
        if isinstance(o, np.ndarray):  return o.tolist()
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
    return {"vendor_name": "MSTR_ROBOTICS", "data": json.dumps(data_d, default=_np_default)}

def _map_viz_type(mstr_type):
    return MSTR_VIZ_TYPE_MAP.get(mstr_type, "custom")

def _cm(d: dict, name_comment: str = None) -> CommentedMap:
    """
    Wrap *d* in a CommentedMap.
    If *name_comment* is given it is added as an inline YAML comment on the 'name' key:
        name: <id>  # <human-readable label>
    """
    cm = CommentedMap(d)
    if name_comment:
        cm.yaml_add_eol_comment(name_comment, key="name")
    return cm


# ── dataset / semantic-model helpers ─────────────────────────────────────────
def parse_ds_obj(obj_def_d):
    rows_l = []
    for ds in obj_def_d["datasets"]:
        ds_id, ds_name = ds["id"], ds["name"]
        row_ids    = {o["id"] for o in ds.get("rows", [])}
        pageby_ids = {o["id"] for o in ds.get("pageBy", [])}
        col_ids    = set()
        for c in ds.get("columns", []):
            if c["type"] == "templateMetrics":
                for e in c.get("elements", []): col_ids.add(e["id"])
            else:
                col_ids.add(c["id"])
        for obj in ds.get("availableObjects", []):
            placement = []
            if obj["id"] in row_ids:    placement.append("rows")
            if obj["id"] in col_ids:    placement.append("columns")
            if obj["id"] in pageby_ids: placement.append("pageBy")
            placement_str = ",".join(placement) if placement else "available"
            base = {"dataset_id": ds_id, "dataset_name": ds_name,
                    "obj_id": obj["id"], "obj_name": obj["name"],
                    "obj_type": obj["type"], "placement": placement_str}
            forms = obj.get("forms", [])
            if forms:
                for f in forms:
                    rows_l.append({**base, "form_id": f["id"], "form_name": f["name"],
                                   "dataType": f["dataType"], "baseFormCategory": f["baseFormCategory"],
                                   "baseFormType": f["baseFormType"]})
            else:
                rows_l.append({**base, "form_id": None, "form_name": None,
                               "dataType": None, "baseFormCategory": None, "baseFormType": None})
    return pd.DataFrame(rows_l)


def find_dataset_relationships(obj_def_d):
    df = parse_ds_obj(obj_def_d)
    attr_df = (df[df["obj_type"] == "attribute"][["dataset_id", "dataset_name", "obj_id", "obj_name"]]
               .drop_duplicates())
    ds_order    = {ds["id"]: i for i, ds in enumerate(obj_def_d["datasets"])}
    shared_attr = attr_df.groupby("obj_id").filter(lambda g: g["dataset_id"].nunique() > 1)
    rels = []
    for obj_id, grp in shared_attr.groupby("obj_id", sort=False):
        ds_list = (grp.drop_duplicates("dataset_id")
                   .assign(order=lambda x: x["dataset_id"].map(ds_order))
                   .sort_values("order").to_dict("records"))
        attr_name = ds_list[0]["obj_name"]
        for i in range(len(ds_list)):
            for j in range(i + 1, len(ds_list)):
                rels.append({"from_dataset_id": ds_list[i]["dataset_id"],
                             "from_dataset_name": ds_list[i]["dataset_name"],
                             "to_dataset_id": ds_list[j]["dataset_id"],
                             "to_dataset_name": ds_list[j]["dataset_name"],
                             "attr_id": obj_id, "attr_name": attr_name})
    return pd.DataFrame(rels) if rels else pd.DataFrame(
        columns=["from_dataset_id","from_dataset_name","to_dataset_id","to_dataset_name","attr_id","attr_name"])


def _build_osi_field(attr_rows, cx=False) -> list:
    """
    Returns one CommentedMap per form (or one entry if no forms).
    name  : <attr_guid>@<form_guid>   # AttrName (FormName)
    name  : <attr_guid>               # AttrName          (no-form fallback)
    """
    first     = attr_rows.iloc[0]
    form_rows = attr_rows[attr_rows["form_id"].notna()]

    if form_rows.empty:
        entry = _cm({
            "name":       first["obj_id"],
            "expression": {"dialects": [{"dialect": "MSTR_ROBOTICS", "expression": first["obj_id"]}]},
        }, name_comment=first["obj_name"])
        if cx:
            entry["custom_extensions"] = [_mstr_ext(
                {"obj_id": first["obj_id"], "obj_type": first["obj_type"], "forms": []})]
        return [entry]

    entries = []
    for _, r in form_rows.iterrows():
        name    = f"{first['obj_id']}@{r['form_id']}"
        comment = f"{first['obj_name']} ({r['form_name']})"
        entry   = _cm({
            "name":       name,
            "expression": {"dialects": [{"dialect": "MSTR_ROBOTICS", "expression": name}]},
        }, name_comment=comment)
        if cx:
            entry["custom_extensions"] = [_mstr_ext({
                "obj_id": first["obj_id"], "obj_type": first["obj_type"],
                "form_id": r["form_id"], "form_name": r["form_name"],
                "dataType": r["dataType"], "baseFormCategory": r["baseFormCategory"],
            })]
        entries.append(entry)
    return entries


def _build_osi_metric_field(row, cx=False):
    """name: <obj_guid>  # MetricName"""
    result = _cm({
        "name":       row["obj_id"],
        "expression": {"dialects": [{"dialect": "MSTR_ROBOTICS", "expression": row["obj_id"]}]},
    }, name_comment=row["obj_name"])
    if cx:
        result["custom_extensions"] = [_mstr_ext({"obj_id": row["obj_id"], "obj_type": row["obj_type"]})]
    return result


def _build_osi_dataset(ds_rows, cx=False):
    """name: <dataset_guid>  # DatasetName"""
    first = ds_rows.iloc[0]
    # attributes: _build_osi_field returns a list (one entry per form)
    attr_fields = []
    for _, grp in ds_rows[ds_rows["obj_type"] == "attribute"].groupby("obj_id", sort=False):
        attr_fields.extend(_build_osi_field(grp.reset_index(drop=True), cx=cx))
    metric_fields = [
        _build_osi_metric_field(r, cx=cx)
        for _, r in ds_rows[ds_rows["obj_type"] == "metric"].drop_duplicates(subset="obj_id").iterrows()
    ]
    fields  = attr_fields + metric_fields
    dataset = _cm({
        "name":   first["dataset_id"],
        "source": first["dataset_id"],
    }, name_comment=first["dataset_name"])
    if cx:
        dataset["custom_extensions"] = [_mstr_ext(
            {"dataset_id": first["dataset_id"], "dataset_name": first["dataset_name"]})]
    if fields:
        dataset["fields"] = fields
    return dataset


def _build_osi_metrics(df, cx=False):
    """name: <obj_guid>  # MetricName"""
    result = []
    for _, r in df[df["obj_type"] == "metric"].drop_duplicates(subset="obj_id").iterrows():
        m = _cm({
            "name":       r["obj_id"],
            "expression": {"dialects": [{"dialect": "MSTR_ROBOTICS", "expression": r["obj_id"]}]},
        }, name_comment=r["obj_name"])
        if cx:
            m["custom_extensions"] = [_mstr_ext({"obj_id": r["obj_id"], "obj_type": r["obj_type"]})]
        result.append(m)
    return result


def _build_osi_relationships(obj_def_d, cx=False):
    """name: <from_guid>__<to_guid>  # FromName__ToName"""
    rel_df = find_dataset_relationships(obj_def_d)
    if rel_df.empty:
        return []
    rels = []
    for (from_id, to_id), grp in rel_df.groupby(["from_dataset_id", "to_dataset_id"], sort=False):
        first     = grp.iloc[0]
        from_name = first["from_dataset_name"]
        to_name   = first["to_dataset_name"]
        attr_ids  = grp["attr_id"].tolist()   # IDs as join keys
        rel = _cm({
            "name":         f"{from_id}__{to_id}",
            "from":         from_id,
            "to":           to_id,
            "from_columns": attr_ids,
            "to_columns":   list(attr_ids),
        }, name_comment=f"{from_name}__{to_name}")
        if cx:
            join_keys = [{"attr_name": r["attr_name"], "attr_id": r["attr_id"]} for _, r in grp.iterrows()]
            rel["custom_extensions"] = [_mstr_ext({
                "from_dataset_id": from_id, "from_dataset_name": from_name,
                "to_dataset_id":   to_id,   "to_dataset_name":   to_name,
                "join_keys":       join_keys,
            })]
        rels.append(rel)
    return rels


def build_osi_semantic_model(conn, dossier_id, custom_extensions=False):
    """name: <dossier_guid>  # DossierName"""
    cx = custom_extensions
    OBJ_TYPE_DOSSIER, OBJ_SUBTYPE_DOSSIER = 55, 14081
    obj_def_d = i_read_gen.get_obj_def(conn=conn, object_id=dossier_id,
                                       obj_type=OBJ_TYPE_DOSSIER, obj_sub_type=OBJ_SUBTYPE_DOSSIER)
    df            = parse_ds_obj(obj_def_d)
    datasets      = [_build_osi_dataset(grp.reset_index(drop=True), cx=cx)
                     for _, grp in df.groupby("dataset_id", sort=False)]
    relationships = _build_osi_relationships(obj_def_d, cx=cx)
    metrics       = _build_osi_metrics(df, cx=cx)

    sm = _cm({"name": dossier_id, "datasets": datasets},
             name_comment=obj_def_d.get("name", ""))
    if relationships: sm["relationships"] = relationships
    if metrics:       sm["metrics"]       = metrics

    return {"version": "0.2.0", "semantic_model": [sm]}


# ── dashboard helpers ─────────────────────────────────────────────────────────
def _build_selector_filter(grp, key_to_name, cx=False):
    """name: <sel_filt_key>  # SelectorName"""
    first    = grp.iloc[0]
    sel_type = first["selector_type"]
    osi_type = MSTR_SEL_TYPE_MAP.get(sel_type, "element_selection")
    # targets stay as visual_keys (already IDs)
    target_keys = grp["target_key"].dropna().unique().tolist() if "target_key" in grp.columns else []
    filt = _cm({"name": first["sel_filt_key"], "type": osi_type},
               name_comment=first["sel_filt_name"])
    if target_keys:
        filt["target"] = {"scope": "visualization", "specific_targets": target_keys}
    if sel_type == "attribute_element_list":
        filt["definition"] = {"attribute": first.get("target_object_name", ""), "selection_type": "include"}
    elif sel_type == "metric_qualification":
        filt["definition"] = {"expression": str(first.get("summary", "")),
                               "applies_to": {"metric": first.get("target_object_name", "")}}
    elif sel_type == "object_replacement":
        filt["definition"] = {"object_type": first.get("target_object_type", ""),
                               "available_objects": grp["target_object_name"].dropna().unique().tolist()}
    elif sel_type == "visualization_as_filter":
        filt["definition"] = {"attribute": first.get("target_object_name", ""), "selection_type": "include"}
    if cx:
        filt["custom_extensions"] = [_mstr_ext({
            "sel_filt_key": first["sel_filt_key"], "selector_type": sel_type,
            "display_style": first.get("display_style", ""),
            "has_all_option": bool(first.get("has_all_option", False)),
        })]
    return filt


def _semantic_ref(obj_rows, ref_type, cx=False) -> list:
    """
    Returns one CommentedMap per form (or one entry if no forms).
    name: <obj_guid>@<form_guid>  # ObjName (FormName)
    name: <obj_guid>              # ObjName               (metric / no-form)
    """
    first     = obj_rows.iloc[0]
    form_rows = obj_rows[obj_rows["form_id"].notna()] if "form_id" in obj_rows.columns else pd.DataFrame()

    if form_rows.empty or ref_type == "metric":
        r = _cm({"name": first["object_id"], "type": ref_type},
                name_comment=first["object_name"])
        if cx:
            r["custom_extensions"] = [_mstr_ext({
                "object_id": first["object_id"], "object_name": first["object_name"],
                "type": first["type"], "row_col_fg": first.get("row_col_fg"),
                "row_col_nr": first.get("row_col_nr"),
            })]
        return [r]

    entries = []
    for _, row in form_rows.iterrows():
        name    = f"{first['object_id']}@{row['form_id']}"
        comment = f"{first['object_name']} ({row['form_name']})"
        r = _cm({"name": name, "type": ref_type}, name_comment=comment)
        if cx:
            r["custom_extensions"] = [_mstr_ext({
                "object_id": first["object_id"], "object_name": first["object_name"],
                "type": first["type"], "form_id": row["form_id"], "form_name": row["form_name"],
                "row_col_fg": first.get("row_col_fg"), "row_col_nr": first.get("row_col_nr"),
            })]
        entries.append(r)
    return entries


def _build_visualization(viz_rows, cx=False):
    """name: <visual_key>  # VisualName"""
    first     = viz_rows.iloc[0]
    attr_rows = viz_rows[viz_rows["type"] == "attribute"]
    met_rows  = viz_rows[viz_rows["type"] == "metric"]

    dims = []
    for _, grp in attr_rows.groupby("object_id", sort=False):
        dims.extend(_semantic_ref(grp.reset_index(drop=True), "field",  cx=cx))
    mets = []
    for _, grp in met_rows.groupby("object_id", sort=False):
        mets.extend(_semantic_ref(grp.reset_index(drop=True), "metric", cx=cx))

    viz = _cm({"name": first["visual_key"], "type": _map_viz_type(first["visualizationType"])},
              name_comment=first["visual_name"])
    if cx:
        viz["custom_extensions"] = [_mstr_ext({
            "visual_key": first["visual_key"], "visualizationType": first["visualizationType"],
        })]
    if dims: viz["dimensions"] = dims
    if mets: viz["metrics"]    = mets
    return viz


def _build_page(page_rows, page_sel_df, key_to_name, cx=False):
    """name: <page_key>  # PageName"""
    first        = page_rows.iloc[0]
    page_filters = []
    if not page_sel_df.empty:
        for _, grp in page_sel_df.groupby("sel_filt_key", sort=False):
            page_filters.append(_build_selector_filter(grp.reset_index(drop=True), key_to_name, cx=cx))
    vizs = [_build_visualization(grp.reset_index(drop=True), cx=cx)
            for _, grp in page_rows.groupby("visual_key", sort=False)]
    page = _cm({"name": first["page_key"]}, name_comment=first["page_name"])
    if cx:
        page["custom_extensions"] = [_mstr_ext({"page_key": first["page_key"]})]
    if page_filters: page["filters"]        = page_filters
    if vizs:         page["visualizations"] = vizs
    return page


def _build_chapter(chap_rows, chap_filt_df, chap_sel_df, key_to_name, cx=False):
    """name: <chapter_key>  # ChapterName"""
    first        = chap_rows.iloc[0]
    chap_filters = []
    if not chap_filt_df.empty:
        for _, grp in chap_filt_df.groupby("sel_filt_key", sort=False):
            chap_filters.append(_build_selector_filter(grp.reset_index(drop=True), key_to_name, cx=cx))
    pages = []
    for page_key, page_rows in chap_rows.groupby("page_key", sort=False):
        page_sel = (chap_sel_df[chap_sel_df["page_key"] == page_key]
                    if not chap_sel_df.empty else pd.DataFrame())
        pages.append(_build_page(page_rows.reset_index(drop=True), page_sel, key_to_name, cx=cx))
    chapter = _cm({"name": first["chapter_key"]}, name_comment=first["chapter_name"])
    if cx:
        chapter["custom_extensions"] = [_mstr_ext({"chapter_key": first["chapter_key"]})]
    if chap_filters: chapter["filters"] = chap_filters
    if pages:        chapter["pages"]   = pages
    return chapter


def build_osi_dashboard(doss_hier_l, filt_sel_d, semantic_model_name, custom_extensions=False):
    """name: <dossier_id>  # DossierName"""
    cx      = custom_extensions
    hier_df = pd.DataFrame(doss_hier_l)
    filt_df = pd.DataFrame(filt_sel_d.get("dos_filt_d_l", []))
    sel_df  = pd.DataFrame(filt_sel_d.get("page_selector_d_l", []))
    key_to_name = (hier_df.drop_duplicates("visual_key").set_index("visual_key")["visual_name"].to_dict()
                   if not hier_df.empty else {})
    dashboards = []
    for dossier_id, doss_rows in hier_df.groupby("dossier_id", sort=False):
        first     = doss_rows.iloc[0]
        doss_filt = filt_df[filt_df["dossier_id"] == dossier_id] if not filt_df.empty else pd.DataFrame()
        doss_sel  = sel_df[sel_df["dossier_id"]   == dossier_id] if not sel_df.empty  else pd.DataFrame()
        chapters  = []
        for chap_key, chap_rows in doss_rows.groupby("chapter_key", sort=False):
            chap_filt = doss_filt[doss_filt["chapter_key"] == chap_key] if not doss_filt.empty else pd.DataFrame()
            chap_sel  = doss_sel[doss_sel["chapter_key"]   == chap_key] if not doss_sel.empty  else pd.DataFrame()
            chapters.append(_build_chapter(chap_rows.reset_index(drop=True),
                                           chap_filt, chap_sel, key_to_name, cx=cx))
        db = _cm({"name": dossier_id, "semantic_model": semantic_model_name, "chapters": chapters},
                 name_comment=first["dossier_name"])
        if cx:
            db["custom_extensions"] = [_mstr_ext({"dossier_id": dossier_id})]
        dashboards.append(db)
    return dashboards


# ── misc helpers ──────────────────────────────────────────────────────────────
def fetch_json_search(conn, search_instance_resp, limit=100):
    dpn_count = search_instance_resp.json()["totalItems"]
    try:
        if dpn_count > 0:
            full_result_d_l, fetched = [], 0
            while fetched < dpn_count:
                search_d_l = browsing.get_search_results(
                    connection=conn, search_id=search_instance_resp.json()["id"],
                    project_id=conn.project_id, offset=fetched, limit=limit).json()
                full_result_d_l.extend(search_d_l)
                fetched += limit
    except Exception as e:
        print(e)
    return full_result_d_l

def read_out_obj(conn, search_instance_resp, search_obj):
    osi_list = fetch_json_search(conn, search_instance_resp, limit=100)
    ol = [{"id": o["id"], "type": o["type"], "subtype": o["subtype"]}
          for o in osi_list if o["subtype"] in [3072, 768, 1024, 3840, 3328]]
    ol.append(search_obj)
    return [i_read_gen.get_obj_def(conn=conn, object_id=o["id"],
                                   obj_type=o["type"], obj_sub_type=o["subtype"]) for o in ol]

def osi_yaml_dump(osi_d: dict, out_path: str):
    """Write *osi_d* to *out_path* using ruamel.yaml so inline comments are preserved."""
    ry = RuamelYAML()
    ry.default_flow_style = False
    ry.allow_unicode = True
    ry.width = 4096          # avoid mid-value line wrapping
    with open(out_path, "w", encoding="utf-8") as f:
        ry.dump(osi_d, f)


def add_ai_context(osi_d: dict, path: str, ai_context: str, out_path: str = None) -> dict:
    """
    Add *ai_context* to every object reachable via *path* inside *osi_d*.

    Parameters
    ----------
    osi_d      : OSI document dict
    path       : dot-separated key path to the list whose items receive the
                 ai_context field.  Each segment is a dict key; list items are
                 traversed automatically.  Examples:
                     "semantic_model"
                     "dashboards"
                     "dashboards.chapters"
                     "dashboards.chapters.pages"
                     "dashboards.chapters.pages.visualizations"
                     "dashboards.chapters.filters"
                     "dashboards.chapters.pages.filters"
    ai_context : free-text description for AI agents consuming this OSI file
    out_path   : optional – if given, write the enriched document to this path

    Returns
    -------
    *osi_d* mutated in-place (and optionally written to disk)

    Examples
    --------
    from mstr_robotics.osi_exporter.export_dashboard import add_ai_context

    # top-level semantic model
    add_ai_context(osi_d, "semantic_model", "Retail KPI model – brand & item analysis.")

    # every chapter inside every dashboard
    add_ai_context(osi_d, "dashboards.chapters", "Chapter-level context for AI routing.")

    # write after all paths are enriched
    add_ai_context(osi_d, "dashboards", "Sales overview EMEA.", out_path="out.yml")
    """
    def _set(node):
        if hasattr(node, "insert"):
            # CommentedMap: insert right after 'name' so ai_context appears
            # near the top of the block, not after deeply-nested children
            keys_list = list(node.keys())
            pos = (keys_list.index("name") + 1) if "name" in keys_list else 0
            node.insert(pos, "ai_context", ai_context)
        else:
            node["ai_context"] = ai_context

    def _walk(node, keys):
        if not keys:
            _set(node)
            return
        key   = keys[0]
        child = node.get(key) if isinstance(node, dict) else None
        if child is None:
            return
        rest = keys[1:]
        if isinstance(child, list):
            for item in child:
                _walk(item, rest)
        else:
            _walk(child, rest)

    _walk(osi_d, path.split("."))

    if out_path:
        osi_yaml_dump(osi_d, out_path)
        print(f"[ai_context @ {path}] Written → {out_path}")

    return osi_d