import json
import time

import pandas as pd
from mstrio.api import cubes, reports
from mstrio.api.cubes import cube_definition
from mstrio.project_objects import OlapCube
from mstrio.project_objects.datasets import super_cube
from mstrio.project_objects.datasets.cube import CubeStates, _Cube
from mstrio.project_objects.report import Report
from mstrio.utils import parser

from mstr_robotics._connectors import MstrApi
from mstr_robotics._helper import Misc, StrFunc
from mstr_robotics.mstr_pandas import DfHelper

i_mstr_api = MstrApi()
i_msic = Misc()
i_str_func = StrFunc()


class Rep:
    def __init__(self):
        self.i_reports = reports
        self.i_parser = parser
        self.i_df_helper = DfHelper()

    def open_Instance(self, conn, report_id):
        rep_instance = self.i_reports.report_instance(connection=conn, report_id=report_id)
        return rep_instance.json()["instanceId"]

    def get_open_prompts(self, conn, report_id, instance_id):
        open_prompts = self.i_reports.get_prompted_instance(
            connection=conn, report_id=report_id, instance_id=instance_id, closed=False
        )
        return open_prompts.json()

    def get_open_prp_stat(self, conn, report_id, instance_id):
        return i_mstr_api.get_open_prp_stat(conn=conn, report_id=report_id, instance_id=instance_id)

    def set_inst_prompt_ans(self, conn, report_id, instance_id, prompt_answ):
        prompt_answ_url = f"{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/answers"
        ret_prompt_ans = conn.put(prompt_answ_url, data=prompt_answ)
        return ret_prompt_ans

    def report_dict(self, conn, report_id, instance_id):
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        report_dict = self.i_parser.Parser(report_ds.json())._Parser__map_attributes(report_ds.json())
        return report_dict

    def save_rep_inst(
        self,
        conn,
        instance_id,
        rep_name,
        save_mode="OVERWRITE",
        promptOption="static",
        setCurrentAsDefaultAnswer=True,
        folder_id=None,
    ):

        data = {
            "name": rep_name,
            "description": "string",
            "folderId": folder_id,
            "saveMode": save_mode,
            "displayMode": "GRID",
            "promptOption": promptOption,
            "setCurrentAsDefaultAnswer": setCurrentAsDefaultAnswer,
            "instanceId": instance_id,
        }
        inst_u = f"{conn.base_url}/api/reports"
        r = conn.post(inst_u, data=json.dumps(data))
        return r

    def bld_export_att_names(self, rep_def_resp):
        att_col_l = []
        for col in rep_def_resp.json()["definition"]["grid"]["rows"]:
            for col_form in col["forms"]:
                att_col_l.append(str(col["name"]).replace(" ", "_") + "@" + str(col_form["name"]).replace(" ", "_"))
        return att_col_l

    def get_rep_attributes(self, conn, report_id):
        rep_def_resp = reports.report_definition(connection=conn, report_id=report_id)
        att_col_l = self.bld_export_att_names(rep_def_resp=rep_def_resp)
        return att_col_l

    def bld_df_of_mstr(self, mstr_data_ds, att_col_l):
        # create attribute data frame, then re-map integer
        # array with corresponding attribute element values  # noqa
        r = self.i_parser.Parser(mstr_data_ds)
        r.parse(mstr_data_ds)

        attribute_df = pd.DataFrame(data=r._mapped_attributes, columns=att_col_l)
        # create metric values data frame
        metric_df = pd.DataFrame(data=r._metric_values_raw, columns=r._metric_col_names)
        mstr_df = pd.concat([attribute_df, metric_df], axis=1)
        return mstr_df

    def rep_to_dataframe(self, conn, report_id, instance_id, att_col_l=None):

        if att_col_l is None:
            att_col_l = self.get_rep_attributes(conn=conn, report_id=report_id)

        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        # create attribute data frame, then re-map integer array with corresponding attribute element values  # noqa
        r = self.i_parser.Parser(report_ds.json())
        r.parse(report_ds.json())
        attribute_df = pd.DataFrame(data=r._mapped_attributes, columns=att_col_l)

        metric_df = pd.DataFrame(data=r._metric_values_raw, columns=r._metric_col_names)
        mstr_df = pd.concat([attribute_df, metric_df], axis=1)

        return mstr_df

    def report_df(self, conn, report_id, instance_id):
        self.i_po_Report = Report(connection=conn, id=report_id, instance_id=instance_id)
        return self.i_po_Report.to_dataframe()

    def get_report_all_def(self, conn, report_id, *args, **kwargs):
        return i_mstr_api.get_report_all_def(conn=conn, report_id=report_id, *args, **kwargs)

    def get_report_def(self, conn, report_id):
        return self.i_reports.report_definition(connection=conn, report_id=report_id)

    def get_rep_prp_l(self, conn, report_id):
        rep = Report(connection=conn, id=report_id)
        return rep.prompts

    def report_has_data(self, conn, report_id, instance_id):
        report_has_data_fg = False
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        if len(report_ds.json()["definition"]["grid"]["rows"][0]["elements"]) > 0:
            report_has_data_fg = True
        return report_has_data_fg

    def bld_rep_library_url(self, conn, report_id):
        rep_url = f"{conn.base_url}/app/{conn.project_id}/{report_id}/share"
        html_link = f'<a href={rep_url} target="_blank">Jump to MSTR Library</a>'
        return html_link

    def web_base_url(self, conn, report_id):
        web_base_url = i_str_func.web_base_url(base_url=conn.base_url)
        server = i_str_func.get_server_base_url(base_url=conn.base_url)
        project_name = i_str_func.get_project_name_base_url(project_name=conn.project_name)
        rep_url = f"{web_base_url}Server={server}&Project={project_name}&evt=4001&src=mstrWeb.4001&reportViewMode=1&reportID={report_id}&currentViewMedia=2"
        print(rep_url)
        # rep_url = f'{conn.base_url}/app/{conn.project_id}/{report_id}/share'
        html_link = f'<a href={rep_url} target="_blank">Jump to MSTR Web</a>'
        return html_link

    def bld_free_form_rep_df(self, conn, report_id, prp_answ_d=None):
        # historically needed to adjust column names
        # can probally be deleted

        instance_id = self.open_Instance(conn=conn, report_id=report_id)

        if prp_answ_d is not None:
            self.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id, prompt_answ=prp_answ_d)
        df = self.rep_to_dataframe(conn, report_id, instance_id)

        df_fin = self.i_df_helper.rem_att_id_forms(df)
        return df_fin

    def get_default_prp_answ(self, conn, report_id):
        return self.i_reports.get_report_prompts(connection=conn, report_id=report_id, closed=None)


class Cube:
    def open_instance(self, conn, cube_id):
        i_api_cubes = cubes
        r = i_api_cubes.cube_instance(connection=conn, cube_id=cube_id)
        return r.json()["instanceId"]

    def get_cube_def(self, conn, cube_id):
        return cube_definition(connection=conn, id=cube_id).json()

    def get_mtdi_cube_col_id(self, conn, cube_l):

        mstr_rag_col_d = {}

        for cube_id in cube_l:
            cube_def = self.get_cube_def(conn=conn, cube_id=cube_id)
            att_col_keys_d = {}
            for att in cube_def["definition"]["availableObjects"]["attributes"]:
                att_col_keys_d[att["name"]] = att["id"]
            # att_col_keys_l.append(att_col_keys_d.copy())

            mstr_rag_col_d[cube_id] = att_col_keys_d.copy()
        return mstr_rag_col_d

    def get_RAG_cube_col_mstr_id(self, conn, cube_id, col_name):
        mstr_rag_col_d = self.get_mtdi_cube_col_id(conn, cube_l=[cube_id])
        col_id = mstr_rag_col_d[cube_id][col_name]
        return col_id

    def load_cube_to_df(self, conn, cube_id):
        # only simgle table cubes are supported
        # instance_id=self.open_instance(conn=conn,cube_id=cube_id)
        # cube_df=self.loop_cube_to_df(conn, cube_id, instance_id)
        cbe = _Cube(connection=conn, id=cube_id)
        df_cbe = cbe.to_dataframe()
        return df_cbe

    def clean_cube_datatypes(self, cube_def_df):
        for c in cube_def_df.columns:
            if cube_def_df[c].dtype in ["int64", "int32", "int8", "float64", "bool"]:
                cube_def_df[c] = cube_def_df[c].astype("float64")
            else:
                cube_def_df[c]
        return cube_def_df

    def cube_upload_1_table(
        self, conn, load_df, tbl_name, updatePolicy="REPLACE", folder_id=None, cube_name=None, mtdi_id=None, force=False
    ):

        self.clean_cube_datatypes(load_df)
        if mtdi_id is None or mtdi_id == "":
            ds = super_cube.SuperCube(connection=conn, name=cube_name)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.create(folder_id=folder_id, force=force)
        else:
            ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.update()
        return ds.id

    def upload_cube_mult_table(
        self, conn, mtdi_id=None, tbl_upd_dict=None, cube_name=None, folder_id=None, force=False
    ):

        if mtdi_id is None:
            ds = super_cube.SuperCube(connection=conn, name=cube_name)
            for t in tbl_upd_dict:
                ds.add_table(
                    name=t["tbl_name"], data_frame=self.clean_cube_datatypes(t["df"]), update_policy=t["update_policy"]
                )
            ds.create(folder_id=folder_id, force=force)
        else:
            ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
            for t in tbl_upd_dict:
                ds.add_table(
                    name=t["tbl_name"], data_frame=self.clean_cube_datatypes(t["df"]), update_policy=t["update_policy"]
                )
            ds.update()

        return ds.id

    def get_cube_status(self, conn, cube_id):
        # the status is a bit mask returned in the X-MSTR-CubeStatus header,
        # the same one mstrio decodes in _Cube.refresh_status()
        resp = cubes.status(connection=conn, cube_id=cube_id, throw_error=False)
        status = int(resp.headers.get("X-MSTR-CubeStatus", 0)) if resp.ok else 0
        state_l = [s.name for s in CubeStates if s.value and status & s.value and "UNKNOWN" not in s.name]
        return {
            "status": status,
            "state_l": state_l,
            "is_ready": bool(status & CubeStates.READY.value),
            "is_processing": bool(status & CubeStates.PROCESSING.value),
        }

    def publish_cube(self, conn, cube_id, wait=True, timeout=600, poll_sec=5):
        # publishing is asynchronous, the server only acknowledges the request,
        # so the result has to be polled from the cube status
        cubes.publish(connection=conn, cube_id=cube_id)
        pub_d = {"cube_id": cube_id}
        pub_d.update(self.get_cube_status(conn=conn, cube_id=cube_id))

        if wait:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                time.sleep(poll_sec)
                pub_d.update(self.get_cube_status(conn=conn, cube_id=cube_id))
                if pub_d["is_ready"] and not pub_d["is_processing"]:
                    break

        pub_d["published"] = pub_d["is_ready"] and not pub_d["is_processing"]
        pub_d["error"] = None
        return pub_d

    def publish_cubes(self, conn, cube_l, wait=True, timeout=600, poll_sec=5, verbose=True):
        # cube_l takes GUID strings or the object dicts coming from
        # MstrGlobal.get_cube_obj_l / get_folder_obj_l
        pub_l = []
        for cube in cube_l:
            cube_id = cube if isinstance(cube, str) else cube["id"]
            cube_name = "" if isinstance(cube, str) else cube.get("name", "")
            try:
                pub_d = self.publish_cube(conn=conn, cube_id=cube_id, wait=wait, timeout=timeout, poll_sec=poll_sec)
            except Exception as err:
                # one unpublishable cube must not abort the whole folder
                pub_d = {
                    "cube_id": cube_id,
                    "status": None,
                    "state_l": [],
                    "is_ready": False,
                    "is_processing": False,
                    "published": False,
                    "error": f"{type(err).__name__}: {err}",
                }
            pub_d["cube_name"] = cube_name
            pub_l.append(pub_d)

            if verbose:
                if pub_d["error"]:
                    print(f"FAIL {cube_name or cube_id}: {pub_d['error']}")
                elif pub_d["published"]:
                    print(f"OK   {cube_name or cube_id} published ({', '.join(pub_d['state_l'])})")
                else:
                    print(f"...  {cube_name or cube_id} still publishing ({', '.join(pub_d['state_l'])})")

        return pub_l

    def quick_query_cube(self, conn, cube_id, attribute_l=None, metric_l=None, attr_elements=None):
        quick_cube = OlapCube(connection=conn, id=cube_id)
        quick_cube.apply_filters(attributes=attribute_l, metrics=metric_l, attr_elements=attr_elements)
        return quick_cube.to_dataframe()


class Prompts:
    def set_expr_prp_answ(self, prompt_id, prp_job_ans_l):
        # move to classes
        expr_ans = {"expression": {"operator": "Or", "operands": prp_job_ans_l}}
        # expr_ans = f'{{"prompts":[{json.dumps(expr_ans)}]}}'
        return self.frame_prp_ans(prompt_id=prompt_id, prp_type="EXPRESSION", prp_ans_JSON_l=expr_ans)

    def bld_expr_prp_answ(self, prompt_id, att_exp_ans_l, operator="And"):
        exp_prp_ans_l = []
        print(att_exp_ans_l)
        for prp_job_ans_d in att_exp_ans_l:
            if len(prp_job_ans_d["filter_val_l"]) > 1:
                exp_prp_ans_l.append(
                    {"operator": operator, "operands": self.bld_att_exp_prp_l(prp_job_ans_d=prp_job_ans_d)}
                )
            else:
                exp_prp_ans_l.append(
                    {"operator": operator, "operands": self.bld_att_exp_prp(prp_job_ans_d=prp_job_ans_d)}
                )

        prp_d = self.set_expr_prp_answ(prompt_id, exp_prp_ans_l)

        return prp_d

    def bld_att_exp_prp(self, prp_job_ans_d):

        att_form_exp_j = {
            "operator": prp_job_ans_d["operator"],
            "operands": [
                {
                    "type": "form",
                    "attribute": {"id": prp_job_ans_d["att_id"]},
                    "form": {"id": prp_job_ans_d["att_form_id"]},
                },
                {
                    "type": "constant",
                    "dataType": prp_job_ans_d["form_data_type"],
                    "value": prp_job_ans_d["filter_val_l"],
                },
            ],
        }
        # expr_JSON_l.append(att_form_exp_j)
        return att_form_exp_j

    def bld_att_exp_prp_l(self, prp_job_ans_d):

        att_form_exp_j = {
            "operator": prp_job_ans_d["operator"],
            "operands": [
                {
                    "type": "form",
                    "attribute": {"id": prp_job_ans_d["att_id"]},
                    "form": {"id": prp_job_ans_d["att_form_id"]},
                },
                {
                    "type": "constants",
                    "dataType": prp_job_ans_d["form_data_type"],
                    "values": prp_job_ans_d["filter_val_l"],
                },
            ],
        }
        # expr_JSON_l.append(att_form_exp_j)

        return att_form_exp_j

    def bld_metric_exp_prp(self, metric_exp_prp):

        # metric_exp_j = {"operator": metric_exp_prp["operator"], "operands":[]}
        metric_exp_j = {
            "operator": metric_exp_prp["operator"],
            "operands": [],
            "level": {"type": metric_exp_prp["level"]},
        }
        metric_exp_j["operands"].append({"type": "metric", "id": metric_exp_prp["met_id"]})

        if metric_exp_prp["operator"] not in ("IsNull", "IsNotNull"):
            metric_exp_j["operands"].append(
                {"type": "constant", "dataType": metric_exp_prp["data_type"], "value": metric_exp_prp["filter_val_l"]}
            )

        if "filter_val_l_1" in metric_exp_prp.keys():
            metric_exp_j["operands"].append(
                {"type": "constant", "dataType": metric_exp_prp["data_type"], "value": metric_exp_prp["filter_val_l_1"]}
            )

        # metric_exp_j["operands"].append({"level": {"type": metric_exp_prp["level"]}})

        return metric_exp_j

    def frame_metric_exp_prp(self, prompt_id, p_ans_d_j):
        prp_answ_block_d = {"id": prompt_id, "type": "EXPRESSION", "answers": {"expression": p_ans_d_j}}
        return prp_answ_block_d

    def get_exp_prp_data_type(self, baseFormType):
        exp_prp_data_type = ""
        if baseFormType in ["fixed_length_string", "n_var_char", "Char", "varChar"]:
            exp_prp_data_type = "Char"

        if baseFormType in ["integer", "double", "numeric", "number", "decimal", "int64", "float"]:
            exp_prp_data_type = "Numeric"

        if baseFormType in ["big_decimal", "bigDecimal"]:
            exp_prp_data_type = "BigDecimal"

        if baseFormType in ["time_stamp", "date", "dateTime", "timeStamp"]:
            exp_prp_data_type = "DateTime"

        if exp_prp_data_type == "":
            exp_prp_data_type = "Char"
            print(str(baseFormType) + " is not mapped")

        return exp_prp_data_type

    def bld_exp_operands_d(self, p_ans_d_d_l, operator="AND"):
        operand_d = {"operator": operator, "operands": p_ans_d_d_l}
        return operand_d

    def bld_prp_exp_d(self, prompt_id, p_ans_d_j, operator="And"):
        prp_answ_d = {
            "id": prompt_id,
            "type": "EXPRESSION",
            "answers": {"expression": {"operator": operator, "operands": [p_ans_d_j]}},
        }
        return prp_answ_d

    def get_rep_prp_all(self, conn, report_id, instance_id):
        inst_u = f"{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts"
        r = conn.get(inst_u, headers=conn.headers)
        return r.json()

    def frame_prp(self, prp_ans):
        return json.dumps({"prompts": prp_ans})

    def frame_prp_ans(self, prompt_id, prp_type, prp_ans_JSON_l):
        return {"id": prompt_id, "type": prp_type, "answers": prp_ans_JSON_l}

    def bld_obj_prp_json(self, object_id, object_type):
        return json.dumps({"id": object_id, "type": object_type})

    def close_open_prp(self, conn, report_id, instance_id, prompt_answ):
        # checks the answered prp
        rep_stat = Rep().get_open_prp_stat(conn=conn, report_id=report_id, instance_id=instance_id)

        if rep_stat == 2:
            prompt_answ_d = json.loads(prompt_answ)
            prp_ans_d_l = prompt_answ_d["prompts"]
            prp_ans_id_l = i_msic.keep_cols_from_dict_l(list_l=prp_ans_d_l, keep_cols=["id"])

            # checks the all prp in report / dashboard
            rep_open_prp_d_l = Rep().get_open_prompts(conn=conn, report_id=report_id, instance_id=instance_id)
            rep_open_prp_id_l = i_msic.keep_cols_from_dict_l(list_l=rep_open_prp_d_l, keep_cols=["id"])

            # checks the prp not answered jet
            open_prp_l = [x for x in rep_open_prp_id_l if x not in prp_ans_id_l]
            open_prp_l = i_msic.get_key_form_dict_l(dict_l=open_prp_l)

            for prp in rep_open_prp_d_l:
                print(prp["id"])
                if prp["id"] in open_prp_l:
                    print(prp)
                    open_prp_d = {}
                    open_prp_d["id"] = prp["id"]
                    open_prp_d["type"] = prp["type"]
                    if prp["type"] in ["OBJECTS", "ELEMENTS"]:
                        open_prp_d["answers"] = []
                    else:
                        open_prp_d["answers"] = {}
                    rep_open_prp_d_l.append(open_prp_d.copy())
            prompt_answ = self.frame_prp(prp_ans=rep_open_prp_d_l)
            Rep().set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id, prompt_answ=prompt_answ)
        return
