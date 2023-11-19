from mstrio.utils import parser
from mstrio.api import reports,cubes
from mstrio.project_objects import Report
from mstr_robotics._connectors import mstr_api
from mstrio.project_objects.datasets import super_cube
from mstrio.project_objects.datasets.cube import _Cube
import pandas as pd
from mstr_robotics.mstr_pandas import df_helper
import json

i_mstr_api=mstr_api()

class rep:

    def __init__(self):
        self.i_reports = reports
        self.i_parser = parser
        self.i_df_helper=df_helper()

    def open_Instance(self, conn, report_id):
        rep_instance = self.i_reports.report_instance(connection=conn, report_id=report_id)
        return rep_instance.json()["instanceId"]

    def get_open_prompts(self, conn, report_id, instance_id):
        open_prompts=self.i_reports.get_prompted_instance(connection=conn, report_id=report_id
                                             ,instance_id=instance_id, closed = False)
        return open_prompts.json()

    def set_inst_prompt_ans(self, conn, report_id, instance_id, prompt_answ):
        prompt_answ_url = f'{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/answers'
        ret_prompt_ans = conn.put(prompt_answ_url, data=prompt_answ)
        return ret_prompt_ans

    def report_dict(self, conn, report_id, instance_id):
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        report_dict = self.i_parser.Parser(report_ds.json())._Parser__map_attributes(report_ds.json())
        return report_dict

    def save_rep_inst_stat(self,conn,instance_id, rep_name,save_mode="OVERWRITE"):
        # print(rep_name)
        folder_id = "04E55C2C46DD0FF8D19A398B5C86EC71"
        data = {
            "name": rep_name,
            "description": "string",
            "folderId": folder_id,
            "saveMode": save_mode,
            "displayMode": "GRID",
            "promptOption": "static",
            "setCurrentAsDefaultAnswer": True,
            "instanceId": instance_id
        }
        inst_u = f'{conn.base_url}/api/reports'
        r = conn.post(inst_u, data=json.dumps(data))
        return r

    def bld_export_att_names(self,rep_def_resp):
        att_col_l = []
        for col in rep_def_resp.json()["definition"]["grid"]["rows"]:

            #print(col["name"])
            for col_form in col["forms"]:
                att_col_l.append(str(col["name"]).replace(" ", "_" ) + '@' + str(col_form["name"]).replace(" ", "_" ))
        return att_col_l

    def get_rep_attributes(self,conn, report_id):
        rep_def_resp = reports.report_definition(connection=conn, report_id=report_id)
        att_col_l = self.bld_export_att_names(rep_def_resp=rep_def_resp)
        return att_col_l

    def bld_df_of_mstr(self,mstr_data_ds, att_col_l):
        # create attribute data frame, then re-map integer
        # array with corresponding attribute element values  # noqa
        r = self.i_parser.Parser(mstr_data_ds)
        r.parse(mstr_data_ds)

        attribute_df = pd.DataFrame(data=r._mapped_attributes, columns=att_col_l)
        # create metric values data frame
        metric_df = pd.DataFrame(data=r._metric_values_raw, columns=r._metric_col_names)
        mstr_df = pd.concat([attribute_df, metric_df], axis=1)
        return mstr_df

    def rep_to_dataframe(self, conn, report_id, instance_id,att_col_l=None):

        if att_col_l==None:
            att_col_l=self.get_rep_attributes(conn=conn, report_id=report_id)

        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        # create attribute data frame, then re-map integer array with corresponding attribute element values  # noqa
        r=self.i_parser.Parser(report_ds.json())
        r.parse(report_ds.json())
        attribute_df = pd.DataFrame(
            data=r._mapped_attributes, columns=att_col_l
        )

        metric_df = pd.DataFrame(data=r._metric_values_raw, columns=r._metric_col_names)
        mstr_df=pd.concat([attribute_df, metric_df], axis=1)

        return mstr_df

    def report_df(self, conn, report_id, instance_id):
        self.i_po_Report = Report(connection=conn, id=report_id, instance_id=instance_id)
        return self.i_po_Report.to_dataframe()


    def get_report_def(self, conn, report_id):
        return self.i_reports.report_definition(connection=conn, report_id=report_id)


    def report_has_data(self, conn, report_id,instance_id):
        report_has_data_fg=False
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        if len(report_ds.json()["definition"]["grid"]["rows"][0]["elements"])>0:
            report_has_data_fg=True

        return report_has_data_fg

    def bld_free_form_rep_df(self,conn, report_id,prp_answ_d=None):

        instance_id = self.open_Instance(conn=conn, report_id=report_id)

        if prp_answ_d!=None:
            self.set_inst_prompt_ans(conn=conn,
                                         report_id=report_id, instance_id=instance_id,
                                         prompt_answ=prp_answ_d)
        df = self.rep_to_dataframe(conn, report_id, instance_id)

        df_fin = self.i_df_helper.rem_att_id_forms(df)
        return df_fin


class cube():

    def open_instance(self,conn, cube_id):
        i_api_cubes = cubes
        r=i_api_cubes.cube_instance(connection=conn,cube_id=cube_id)
        return r.json()["instanceId"]

    def load_cube_to_df(self, conn, cube_id):
        #only simgle table cubes are supported
        instance_id=self.open_instance(conn=conn,cube_id=cube_id)
        #cube_df=self.loop_cube_to_df(conn, cube_id, instance_id)
        cbe = _Cube(connection=conn, id=cube_id)
        df_cbe = cbe.to_dataframe()
        return df_cbe

    def cube_upload_1_table(self, conn, load_df, tbl_name, updatePolicy="REPLACE",
                            folder_id=None, cube_name=None, mtdi_id=None, force=False):
        if mtdi_id == None or mtdi_id =="":
            ds = super_cube.SuperCube(connection=conn, name=cube_name)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.create(folder_id=folder_id,force=force)
        else:
            ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.update()
        return ds.id

    def upload_cube_mult_table(self, conn, mtdi_id=None, tbl_upd_dict=None,
                               cube_name=None, folder_id=None, force=False):

        if mtdi_id ==None:
            ds = super_cube.SuperCube(connection=conn, name=cube_name)
            for t in tbl_upd_dict:
                ds.add_table(name=t["tbl_name"],
                             data_frame=t["df"],
                             update_policy=t["update_policy"])
            ds.create(folder_id=folder_id,force=force)
        else:
            ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
            for t in tbl_upd_dict:
                ds.add_table(name=t["tbl_name"],
                             data_frame=t["df"],
                             update_policy=t["update_policy"])
            ds.update()

        return ds.id

class prompts():

    def set_expr_prp_answ(self,prompt_id, prp_job_ans_l):
        # move to classes
        expr_ans = {"expression": {"operator": "Or", "operands": prp_job_ans_l}}
        # expr_ans = f'{{"prompts":[{json.dumps(expr_ans)}]}}'
        return self.frame_prp_ans(prompt_id=prompt_id, prp_type="EXPRESSION",
                                       prp_ans_JSON_l=expr_ans)

    def bld_expr_prp_answ(self, prompt_id, att_exp_ans_l):
        exp_prp_ans_l = []

        for prp_job_ans_l in att_exp_ans_l:
            exp_prp_ans_l.append({"operator": "And", 'operands': self.loop_att_exp_prp(prp_job_ans_l=prp_job_ans_l)})

        prp_d = self.set_expr_prp_answ(prompt_id, exp_prp_ans_l)

        return prp_d

    def loop_att_exp_prp(self,prp_job_ans_l):
        expr_JSON_l = []
        for att_exp_prp in prp_job_ans_l:
            att_form_exp_j = {"operator": "In", "operands":
                [
                    {"type": "form", "attribute": {"id": att_exp_prp["att_id"]},
                     "form": {"id": att_exp_prp["att_form_id"]}
                     },
                    {"type": "constants", "dataType": att_exp_prp["att_form_data_type"],
                     "values": [att_exp_prp["filter_val_l"]]
                     }
                ]
                              }
            expr_JSON_l.append(att_form_exp_j)

        return expr_JSON_l

    def get_form_type(self,baseFormType):
        form_type_d={"number":"Numeric",
                     "big_decimal":"BigDecimal"}
        return form_type_d[baseFormType]

    def get_rep_prp_all(self,conn, report_id, instance_id):
        inst_u = f"{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts"
        r = conn.get(inst_u, headers=conn.headers)
        return r.json()

    def frame_prp(self, prp_ans):
        return json.dumps({"prompts": prp_ans})

    def frame_prp_ans(self, prompt_id,prp_type, prp_ans_JSON_l):
       return {"id": prompt_id, "type": prp_type,"answers": prp_ans_JSON_l}


class dossiers():
    visual_dict = {}
    visual_list=[]

    def read_visual_hier(self, page):
        for v in page["visualizations"]:
            self.visual_dict["visual_key"] = v["key"]
            self.visual_dict["visual_name"] = v["name"]
            self.visual_dict["visualizationType"] = v["visualizationType"]
            self.visual_list.append(self.visual_dict.copy())


    def read_pages_hier(self, chapter):
        for page in chapter["pages"]:
            self.visual_dict["page_key"] = page["key"]
            self.visual_dict["page_name"] = page["name"]
            self.read_visual_hier(page=page)

    def get_dossier_def(self,conn,dossier_id):
        url = f'{conn.base_url}/api/v2/dossiers/{dossier_id}/definition'
        doss_hier = conn.get(url)
        return doss_hier.json()


    def run_read_out_doss_hier(self, conn, dossier_l):
        self.visual_list=[]
        for dossier_id in dossier_l:
            doss_hier=self.get_dossier_def(conn,dossier_id)
            #print(doss_hier)
            self.visual_dict = {}
            self.visual_dict["dossier_id"] = dossier_id
            #self.visual_dict["dossier_name"] = d.name
            self.visual_dict["error_msg"] = ''
            try:
                for chapter in doss_hier["chapters"]:
                    # print(chapter)
                    self.visual_dict["chapter_key"] = chapter["key"]
                    self.visual_dict["chapter_name"] = chapter["name"]
                    self.read_pages_hier(chapter=chapter)

            except Exception as err:

                self.visual_dict["chapter_key"] = ""
                self.visual_dict["chapter_name"] = ""
                self.visual_dict["page_key"] = ""
                self.visual_dict["page_name"] = ""
                self.visual_dict["visual_key"] = ""
                self.visual_dict["visual_name"] = ""
                self.visual_dict["visualizationType"] = ""
                self.visual_dict["error_msg"] = err
        #   print(visual_dict)
        return self.visual_list

    def doss_hier_to_df(self,conn,dossier_l):
        doss_hier_l=self.run_read_out_doss_hier(conn=conn, dossier_l=dossier_l)
        doss_hier_df = pd.DataFrame.from_dict(doss_hier_l)
        return doss_hier_df


    def create_dossier_instance(self,conn, dossier_id, body, error_msg=None):

        endpoint = f'{conn.base_url}/api/dossiers/{dossier_id}/instances'
        return conn.post(
            url=endpoint, json=body
        )