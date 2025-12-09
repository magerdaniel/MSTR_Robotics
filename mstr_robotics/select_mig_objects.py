from mstr_robotics.report import cube, rep
from mstr_robotics.mstr_classes import mstr_global,md_searches,get_conn
from mstr_robotics._mod_prj_obj import bld_short_cuts
from mstr_robotics._connectors import mstr_api
from datetime import datetime
from mstr_robotics._helper import str_func
import pandas as pd
from typing import Optional
from IPython.display import display

class open_conn():

    def login(self,base_url,*args,**kwargs):
        return get_conn(self, *args,**kwargs)

class get_change_log:

    def __init__(self):
        self.str= str_func()
        self.rep = rep()
        self.mstr_api = mstr_api()
        self.glob = mstr_global()
        self.run_shortcut=bld_short_cuts()

    def set_md_rep_params(self,conn,change_log_report):
        self.conn=conn
        #self.chg_log_rep_proj_id = change_log_report["chg_log_rep_proj_id"]
        #self.chg_log_report_id = change_log_report["chg_log_report_id"]  # 4
        self.chg_log_from_date_prompt_id = change_log_report["chg_log_from_date_prompt_id"]
        self.chg_log_to_date_prompt_id = change_log_report["chg_log_to_date_prompt_id"]
        self.chg_log_proj_prompt_id = change_log_report["chg_log_proj_prompt_id"]

    #@logger
    def _bld_desc_str(self, all_changes=None):
        desc_str = ""
        all_changes_sum = all_changes.groupby(["Account@GUID", "Account@Login", "Object@GUID", "Object@Name", "OBJECT_TYPE_ID"])[
            "Timestamp"].agg("count").reset_index()
        for index, s in all_changes_sum.iterrows():
            desc_str += self._empty_login(s["Account@Login"]) + " (" + str(s["Timestamp"]) + "), "
        return self.str._rem_last_char(desc_str,2)

    def _empty_login(self,login):
        if login=="<Empty>":
            login="Unkown"
        return login

    def bld_change_log_shortcut_df(self,conn,change_log_df):

        obj_df = change_log_df.groupby(["Account@GUID", "Account@Login", "Object@GUID", "Object@Name", "OBJECT_TYPE_ID"])[
            "Timestamp"].agg("count").reset_index()

        #obj_df["NEW_OBJECT_NAME"]=""
        shortcut_l=[]
        for index, obj in obj_df.iterrows():
            #short_cut_obj=self.glob.get_short_cut_obj(conn=conn, project_id="project_id",object_id=obj["OBJECT_ID"], type=18)
            #builds the Strings for shortCut Name & Descriptions
            #name user_login__object_name__object_guid
            shortcut = []
            shortcut.append(obj["Object@GUID"])
            shortcut.append(obj["OBJECT_TYPE_ID"])
            shortcut.append(self._empty_login(obj["Account@Login"])+ "__" + obj["Object@Name"] + "__" + obj["Object@GUID"])
            shortcut.append(self._bld_desc_str(change_log_df.loc[change_log_df["Object@GUID"] == obj["Object@GUID"]]))
            shortcut_l.append(shortcut)
        shortcut_df=pd.DataFrame(shortcut_l, columns=["Object@GUID","OBJECT_TYPE_ID","Object@Name","OBJECT_DESC"])
        return shortcut_df

    def get_mig_obj_logs(self,conn,ch_conn,project_id,from_date,to_date,change_log_report, chg_log_source="pa"):
        #you find the change logs either in Platform Analytics or
        #in the meta data. For PA you'll be able to use my report.
        # For the meta data I provide a free form SQL report for SQL Server.
        # Both reports are accesable for you as a OM package in GitHub

        prp_answ_d=self._build_val_answ(ch_conn=ch_conn,
                                        project_id=project_id,
                                        change_log_report=change_log_report,
                                        chg_log_proj_id=project_id,
                                        chg_log_from_date =from_date,
                                        chg_log_to_date=to_date)

        #self.glob.set_project_id(conn=conn, project_id=self.chg_log_rep_proj_id)

        instance_id = self.rep.open_Instance(conn=ch_conn,
                                             report_id=change_log_report["chg_log_report_id"])

        self.rep.set_inst_prompt_ans(conn=ch_conn,
                                     report_id=change_log_report["chg_log_report_id"], instance_id=instance_id,
                                     prompt_answ=prp_answ_d)

        rep_def=self.rep.get_report_def(conn=ch_conn,report_id=change_log_report["chg_log_report_id"])
        #cols=self.parse_chglog_rep_cols(rep_def)

        #rep_has_data_fg=self.rep.report_has_data(conn=self.conn,report_id=change_log_report["chg_log_report_id"],instance_id=instance_id)

        #if rep_has_data_fg:
        if 1==1:

            if chg_log_source=="md":

                report_df=self.rep.bld_free_form_rep_df(conn=ch_conn,
                                                        report_id=change_log_report["chg_log_report_id"],
                                                        prp_answ_d=prp_answ_d)

                report_df["Project@GUID"] = report_df["Project@GUID"].apply(lambda x: self.str.bld_mstr_obj_guid_sql_server(x))
                report_df["Account@GUID"] = report_df["Account@GUID"].apply(lambda x: self.str.bld_mstr_obj_guid_sql_server(x))
                report_df["Object@GUID"]  = report_df["Object@GUID"].apply(lambda x: self.str.bld_mstr_obj_guid_sql_server(x))

            if chg_log_source=="pa":
                report_df = self.rep.report_df(conn=ch_conn,
                                                   report_id=change_log_report["chg_log_report_id"], instance_id=instance_id)
                report_df['OBJECT_TYPE_ID'] = report_df['ObjectType@ID'].apply(lambda x: self.glob.pa_get_obj_type_id(pa_obj_id=x))

            return report_df
        else:
            raise ValueError('No changed objects found. Please review your prompt answers')

    def _build_val_answ(self,ch_conn,
                        project_id,
                        change_log_report,
                        chg_log_proj_id=None,
                        chg_log_from_date=None,
                        chg_log_to_date=None,
                        obj_where_clause_str=None):
        prompt_ans = None
        if chg_log_proj_id:
            prompt_ans = f'{{"key":"{change_log_report["chg_log_proj_prompt_id"]}@0@10","type":"VALUE","answers": "{self.mstr_api.get_project_name(conn=ch_conn,project_id=project_id)}"}},'
        if chg_log_from_date:
            prompt_ans += f'{{"key":"{change_log_report["chg_log_from_date_prompt_id"]}@0@10","type":"VALUE","answers": "{chg_log_from_date}"}},'
        if chg_log_to_date:
            prompt_ans += f'{{"key":"{change_log_report["chg_log_to_date_prompt_id"]}@0@10","type":"VALUE","answers": "{chg_log_to_date}"}}'
        if obj_where_clause_str:
            prompt_ans = f'{{"key":"{change_log_report["chg_log_proj_prompt_id"]}@0@10","type":"VALUE","answers": "{obj_where_clause_str}"}}'

        if prompt_ans:
            prompt_ans = f'{{"prompts":[{prompt_ans}]}}'
        return prompt_ans

class bld_mig_content():

    def from_folder(self, fold_short_cut_l,action="FORCE_REPLACE",include_dependents=False):
        #purpose of this fucntion is to read out the base objects of short cuts
        #stored in certain folder. This is an typical input for migrations
        mig_l=[]
        for sh in fold_short_cut_l:
            obj_d = {}
            if sh["type"] == 18:
                obj_d["id"] = sh["target_info"]["id"]
                obj_d["type"] = sh["target_info"]["type"]
                obj_d["action"] = action
                obj_d["include_dependents"] = include_dependents
                mig_l.append(obj_d.copy())
        return mig_l

