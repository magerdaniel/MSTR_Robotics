from mstr_robotics.mstr_classes import rep
from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics.mstr_classes import get_conn
from datetime import datetime
from mstr_robotics.helper import str_func
import pandas as pd
from typing import Optional

class open_conn():

    def login(self,base_url,project_id=None,*args,**kwargs):
        return get_conn(self,base_url=base_url, project_id=project_id,*args,**kwargs)
"""
class prepare_mig_obj():

    def set_env_var(self, conn_det=None, change_log_report=None, change_query=None):

        self.i_datetime = datetime
        self.conn_det = conn_det
        self.conn = self.glob.get_conn(conn_det=conn_det, project_id=change_log_report["chg_log_rep_proj_id"])
"""
class cubes:
    def __init__(self):
        self.glob = mstr_global

    def up_load_to_cube(self,conn,cube_upload_param):

        self.glob.cube_upload(self,conn=conn
                              ,load_df=cube_upload_param["load_df"]
                              ,updatePolicy="REPLACE"
                              ,cube_name=cube_upload_param["cube_name"]
                              ,tbl_name="changes"
                              ,folder_id=cube_upload_param["migration_folder_id"]
                              ,mtdi_id=cube_upload_param["mtdi_id"])

class bld_short_cuts:

    def __init__(self):
        self.str= str_func()
        self.rep = rep()
        self.glob = mstr_global()

    def bld_short_cut(self,conn,project_id,folder_id,object_id,
                      object_type,new_name,object_desc):
        #In Version 11 update 7, it's not possible to define a free name and description
        # when creating a short cut. Thus I create the shurt cut first and rename it later
        short_cut_obj_rep = self.glob._cr_short_cut(conn=conn,object_id=object_id,object_type=object_type,folder_id=folder_id)
        self.glob._rename_object(conn=conn,project_id=project_id,object_id=short_cut_obj_rep.json()["id"],object_type=18,new_name=new_name, desc_str=object_desc)

    def run_short_cut_build(self, conn,project_id,short_cut_df,folder_id,df_key_cols_d=None):
        #requiers a pandas data frame with 4 colmuns
        # object_id, object_type, object_name, object_desc
        if not df_key_cols_d:
            df_key_cols_d={"object_id":"OBJECT_ID",
                           "object_type":"OBJECT_TYPE",
                           "object_name":"OBJECT_NAME",
                           "object_desc":"OBJECT_DESC"}

        #before adding new short cuts to the shortcut folder, all shortcuts gonna be deleted
        self.clean_shortcut_folder(conn=conn,project_id=project_id,folder_id=folder_id)
        short_cut_folder_j = f'{{"folderId": "{folder_id}"}}'
        #iterrating throuh all the objects in the list
        short_cut_df.reset_index()
        for index, obj in short_cut_df.iterrows():
            #short_cut_url = f'{conn.base_url}/objects/{obj[2]}/type/{str(obj[4])}/shortcuts'
            if obj[df_key_cols_d["object_type"]]!=str(18):
                self.bld_short_cut(conn=conn
                                   ,project_id=project_id
                                   ,folder_id=folder_id
                                   ,object_id=obj[df_key_cols_d["object_id"]]
                                   ,object_type=obj[df_key_cols_d["object_type"]]
                                   ,new_name=obj[df_key_cols_d["object_name"]]
                                   #max lentgh of descriptions in MSTR
                                   ,object_desc=self.str._get_first_x_chars(str_=obj[df_key_cols_d["object_desc"]],i=249)
                                   )
            else:
                print(f'Obect: {obj[2]} is a shortcut. MSTR does not allow short cuts o short cuts,'
                      f' thus they are not supported'  )

    #@logger
    def clean_shortcut_folder(self,conn,project_id,folder_id):
        existing_obj_l = self.glob.get_folder_obj_l(conn=conn, project_id=project_id, folder_id=folder_id)
        obj_l = self._get_col_from_obj_l(dict_l=existing_obj_l, key_col="id")
        self.glob._delete_object(conn=conn,project_id=project_id,existing_obj_l=existing_obj_l)

    #@logger
    def _get_col_from_obj_l(self, dict_l=None, key_col=None):
        key_val_l = []
        for o in dict_l:
            key_val_l.append(o[key_col])
        return key_val_l

class get_change_log:

    def __init__(self):
        self.str= str_func()
        self.rep = rep()
        self.glob = mstr_global()
        self.run_shortcut=bld_short_cuts()

    def set_md_rep_params(self,conn,change_log_report,change_query):
        self.conn=conn
        self.chg_log_rep_proj_id = change_log_report["chg_log_rep_proj_id"]
        self.chg_log_report_id = change_log_report["chg_log_report_id"]  # 4
        self.chg_log_from_date_prompt_id = change_log_report["chg_log_from_date_prompt_id"]
        self.chg_log_to_date_prompt_id = change_log_report["chg_log_to_date_prompt_id"]
        self.chg_log_proj_prompt_id = change_log_report["chg_log_proj_prompt_id"]
        self.chg_log_obj_prompt_id = change_log_report["chg_log_obj_prompt_id"]
        """
        self.short_cut_proj_id = change_query["short_cut_proj_id"]
        self.chg_log_proj_name = "MicroStrategy Tutorial"
        self.chg_log_from_date = change_query["chg_log_from_date"]
        self.chg_log_to_date = change_query["chg_log_to_date"]
        #self.chg_log_dep_bool = change_query["chg_log_dep_bool"]
        """
        #self.short_cut_folder_id=None

    #@logger
    def _bld_desc_str(self, all_changes=None):
        desc_str = ""
        all_changes_sum = all_changes.groupby(["USER_ID", "USER_LOGIN", "OBJECT_ID", "OBJECT_NAME", "OBJECT_TYPE"])[
            "TRANSACTION_ID"].agg("count").reset_index()
        for index, s in all_changes_sum.iterrows():
            desc_str += s["USER_LOGIN"] + " (" + str(s["TRANSACTION_ID"]) + "), "
        return desc_str

    def bld_change_log_shortcut_df(self,conn,change_log_df):

        short_cut_df = change_log_df.groupby(["USER_ID", "USER_LOGIN", "OBJECT_ID", "OBJECT_NAME", "OBJECT_TYPE"])[
            "TRANSACTION_ID"].agg("count").reset_index()

        short_cut_df["NEW_OBJECT_NAME"]=""
        short_cut_df["DESC_STR"] = ""
        for index, obj in short_cut_df.iterrows():
            #short_cut_obj=self.glob.get_short_cut_obj(conn=conn, project_id="project_id",object_id=obj["OBJECT_ID"], type=18)
            #builds the Strings for shortCut Name & Descriptions
            #name user_login__object_name__object_guid
            short_cut_df.at[index,"NEW_OBJECT_NAME"]  = obj["USER_LOGIN"]+ "__" + obj["OBJECT_NAME"] + "__" + obj["OBJECT_ID"]
            short_cut_df.at[index, "NEW_OBJECT_DESC"]=self._bld_desc_str(change_log_df.loc[change_log_df["OBJECT_ID"] == obj["OBJECT_ID"]])
        return short_cut_df

    def parse_chglog_rep_cols(self,rep_def):
        cols=[]
        for r in rep_def.json()["definition"]["grid"]["rows"]:
            if r["type"] == "attribute":
                cols.append(r["name"])
        return  cols

    def chk_chg_log_cols(self,conn,report_id):
        rep_def=self.rep.get_report_def(conn=conn,report_id=report_id)
        cols= self.parse_chglog_rep_cols(rep_def)
        return self.comp_col_chg_log_rep(cols)

    def comp_col_chg_log_rep(self,cols):
        original_col_order=["TRANSACTION_ID", "PROJECT_ID", "PROJECT_NAME", "USER_ID", "USER_LOGIN", "USERNAME", "COMMENT_VAL", "OBJECT_ID", "OBJECT_NAME", "SUBTYPE", "OBJECT_TYPE", "TRANSACTION_TIMESTAMP", "TRANSACTION_NAME", "TRANSACTION_TYPE", "TRANSACTION_SOURCE", "TRANSACTION_PROJECT_ID"]
        comp_res={}
        if original_col_order==cols:
            comp_res["cols_ok_txt"]="Attributes names and order ok!"
        else:
            comp_res["number_of_cols_expected"] =   len(original_col_order)
            comp_res["number_of_cols_provided"] =   len(cols)
            comp_res["missing_cols"]=list(set(original_col_order) - set(cols))
            comp_res["unkwon_cols"]=list(set(cols) - set(original_col_order))
            comp_res["order_of_cols_expected"] =original_col_order
            comp_res["order_of_cols_provided"] =cols

        return comp_res

    def chg_rep_to_df(self,conn,prompt_answ):

        instance_id = self.rep.open_Instance(conn=conn, project_id=self.chg_log_rep_proj_id,
                                             report_id=self.chg_log_report_id)

        self.rep.set_inst_prompt_ans(conn=conn,
                                     report_id=self.chg_log_report_id, instance_id=instance_id,
                                     prompt_answ=prompt_answ)

        rep_def=self.rep.get_report_def(conn=self.conn,report_id=self.chg_log_report_id)
        cols=self.parse_chglog_rep_cols(rep_def)
        cols.append("path")

        report_dict = self.rep.report_dict(conn=self.conn,
                                           report_id=self.chg_log_report_id, instance_id=instance_id)

        report_list = self.read_obj(report_dict)
        report_df = pd.DataFrame(report_list, columns=cols)

        return report_df

    def get_mig_obj_logs(self,conn,proect_id,from_date,to_date):
        #get raw data from change logs
        return self.chg_rep_to_df(conn, prompt_answ =self._build_val_answ(
                        chg_log_rep_proj_id=proect_id,
                        chg_log_from_date =from_date,
                        chg_log_to_date=to_date))
        #group by developer /user_id and object
        #generate strings for name and description of the short cuts
        short_cut_df = self.bld_change_log_shortcut_df(conn=self.conn, change_log_df=chg_log_df)
        #define cols for short cut run
        df_key_cols_d = {"object_id": "OBJECT_ID",
                         "object_type": "OBJECT_TYPE",
                         "object_name": "NEW_OBJECT_NAME",
                         "object_desc": "NEW_OBJECT_DESC"}
        self.run_shortcut.run_short_cut_build(conn=self.conn, project_id=self.short_cut_proj_id,
                                          short_cut_df=short_cut_df, folder_id=self.short_cut_folder_id,
                                          df_key_cols_d=df_key_cols_d)

    def read_obj(self, report_dict=None):
        project_id_ind=1
        obj_g_ind = 7
        obj_typ_ind = 10
        val_l = []
        all_obj_l = []
        cnt_obj = 0

        for o in report_dict:
            conn=self.glob.set_project_id(conn=self.conn,project_id=self.glob.bld_mstr_obj_guid(o[project_id_ind])) #project_id_ind
            # self.MstrConn.cHeaders.update({'X-MSTR-ProjectID': o[proj_g_ind] })
            obj_d = self.glob.get_object_info(conn=conn,
                                              object_id=self.glob.bld_mstr_obj_guid(o[obj_g_ind]), #obj_g_ind
                                              type=o[obj_typ_ind], #obj_typ_ind
                                              project_id=self.glob.bld_mstr_obj_guid(o[project_id_ind]))     #project_id_ind

            # obj_l=self.bld_obj_l(obj_d=obj_d.json(),proj_g=self.glob.bld_mstr_obj_guid(o[project_id_ind]))

            if obj_d.status_code == 200 \
                    and self.glob.bld_mstr_obj_guid(o[obj_typ_ind]) != 18: # excluding short cuts
                fold_path = str(self.glob.bld_obj_path(fld_d=obj_d.json()["ancestors"], proj_id=self.glob.bld_mstr_obj_guid(o[project_id_ind]),
                                                       proj_name=self.glob.get_project_name(conn=self.conn,project_id=self.glob.bld_mstr_obj_guid(o[project_id_ind]))))

                if fold_path != "\\Managed Objects" \
                        and self.str._get_first_x_chars(str_=fold_path, i=15) != "\System Objects" \
                        and len(fold_path) > 4:

                        all_obj_l.append(self.bld_chg_l(val_l=val_l,o=o,fold_path=fold_path))
                val_l = []

        return all_obj_l

    def bld_chg_l(self,val_l,o,fold_path):
        val_l.append(o[0])  # transaction_id_ind
        val_l.append(self.glob.bld_mstr_obj_guid(o[1]))  # project_id_ind
        val_l.append(o[2])  # project_name_ind
        val_l.append(self.glob.bld_mstr_obj_guid(o[3]))  # user_id_ind
        val_l.append(o[4])  # username_ind
        val_l.append(o[5])  # user_login_ind
        val_l.append(o[6])  # comment_val_ind
        val_l.append(self.glob.bld_mstr_obj_guid(o[7]))  # object_id_ind
        val_l.append(o[8])  # object_name_ind
        val_l.append(o[9])  # subtype_ind
        val_l.append(o[10])  # object_type_ind
        val_l.append(o[11])  # transaction_timestamp_ind
        val_l.append(o[12])  # transaction_name_ind
        val_l.append(o[13])  # transaction_type_ind
        val_l.append(o[14])  # transaction_source_ind
        val_l.append(self.glob.bld_mstr_obj_guid(o[15]))  # transaction_project_id_ind
        val_l.append(fold_path)
        return val_l

    def _build_val_answ(self,
                        chg_log_rep_proj_id=None,
                        chg_log_from_date =None,
                        chg_log_to_date=None,
                        obj_where_clause_str=None):
        prompt_ans=None
        if chg_log_rep_proj_id:
            prompt_ans = f'{{"key":"{self.chg_log_proj_prompt_id}@0@10","type":"VALUE","answers": "{self.glob.get_project_name(conn=self.conn,project_id=chg_log_rep_proj_id)}"}},'
        if chg_log_from_date:
            prompt_ans += f'{{"key":"{self.chg_log_from_date_prompt_id}@0@10","type":"VALUE","answers": "{chg_log_from_date}"}},'
        if chg_log_to_date:
            prompt_ans += f'{{"key":"{self.chg_log_to_date_prompt_id}@0@10","type":"VALUE","answers": "{chg_log_to_date}"}}'
        if obj_where_clause_str:
            prompt_ans = f'{{"key":"{self.chg_log_obj_prompt_id}@0@10","type":"VALUE","answers": "{obj_where_clause_str}"}}'

        if prompt_ans:
            prompt_ans = f'{{"prompts":[{prompt_ans}]}}'
        return prompt_ans

    def bld_prompt_obj_where_list(self,obj_l):
        where_str=""
        for o in obj_l:
            where_str+="'" + self.glob.bld_mstr_obj_md_guid(o) +"',"

        return self.str._rem_last_char(where_str,1)

    def obj_id_from_df_l(self, report_df,obj_col_ind,type_col_ind):
        obj_l=[]
        for index, obj in report_df.iterrows():
            obj_l.append(obj[obj_col_ind] + ";"+obj[type_col_ind])
        return dict.keys(dict.fromkeys(obj_l))

    def get_change_log_df_from_list(self,chg_log_rep_proj_id,obj_l ):
        obj_where_clause_str = self.bld_prompt_obj_where_list(obj_l=obj_l)
        report_df = self.chg_rep_to_df(self._build_val_answ(
                            chg_log_rep_proj_id=self.chg_log_rep_proj_id,
                            obj_where_clause_str=obj_where_clause_str))
        return report_df

    def get_obj_l_from_search_l(self,search_l):
        obj_l=[]
        for o in search_l:
            obj_l.append(o["object_id"])

        return obj_l

class searches:

    def __init__(self):
        self.glob = mstr_global()

    def used_by_obj_l_rec(self,conn,project_id,obj_id_l):
        return list(self.glob.used_by_obj_rec_l(conn=conn,
                                                project_id=project_id,
                                                obj_l=obj_id_l)
                    )
    def bld_df_from_search_result(self, conn,search_l):
        return self.glob.bld_df_from_search_result(conn=conn, search_l=search_l)

