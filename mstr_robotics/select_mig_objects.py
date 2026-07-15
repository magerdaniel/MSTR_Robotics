import pandas as pd
from mstrio.api import browsing
from mstrio.object_management import folder
from mstrio.object_management.migration.package import Action

from mstr_robotics._connectors import MstrApi
from mstr_robotics._helper import StrFunc
from mstr_robotics._mod_prj_obj import BldShortCuts
from mstr_robotics.mstr_classes import MstrGlobal, get_conn
from mstr_robotics.report import Rep


class OpenConn:
    def login(self, base_url, *args, **kwargs):
        return get_conn(self, *args, **kwargs)


class GetChangeLog:
    def __init__(self):
        self.str = StrFunc()
        self.rep = Rep()
        self.mstr_api = MstrApi()
        self.glob = MstrGlobal()
        self.run_shortcut = BldShortCuts()

    def set_md_rep_params(self, conn, change_log_report):
        self.conn = conn
        # self.chg_log_rep_proj_id = change_log_report["chg_log_rep_proj_id"]
        # self.chg_log_report_id = change_log_report["chg_log_report_id"]  # 4
        self.chg_log_from_date_prompt_id = change_log_report["chg_log_from_date_prompt_id"]
        self.chg_log_to_date_prompt_id = change_log_report["chg_log_to_date_prompt_id"]
        self.chg_log_proj_prompt_id = change_log_report["chg_log_proj_prompt_id"]

    # @logger
    def _bld_desc_str(self, all_changes=None):
        desc_str = ""
        all_changes_sum = (
            all_changes.groupby(["Account@GUID", "Account@Login", "Object@GUID", "Object@Name", "OBJECT_TYPE_ID"])[
                "Timestamp"
            ]
            .agg("count")
            .reset_index()
        )
        for _index, s in all_changes_sum.iterrows():
            desc_str += self._empty_login(s["Account@Login"]) + " (" + str(s["Timestamp"]) + "), "
        return self.str._rem_last_char(desc_str, 2)

    def _empty_login(self, login):
        if login == "<Empty>":
            login = "Unkown"
        return login

    def bld_change_log_shortcut_df(self, conn, change_log_df):

        obj_df = (
            change_log_df.groupby(["Account@GUID", "Account@Login", "Object@GUID", "Object@Name", "OBJECT_TYPE_ID"])[
                "Timestamp"
            ]
            .agg("count")
            .reset_index()
        )

        # obj_df["NEW_OBJECT_NAME"]=""
        shortcut_l = []
        for _index, obj in obj_df.iterrows():
            # short_cut_obj=self.glob.get_short_cut_obj(conn=conn, project_id="project_id",object_id=obj["OBJECT_ID"], type=18)
            # builds the Strings for shortCut Name & Descriptions
            # name user_login__object_name__object_guid
            shortcut = []
            shortcut.append(obj["Object@GUID"])
            shortcut.append(obj["OBJECT_TYPE_ID"])
            shortcut.append(
                self._empty_login(obj["Account@Login"]) + "__" + obj["Object@Name"] + "__" + obj["Object@GUID"]
            )
            shortcut.append(self._bld_desc_str(change_log_df.loc[change_log_df["Object@GUID"] == obj["Object@GUID"]]))
            shortcut_l.append(shortcut)
        shortcut_df = pd.DataFrame(shortcut_l, columns=["Object@GUID", "OBJECT_TYPE_ID", "Object@Name", "OBJECT_DESC"])
        return shortcut_df

    def get_mig_obj_logs(self, conn, ch_conn, project_id, from_date, to_date, change_log_report, chg_log_source="pa"):
        # you find the change logs either in Platform Analytics or
        # in the meta data. For PA you'll be able to use my report.
        # For the meta data I provide a free form SQL report for SQL Server.
        # Both reports are accesable for you as a OM package in GitHub

        prp_answ_d = self._build_val_answ(
            ch_conn=ch_conn,
            project_id=project_id,
            change_log_report=change_log_report,
            chg_log_proj_id=project_id,
            chg_log_from_date=from_date,
            chg_log_to_date=to_date,
        )

        # self.glob.set_project_id(conn=conn, project_id=self.chg_log_rep_proj_id)

        instance_id = self.rep.open_Instance(conn=ch_conn, report_id=change_log_report["chg_log_report_id"])

        self.rep.set_inst_prompt_ans(
            conn=ch_conn,
            report_id=change_log_report["chg_log_report_id"],
            instance_id=instance_id,
            prompt_answ=prp_answ_d,
        )

        self.rep.get_report_def(conn=ch_conn, report_id=change_log_report["chg_log_report_id"])
        # cols=self.parse_chglog_rep_cols(rep_def)

        # rep_has_data_fg=self.rep.report_has_data(conn=self.conn,report_id=change_log_report["chg_log_report_id"],instance_id=instance_id)

        # if rep_has_data_fg:
        if 1 == 1:
            if chg_log_source == "md":
                report_df = self.rep.bld_free_form_rep_df(
                    conn=ch_conn, report_id=change_log_report["chg_log_report_id"], prp_answ_d=prp_answ_d
                )

                report_df["Project@GUID"] = report_df["Project@GUID"].apply(
                    lambda x: self.str.bld_mstr_obj_guid_sql_server(x)
                )
                report_df["Account@GUID"] = report_df["Account@GUID"].apply(
                    lambda x: self.str.bld_mstr_obj_guid_sql_server(x)
                )
                report_df["Object@GUID"] = report_df["Object@GUID"].apply(
                    lambda x: self.str.bld_mstr_obj_guid_sql_server(x)
                )

            if chg_log_source == "pa":
                report_df = self.rep.report_df(
                    conn=ch_conn, report_id=change_log_report["chg_log_report_id"], instance_id=instance_id
                )
                report_df["OBJECT_TYPE_ID"] = report_df["ObjectType@ID"].apply(
                    lambda x: self.glob.pa_get_obj_type_id(pa_obj_id=x)
                )

            return report_df
        else:
            raise ValueError("No changed objects found. Please review your prompt answers")

    def _build_val_answ(
        self,
        ch_conn,
        project_id,
        change_log_report,
        chg_log_proj_id=None,
        chg_log_from_date=None,
        chg_log_to_date=None,
        obj_where_clause_str=None,
    ):
        prompt_ans = None
        if chg_log_proj_id:
            prompt_ans = f'{{"key":"{change_log_report["chg_log_proj_prompt_id"]}@0@10","type":"VALUE","answers": "{self.mstr_api.get_project_name(conn=ch_conn, project_id=project_id)}"}},'
        if chg_log_from_date:
            prompt_ans += f'{{"key":"{change_log_report["chg_log_from_date_prompt_id"]}@0@10","type":"VALUE","answers": "{chg_log_from_date}"}},'
        if chg_log_to_date:
            prompt_ans += f'{{"key":"{change_log_report["chg_log_to_date_prompt_id"]}@0@10","type":"VALUE","answers": "{chg_log_to_date}"}}'
        if obj_where_clause_str:
            prompt_ans = f'{{"key":"{change_log_report["chg_log_proj_prompt_id"]}@0@10","type":"VALUE","answers": "{obj_where_clause_str}"}}'

        if prompt_ans:
            prompt_ans = f'{{"prompts":[{prompt_ans}]}}'
        return prompt_ans


class BldMigContent:
    @staticmethod
    def _norm_action(action):
        # The I-Server expects the mstrio Action enum *value* (e.g. "force_replace"),
        # not the uppercase name ("FORCE_REPLACE"). Passing the name causes the async
        # package build to fail with PackageStatus.CREATE_FAILED. Accept an Action
        # enum, its name, or its value and always return the value string.
        if isinstance(action, Action):
            return action.value
        try:
            return Action[action].value  # by name, e.g. "FORCE_REPLACE"
        except KeyError:
            return Action(action).value  # by value, e.g. "force_replace"

    def from_folder(self, fold_short_cut_l, action="FORCE_REPLACE", include_dependents=False):
        # purpose of this fucntion is to read out the base objects of short cuts
        # stored in certain folder. This is an typical input for migrations
        mig_l = []
        for sh in fold_short_cut_l:
            obj_d = {}
            if sh["type"] == 18:
                obj_d["id"] = sh["target_info"]["id"]
                obj_d["type"] = sh["target_info"]["type"]
                obj_d["action"] = self._norm_action(action)
                obj_d["include_dependents"] = include_dependents
                obj_d["subtype"] = sh["target_info"]["subtype"]
                mig_l.append(obj_d.copy())
        return mig_l

    def from_folder_id(self, conn, folder_id, action="FORCE_REPLACE", include_dependents=False):
        # same as from_folder, but fetches the folder contents itself from a folder id
        i_folder = folder.Folder(connection=conn, id=folder_id)
        fold_short_cut_l = i_folder.get_contents(to_dictionary=True)
        return self.from_folder(fold_short_cut_l, action=action, include_dependents=include_dependents)

    def from_excel(self, conn, excel_df, action="FORCE_REPLACE", include_dependents=False):
        # builds a migration list from an excel sheet holding object guids in an
        # "object_guid" column. Type and subtype are resolved via quick search.
        guid_l = excel_df["object_guid"].to_list()
        mig_l = []
        for guid in guid_l:
            body = {"projectIdAndObjectIds": [{"projectId": conn.project_id, "objectIds": [guid]}]}
            obj_d = browsing.get_objects_from_quick_search(connection=conn, body=body).json()
            mig_d = {}
            mig_d["id"] = obj_d["result"][0]["id"]
            mig_d["type"] = obj_d["result"][0]["type"]
            mig_d["action"] = self._norm_action(action)
            mig_d["include_dependents"] = include_dependents
            mig_d["subtype"] = obj_d["result"][0]["subtype"]
            mig_l.append(mig_d.copy())
        return mig_l
