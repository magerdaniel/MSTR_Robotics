"""
This module is designed to control workflows round regression testing based on PA data
"""
import warnings
import uuid
from mstr_robotics.report import rep, prompts, cube
from mstr_robotics.report import rep,prompts,cube
from mstr_robotics.dossier import doss_read_out,doss_read_out_det,dossier_global
from mstr_robotics._helper import msic
from mstr_robotics.mstr_classes import mstr_global, get_conn
from mstr_robotics.read_out_prj_obj import read_out_hierarchy,io_attributes
from mstr_robotics._pa_etl import run_prp_ans_bld, parse_pa
from mstr_robotics._export import file_io
from mstr_robotics._connectors import mstr_api
from mstrio.api import reports
import pandas as pd

warnings.filterwarnings("ignore")

#i_dossiers=dossiers()
i_reports = reports
i_mstr_global = mstr_global()
i_mstr_api = mstr_api()
i_get_conn = get_conn
i_rep = rep()
i_io_attributes=io_attributes()
i_prompts = prompts()
i_msic = msic()
i_cube=cube()
i_file_io=file_io()
i_load_master_data = read_out_hierarchy()
i_run_prp_ans_bld = run_prp_ans_bld()
i_parse_pa = parse_pa()
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

#notebook

import json

def bld_test_rep_name(unique_job_d):
    return unique_job_d["report_name"]+"_"+unique_job_d["report_id"]+"_sess_" +unique_job_d["session"]+"_rep_job"+str(unique_job_d["rep_job"])


class regam_jobs():
    def read_from_report(self,conn,pa_rep_l):
        #the output is a dataframe containing user report jobs
        #this is used as input to execute the tests
        i=0
        for pa_rep in pa_rep_l:

            if not 'all_jobs_df' in locals():
                all_jobs_df=pa_jobs_df=regam().fetch_pa_rep_jobs(conn=conn,                                              report_id=pa_rep["report_id"])
            else:
                all_jobs_df=all_jobs_df.append(regam().fetch_pa_rep_jobs(conn=conn,
                                                                                report_id=pa_rep["report_id"])

                )
        return pa_jobs_df

    def ZZZ_read_regam_job_doss_vis(self, conn, dossier_l):
        for dossier_id in dossier_l:
            doss_hier_df = i_dossiers.doss_hier_to_df(conn=conn, dossier_l=dossier_l)
            filtered_df = doss_hier_df[doss_hier_df['visual_name'].str.contains("REGAM", case=False)]
            rep_job_l = []
            for vis in filtered_df.itertuples():
                instance_id = i_dossiers.create_dossier_instance(conn=conn, dossier_id=dossier_id, body={}).json()["mid"]
                rep_job_l.extend(
                    i_mstr_api._get_vis_raw_metric_val(conn=conn, dossier_id=dossier_id, instance_id=instance_id,
                                                       chapter_key=vis.chapter_key, visual_key=vis.visual_key)
                    )
        return rep_job_l


    def get_att_from_job_metric(self,conn,job_metrics_l,job_prp_ans_d):
        att_d=job_prp_ans_d
        att_l=[]
        for metric in job_metrics_l:
            #print(metric_id)
            if metric["derived"]==False:
                url = f'{conn.base_url}/api/model/metrics/{metric["id"]}?showExpressionAs=tokens&showFilterTokens=true'
                metric_d = conn.get(url).json()
                #metric.json()
                for t in metric_d["expression"]["tokens"]:
                    if "target" in t.keys():
                        if "attribute" == t["target"]["subType"]:
                            print(t["attributeForm"]["objectId"])
                            print(t["target"]["objectId"])
                            att_d["metric_id"] =metric["id"]
                            att_d["metric_name"] =metric_d["information"]["name"]
                            att_d["att_id"] = t["target"]["objectId"]
                            att_d["att_form_id"] = t["attributeForm"]["objectId"]
                            att_d["displayFormat"]= i_io_attributes.read_att_form_exp(conn=conn, att_id_l=[att_d["att_id"]] )["all_att_maps_l"][0]["displayFormat"]
                            att_l.append(att_d.copy())
            else:
                print("Derived metrics are not to represent REGAM Tests")
                exit

        return att_l

    def join_prp_job_ans_l(self,vis_raw_data):
        #to set the prompts for the jobs, we need to map
        #attribute_id, form_id & the data type of the form
        #with the job and the session ID as unique identyfier
        #this happens over the metric name
        prp_ans_d = {}
        prp_ans_l = []
        for job in vis_raw_data["vis_val_df"].itertuples():
            # loops through all jobs in the visualisation
            prp_job_filt_l = []
            job_col_ind = 0
            for job_col in vis_raw_data["vis_val_df"].keys():
                # loops through all columns of a job. i.e session, jobId
                for att in vis_raw_data["vis_att_df"].itertuples():
                    # to answer the prompts, we need to map answer value (i.e. JobId) with the underlying attribute guid
                    # the mapping works over the metric name
                    if job_col == att.metric_name:
                        prp_ans_d["att_id"] = att.att_id
                        prp_ans_d["att_form_id"] = att.att_form_id
                        prp_ans_d["att_form_data_type"]=i_prompts.get_exp_prp_data_type(baseFormType=att.displayFormat)
                        prp_ans_d["filter_val_l"] = job[job_col_ind + 1]
                        prp_job_filt_l.append(prp_ans_d.copy())
                job_col_ind += 1

            prp_ans_l.append(prp_job_filt_l.copy())
        return prp_ans_l

    def run_read_out_job_vis(self,conn, filtered_df):
        prp_ans_l=[]
        for vis in filtered_df.to_dict(orient="records"):
            try:
                #unique identifier for a visualisation
                job_prp_ans_d = {
                                "dossier_id":vis["dossier_id"],
                                "chapter_key":vis["chapter_key"],
                                "page_key": vis["page_key"],
                                "visual_key":vis["visual_key"]
                                }
                #regam_job_vis_d=self.read_regam_job_vis(conn=conn, filtered_df=filtered_df)
                instance_id = i_dossiers.create_dossier_instance(conn=conn, dossier_id=vis["dossier_id"], body={}).json()["mid"]
                vis_raw_data=i_mstr_api._get_vis_raw_metric_val(conn=conn, dossier_id=vis["dossier_id"], instance_id=instance_id,
                                                                chapter_key=vis["chapter_key"], visual_key=vis["visual_key"])

                att_l=self.get_att_from_job_metric(conn=conn,
                                                   job_metrics_l=vis_raw_data["metric_guid_l"],
                                                   job_prp_ans_d=job_prp_ans_d)
                vis_att_df=pd.DataFrame.from_dict(att_l)
                vis_val_df=pd.DataFrame.from_dict(vis_raw_data["metric_val_l"])
                prp_ans_l.extend(self.join_prp_job_ans_l(vis_raw_data={"vis_att_df":vis_att_df,"vis_val_df":vis_val_df}))
            except Exception as err:
                print(err)
        return prp_ans_l


class regam():
    """
    this class controls the extraction and parsing of
    the PA raw data
    """

    def fetch_pa_rep_jobs(self, pa_conn,  pa_report_id,prompt_answ=None):
        # PA report to fetch the user executions
        #base function to fetch the raw data from PA over a simple report
        #imput will be a uniqui job identification
        pa_regam_inst_id = i_rep.open_Instance(conn=pa_conn, report_id=pa_report_id)
        if prompt_answ!=None:
            i_rep.set_inst_prompt_ans(conn=pa_conn, report_id=pa_report_id, instance_id=pa_regam_inst_id,
                                      prompt_answ=prompt_answ)
        att_col_l = i_rep.get_rep_attributes(conn=pa_conn, report_id=pa_report_id)
        pa_raw_data_df = i_rep.rep_to_dataframe(conn=pa_conn, report_id=pa_report_id,
                                                       instance_id=pa_regam_inst_id, att_col_l=att_col_l)
        return pa_raw_data_df

    def refresh_hier_att_cube(self, conn,REGAM_cube_folder_id,proj_prp_hier_l=None,
                      hier_att_cube_id=None):
        #to avoid reading out attribute / hierarchies on each execution
        #the information is cubed. To keep consistancy the cube must
        #be updated from time to time
        hier_att_df = i_load_master_data.read_out_sys_hier(conn=conn)
        print(hier_att_df)
        tbl_upd_dict=[{"tbl_name":"hier_att_df", "df":hier_att_df,"update_policy":"REPLACE"}]
        #print(tbl_upd_dict)
        hier_att_cube_id = i_cube.upload_cube_mult_table(conn, mtdi_id=hier_att_cube_id, tbl_upd_dict=tbl_upd_dict,
                                                         cube_name="System hier_att", folder_id=REGAM_cube_folder_id, force=True)
        return hier_att_cube_id

    def load_hier_att_df(self,conn,hier_att_cube_id):
        #if you run mutliple test without touching attributes
        # you do not need to read all out
        hier_att_df = i_cube.load_cube_to_df(conn=conn, cube_id=hier_att_cube_id)
        return hier_att_df

    def run_bld_job_prp_JSON(self,conn,pa_raw_data_df,hier_att_df):
        #previously we created a pandas df containing 1:1
        #the PA prompt answers logs. In this function
        #we loop job by job to genenerate the prompt
        #answer JSON files for the whole test set

        all_jobs_prp_ans_JSON_l = []
        run_id = uuid.uuid1().__str__()
        for report_job in pa_raw_data_df.iterrows():
            prompt_ans_JSON_l=[]

            try:
                project_id=report_job[1]["Project@GUID"]
                #the unique identifier for a certain report job
                run_prop_d={"run_id":run_id,"proj_id":project_id,"report_id":report_job[1]["Object@GUID"],
                              "report_name":report_job[1]["Object@Name"],"session":report_job[1]["Session@ID"],"rep_job":report_job[1]["Job@ID"]}

                instance_id = i_rep.open_Instance(conn=conn, report_id=report_job[1]["Object@GUID"])
                rep_inst_prp_l=i_prompts.get_rep_prp_all(conn=conn, report_id=report_job[1]["Object@GUID"], instance_id=instance_id)
                pa_prp_id_l=i_parse_pa.get_pa_prp_id_l(get_pa_raw_data_df=pa_raw_data_df)

                if len(pa_prp_id_l)!=0:
                    action_prp_l=i_msic.get_dict_with_id_in_l(dict_l=rep_inst_prp_l,search_l=pa_prp_id_l)
                    prompt_ans_JSON_l=i_run_prp_ans_bld.bld_pa_job_prp_JSON(conn=conn
                                                                            ,action_prp_l=action_prp_l
                                                                            ,pa_raw_data_df=pa_raw_data_df
                                                                            ,hier_att_df=hier_att_df
                                                                            ,report_id=report_job[1]["Object@GUID"]
                                                                            ,instance_id=instance_id )

                rep_job_prp_ans_d=run_prop_d
                rep_job_prp_ans_d["sucsess_fg"]=True
                rep_job_prp_ans_d["cnt_prompts"] = len(pa_prp_id_l)
                rep_job_prp_ans_d["prompt_ans"]=i_prompts.frame_prp(prompt_ans_JSON_l)
                all_jobs_prp_ans_JSON_l.append(rep_job_prp_ans_d)


            except Exception as err:
                rep_job_prp_ans_d = run_prop_d
                rep_job_prp_ans_d["sucsess_fg"]=False
                rep_job_prp_ans_d["error_msg"]=err.args[0]
                print(err.args[0])
                all_jobs_prp_ans_JSON_l.append(rep_job_prp_ans_d)


        return {"run_id": run_id,"all_jobs_prp_ans_JSON_l":all_jobs_prp_ans_JSON_l}

class test_exe():

    def run_test_exe(self,conn,all_jobs_prp_ans_JSON_l):

        for job in all_jobs_prp_ans_JSON_l:
            # create instance
            # and answer prompts
            #save report
            #print(job)
            if job["sucsess_fg"] == True:
                #print(job["rep_job"])
                prompt_answ = job["prompt_ans"]
                report_id = job["report_id"]
                rep_name = bld_test_rep_name(job)
                #print(prompt_answ)
                instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
                i_rep.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                                           prompt_answ=prompt_answ)

                i_rep.get_open_prompts(conn=conn, report_id=report_id, instance_id=instance_id)

                try:
                    #rep_df = i_rep.rep_to_dataframe(conn=conn, report_id=report_id, instance_id=instance_id)
                    r = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=rep_name)
                    print("Report: " +rep_name + " has been created successfully" )

                except Exception as err:
                    print("Report: " + rep_name + " creation failed. Error msg:"+ str(err))
                #print(rep_df)
