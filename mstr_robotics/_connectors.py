import json
import uuid
from mstr_robotics._helper import msic
i_msic=msic()

class mstr_api():

    def get_prj_tbl(self,conn):
        inst_u = f"{conn.base_url}/api/model/tables"
        r = conn.get(inst_u)
        return r

    def get_dossier_prp_l(self,conn,dossier_id):
        endpoint = f'/api/documents/{dossier_id}/prompts'
        prp_l = conn.get(endpoint=endpoint).json()
        return prp_l

    def get_ele_prp_ans(self,conn,report_id,instance_id,prompt_id,att_form_str=None,offset=0, limit=200):
        #get availeble objects from an element prompt
        #be carefull, this can be an performance issue
        #if the attribute has too many elements
        rest_limit=limit
        all_values=[]
        while rest_limit>=0:
            if att_form_str == None:
                inst_u = f"{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/{prompt_id}/elements?offset={offset}&limit={limit}"
            else:
                inst_u = f'{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/{prompt_id}/elements?offset={offset}&limit={limit}&searchPattern={att_form_str}'
            r = conn.get(inst_u, headers=conn.headers)

            all_values.append(r.json()["elements"])
            rest_limit=int(r.headers["x-mstr-total-count"])-offset
            offset += limit
        return all_values

    def create_dossier_instance(self, conn, dossier_id, body=None, error_msg=None):
        endpoint = f'{conn.base_url}/api/dossiers/{dossier_id}/instances'
        return conn.post(url=endpoint, json=body).json()["mid"]

    def get_prp_ans(self,conn,report_id,instance_id,prompt_id,offset=0, limit=200):
        #get availeble objects from an object prompt
        rest_limit=limit
        prp_ans_base_l=[]
        while rest_limit>=0:
            inst_u = f'{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/{prompt_id}/objects?offset={offset}&limit={limit}'
            r = conn.get(inst_u, headers=conn.headers)
            prp_ans_base_l.extend(r.json()["objects"])
            rest_limit=int(r.headers["x-mstr-total-count"])-offset
            offset +=limit

        return prp_ans_base_l

    def cr_short_cut(self, conn, object_id, object_type, folder_id):
        #create an short.
        short_cut_folder_j = f'{{"folderId": "{folder_id}"}}'
        short_cut_url = f'{conn.base_url}/api/objects/{object_id}/type/{str(object_type)}/shortcuts'
        short_cut_obj_j = conn.post(short_cut_url, data=short_cut_folder_j)
        return short_cut_obj_j

    def rename_object(self, conn, object_id, object_type, new_name, desc_str):
        #rename an existing object
        # if an object of the same type allready exists in
        #the same folder, it simply adds a random string
        #fuck nows the meaning of flags=70. I just took it over
        #from the swagger page
        base_url=f'{conn.base_url}/api/objects/{object_id}?type={object_type}&flags=70'
        try:
            #short_cut_obj.alter(name=str(new_name) , description=str(desc_str))
            body='{'+f'"name":"{new_name}","description":"{str(desc_str)}"'+'}'
            resp=conn.put(base_url,data=str(body))
        except:
            #short_cut_obj.alter(name=str(new_name+ uuid.uuid1().hex), description=str(desc_str))
            body={"name":str(new_name + uuid.uuid1().hex), "description":str(desc_str)}
            body = '{' + f'"name":"{new_name}{uuid.uuid1().hex}","description":"{str(desc_str)}"'+'}'
            resp=conn.put(base_url,data=str(body))

        return resp

    def get_project_name(self, conn,project_id):
        #provides a readable project name for humans
        project_resp = conn.get(f'{conn.base_url}/api/projects')
        project_name=""
        for p in project_resp.json():
            if p["id"] == project_id:
                project_name= p["name"]

        if project_name!="":
            return project_name
        else:
            raise

    def get_prompt_def(self,conn,prompt_id):
        inst_u = f'{conn.base_url}/api/model/prompts/{prompt_id}?showExpressionAs=tree'
        r = conn.get(inst_u, headers=conn.headers)
        return r.json()

    def get_custom_group(self,conn, custom_group_id, show_expression_as="tokens"):
        url = f'{conn.base_url}/api/model/customGroups/{custom_group_id}?showExpressionAs={show_expression_as}'
        custom_group_def = conn.get(url)
        return custom_group_def.json()

    def get_consolidation(self,conn, consolidation_id):
        url = f'{conn.base_url}/api/model/consolidations/{consolidation_id}'
        consolidation_def = conn.get(url)
        return consolidation_def.json()

    def get_hier_att(self,conn,hier_id,displayForms="All",offset=0, limit=200):
        #Available displayForms: all, browseForms, reportDisplayForms, customForms, unknown
        #I develope initially to get the form_ids for attribute qualification prompts
        #what I like here is, that you get th most relevant infos of attributes
        #in an simple way
        rest_limit=limit
        prp_ans_base_l=[]
        while rest_limit>=0:
            inst_u = f'{conn.base_url}/api/hierarchies/{hier_id}/attributes?displayForms={displayForms}&offset={offset}&limit={limit}'
            r = conn.get(inst_u, headers=conn.headers)
            prp_ans_base_l.append(r.json())
            rest_limit=int(r.headers["x-mstr-total-count"])-offset
            offset +=limit

        return prp_ans_base_l

    def get_report_sql(self,conn,report_id,instance_id):
        #if a report runs into an error due to wrong SQL or
        #doesn't return data, this can bee a big help

        url=f'{conn.base_url}/api/v2/reports/{report_id}/instances/{instance_id}/sqlView'
        rep_sql=conn.get(url)
        return rep_sql

    def get_report_all_def(self,conn,report_id, show_expression_as="tokens",show_filter_tokens="true",show_advanced_properties="true"):
        url=f'{conn.base_url}/api/model/reports/{report_id}?showExpressionAs={show_expression_as}&showFilterTokens={show_filter_tokens}&showAdvancedProperties={show_advanced_properties}'
        #print(url)
        report_def=conn.get(url)
        return report_def.json()

    def get_report_raw(self,conn,report_id,instance_id):
        #normaly I use the report libraries from mstrio
        # to fetch the data. Sometimes they bring back an error. so this one is for trouble
        #shooting
        url=f'{conn.base_url}/api/v2/reports/{report_id}/instances/{instance_id}/sqlView'
        rep_sql=conn.get(url)
        return rep_sql

    def _get_vis_raw_metric_val(self, conn, dossier_id, instance_id, chapter_key,
                                visual_key, offset=0, limit=100):
        #reads out the metric values of a visualisation
        #primary use is to read out REGAM JOBs
        #technicaly it's an interim solution as I hope there will be
        #a standard class in mstrio to export data from visuals
        total = limit + 1
        raw_val_l = []
        metric_guid_d={}
        while total > offset:
            regam_jobs_col_l = []
            regam_job_metric_guid_l=[]
            #endpoint brings back data and the definition of the visual
            url = f'{conn.base_url}/api/v2/dossiers/{dossier_id}/instances/{instance_id}/chapters/{chapter_key}/visualizations/{visual_key}?offset={offset}&limit={limit}'
            data_viz = conn.get(url).json()
            #read out the metric_id's in the viz

            for m in data_viz["definition"]["grid"]["columns"][0]["elements"]:
                regam_jobs_col_l.append(m["name"])
                if "derived" in m.keys():
                    metric_guid_d["derived"]=m["derived"]
                else:
                    metric_guid_d["derived"] = False

                metric_guid_d["id"]=m["id"]
                regam_job_metric_guid_l.append(metric_guid_d.copy())

            #read out the metric values. values & ID's are machted over the order
            regam_jobs_val_l = data_viz["data"]["metricValues"]["raw"]
            raw_val_l.extend(i_msic.list_to_dict(list_in_l=regam_jobs_val_l,
                                          col_l=regam_jobs_col_l)
                             )
            total = data_viz["data"]["paging"]["total"]
            offset += limit

        return {"metric_val_l":raw_val_l,"metric_guid_l":regam_job_metric_guid_l}

    def get_dossier_def(self,conn,dossier_id):
        url = f'{conn.base_url}/api/v2/dossiers/{dossier_id}/definition'
        doss_hier = conn.get(url)
        return doss_hier.json()

    def get_dossier_detail(self, conn, dossier_id, instance_id, chapter_key, vis_id):
        url = f"{conn.base_url}/api/v2/dossiers/{dossier_id}/instances/"
        url += f"{instance_id}/chapters/{chapter_key}/visualizations/{vis_id}?offset=0&limit=1000"
        return conn.get(url).json()

    def get_v2_cube_instance(self,conn,cube_id,offset_val,limit_val):
        url = f'{conn.base_url}/api/v2/cubes/{cube_id}/instances?offset={offset_val}&limit={limit_val}'
        return conn.post(url)



    def zzz_fetch_cube_elements(self,conn, cube_id, attribute_id, limit_val=100):
        all_element_l = []
        offset_val = 0
        tot_count = 0
        while tot_count >= offset_val:
            url = f"{conn.base_url}/api/cubes/{cube_id}/attributes/{attribute_id}/elements?offset={offset_val}&limit={limit_val}"
            resp = conn.get(url)
            tot_count = int(resp.headers["x-mstr-total-count"])
            offset_val += limit_val
            all_element_l.extend(resp.json())
        return all_element_l



    def ZZZ_get_cube_data(self,conn,cube_id,instance_id,offset,limit) :
        inst_u = f'{conn.base_url}/api/v2/cubes/{cube_id}/instances/{instance_id}?offset={offset}&limit={limit}'
        cube_ds = conn.get(inst_u, headers=conn.headers)
        return cube_ds

    def ZZZ_save_rep_sat_inst_as(self,conn,report_id,instance_id,folder_id,rep_name):
        data={"overwrite": True,
              "name": rep_name,
              "destinationFolderId": folder_id,
              "promptOptions": {
                "saveAsWithAnswers": True,
                "saveAsFilterWithPrompts": True,
                "saveAsTemplateWithPrompts": True
                                }
            }
        conn.headers["X-MSTR-MS-Instance"]=instance_id
        inst_u = f'{conn.base_url}/api/model/reports/{report_id}/instances/saveAs'
        r = conn.post(inst_u,  data=json.dumps(data))
        del conn.headers["X-MSTR-MS-Instance"]
        return r.json()