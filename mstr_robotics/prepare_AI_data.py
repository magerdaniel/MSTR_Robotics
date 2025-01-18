import os
import pandas as pd

from mstr_robotics.read_out_prj_obj import i_prompts
from mstr_robotics.report import cube,rep
from mstr_robotics._helper import msic
from mstr_robotics._connectors import mstr_api
from mstr_robotics.mstr_classes import mstr_global
from mstrio.project_objects import OlapCube
from mstr_robotics.read_out_prj_obj import io_attributes

io_att=io_attributes()
from flashtext import KeywordProcessor
from itertools import chain
import json

i_cube=cube()
i_msic=msic()
i_mstr_global=mstr_global()
i_rep=rep()
i_KeywordProcessor = KeywordProcessor()
i_chain=chain()
i_mstr_api=mstr_api()

class read_out_cube_att():

    def trans_cbe_el_prp(self, ele_str):

        el_l = ele_str.split(":")
        el_ans_str = "h" + ':'.join(el_l[-1 * (len(el_l) - 1):])
        el_ans_d = {"id": ':'.join(el_l[-1 * (len(el_l) - 1):]) + ";" + el_l[0]}
        return el_ans_d


    def read_cube_att_elements(self,conn,cube_list_l ):
        key_val_l=[]

        for cube_id in cube_list_l:
            #print(cube_id["id"])
            cube = OlapCube(connection=conn, id=cube_id["id"])
            att_ell_l=cube.attr_elements
            for att in att_ell_l:
                #print(att["attribute_name"])
                key_val_d={}
                key_val_d["attribute_id"]=att["attribute_id"]
                key_val_d["attribute_name"]=att["attribute_name"]
                for element in att["elements"]:
                    #print(element)
                    key_val_d["element_ans"]=self.trans_cbe_el_prp(ele_str=element["id"])
                    key_val_d["element_val_key"]=element["formValues"][0]
                    key_val_l.append(key_val_d.copy())

        return key_val_l

    def read_cube_att_forms(self,conn,cube_def, att_name, form_name=None):
        #reads out attorm definitions
        rag_att_form_id_d = {}
        for att in cube_def["definition"]["availableObjects"]["attributes"]:
            i=0
            for form in att["forms"]:
                #print(form)
                rag_att_form_id_d["project_id"] = conn.project_id
                rag_att_form_id_d["attribute_id"] = att["id"]
                rag_att_form_id_d["attribute_name"] = att["name"]
                rag_att_form_id_d["form_id"] = form["id"]
                rag_att_form_id_d["form_name"] = form["name"]
                rag_att_form_id_d["form_dataType"] = i_prompts.get_exp_prp_data_type(form["dataType"])
                if att["name"] == att_name and form["name"] == form_name:
                    return rag_att_form_id_d
                #elif att["name"] == att_name and form_name==None and i==0:
                #    return rag_att_form_id_d
                i+=1

        return rag_att_form_id_d

    #def read_cube_att_form_exp_val_l(conn, cube_id,*args,**kwargs):


    def fetch_cube_elements(self, conn, cube_id,limit_val=10000,*args,**kwargs):
        rag_att_form_l = []
        offset_val = 0
        row_count = 1
        while row_count > 0:
            resp=i_mstr_api.get_v2_cube_instance(conn=conn, cube_id=cube_id, offset_val=offset_val,limit_val=limit_val,*args,**kwargs)
            #url = f'{conn.base_url}/api/v2/cubes/{cube_id}/instances?offset={offset_val}&limit={limit_val}'
            #resp = conn.post(url)
            cube_def= resp.json()
            for att in cube_def["definition"]["grid"]["rows"]:
                form_nr = 0
                rag_att_form_d = {}
                rag_att_form_d["project_id"] = conn.project_id
                rag_att_form_d["attribute_id"] = att["id"]
                rag_att_form_d["attribute_name"] = att["name"]
                row_count = len(att["elements"])
                for form in att["forms"]:
                    rag_att_form_d["form_id"] = form["id"]
                    rag_att_form_d["form_name"] = form["name"]
                    rag_att_form_d["form_dataType"] = i_prompts.get_exp_prp_data_type(form["dataType"])
                    for element in att["elements"]:
                        rag_att_form_d["key"] = element["formValues"][form_nr]
                        rag_att_form_d["ele_prp_ans"] = element["id"]
                        rag_att_form_l.append(rag_att_form_d.copy())
            offset_val += limit_val
            resp.close()
        return rag_att_form_l

    def read_cube_att_form_exp_val_l(self, conn, cube_list_l,*args,**kwargs):
        all_cube_element_l=[]
        for cube in cube_list_l:
            cube_element_l=self.fetch_cube_elements(conn=conn,cube_id=cube["id"],*args,**kwargs)
            all_cube_element_l.extend(cube_element_l)
        return all_cube_element_l

    def zzz_read_cube_att_form_exp_val_l(self,conn,cube_list_l):
        element_d_l = []
        for cube_id in cube_list_l:
            cube = OlapCube(connection=conn, id=cube_id["id"])
            cube_df = cube.to_dataframe()
            cube_def = i_cube.get_cube_def(conn=conn, cube_id=cube_id["id"])

            for col in cube_df.columns:
                #print(cube_id["id"])
                #print(col)
                col_att_form_l = col.split("@")
                if len(col_att_form_l) > 1:
                    #print("col added")
                    att_name = col_att_form_l[0]
                    form_name = col_att_form_l[1]
                    att_form_d = self.read_cube_att_forms(conn=conn,cube_def=cube_def,att_name=att_name,form_name=form_name)
                    distinct_val = cube_df[col].unique()
                    for val in distinct_val:
                        att_form_d["key"] = val
                        #att_form_d["prp_ans"] = val
                        element_d_l.append(att_form_d.copy())
                else:
                    #print("col not added")
                    print("att_name :" +str(col))
                    att_name = col
                    att_form_d = self.read_cube_att_forms(conn=conn,cube_def=cube_def,att_name=att_name)
                    distinct_val = cube_df[col].unique()
                    for val in distinct_val:
                        att_form_d["key"] = val
                        element_d_l.append(att_form_d.copy())


        return element_d_l

class map_objects():

    def get_doss_rep_prp(self,conn, object_l):
        prp_rep_l = []
        for rep_dos in object_l:
            if rep_dos["type"] == 3:
                for prp in i_rep.get_rep_prp_l(conn=conn, report_id=rep_dos["id"]):
                    rep_prp_d = {}
                    rep_prp_d["project_id"]=conn.project_id
                    rep_prp_d["rep_dos_id"] = rep_dos["id"]
                    rep_prp_d["type"] = rep_dos["type"]
                    rep_prp_d["sub_type"] = rep_dos["sub_type"]
                    rep_prp_d["prompt_id"] = prp.id
                    prp_rep_l.append(rep_prp_d.copy())
            elif rep_dos["type"] == 55:
                for prp in i_mstr_api.get_dossier_prp_l(conn=conn, dossier_id=rep_dos["id"]):
                    rep_prp_d = {}
                    rep_prp_d["project_id"]=conn.project_id
                    rep_prp_d["rep_dos_id"] = rep_dos["id"]
                    rep_prp_d["type"] = rep_dos["type"]
                    rep_prp_d["sub_type"] = rep_dos["sub_type"]
                    rep_prp_d["prompt_id"] = prp["id"]
                    prp_rep_l.append(rep_prp_d.copy())

        return prp_rep_l

    def get_RAG_cube_attribute_id(self,conn, cube_l):
        cube_att_l = []
        for cube_id in cube_l:
            cube_def = i_cube.get_cube_def(conn=conn, cube_id=cube_id)
            cube_att_d = {"cube_id": cube_def["id"], "cube_name": cube_def["name"]}
            for att in cube_def["definition"]["availableObjects"]["attributes"]:
                cube_att_d["attribute_id"] = att["id"]
                cube_att_d["attribute_name"] = att["name"]
                cube_att_d["type"] = att["type"]
                cube_att_l.append(cube_att_d.copy())
        return cube_att_l

    def get_rag_rep_prp_df(self,conn,cube_id,rep_dos_id,key_word_l,mstr_rag_col_d):
        rag_rep_prp_df=self.bld_ai_prp_ans(conn=conn,cube_id=cube_id,mstr_rag_col_d=mstr_rag_col_d, rep_dos_id=rep_dos_id, key_word_l=key_word_l)
        return rag_rep_prp_df

    def bld_ai_prp_ans(self,conn, cube_id, mstr_rag_col_d, rep_dos_id, key_word_l):
        obj_prp_ans_l = []
        rag_files = []
        cube_disp_col_l = ["prompt_id", "prp_subType", "rep_dos_id", "object_id", "attribute_id", "obj_type",
                           "obj_prp_ans", "obj_prp_rel_dummy", "rep_prp_rel_dummy", "obj_prp_rel_dummy"]
        cube_disp_col_l = ["attribute_id"]
        key_val_l = []
        disp_col_ids_l = []
        attr_elements_l = []
        key_val_l = ["key"]  #
        # print(mstr_rag_col_d)
        for col_name in mstr_rag_col_d[cube_id].keys():
            cube_disp_col_l = mstr_rag_col_d[cube_id].keys()
            # print(col_name)
            if col_name == 'rep_dos_id':
                rep_dos_id_filt = mstr_rag_col_d[cube_id][col_name] + ":" + rep_dos_id
                attr_elements_l.append(rep_dos_id_filt)

            if col_name in cube_disp_col_l:
                disp_col_ids_l.append(mstr_rag_col_d[cube_id][col_name])

            if col_name in key_val_l:
                for key in key_word_l:
                    prp_ans_j = mstr_rag_col_d[cube_id][col_name] + ":" + str(key)
                    attr_elements_l.append(prp_ans_j)

        df = i_cube.quick_query_cube(conn=conn, cube_id=cube_id, attribute_l=disp_col_ids_l, metric_l=None,
                              attr_elements=attr_elements_l)
        return df


class zzz_bld_obj_ans():

    def zzz_trans_att_exp_prp(self,attributes_l):
        att_form_d_l = []
        for att_form in attributes_l:
            att_form_d = {}
            att_form_d["attribute_id"] = att_form["attribute_id"]
            att_form_d["attribute_name"] = att_form["attribute_name"]
            att_form_d["form_id"] = att_form["form_id"]
            att_form_d["form_dataType"] = att_form["form_dataType"]
            att_form_d["key"] = att_form["form_name"]
            att_form_d["display_form_nr"] = att_form["display_form_nr"]
            att_form_d["browse_form_nr"] = att_form["browse_form_nr"]
            att_form_d_l.append(att_form_d.copy())
            att_form_d_l=i_msic.rem_dbl_dict_in_l(dict_l=att_form_d_l)
        return att_form_d_l

    def bld_att_obj_ans(self,attributes_l):
        # Now 'data' contains the parsed JSON as a Python dictionary or list
        att_d_l = []
        att_l = []
        key_word_l = []
        # documents_l=[]
        for m in attributes_l:
            if m["attribute_id"] not in att_l:
                att_l.append(m["attribute_id"])
                att_d = {}
                att_d["object_id"] = m["attribute_id"]
                att_d["type"] = "attribute"
                att_d["key"] = m["attribute_name"]
                att_d["ans"]=json.dumps({"id":m["attribute_id"] , "type":"attribute"})

                att_d_l.append(att_d.copy())
        #att_d_l = i_msic.rem_dbl_dict_in_l(dict_l=att_d_l)
        return att_d_l

    def bld_met_obj_ans(self,metrics_l):
        all_met_d_l = []
        for m in metrics_l:
            if "E0CCB9CF22104A489CBE78D974AFD19E" in m["metric_path_ids"]:
                row_d = {}
                row_d["object_id"] = m["metric_id"]
                row_d["type"] = "metric"
                row_d["key"] = m["metric_name"]
                row_d["ans"] = json.dumps({"id": m["metric_id"], "type": "metric"})
                all_met_d_l.append(row_d.copy())
        return list(all_met_d_l)