import pandas as pd
import json
from mstr_robotics.report import prompts,cube,rep
from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics._helper import msic
from mstr_robotics.prepare_AI_data import map_objects
from mstr_robotics.read_out_prj_obj import read_gen
import ast

i_read_gen=read_gen()
i_rep=rep()
i_prompts=prompts()
i_df_helper=df_helper()
i_cube=cube()
i_map_objects=map_objects()
i_msic=msic()

class answer_prompts():


    def __init__(self,obj_prp_rel_df=None
                 ,attribute_form_elements_df=None
                 ,attribute_elements_df=None
                 ,dos_rep_prp_rel_df=None
                 ,dashboard_definitions_df=None
                 ,dashboard_chapter_filter_df=None
                 ,dashboard_selector_filter_df=None
                 ,att_form_def_df=None
                 ,report_def_df=None):

        self.obj_prp_rel_df=obj_prp_rel_df
        self.att_form_def_df=att_form_def_df
        self.attribute_form_elements_df=attribute_form_elements_df
        self.attribute_elements_df=attribute_elements_df
        self.dos_rep_prp_rel_df=dos_rep_prp_rel_df
        self.dashboard_definitions_df=dashboard_definitions_df
        self.dashboard_chapter_filter_df=dashboard_chapter_filter_df
        self.dashboard_selector_filter_df=dashboard_selector_filter_df
        self.report_def_df=report_def_df

    def bld_exp_prp_met_ans(self,obj,operator,filter_val_l):
        metric_exp_prp = {}
        metric_exp_prp["met_id"] = obj["object_id"]
        metric_exp_prp["data_type"] = "Real"
        metric_exp_prp["operator"] = operator
        metric_exp_prp["filter_val_l"] = filter_val_l
        if operator =="Between":
            filter_val_l_l=json.loads(filter_val_l)
            metric_exp_prp["filter_val_l"] = str(filter_val_l_l[0])
            metric_exp_prp["filter_val_l_1"] = str(filter_val_l_l[1])
        metric_exp_prp["level"] = "default"
        prp_ans_d= i_prompts.bld_metric_exp_prp(metric_exp_prp=metric_exp_prp.copy())
        return prp_ans_d

    def bld_met_prp_ans(self,d_prp_ans_d,operator, filter_val_l):

        d_prp_ans_d["prp_ans_d"] = self.bld_exp_prp_met_ans(obj=d_prp_ans_d
                                                            ,operator=operator
                                                            ,filter_val_l=filter_val_l)
        return d_prp_ans_d

    def bld_att_element_prp_ans(self,d_prp_ans_d, filt_obj_rag_df,filter_val_l=None):
        ele_ans_JSON_l = []

        filt_att_element_df=self.attribute_elements_df[self.attribute_elements_df["element_val"].isin(filter_val_l)]
        filt_df = pd.merge(filt_obj_rag_df[["project_id","prompt_id", "prp_subType", "object_id", "obj_type"]],
                            filt_att_element_df[
                                ["project_id","prompt_id", "prp_subType", "attribute_id", "attribute_name","ele_prp_ans","element_val"]],
                            left_on=["project_id",'object_id'], right_on=["project_id",'attribute_id'], how='inner')

        filt_df = i_df_helper.clean_double_col(df=filt_df)
        filt_df.drop_duplicates(inplace=True)
        for i, ans in filt_df.iterrows():
            ele_ans_JSON_l.append(str(ast.literal_eval(ans["ele_prp_ans"])))
            prompt_id = ans["prompt_id"]

        att_element_ans_d=i_prompts.frame_prp_ans(prompt_id=prompt_id, prp_type="ELEMENTS", prp_ans_JSON_l=ele_ans_JSON_l)

        d_prp_ans_d["prp_ans_d"] = att_element_ans_d
        return d_prp_ans_d

    def bld_exp_elem_prp_ans(self,d_prp_ans_d, filt_obj_rag_df,operator,filter_val_l=None):

        filter_val_df=self.attribute_form_elements_df[self.attribute_form_elements_df["element_val"].isin(filter_val_l)]
        filt_df = pd.merge(filt_obj_rag_df[["project_id","prompt_id", "prp_subType", "object_id", "obj_type"]],
                           filter_val_df[
                               ["project_id","prompt_id", "prp_subType", "attribute_id", "attribute_name", "form_id",
                                "form_name", "form_dataType", "element_val"]],
                           left_on=["project_id",'object_id'], right_on=["project_id",'attribute_id'], how='inner')
        filt_df = i_df_helper.clean_double_col(df=filt_df)
        # print(filt_df)
        filter_val_l = []
        filt_df.drop_duplicates(inplace=True)
        for index, f_form_ele in filt_df.iterrows():
            form_id = f_form_ele["form_id"]
            form_dataType = f_form_ele["form_dataType"]
            filter_val_l.append(str(f_form_ele["element_val"]))

        att_exp_ans_d = {"att_id": d_prp_ans_d["object_id"], "att_form_id": form_id,
                         "form_data_type": form_dataType, "operator": operator,
                         "filter_val_l": filter_val_l}

        att_exp_ans_d = i_prompts.bld_att_exp_prp_l(prp_job_ans_d=att_exp_ans_d)
        d_prp_ans_d["prp_ans_d"] = att_exp_ans_d
        #prp_ans_d_l.append(d_prp_ans_d.copy())

        return d_prp_ans_d

    def bld_att_qual_prp_ans(self, attribute, d_prp_ans_d, operator, filter_val_l):


        filt_obj_rag_df=self.att_form_def_df[self.att_form_def_df["attribute_name"]==attribute]
        filt_obj_rag_df_sorted = filt_obj_rag_df.sort_values(by='display_form_nr', ascending=False)
        

        for index, form in filt_obj_rag_df_sorted.iterrows():
            if int(form["display_form_nr"]) > 0:
                att_exp_ans_d = {"att_id": form["attribute_id"], "att_form_id": form["form_id"],
                                 "form_data_type": form["REST_form_type"], "operator": operator,
                                 "filter_val_l": filter_val_l}

                att_exp_ans_d = i_prompts.bld_att_exp_prp(prp_job_ans_d=att_exp_ans_d)
                d_prp_ans_d["prp_ans_d"] = att_exp_ans_d

        return d_prp_ans_d

 
    def AI_mstr_prp_page_ans(self,vector_store, bi_request_d,rep_dos_id ):
        
        b_filter_d=bi_request_d["filter"]
        rep_dos_obj_prp_rel_df=pd.merge(self.dos_rep_prp_rel_df,
                   self.obj_prp_rel_df, 
                   left_on=["project_id","prompt_id"], right_on=["project_id","prompt_id"],
                     how="inner")
        rep_dos_obj_prp_rel_df=rep_dos_obj_prp_rel_df[rep_dos_obj_prp_rel_df["rep_dos_id"]==rep_dos_id]
        prp_ans_d_l = []
        all_key_word_l = vector_store.extract_keywords(msg_t=str(b_filter_d))
        #print(rep_dos_id)
        #filt_obj_rag_df = i_map_objects.bld_ai_prp_ans(conn, cube_id=cube_obj_prp_rel_id,
        #                                               rep_dos_id=rep_dos_id, key_word_l=key_word_l)

        try:
            all_obj_key_l=[]
            for f in b_filter_d.keys():
                try:

                    d_prp_ans_d = {}
                    if f[:9] == "att_eleme":
                        # f={bi_request_d["filter"][f]["attribute"] : {filter_d[f]["operator"]: filter_d[f]["element_list"]}}
                        # prp_ans_d=filter_d[b_filter_d[f]["attribute"]]={b_filter_d[f]["operator"]: b_filter_d[f]["element_list"]}
                        prp_filt_obj_rag_df = rep_dos_obj_prp_rel_df[(rep_dos_obj_prp_rel_df["object_name"] == b_filter_d[f]["attribute"]) & (
                                    rep_dos_obj_prp_rel_df["prp_subType"].isin(["prompt_expression","prompt_elements"]))]
                        for i, obj in prp_filt_obj_rag_df.iterrows():
                            d_prp_ans_d["prompt_id"] = obj["prompt_id"]
                            d_prp_ans_d["prp_subType"] = obj["prp_subType"]
                            d_prp_ans_d["object_id"] = obj["object_id"]
                            d_prp_ans_d["obj_type"] = obj["obj_type"]
                            d_prp_ans_d["operator"] = b_filter_d[f]["operator"]
                            prp_subType= obj["prp_subType"]

                            if prp_subType=="prompt_elements":

                                prp_ans_d = self.bld_att_element_prp_ans( d_prp_ans_d=d_prp_ans_d
                                                                    , filt_obj_rag_df=prp_filt_obj_rag_df
                                                                        ,filter_val_l=b_filter_d[f]["element_list"]
                                                                    )

                            elif  prp_subType=="prompt_expression":
                                prp_ans_d = self.bld_exp_elem_prp_ans( d_prp_ans_d=d_prp_ans_d
                                                                                    , filt_obj_rag_df=prp_filt_obj_rag_df
                                                                                    , operator=b_filter_d[f]["operator"],
                                                                                    filter_val_l=i_msic.list_elements_to_str(b_filter_d[f]["element_list"]))

                        # print(prp_ans_d)
                        prp_ans_d_l.append(prp_ans_d.copy())

                    if f[:9] == "att_qual_":

                        prp_filt_obj_rag_df = rep_dos_obj_prp_rel_df[(rep_dos_obj_prp_rel_df["object_name"] == b_filter_d[f]["attribute"]) & (
                                    rep_dos_obj_prp_rel_df["prp_subType"] == "prompt_expression")]

                        for i, obj in prp_filt_obj_rag_df.iterrows():
                            d_prp_ans_d["prompt_id"] = obj["prompt_id"]
                            d_prp_ans_d["prp_subType"] = obj["prp_subType"]
                            d_prp_ans_d["object_id"] = obj["object_id"]
                            d_prp_ans_d["obj_type"] = obj["obj_type"]
                            d_prp_ans_d["operator"] = b_filter_d[f]["operator"]

                        prp_ans_d = self.bld_att_qual_prp_ans( attribute=b_filter_d[f]["attribute"]
                                                            ,d_prp_ans_d=d_prp_ans_d
                                                            , operator=b_filter_d[f]["operator"]
                                                            , filter_val_l=b_filter_d[f]["value"])

                        prp_ans_d_l.append(prp_ans_d.copy())

                    if f[:9] == "metric_fi":
                        prp_filt_obj_rag_df = rep_dos_obj_prp_rel_df[(rep_dos_obj_prp_rel_df["object_name"] == b_filter_d[f]["metric"]) & (
                                    rep_dos_obj_prp_rel_df["prp_subType"] == "prompt_expression")]
                        for i, obj in prp_filt_obj_rag_df.iterrows():
                            d_prp_ans_d["prompt_id"] = obj["prompt_id"]
                            d_prp_ans_d["prp_subType"] = obj["prp_subType"]
                            d_prp_ans_d["object_id"] = obj["object_id"]
                            d_prp_ans_d["obj_type"] = obj["obj_type"]
                            d_prp_ans_d["operator"] = b_filter_d[f]["operator"]

                        prp_ans_d = self.bld_met_prp_ans(d_prp_ans_d=d_prp_ans_d
                                                                    , operator=b_filter_d[f]["operator"]
                                                                    , filter_val_l=str(b_filter_d[f]["value"]))
                        prp_ans_d_l.append(prp_ans_d.copy())

                    print(f[:10])
                    if f[:10] == "obj_prompt":
                        #rep_dos_obj_prp_rel_df=rep_dos_obj_prp_rel_df[(rep_dos_obj_prp_rel_df["object_name"].isin(b_filter_d[f] ))& (
                        #            rep_dos_obj_prp_rel_df["prp_subType"].isin(["prompt_objects"]))]
                        #prp_ans_d = self.bld_ai_obj_ans_prp(obj_rel_df=rep_dos_obj_prp_rel_df,key_l=b_filter_d[f])
                        
                        #prp_ans_d_l.append(prp_ans_d.copy())

                        all_obj_key_l.extend(b_filter_d[f])
                except Exception as err:
                    print(err)
            merged_filt_prp_l=self.merge_exp_prp_ans_l(prp_ans_d_l)
            
            all_obj_key_l.extend(bi_request_d["attributes"])
            all_obj_key_l.extend(bi_request_d["metrics"])
            
            bld_ai_obj_ans_prp_d_l = self.bld_ai_obj_ans_prp(obj_rel_df=rep_dos_obj_prp_rel_df
                                                           ,key_l=all_obj_key_l)
            merged_filt_prp_l.extend(bld_ai_obj_ans_prp_d_l)

        except Exception as err:
                print(err)
        
        prompt_answ=i_prompts.frame_prp(prp_ans=merged_filt_prp_l) 
        
        return prompt_answ

    def bld_ai_obj_ans_prp(self,obj_rel_df,key_l):


        obj_prp_rag_df = obj_rel_df[obj_rel_df['prp_subType'].isin(['prompt_objects'])]
        obj_prp_rag_df = obj_prp_rag_df[obj_prp_rag_df['object_name'].isin(key_l)]

        obj_prp_rag_df = obj_prp_rag_df[['prompt_id', 'prp_subType', "object_id", "obj_type"]]
        obj_prp_rag_df.drop_duplicates(inplace=True)

        obj_prp_rag_df.rename(columns={'object_id': 'id', 'obj_type': 'type'}, inplace=True)
        obj_prp_rag_df['type'] = obj_prp_rag_df['type'].apply(lambda x: i_read_gen.find_type_subtype(x))
        prp_ans_d_l = obj_prp_rag_df.groupby(['prompt_id']).apply(
            lambda x: x[['id', 'type']].to_dict(orient='records')).to_dict()
        obj_prp_ans_l = []
        for prp in prp_ans_d_l:
            obj_prp_ans_d = {}
            obj_prp_ans_d["id"] = prp
            obj_prp_ans_d["type"] = "OBJECTS"
            obj_prp_ans_d["answers"] = prp_ans_d_l[prp]
            obj_prp_ans_l.append(obj_prp_ans_d.copy())

        return obj_prp_ans_l

    def merge_prompts(self,ai_prp_filt_ans_d):
        all_prompts_l=[]
        prp_exp_l=[]
        for p in ai_prp_filt_ans_d:
            if p["prp_subType"]!="prompt_expression":
                all_prompts_l.append(p["prp_ans_d"])
            else:
                prp_exp_l.append(p["prp_ans_d"])

        all_prompts_l.extend(self.merge_exp_prp_ans_l(ai_prp_filt_ans_d))
        return all_prompts_l

    def merge_exp_prp_ans_l(self,ai_prp_filt_ans_d):
        prp_ans_l = []
        prp_id_l = i_msic.get_key_form_dict_l(dict_l=ai_prp_filt_ans_d, key="prompt_id")
        prp_ans_d_all = {}
        operand_X = "AND"
        for prp_id in prp_id_l:
            prp_ans_d_all[prp_id] = []

        for prp_ans in ai_prp_filt_ans_d:
            for prp_id in prp_ans_d_all.keys():
                if prp_ans["prompt_id"] == prp_id:
                    prp_ans_d_all[prp_id].append(prp_ans["prp_ans_d"])

        for prp_id in prp_ans_d_all:
            # print(prp_ans_d_all[prp_id])
            prp_ans_d_all[prp_id] = i_prompts.bld_exp_operands_d(p_ans_d_d_l=prp_ans_d_all[prp_id], operator="AND")

        for prp_ans_id in prp_ans_d_all:
            for prp in ai_prp_filt_ans_d:
                if prp_ans_id == prp["prompt_id"]:
                    if prp["prp_subType"] == "prompt_expression" and (prp["obj_type"] == "attribute" or prp["obj_type"] == "12"):
                        prp_ans_l.append(i_prompts.bld_prp_exp_d(prompt_id=prp_ans_id,
                                                       p_ans_d_j=prp_ans_d_all[prp_ans_id], operator="And"))

                    if prp["prp_subType"] == "prompt_expression" and (prp["obj_type"] == "metric" or prp["obj_type"] == "4"):
                        prp_ans_l.append(i_prompts.frame_metric_exp_prp(prompt_id=prp_ans_id, p_ans_d_j=prp_ans_d_all[prp_ans_id]))


                    break
        return prp_ans_l

    def save_AI_rep(self, conn, report_id, prompt_answ, ai_rep_name, ai_rep_folder_id, promptOption="static"):
        instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
        first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                                prompt_answ=prompt_answ)


        rep_id = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=ai_rep_name
                                          , save_mode="OVERWRITE"
                                          , promptOption=promptOption
                                          , setCurrentAsDefaultAnswer=True)

        return rep_id


 

class mstr_objects():


    def zzz_fetch_mstr_keys(self,conn, cube_id, key_word_l, key_val_l=["key"]):

        disp_col_ids_l = []
        attr_elements_l = []
        #
        # print(mstr_rag_col_d)
        mstr_rag_col_d = i_cube.get_mtdi_cube_col_id(conn, cube_l=[cube_id])
        for col_name in mstr_rag_col_d[cube_id].keys():
            # cube_disp_col_l = mstr_rag_col_d[cube_id].keys()
            # print(col_name)

            if col_name in key_val_l:
                for key in key_word_l:
                    prp_ans_j = mstr_rag_col_d[cube_id][col_name] + ":" + str(key)
                    attr_elements_l.append(prp_ans_j)

            disp_col_ids_l.append(mstr_rag_col_d[cube_id][col_name])

        df = i_cube.quick_query_cube(conn=conn, cube_id=cube_id, attribute_l=disp_col_ids_l, metric_l=None,
                                     attr_elements=attr_elements_l)
        return df

    def get_att_elem_str(self, element_df_d_l, key_word_l):
        element_rag_d_l = []
        for df in element_df_d_l:
            element_df = df["df"]
            filt_df=element_df[df["rag_cols"]][element_df[df["key_col"]].isin(key_word_l)]
            element_rag_d_l.extend(filt_df.to_dict(orient='records'))

        element_rag_d_l= i_msic.rem_dbl_dict_in_l(element_rag_d_l)
        
        return element_rag_d_l
    
