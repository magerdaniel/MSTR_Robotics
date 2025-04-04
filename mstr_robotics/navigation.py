import pandas as pd
from mstr_robotics.report import prompts,cube,rep
from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics._helper import msic
from mstr_robotics.prepare_AI_data import map_objects

i_rep=rep()
i_prompts=prompts()
i_df_helper=df_helper()
i_cube=cube()
i_map_objects=map_objects()
i_msic=msic()

class answer_prompts():

    def __init__(self,cube_obj_prp_rel_id
                 ,cube_RAG_form_val_ans_id
                 ,cube_att_form_def_id
                 ,rep_dos_id):

        self.cube_obj_prp_rel_id=cube_obj_prp_rel_id
        self.cube_RAG_form_val_ans_id=cube_RAG_form_val_ans_id
        self.rep_dos_id=rep_dos_id
        self.cube_att_form_def_id=cube_att_form_def_id

    def bld_exp_prp_met_ans(self,obj,operator,filter_val_l):
        metric_exp_prp = {}
        metric_exp_prp["met_id"] = obj["object_id"]
        metric_exp_prp["data_type"] = "Real"
        metric_exp_prp["operator"] = operator
        metric_exp_prp["filter_val_l"] = filter_val_l
        metric_exp_prp["level"] = "default"
        prp_ans_d= i_prompts.bld_metric_exp_prp(metric_exp_prp=metric_exp_prp.copy())
        return prp_ans_d

    def bld_exp_prp_att_ans(self,conn,d_prp_ans_d,prp_ans_d_l, filt_obj_rag_df,operator,filter_val_l):

        if operator in ("In", "notIn"):

            filter_val_df = i_map_objects.bld_ai_prp_ans(conn, cube_id=self.cube_RAG_form_val_ans_id,
                                                         rep_dos_id=self.rep_dos_id,
                                                         key_word_l=filter_val_l)
            filt_df = pd.merge(filt_obj_rag_df[["prompt_id", "prp_subType", "object_id", "obj_type"]],
                               filter_val_df[
                                   ["prompt_id", "prp_subType", "attribute_id", "attribute_name", "form_id",
                                    "form_name", "form_dataType", "key"]],
                               left_on=['object_id'], right_on=['attribute_id'], how='inner')
            filt_df = i_df_helper.clean_double_col(df=filt_df)
            # print(filt_df)
            filter_val_l = []
            for index, f_form_ele in filt_df.iterrows():
                form_id = f_form_ele["form_id"]
                form_dataType = f_form_ele["form_dataType"]
                filter_val_l.append(f_form_ele["key"])

            att_exp_ans_d = {"att_id": d_prp_ans_d["object_id"], "att_form_id": form_id,
                             "form_data_type": form_dataType, "operator": operator,
                             "filter_val_l": filter_val_l}

            att_exp_ans_d = i_prompts.bld_att_exp_prp_l(prp_job_ans_d=att_exp_ans_d)
            d_prp_ans_d["prp_ans_d"] = att_exp_ans_d
            prp_ans_d_l.append(d_prp_ans_d.copy())
            # print(d_prp_ans_d)

        else:
            col_id = i_cube.get_RAG_cube_col_mstr_id(conn=conn, cube_id=self.cube_att_form_def_id,
                                                     col_name="attribute_id")
            attr_elements = col_id + ":" + d_prp_ans_d["object_id"]
            df = i_cube.quick_query_cube(conn, cube_id=self.cube_att_form_def_id, attribute_l=None,
                                         metric_l=None,
                                         attr_elements=attr_elements)

            for index, form in df.iterrows():
                if int(form["display_form_nr"]) > 0:
                    att_exp_ans_d = {"att_id": form["attribute_id"], "att_form_id": form["form_id"],
                                     "form_data_type": form["REST_form_type"], "operator": operator,
                                     "filter_val_l": filter_val_l}

                    att_exp_ans_d = i_prompts.bld_att_exp_prp(prp_job_ans_d=att_exp_ans_d)
                    d_prp_ans_d["prp_ans_d"] = att_exp_ans_d
                    prp_ans_d_l.append(d_prp_ans_d.copy())

        return prp_ans_d_l

    def bld_exp_prp_ans(self,conn,prp_ans_d_l,filt_obj_rag_df,operator,filter_val_l):
        d_prp_ans_d = {}

        for index, obj in filt_obj_rag_df.iterrows():
            d_prp_ans_d["prompt_id"] = obj["prompt_id"]
            d_prp_ans_d["prp_subType"] = obj["prp_subType"]
            d_prp_ans_d["object_id"] = obj["object_id"]
            d_prp_ans_d["obj_type"] = obj["obj_type"]
            d_prp_ans_d["operator"] = operator

            if obj["obj_type"] == "attribute":
                prp_ans_d_l=self.bld_exp_prp_att_ans(conn,d_prp_ans_d,prp_ans_d_l, filt_obj_rag_df,operator,filter_val_l)

                            # print(d_prp_ans_d)

            if obj["obj_type"] == "metric":
                d_prp_ans_d["prp_ans_d"] = self.bld_exp_prp_met_ans(obj,operator,filter_val_l )
                prp_ans_d_l.append(d_prp_ans_d.copy())

        return prp_ans_d_l

    def bld_AI_filt_prp_ans(self,conn,vector_store,filter_d,*args,**kwargs):
        prp_ans_d_l = []

        for f in list(filter_d.keys()):
            print(f)
            filter_obj_name=vector_store.check_keyword( filt_obj_str=f)

            filt_obj_rag_df = i_map_objects.bld_ai_prp_ans(conn, cube_id=self.cube_obj_prp_rel_id,
                                                           rep_dos_id=self.rep_dos_id, key_word_l=[filter_obj_name])

            filt_obj_rag_df = filt_obj_rag_df[filt_obj_rag_df['prp_subType'].isin(['prompt_expression'])]

            operator = next(iter(filter_d[f]))
            filter_val_l = filter_d[f][operator]

            prp_ans_d_l=self.bld_exp_prp_ans(conn=conn, prp_ans_d_l=prp_ans_d_l
                                 , filt_obj_rag_df=filt_obj_rag_df
                                 , operator=operator, filter_val_l=filter_val_l)

        return prp_ans_d_l

    def bld_ai_obj_ans_prp(self,conn,json_t_d):
        key_l = []
        obj_l = []

        for obj_type in json_t_d["template"].keys():
            for obj in json_t_d["template"][obj_type]:
                template_obj_d = {"obj_type": obj_type, "obj": obj}
                obj_l.append(template_obj_d.copy())

        for obj in obj_l:
            key_l.append(obj["obj"])

        template_obj_rag_df = i_map_objects.bld_ai_prp_ans(conn, cube_id=self.cube_obj_prp_rel_id,
                                                           rep_dos_id=self.rep_dos_id, key_word_l=key_l)
        template_obj_rag_df = template_obj_rag_df[template_obj_rag_df['prp_subType'].isin(['prompt_objects'])]
        template_obj_rag_df = template_obj_rag_df[['prompt_id', 'prp_subType', "object_id", "obj_type"]]

        template_obj_rag_df.rename(columns={'object_id': 'id', 'obj_type': 'type'}, inplace=True)
        prp_ans_d_l = template_obj_rag_df.groupby(['prompt_id']).apply(
            lambda x: x[['id', 'type']].to_dict(orient='records')).to_dict()
        obj_prp_ans_l = []
        for prp in prp_ans_d_l:
            obj_prp_ans_d = {}
            obj_prp_ans_d["id"] = prp
            obj_prp_ans_d["type"] = "OBJECTS"
            obj_prp_ans_d["answers"] = prp_ans_d_l[prp]
            obj_prp_ans_l.append(obj_prp_ans_d)

        return obj_prp_ans_l

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
                    if prp["prp_subType"] == "prompt_expression" and prp["obj_type"] == "attribute":
                        prp_ans_l.append(i_prompts.bld_prp_exp_d(prompt_id=prp_ans_id,
                                                       p_ans_d_j=prp_ans_d_all[prp_ans_id], operator="And"))

                    if prp["prp_subType"] == "prompt_expression" and prp["obj_type"] == "metric":
                        prp_ans_l.append(i_prompts.frame_metric_exp_prp(prompt_id=prp_ans_id, p_ans_d_j=prp_ans_d_all[prp_ans_id]))
                    break
        return prp_ans_l

    def save_AI_rep(self, conn, report_id, prompt_answ, ai_rep_name, ai_rep_folder_id, promptOption="static"):
        instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
        first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                                prompt_answ=prompt_answ)

        """
        rep_stat = i_rep.get_open_prp_stat(conn=conn, report_id=report_id, instance_id=instance_id)


        if rep_stat == 2:
            prompt_answ = i_prompts.close_open_prp(conn=conn, report_id=report_id, instance_id=instance_id,
                                         prompt_answ=prompt_answ)

            first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                                prompt_answ=prompt_answ)
        """
        rep_id = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=ai_rep_name
                                          , save_mode="OVERWRITE"
                                          , promptOption=promptOption
                                          , setCurrentAsDefaultAnswer=True)

        return rep_id
