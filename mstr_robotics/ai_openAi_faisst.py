import os
import pandas as pd
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from openai import OpenAI
from flashtext import KeywordProcessor
from itertools import chain
from mstr_robotics.report import prompts,cube,rep
from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics._helper import msic
from mstr_robotics.prepare_AI_data import map_objects
import json

i_KeywordProcessor = KeywordProcessor()
i_chain=chain()
i_rep=rep()
i_prompts=prompts()
i_df_helper=df_helper()
i_cube=cube()
i_map_objects=map_objects()
i_msic=msic()

class vectorDB_faisst():

    def __init__(self,sKey):
        #self.sKey=sKey
        os.environ["OPENAI_API_KEY"] = sKey
        self.i_embeddings = OpenAIEmbeddings()

    def load_vector_store(self,key_l):
        # Flatten and ensure all items are strings
        # Create FAISS vector store
        vector_store = FAISS.from_texts(key_l, self.i_embeddings)

        return vector_store

    def load_keyword_processor(self,key_l):
        for val in key_l:
            i_KeywordProcessor.add_keyword(val)
        return i_KeywordProcessor

    def extract_keywords(self,msg_t):
        keywords_l = i_KeywordProcessor.extract_keywords(msg_t)
        return keywords_l

class run_chat_request():

    def __init__(self,cube_obj_prp_rel_id
                 ,cube_RAG_form_val_ans_id
                 ,cube_att_form_def_id
                 ,rep_dos_id
                 ,mstr_rag_col_d):

        self.cube_obj_prp_rel_id=cube_obj_prp_rel_id
        self.cube_RAG_form_val_ans_id=cube_RAG_form_val_ans_id
        self.rep_dos_id=rep_dos_id
        self.mstr_rag_col_d=mstr_rag_col_d
        self.cube_att_form_def_id=cube_att_form_def_id


    def bld_AI_filt_prp_ans(self,conn,vector_store,filter_d):
        # print(json_f)
        # v_andOr_s="And"
        # att_filt_prp_id="1F21042E4FC08886A9D7E4920CA4EC59"
        # met_filt_prp_id="14B57F1F46127BC6E6F746888BC3C7CB"
        prp_ans_d_l = []

        # ={}
        last_p_ans_d_j = {}

        for f in list(filter_d.keys()):
            print(f)
            results = vector_store.similarity_search_with_score(query=f, top_k=1)

            filter_obj_name = results[0][0].page_content
            #print(filter_obj_name)
            #print(self.mstr_rag_col_d)
            filt_obj_rag_df = i_map_objects.get_rag_rep_prp_df(conn, cube_id=self.cube_obj_prp_rel_id, rep_dos_id=self.rep_dos_id,
                                                 key_word_l=[filter_obj_name],mstr_rag_col_d=self.mstr_rag_col_d)

            filt_obj_rag_df = filt_obj_rag_df[filt_obj_rag_df['prp_subType'].isin(['prompt_expression'])]

            operator = next(iter(filter_d[f]))
            filter_val_l = filter_d[f][operator]
            #print("DDDDDDDDDDDDDDDDD")
            #print(filt_obj_rag_df)
            d_prp_ans_d = {}
            for index, obj in filt_obj_rag_df.iterrows():
                d_prp_ans_d["prompt_id"] = obj["prompt_id"]
                d_prp_ans_d["prp_subType"] = obj["prp_subType"]
                d_prp_ans_d["object_id"] = obj["object_id"]
                d_prp_ans_d["obj_type"] = obj["obj_type"]
                d_prp_ans_d["operator"] = operator
                if obj["obj_type"] == "attribute":
                    if operator in ("In", "notIn"):

                        filter_val_df = i_map_objects.get_rag_rep_prp_df(conn, cube_id=self.cube_RAG_form_val_ans_id,
                                                           rep_dos_id=self.rep_dos_id, key_word_l=filter_val_l,mstr_rag_col_d=self.mstr_rag_col_d)
                        filt_df = pd.merge(filt_obj_rag_df[["prompt_id", "prp_subType", "object_id", "obj_type"]],
                                           filter_val_df[
                                               ["prompt_id", "prp_subType", "attribute_id", "attribute_name", "form_id",
                                                "form_name", "form_dataType", "key"]],
                                           left_on=['object_id'], right_on=['attribute_id'], how='inner')
                        filt_df = i_df_helper.clean_double_col(df=filt_df)
                        #print(filt_df)
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
                        #print(d_prp_ans_d)

                    else:
                        col_id = i_cube.get_RAG_cube_col_mstr_id(cube_id=self.cube_att_form_def_id, col_name="attribute_id",mstr_rag_col_d=self.mstr_rag_col_d)
                        attr_elements = col_id + ":" + obj["object_id"]
                        df = i_cube.quick_query_cube(conn, cube_id=self.cube_att_form_def_id, attribute_l=None, metric_l=None,
                                              attr_elements=attr_elements)

                        for index, form in df.iterrows():
                            if int(form["display_form_nr"]) > 0:
                                att_exp_ans_d = {"att_id": form["attribute_id"], "att_form_id": form["form_id"],
                                                 "form_data_type": form["REST_form_type"], "operator": operator,
                                                 "filter_val_l": filter_val_l}

                                att_exp_ans_d = i_prompts.bld_att_exp_prp(prp_job_ans_d=att_exp_ans_d)
                                d_prp_ans_d["prp_ans_d"] = att_exp_ans_d
                                prp_ans_d_l.append(d_prp_ans_d.copy())
                                #print(d_prp_ans_d)

                if obj["obj_type"] == "metric":
                    metric_exp_prp = {}
                    metric_exp_prp["met_id"] = obj["object_id"]
                    metric_exp_prp["data_type"] = "Real"
                    metric_exp_prp["operator"] = operator
                    metric_exp_prp["filter_val_l"] = filter_val_l
                    metric_exp_prp["level"] = "default"
                    d_prp_ans_d["prp_ans_d"] = i_prompts.bld_metric_exp_prp(metric_exp_prp=metric_exp_prp.copy())
                    prp_ans_d_l.append(d_prp_ans_d.copy())
                    #print(print(d_prp_ans_d))

            # prp_ans_d_l.append(last_p_ans_d_j.copy())
        return prp_ans_d_l

    def bld_ai_obj_ans_prp(self,conn,json_t_d):
        key_l = []
        col_id = i_cube.get_RAG_cube_col_mstr_id(cube_id=self.cube_obj_prp_rel_id, col_name="object_id",mstr_rag_col_d=self.mstr_rag_col_d)
        obj_l = []
        template_obj_d = {}

        for obj_type in json_t_d["template"].keys():
            for obj in json_t_d["template"][obj_type]:
                template_obj_d = {"obj_type": obj_type, "obj": obj}
                obj_l.append(template_obj_d.copy())

        for obj in obj_l:
            key_l.append(obj["obj"])

        template_obj_rag_df = i_map_objects.get_rag_rep_prp_df(conn, cube_id=self.cube_obj_prp_rel_id, rep_dos_id=self.rep_dos_id,
                                                 key_word_l=key_l,mstr_rag_col_d=self.mstr_rag_col_d)
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

    def save_AI_rep(self, conn, rep_wizzard_id, prompt_answ, ai_rep_name, ai_rep_folder_id):
        instance_id = i_rep.open_Instance(conn=conn, report_id=rep_wizzard_id)
        first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=rep_wizzard_id, instance_id=instance_id,
                                                     prompt_answ=prompt_answ)
        rep_id = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=ai_rep_name
                                          , save_mode="OVERWRITE"
                                          , promptOption="static"
                                          , setCurrentAsDefaultAnswer=True)

        return rep_id


class openAI():

    def __init__(self,sKey):
        #self.sKey=sKey
        os.environ["OPENAI_API_KEY"] = sKey
        self.i_embeddings = OpenAIEmbeddings()

    def check_trans_chatGPT(self,sKey,messages,*args,**kwargs):
        chat_completion = self.call_open_AI(sKey=sKey,messages=messages,*args,**kwargs)
        js = json.loads(chat_completion.json())
        return js

    def call_open_AI(self, sKey, messages, max_tokens=1000, temperature=0.3, model="gpt-4o-mini"):
        # This is the default and can be omitted
        client = OpenAI(api_key=sKey)

        chat_completion = client.chat.completions.create(
            messages=messages,
            # model="gpt-3.5-turbo",
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
        js = json.loads(chat_completion.json())
        return js

