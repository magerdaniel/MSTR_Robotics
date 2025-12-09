
import pandas as pd
import json
import os
from flashtext import KeywordProcessor
from langchain.vectorstores import FAISS


from mstrio.connection import Connection
from mstr_robotics import mstr_classes,report
from mstr_robotics.read_out_prj_obj import io_attributes
from mstr_robotics._helper import msic
from mstr_robotics.report import prompts,rep
from langchain.embeddings import OpenAIEmbeddings

# Load OpenAI API key from environment variable
# Set OPENAI_API_KEY environment variable before running this script

keyword_processor = KeywordProcessor()
element_key_process = KeywordProcessor()


i_prompts=prompts()
i_cube=report.cube()
i_md_searches= mstr_classes.md_searches()
i_rep=report.rep()
io_att= io_attributes()
i_msic=msic()
i_rep=rep()



class rag_files():

    def __init__(self,sKey):
        #self.sKey=sKey
        os.environ["OPENAI_API_KEY"] = sKey
        self.i_embeddings = OpenAIEmbeddings()

    def trans_att_exp_prp(self,attributes_l):
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


    def trans_att_obj_prp(self,attributes_l):
        # Now 'data' contains the parsed JSON as a Python dictionary or list
        att_d_l = []
        att_l = []
        key_word_l = []
        # documents_l=[]
        for m in attributes_l:
            if m["attribute_id"] not in att_l:
                att_l.append(m["attribute_id"])
                att_d = {}
                att_d["attribute_id"] = m["attribute_id"]
                att_d["key"] = m["attribute_name"]
                att_d["type"] = "attribute"
                att_d_l.append(att_d.copy())
        att_d_l = i_msic.rem_dbl_dict_in_l(dict_l=att_d_l)
        return att_d_l

    def trans_metrics(self,metrics_l):
        all_met_d_l = []
        for m in metrics_l:
            if "E0CCB9CF22104A489CBE78D974AFD19E" in m["metric_path_ids"]:
                row_d = {}
                row_d["metric_id"] = m["metric_id"]
                row_d["key"] = m["metric_name"]
                row_d["type"] = "metric"
                all_met_d_l.append(row_d.copy())
        return list(all_met_d_l)

    def read_out_pd_series(self,df, rag_att_form_id_d):
        form_key_d_l = []
        for index, key in df.iterrows():
            rag_att_form_id_d["key"] = key[0]
            form_key_d_l.append(rag_att_form_id_d.copy())

        return form_key_d_l


    def bld_att_exp_prp_ans(self,prompt_id, att_exp_ans_l):
        att_form_exp_prp_l = []
        att_exp_prp_df = pd.DataFrame(att_exp_ans_l)
        att_exp_prp_sort_df = att_exp_prp_df.sort_values(by=['attribute_id', 'form_id'])
        last_row = ""
        att_form_exp_prp_d = {}
        prp_d = {}
        for index, row in att_exp_prp_sort_df.iterrows():
            if last_row != row["attribute_id"] + row["form_id"]:
                #last_row = last_row = row["attribute_id"] + row["form_id"]
                att_form_exp_prp_d["att_id"] = row["attribute_id"]
                att_form_exp_prp_d["operator"] = "IN"
                att_form_exp_prp_d["att_form_id"] = row["form_id"]
                att_form_exp_prp_d["form_data_type"] = i_prompts.get_exp_prp_data_type(row["form_dataType"])
                att_form_exp_prp_d["filter_val_l"] = row["key"]
                att_form_exp_prp_l.append(att_form_exp_prp_d.copy())
        prp_d = i_prompts.bld_expr_prp_answ(prompt_id=prompt_id, att_exp_ans_l=att_form_exp_prp_l)

        return prp_d

    def bld_rag_att_elements(self,conn,rag_rep_id_l):
        element_d_l = []
        for rag_rep_id in rag_rep_id_l:

            rep_def = i_rep.get_report_def(conn=conn, report_id=rag_rep_id)

            instance_id = i_rep.open_Instance(conn=conn, report_id=rag_rep_id)
            df = i_rep.report_df(conn=conn, report_id=rag_rep_id, instance_id=instance_id)
            # print(df)
            for att in rep_def.json()["definition"]["grid"]["rows"]:
                rag_att_form_id_d = {}
                rag_att_form_id_d["type"] = "att_element"
                rag_att_form_id_d["attribute_id"] = att["id"]
                rag_att_form_id_d["attribute_name"] = att["name"]
                for form in att["forms"]:
                    rag_att_form_id_d["form_id"] = form["id"]
                    rag_att_form_id_d["form_dataType"] = i_prompts.get_exp_prp_data_type(form["dataType"])
                    element_d_l.extend(self.read_out_pd_series(df=df, rag_att_form_id_d=rag_att_form_id_d.copy()))

        return element_d_l


    def save_AI_report(self,conn,rep_wizzard_id, prompt_answ, ai_rep_name, ai_rep_folder_id):
        instance_id = i_rep.open_Instance(conn=conn, report_id=rep_wizzard_id)
        first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=rep_wizzard_id, instance_id=instance_id,
                                                prompt_answ=prompt_answ)
        print(first_draft)
        rep_id = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=ai_rep_name
                                     , save_mode="OVERWRITE"
                                     , promptOption="filterAndTemplate"
                                     , setCurrentAsDefaultAnswer=True)

        rep_url = f'{conn.base_url}/app/{conn.project_id}/{rep_id.json()["id"]}/share'
        html_link = f'<a href={rep_url} target="_blank">Jump to MSTR Library</a>'

        return html_link

    def save_AI_rep(self,conn,rep_wizzard_id, prompt_answ, ai_rep_name, ai_rep_folder_id):
        instance_id = i_rep.open_Instance(conn=conn, report_id=rep_wizzard_id)
        first_draft = i_rep.set_inst_prompt_ans(conn=conn, report_id=rep_wizzard_id, instance_id=instance_id,
                                                prompt_answ=prompt_answ)
        rep_id = i_rep.save_rep_inst(conn=conn, instance_id=instance_id, rep_name=ai_rep_name
                                     , save_mode="OVERWRITE"
                                     , promptOption="static"
                                     , setCurrentAsDefaultAnswer=True)

        return rep_id


    def bld_ai_prp_ans(self,obj_prp_ans_d
                       ,obj_prp_att_exp_id
                       ,obj_prp_att_id
                       ,obj_prp_met_id):

        att_exp_prp_ans = self.bld_att_exp_prp_ans(prompt_id=obj_prp_att_exp_id, att_exp_ans_l=obj_prp_ans_d["obj_prp_att_exp"])
        obj_prp_att_ans = i_prompts.frame_prp_ans(prompt_id=obj_prp_att_id,
                                                  prp_type="OBJECTS",
                                                  prp_ans_JSON_l=obj_prp_ans_d["obj_prp_att"])

        obj_prp_met_ans = i_prompts.frame_prp_ans(prompt_id=obj_prp_met_id,
                                                  prp_type="OBJECTS",
                                                  prp_ans_JSON_l=obj_prp_ans_d["obj_prp_metric"])

        all_prp_ans_l = [att_exp_prp_ans, obj_prp_att_ans, obj_prp_met_ans]
        prompt_answ = i_prompts.frame_prp(prp_ans=all_prp_ans_l)
        return prompt_answ

    def keywords_to_obj(self,vector_store, keywords_l,all_key_d_l):
        # bld_md_obj_ans(msg_t,keywords_l)
        rag_key_d_l=[]
        for k in keywords_l:
            results = vector_store.similarity_search_with_score(query=k, top_k=1)
            rag_key_d_l.append(results[0][0].page_content)
        obj_prp_ans_d= self.get_rag_for_keyword(rag_key_d_l,all_key_d_l)
        return obj_prp_ans_d

    def get_rag_for_keyword(self,rag_key_d_l,all_key_d_l):

        obj_prp_metric_l = []
        obj_prp_att_l = []
        obj_prp_att_exp_l = []
        element_key_l=[]
        for keyword in rag_key_d_l:
            #print("keyword: " + str(keyword))
            for element in all_key_d_l:
                if str(keyword) == element["key"]:
                    #print("hello Rag element" + str(element))
                    #print(element["type"])
                    if element["key"] not in element_key_l:
                        element_key_l.append(element["key"])
                        if element["type"] == "att_element":
                            obj_prp_att_exp_l.append(element.copy())
                        if element["type"] == "attribute":
                            obj_prp_att_l.append({"id": element["attribute_id"], "type": element["type"]}.copy())
                        if element["type"] == "metric":
                            obj_prp_metric_l.append({"id": element["metric_id"], "type": element["type"]}.copy())

        obj_prp_ans_d = {}
        obj_prp_ans_d["obj_prp_att_exp"] = obj_prp_att_exp_l
        obj_prp_ans_d["obj_prp_att"] = obj_prp_att_l
        obj_prp_ans_d["obj_prp_metric"] = obj_prp_metric_l
        return obj_prp_ans_d

    def bld_key_word_checker(self,msg_t,vector_store, all_key_d_l):
        keywords_l = keyword_processor.extract_keywords(msg_t)
        rag_d_l = []
        obj_prp_ans_d=self.keywords_to_obj(vector_store, keywords_l,all_key_d_l)

        return obj_prp_ans_d

    def load_key_words(self,att_obj_prp_d_l,att_exp_prp_d_l,metrics_d_l,element_d_l):
        all_key_d_l = att_obj_prp_d_l
        all_key_d_l.extend(att_exp_prp_d_l)
        all_key_d_l.extend(metrics_d_l)
        all_key_d_l.extend(element_d_l)

        vector_store = FAISS.from_texts(i_msic.get_key_form_dict_l(all_key_d_l, key="key"), self.i_embeddings)

        # all_val_l=[att_l,metrics_d_l,element_l]
        # all_val_l=[att_l,metrics_l,element_l]
        for val in all_key_d_l:
            keyword_processor.add_keyword(val["key"])
        return {"all_key_d_l": all_key_d_l,"keyword_processor":keyword_processor,"vector_store":vector_store }

    def extract_keywords(self,msg_t):
        keywords_l = keyword_processor.extract_keywords(msg_t)
        return keywords_l

    def find_rag_files(self,keywords_l,vector_store):
        rag_d_l = []

        # bld_md_obj_ans(msg_t,keywords_l)
        for k in keywords_l:
            results = vector_store.similarity_search_with_score(query=k, top_k=1)
            rag_d_l.append(results[0][0].page_content)
        return rag_d_l