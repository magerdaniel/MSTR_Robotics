import json
import ast
import re
from openai import OpenAI
from flashtext import KeywordProcessor
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings

class keyword_processor():

    def __init__(self):
        self.KeywordProcessor = KeywordProcessor()

    def load_keyword_processor(self,key_l):
        for val in key_l:
            self.KeywordProcessor.add_keyword(val)
        return self.KeywordProcessor

    def extract_keywords(self,msg_t):
        keywords_l = self.KeywordProcessor.extract_keywords(msg_t)
        return keywords_l

    def check_keyword(self,filt_obj_str):
        print(filt_obj_str)
        keywords_l = self.KeywordProcessor.extract_keywords(filt_obj_str)
        return keywords_l[0]

    def check_keyword_all(self,filt_obj_str):
        print(filt_obj_str)
        keywords_l = self.KeywordProcessor.extract_keywords(filt_obj_str)
        return keywords_l

class vectorDB_faisst():

    def __init__(self,sKey):
        self.i_embeddings = OpenAIEmbeddings(openai_api_key=sKey)

    def load_vector_store(self,key_l):
        # Flatten and ensure all items are strings
        # Create FAISS vector store
        self.vector_store = FAISS.from_texts(key_l, self.i_embeddings)

        return self.vector_store

    def check_keyword(self, filt_obj_str):
        results = self.vector_store.similarity_search_with_score(query=filt_obj_str, top_k=1)
        filter_obj_name = results[0][0].page_content
        return filter_obj_name

class mstr_openAI():

    def __init__(self):
        pass
        #self.sKey=sKey
        #self.i_embeddings = OpenAIEmbeddings(openai_api_key=sKey)


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

class chat_bot():

    def run_chat_msg(self,msg_t,sKey,model,vector_store,max_tokens,temperature):
        key_word_l = vector_store.check_keyword_all( filt_obj_str=msg_t)
        rag_rep_prp_l = []

        # rag_rep_prp_filt_d=get_rag_rep_prp_d(conn=conn,cube_id=cube_RAG_form_val_ans_id, mstr_rag_col_d=mstr_rag_col_d,rep_dos_id=rep_dos_id,key_word_l=key_word_l)

        messages = self.split_AI_msg(msg_t, key_word_l)

        json_t = mstr_openAI().call_open_AI(sKey=sKey, messages=messages, max_tokens=max_tokens, temperature=temperature, model=model)
        json_t_d = json.loads(json_t["choices"][0]["message"]["content"])
        msg_filter_t = json_t_d["filter"]
        messages = self.filter_RAG_l(msg_t=msg_t, key_word_l=key_word_l, msg_filter_t=msg_filter_t)
        json_f = mstr_openAI().call_open_AI(sKey=sKey, messages=messages, max_tokens=max_tokens, temperature=temperature, model=model)
        json_f = json_f["choices"][0]["message"]["content"]
        filter_d = ast.literal_eval(json_f)
        json_fin = json_t_d.copy()
        json_fin["filter"] = filter_d
        return json_fin

    def filter_RAG_l(self,msg_t, key_word_l, msg_filter_t):
        messages = [
            {
                "role": "system",
                "content": f"You will be provided unstructured filter definition of the BI report request. the full message is {msg_t}"
            },
            {
                "role": "system",
                "content": "Always use as Filteroperator expressions like In, GreaterEqual,Equals,IsNull,IsNotNull,Greater,Between,BeginsWith,Like"
            },
            {
                "role": "system",
                "content": "When filtering a list, please ensure, that you use In od NotIn as operator"
            },
            {
                "role": "system",
                "content": "Ensure, that the out pu is a dictionary consiting of keywords"
            },
            {"role": "system",
             "content": "top_n or bottom_n is part of the filter statement"
             },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "system",
                "content": " Please provide the filters as a valid Python dictionary without any code block formatting or syntax highlighting, where the kefilter objects"
            },
            {
                "role": "system",
                "content": "Set brackets to split multiple filter criteria"
            },
            {
                "role": "system",
                "content": "Ignore keywords, if they are not relevant for filtering"
            },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "user",
                "content": f"{msg_filter_t}"
            }
        ]
        return messages

    def split_AI_msg(self,msg_t, key_word_l):
        messages = [
            {
                "role": "system",
                "content": "You will be provided unstructured BI report request."
            },
            {
                "role": "system",
                "content": " Please provide the content as a valid Python dictionary without any code block formatting or syntax highlighting, where the keywords are splited in template and filter objects"
            },
            {
                "role": "system",
                "content": "Please use the key template and filter split"
            },
            {
                "role": "system",
                "content": "Set brackets to split multiple filter criteria"
            },
            {
                "role": "system",
                "content": "Please include in the JSON file, the filter critria as string"
            },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "user",
                "content": f"{msg_t}"
            }
        ]
        return messages

class mstr_openAI():

    def __init__(self):
        pass
        #self.sKey=sKey
        #self.i_embeddings = OpenAIEmbeddings(openai_api_key=sKey)


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

class perplexity():

    def clean_json(self, bad_json):
        cleaned = bad_json.replace('\\n', '')  # Escape newline
        cleaned = cleaned.replace('\\r', '')  # Escape carriage return
        cleaned = cleaned.replace('\\t', '')
        return cleaned

    def extract_json(self,response):
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            try:
                clean_j = self.clean_json(bad_json=match.group())
                return clean_j
            except json.JSONDecodeError:
                print(json.JSONDecodeError)
                return response
        return response

    def rag_sys_cont(self,key_word_l, att_elem_str, bi_obj_str):
        key_word_l_str = ', '.join([f'"{item}"' for item in key_word_l])
        rag_sys_cont = f'''
                The user will send a BI and you need to exctract the logic into a validt Python dictionary
                - Return pure Python dict without code formatting
                - If you are creating list with strings, use always single quotes
                - Ensure, that you always use the following output structure {{"template": {{"attributes": ["Country", "Category"],  "metrics": ["Revenue", "Cost", "Profit"]}}, "filter": {{}},"question":"","others":""}}
                - Please put special attenion, which attributes and metrics are part of the template and which are used in the filter
                - If you spot spelling errors of words in the user message compared to the keywords, please fix them in the output
                - If you need a better specification of the BI Request. Raise the question and use the key 'question'
                - If you have other content in the message use the key 'other'
                - Under template, please distinct between Attributes and metrics
                - If there is no filter in the request, just let the key filter empty
                - Keywords: {key_word_l_str}
                - {att_elem_str}
                - {bi_obj_str}
                - For filtering: 
                - if it is a Ranking filter, check if the criteria top / bottom and a metric is defined. If not complete. please ask
                - to not ask business or controlling questions, where you challange if the request makes sense buinsess wise
                - use for Ranking the following structure {{"ranking": {{"by": "Profit", "order": "top", "level": "Employee", "limit": 3, "group_by": "Manager"}}}}
                - for metric filter please use the following structure "metric_filter_1":{{"level": "Customer", "metric": "Revenue", "operator": "less than", "value": 5000}},"metric_filter_2":{{"level": None, "metric": "Profit", "operator": "greater than", "value": 5000}}
                - for Attribute element filter please use the following structure "att_element_1":{{"attribute":"Country","operator":"notIn/In","element_list": ["US","Germany"]}}
                - for Attribute qualification filter please use the following structure "att_qual_1":{{"attribute":"Region","Column":"desc","operator": "startsWith", "value": "D"}}
                - ensure that you use one of the following operators "GreaterEqual", "Between", "IsNull", "Rank.Top", "Rank.ExcludeTop", "LessEqual", "Percent.Top", "Equals", "BeginsWith", "Like", "In", "NotIn", "Greater", "GreaterEqual", "Less", "NotEqual", "NotBetween", "IsNotNull", "Contains", "EndsWith", "NotBeginsWith", "NotLike", "NotContains", "NotEndsWith", "Equals"
                '''
        return rag_sys_cont

    def rag_sys_template(key_word_l, att_elem_str, bi_obj_str):
        rag_sys_template = f''' The user will send a BI and you need to exctract the logic into a validt Python dictionary
                - Return pure Python dict without code formatting
                - If you are creating list with strings, use always single quotes
                - If you spot spelling errors of words in the user message compared to the keywords, please fix them in the output
                - your task is, to identify attributes and metrics, which are part of the template and which are part of the filter
                - please ensure the folowing output dict has the keys template, filter, question and other
                - {bi_obj_str}
                    '''
        return rag_sys_template

    def call_perplexity(self, msg_t, sys_cont, message_check_d, temperature=0.1):
        # print(msg_t)
        # key_word_l = vector_store.extract_keywords(msg_t)
        import os
        client = OpenAI(
            api_key=os.environ.get("PERPLEXITY_API_KEY"),
            # Set your API key in environment variables
            base_url="https://api.perplexity.ai"
        )
        try:
            # Create chat completion request
            messages = [
                {
                    "role": "system",
                    "content": sys_cont
                },
                {"role": "user", "content": msg_t}
            ]
            # print(messages)
            response = client.chat.completions.create(
                model="sonar-pro",  # Official model name for Perplexity-API
                messages=messages,
                temperature=temperature
            )

            # print(response)
            json_t = json.loads(response.json())
            #print(json_t.keys())
            json_t_cont = self.extract_json(json_t["choices"][0]["message"]["content"])
            json_t_cont_s = self.clean_json(json_t_cont)

            json_t_cont_d = ast.literal_eval(json_t_cont_s)

            message_check_d["llm_msg"] = str(messages)
            message_check_d["llm_ans"] = json_t["choices"][0]["message"]["content"]
            message_check_d["ans_d"] = json_t_cont_d
            message_check_d["err"] = None
            message_check_d["valid_d_fg"] = 1
        except Exception as err:
            print(err)
            # print(json.loads(response.json()))
            message_check_d["llm_msg"] = messages
            message_check_d["llm_ans"] = json_t
            message_check_d["ans_d"] = None
            message_check_d["err"] = err
            message_check_d["valid_d_fg"] = 0
        return message_check_d

    def parse_and_structure(self,sonar_out_d_l):
    
        for m in sonar_out_d_l:
            clean_structure_d = {"msg_nr": m["msg_nr"],
                                 "msg_t": m["msg_t"],
                                 "attributes": [],
                                 "metrics": [],
                                 "filter": {},
                                 "question": ""
                                 }
            if m["ans_d"] != None:
                try:
                    if "attributes" in m["ans_d"]["template"].keys():
                        clean_structure_d["attributes"] = m["ans_d"]["template"]["attributes"]
                    if "metrics" in m["ans_d"]["template"].keys():
                        clean_structure_d["metrics"] = m["ans_d"]["template"]["metrics"]
                    if "filter" in m["ans_d"].keys():
                        clean_structure_d["filter"] = m["ans_d"]["filter"]
                    if "question" in m["ans_d"].keys():
                        clean_structure_d["question"] = m["ans_d"]["question"]
                    if "other" in m["ans_d"].keys():
                        clean_structure_d["other"] = m["ans_d"]["other"]

                except Exception as err:
                    # print(err)
                    # print( m["ans_d"])
                    pass

        #m_AI_ans_fin_d = self.merge_AI_ans_d(merge_AI_ans_d_l=clean_structure_d_l)
        return clean_structure_d
    
    def merge_AI_ans_d(self,merge_AI_ans_d_l):
        m_AI_ans_fin_d = {"attributes": [],
                         "metrics": [],
                         "filter": {},
                         "question": ""
                         }
        for m in merge_AI_ans_d_l:
            if m["attributes"] != []:
                m_AI_ans_fin_d["attributes"] = list(set(m_AI_ans_fin_d["attributes"] + m["attributes"]))
            if m["metrics"] != []:
                m_AI_ans_fin_d["metrics"] = list(set(m_AI_ans_fin_d["metrics"] + m["metrics"]))
            if m["filter"] != {}:
                m_AI_ans_fin_d["filter"].update(m["filter"])
            if m["question"] != "":
                if m_AI_ans_fin_d["question"] == "":
                    m_AI_ans_fin_d["question"] = m["question"]
                else:
                    m_AI_ans_fin_d["question"] = m_AI_ans_fin_d["question"] + " ; " + m["question"]
        return m_AI_ans_fin_d