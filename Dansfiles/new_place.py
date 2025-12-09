from IPython.display import HTML
import pandas as pd
pd.set_option('display.max_colwidth', 1000)
import warnings
import json
import ast

from mstr_robotics.navigation import answer_prompts,map_objects
from openai import OpenAI
from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics._helper import msic
from mstr_robotics._connectors import mstr_api
from mstr_robotics.report import rep, prompts, cube

from mstrio.connection import Connection

from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics.user_RAG import keyword_processor, vectorDB_faisst,mstr_openAI,chat_bot

user_path="..\\config\\user_d.json"
with open(user_path, 'r') as file:
    user_d = json.load(file)

jupyter_path="..\\config\\jupyter_objects_d.json"
with open(jupyter_path, 'r') as file:
    jupyter_objects_d = json.load(file)

conn_params =  user_d["conn_params"]
sKey=user_d["sKey"]

#GUID of report wizzard & prompts
project_id=jupyter_objects_d["turtorial_RAG"]["project_id"]          #Tutorial project
#Mstr prompted report
rep_dos_id=jupyter_objects_d["chat_bot_rep"]["rep_dos_id"]
#GUID of output folder for generated reports
ai_rep_folder_id = jupyter_objects_d["chat_bot_rep"]["ai_rep_folder_id"]

#cubes generated in load_rag_cubes
cube_RAG_form_val_ans_name=jupyter_objects_d["turtorial_RAG"]["cube_RAG_form_val_ans_name"]
cube_RAG_form_val_ans_id=jupyter_objects_d["turtorial_RAG"]["cube_RAG_form_val_ans_id"]

cube_obj_prp_rel_name=jupyter_objects_d["turtorial_RAG"]["cube_obj_prp_rel_name"]
cube_obj_prp_rel_id=jupyter_objects_d["turtorial_RAG"]["cube_obj_prp_rel_id"]

cube_att_form_def_name=jupyter_objects_d["turtorial_RAG"]["cube_att_form_def_name"]
cube_att_form_def_id=jupyter_objects_d["turtorial_RAG"]["cube_att_form_def_id"]


warnings.filterwarnings("ignore")
i_mstr_global = mstr_global()


i_rep = rep()
i_cube = cube()
i_df_helper = df_helper()
i_mstr_global = mstr_global()
i_msic = msic()
i_prompts = prompts()
i_mstr_api = mstr_api()
i_chat_bot=chat_bot()
i_map_objects=map_objects()

i_keyword_processor=keyword_processor()
i_vectorDB_faisst=vectorDB_faisst(sKey=sKey)
#i_openAI=OpenAI(sKey=sKey)
i_OpenAI=OpenAI
i_mstr_openAI=mstr_openAI()
#i_openai.api_key = sKey

#Open connection to MSTR
conn_params["project_id"]=project_id
conn = Connection(**conn_params)
conn.headers['Content-type'] = "application/json"

#cube with key cols for vector DB
cube_l=[cube_RAG_form_val_ans_id,cube_obj_prp_rel_id]

#all mtdi cubes
mtdi_cube_l=[cube_RAG_form_val_ans_id,cube_obj_prp_rel_id,cube_att_form_def_id]
mstr_rag_col_d=i_cube.get_mtdi_cube_col_id(conn,cube_l=mtdi_cube_l)
i_run_chat_request=answer_prompts(cube_obj_prp_rel_id=cube_obj_prp_rel_id,
                                  cube_RAG_form_val_ans_id=cube_RAG_form_val_ans_id,
                                  cube_att_form_def_id=cube_att_form_def_id,
                                  rep_dos_id=rep_dos_id)
# here we load attribute form values and attribute and metric names
#we only load columns with the name key
key_l=[]
for cube_id in cube_l:
    key_df=i_cube.quick_query_cube(conn=conn,cube_id=cube_id)
    key_l.extend(key_df["key"].astype(str).to_list())

#vector_store=i_vectorDB_faisst.load_vector_store(key_l)
#i_vectorDB_faisst.load_vector_store(key_l)
vector_store=i_keyword_processor.load_keyword_processor(key_l)

# Initialize client with Perplexity's API endpoint
def call_perplexity(msg_t, sys_cont, temperature=0.1):
    key_word_l = vector_store.extract_keywords(msg_t)
    client = OpenAI(
        api_key=os.environ.get("PERPLEXITY_API_KEY"),  # Set your API key in environment variables
        base_url="https://api.perplexity.ai"
    )

    # Create chat completion request
    response = client.chat.completions.create(
        model="sonar-pro",  # Official model name for Perplexity-API

        messages=[
            {
                "role": "system",
                "content": sys_cont
            },
            {"role": "user", "content": msg_t}
        ]

        ,
        temperature=temperature,
    )
    try:
        json_t_d = {}

        json_t = json.loads(response.json())
        # json_t_d = json.loads(json_t["choices"][0]["message"]["content"])
        json_t_d["ans_d"] = json.loads(json_t["choices"][0]["message"]["content"])
        json_t_d["valid_d_fg"] = 1
    except Exception as err:
        print(err)
        json_t_d["ans_d"] = json.loads(response.json())
        json_t_d["valid_d_fg"] = 0
    return json_t_d


def sys_cont_filter_d(msg_t, fiter_def_d=""):
    key_word_l = vector_store.extract_keywords(msg_t)
    sys_cont_filt = f"""
                    The user message BI Request which must be converted to a valid Python dictionary. 
                    1. This request is a sub taskt, to verify the first draft of the filter generation
                    2. The frist draft of the filter definition is {str(fiter_def_d)}
                    3. if it is a Ranking filter, check if the criteria top / bottom and a metric is defined. If not complete. please ask
                    4. If a filter list contains a 'all' statement, please remove the complete key
                    5. I use your answer, to replace the old filter definition
                    6. Keywords of message: {str(key_word_l)}


                   """
    return sys_cont_filt


message_check_d_l = []
i = 0
sys_cont = f"""
            Convert this BI requests into a validt Python dictionary with these rules:
            1. Structure output under 'template' and 'filter' keys.
            2. If you need a better specification of the BI Request. Raise the question and use the key 'question'
            3. Under template, please distinct between Attributes and metrics
            4. If there is no filter in the request, just let the key filter empty
            5. 
            6. Keywords: {str(key_word_l)}
            7. Return pure Python dict without code formatting
            """
sys_cont_fail = f""" 1.In the user message you find the keys 'template' and 'filter'. 
                   2. Please convert them to valid Python dicts, considering:
                   3. Please ensure, that the output dict, has the keys 'template' and 'filter'

              """

for r in sample_list_l[:5]:
    prp_l = i_rep.get_default_prp_answ(conn=conn, report_id=r["id"]).json()
    for p in prp_l:
        if p["id"] == "89876B7A451E60DB5AC73AAE829778E6":
            message_check_d = {}
            message_check_d["nr"] = i
            message_check_d["msg_t"] = p["answers"]
            try:
                message_check_d["llm_ans_d"] = call_perplexity(msg_t=p["answers"], sys_cont=sys_cont)
                if message_check_d["llm_ans_d"]["valid_d_fg"] == 0:
                    # print(message_check_d["llm_ans_d"]["ans_d"])
                    print("SSS")
                    message_check_d["llm_ans_d"] = call_perplexity(msg_t=message_check_d["llm_ans_d"]["ans_d"],
                                                                   sys_cont=sys_cont_fail)
                    if message_check_d["llm_ans_d"]["valid_d_fg"] == 0:
                        message_check_d["llm_ans_d"]["ans_d"]["filter"] = call_perplexity(msg_t=p["answers"],
                                                                                          sys_cont=sys_cont_filter_d(
                                                                                              msg_t=p["answers"],
                                                                                              fiter_def_d=
                                                                                              message_check_d[
                                                                                                  "llm_ans_d"]["ans_d"][
                                                                                                  "filter"]))
            except Exception as err:
                print(err)
                print(i)
                message_check_d["llm_ans_d"] = None

            message_check_d_l.append(message_check_d.copy())
            i += 1

message_check_d_l


