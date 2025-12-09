import os
import base64
import requests
import pandas as pd
from mstrio.api import objects
from openai import OpenAI
import ast
import json
from mstrio.connection import Connection
i_obj=objects


class translate():

    def trans_chatGPT(self,sKey, base_words_l,trans_lang_l):
        base_words = ', '.join(base_words_l)
        trans_lang = ', '.join(trans_lang_l)
        content = f'Please translate the following words {base_words} into {trans_lang}. Please separte them with a | charackter. Use the | charackter to sepparate the base and the translations',
        chat_completion=self.call_open_AI(content)
        js = json.loads(chat_completion.json())
        #js["choices"][0]['message']['content']
        return js

    def check_trans_chatGPT(self,sKey,messages,*args,**kwargs):
        chat_completion = self.call_open_AI(sKey=sKey,messages=messages,*args,**kwargs)
        js = json.loads(chat_completion.json())
        return js

    def call_open_AI(self,sKey,messages, max_tokens=1000,temperature=0.3, model="gpt-4"):
        # This is the default and can be omitted
        client = OpenAI(api_key=sKey )

        chat_completion = client.chat.completions.create(
            messages=messages,
            # model="gpt-3.5-turbo",
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
        return chat_completion

    def extract_translations(self,conn,obj_l,project_id):
        all_obj_trans_l = []
        obj_trans_raw_d = {}
        for obj in obj_l:

            obj_trans_raw_d["obj_id"] = obj["id"]
            obj_trans_raw_d["type_id"] = obj["type"]
            obj_trans_raw_d["project_id"] = project_id
            obj_trans_raw_d["obj_trans_l"] = i_obj.get_translations(connection=conn, project_id=project_id, id=obj["id"]
                                                              , object_type=int(obj["type"]), fields=[]).json()
            #print(obj_trans_raw_d["obj_trans_l"])
            obj_trans_d=self.obj_trans_j_to_d(obj_trans_raw_d)
            all_obj_trans_l.extend(obj_trans_d.copy())

        return all_obj_trans_l

    def obj_trans_j_to_d(self,obj_trans_raw_d):
        obj_t_lang_l = []
        obj_t_lang_d={}
        run_key = 0
        obj_t_lang_d["project_id"] = obj_trans_raw_d["project_id"]
        obj_t_lang_d["obj_id"]=obj_trans_raw_d["obj_id"]
        obj_t_lang_d["type_id"] = obj_trans_raw_d["type_id"]
        obj_t_lang_d["default_lang"] = obj_trans_raw_d["obj_trans_l"]["defaultLanguage"]
        obj_t_lang_d["default_lang_name"]=obj_trans_raw_d["obj_trans_l"]["localeName"]
        default_lang_code=self.get_lang_code_from_name(name=obj_trans_raw_d["obj_trans_l"]["localeName"] )
        text_key = list(obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"].keys())[0]
        obj_t_lang_d["default_lang_code"] = default_lang_code
        obj_t_lang_d["org_name"] = obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"][text_key]["translationValues"][
                                obj_t_lang_d["default_lang_code"]]["translation"]
        for t in list(obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"].keys()):

            obj_t_lang_d["text_field_key"] = t
            obj_t_lang_d["translationTargetName"] = obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"][t]["translationTargetName"]
            obj_t_lang_d["anz_trans"] = len(
                list(obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"][t]["translationValues"].keys()))
            trans_lang_d={}
            for lang in list(obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"][t]["translationValues"].keys()):
                trans_lang_d[self.get_name_from_lang_code(code=lang)] = obj_trans_raw_d["obj_trans_l"]["localesAndTranslations"][t]["translationValues"][lang]["translation"]
            #trans_lang_d["org_lang_code"]=
            obj_t_lang_d["trans_lang_d"]=trans_lang_d
            obj_t_lang_l.append(obj_t_lang_d.copy())

            run_key += 1
        return obj_t_lang_l

    def gpt_out_trans_j_to_d(self,all_trans_output_l):
        clean_trans_output_l = []
        clean_trans_output_d = {}
        changes_trans_out_l=[]

        output_d={}

        i = 0
        for o in all_trans_output_l:
            changes_trans_out_d = {}
            try:

                clean_trans_output_d["project_id"] = o["project_id"]
                clean_trans_output_d["obj_id"] = o["obj_id"]
                clean_trans_output_d["type_id"] = o["type_id"]
                clean_trans_output_d["default_lang"] = o["default_lang"]
                clean_trans_output_d["default_lang_code"] = o["default_lang_code"]
                clean_trans_output_d["default_lang_name"] = o["default_lang_name"]
                clean_trans_output_d["text_field_key"] = o["text_field_key"]
                clean_trans_output_d["translationTargetName"] = o["translationTargetName"]
                clean_trans_output_d["model"] = o["trans_data"]["model"]
                clean_trans_output_d["object"] = o["trans_data"]["object"]
                # clean_trans_output_d["role"]=o["trans_data"]["role"]
                clean_trans_output_d["created"] = o["trans_data"]["created"]
                clean_trans_output_d["completion_tokens"] = o["trans_data"]["usage"]["completion_tokens"]
                clean_trans_output_d["total_tokens"] = o["trans_data"]["usage"]["total_tokens"]
                gpt_ans_d = ast.literal_eval(o["trans_data"]["choices"][0]["message"]["content"])
                clean_trans_output_d["org_name"] = o["org_name"]
                clean_trans_output_d["gpt_comment"] = gpt_ans_d["gpt_comment"]
                clean_trans_output_d["correct_fg"] = gpt_ans_d["correct_fg"]
                #clean_trans_output_d["special_char_fg"] = gpt_ans_d["special_char_fg"]

                count_lang = 0
                for lang in self.mstr_lang_codes():

                    if lang["name"] in o["trans_lang_d"].keys():
                        count_lang += 1
                        clean_trans_output_d[lang["name"]] = o["trans_lang_d"][lang["name"]]
                        clean_trans_output_d["count_lang"] = 1
                    #gpt_ans_d = ast.literal_eval(o["trans_data"]["choices"][0]["message"]["content"])
                    #gpt_ans_d = json.loads(o["trans_data"]["choices"][0]["message"]["content"])

                    if lang["name"] in gpt_ans_d.keys():

                        if o["trans_lang_d"][lang["name"]] != gpt_ans_d[lang["name"]]:
                            changes_trans_out_d["project_id"] = clean_trans_output_d["project_id"]
                            changes_trans_out_d["obj_id"] = clean_trans_output_d["obj_id"]
                            changes_trans_out_d["text_field_key"] = clean_trans_output_d["text_field_key"]
                            changes_trans_out_d["code"]=lang["code"]
                            changes_trans_out_d["org_name"] = o["org_name"]

                            changes_trans_out_d["org_trans"]=o["trans_lang_d"][lang["name"]]
                            changes_trans_out_d["lang_name"] = lang["name"]
                            changes_trans_out_d["translation"] = gpt_ans_d[lang["name"]]

                    if len(list(changes_trans_out_d.keys()))>0:
                        changes_trans_out_l.append(changes_trans_out_d.copy())

                clean_trans_output_l.append(clean_trans_output_d.copy())


                output_d = {"clean_read_out":clean_trans_output_l,
                            "gpt_change":changes_trans_out_l}
                i+=1

            except Exception as err:
                print(err)

        return output_d

    def mstr_lang_codes(self):
        mstr_lang_codes_l=[
        {"code": "2052", "name": "Chinese (Simplified)", "language": "Chinese", "short": "zh-CN"},
        {"code": "1028", "name": "Chinese (Traditional)", "language": "Chinese", "short": "zh-TW"},
        {"code": "1030", "name": "Danish (Denmark)", "language": "danish", "short": "da"},
        {"code": "1043", "name": "Dutch (Netherlands)", "language": "Dutch", "short": "nl"},
        {"code": "2057", "name": "English (United Kingdom)", "language": "English", "short": "en"},
        {"code": "1033", "name": "English (United States)", "language": "English", "short": "en"},
        {"code": "2060", "name": "French (Belgium)", "language": "French", "short": "fr"},
        {"code": "1036", "name": "French (France)", "language": "French", "short": "fr"},
        {"code": "4108", "name": "French (Switzerland)", "language": "French", "short": "fr"},
        {"code": "1031", "name": "German (Germany)", "language": "German", "short": "de"},
        {"code": "2055", "name": "German (Switzerland)", "language": "German", "short": "de"},
        {"code": "1040", "name": "Italian (Italy)", "language": "Italian", "short": "it"},
        {"code": "2064", "name": "Italian (Switzerland)", "language": "Italian", "short": "it"},
        {"code": "1041", "name": "Japanese", "language": "Japanese", "short": "ja"},
        {"code": "1042", "name": "Korean", "language": "Korean", "short": "ko"},
        {"code": "1045", "name": "Polish", "language": "Polish", "short": "pl"},
        {"code": "1046", "name": "Portuguese (Brazil)", "language": "Portuguese", "short": "pt"},
        {"code": "1049", "name": "Russian", "language": "Russian", "short": "ru"},
        {"code": "3082", "name": "Spanish (Spain)", "language": "Spanish", "short": "es"},
        {"code": "1053", "name": "Swedish (Sweden)", "language": "Swedish", "short": "sv"}
        ]
        return mstr_lang_codes_l

    def get_name_from_lang_code(self,code):
        for l in self.mstr_lang_codes():
            if l["code"] == code:
                return str(l["name"])


    def get_lang_code_from_name(self,name):
        for l in self.mstr_lang_codes():
            if l["name"] == name:
                return str(l["code"])

class check_chart():

    def base_checker(self,api_key,image_path, max_tokens=300):
        with open(image_path, "rb") as image_file:
            base64.b64encode(image_file.read()).decode('utf-8')

        # Getting the base64 string
        base64_image = encode_image(image_path)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        payload = {
            "model": "gpt-4-vision-preview",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Can you identify error in IBCS charts"
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": max_tokens
        }

        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)

        # chart_check=json.loads(response)
        return response.json()
