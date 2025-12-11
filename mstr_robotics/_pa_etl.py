from mstr_robotics.report import rep
from mstr_robotics.report import prompts
from mstr_robotics._connectors import mstr_api
#from mstr_robotics.export import trans_data

from datetime import datetime
i_rep=rep()
i_prompts=prompts()
i_mstr_api=mstr_api()
#i_trans_data=trans_data()

class parse_pa():
    stop_job_exe=False
    def rem_braket(self, exp):
        #attribute forms are marked with brakets in PA
        #with them we indentify them
        if exp[:1]=="(":
            exp = exp.replace("(", "")
            exp = exp.replace(")", "")
        return exp

    def rem_curly(self, att):
        #used for multiple prompts to distinct multiple answers
        if att[:1] == "{":
            att = att.replace("{", "")
            att = att.replace("}", "")
        return att

    def zzz_fetch_pa_data_prp(self, conn, report_id,prompt_answ):
        # base function to fetch the raw data from PA over a simple report
        # imput will be a uniqui job identification
        instance_id = i_rep.open_Instance(conn=conn, report_id=report_id)
        i_rep.set_inst_prompt_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                         prompt_answ=prompt_answ)
        att_col_l = i_rep.get_rep_attributes(conn=conn, report_id=report_id)
        pa_raw_data_df = i_rep.rep_to_dataframe(conn=conn, report_id=report_id,
                                                instance_id=instance_id, att_col_l=att_col_l)
        return pa_raw_data_df

    def ZZZ_fetch_pa_data(self,conn,report_id):
        #base function to fetch the raw data from PA over a simple report
        #imput will be a uniqui job identification
        pa_regam_inst_id = i_rep.open_Instance(conn=conn, report_id=report_id)

        att_col_l = i_rep.get_rep_attributes(conn=conn, report_id=report_id)
        pa_raw_data_df = i_rep.rep_to_dataframe(conn=conn, report_id=report_id,
                                                       instance_id=pa_regam_inst_id, att_col_l=att_col_l)
        return pa_raw_data_df

    def get_pa_prp_row_ans(self,pa_raw_data_d,prompt_id):
        #select the answer for a certain prompt
        #for a report for a certain report job
        for p_ans_row in pa_raw_data_d.iterrows():
            if p_ans_row[1]["Prompt@GUID"]==prompt_id:
                prp_ans=p_ans_row[1]["Prompt_Answer@ID"]
        return prp_ans

    def get_pa_prp_id_l(self,get_pa_raw_data_df):
        id_l = []
        for prp_l in get_pa_raw_data_df.iterrows():
            id_l.append(prp_l[1]["Prompt@GUID"])
        return list(dict.fromkeys(id_l))

class pa_parse_prp():
    #this class handels the extraction, parsing
    #and enrichment of element, object and value prompts
    def pa_parse_ele_ans(self, pa_raw_ans_str):
        #parses multiple elements from a comma separated
        #string to a list of elements
        #the attribute form values, for compount keys of attributes
        #are
        em_ans_ele_l = pa_raw_ans_str.split(",")
        ans_prsd_l = []
        for element in em_ans_ele_l:
            ans_prsd_l.append({"List":element.split(":"),"String": element})
        return ans_prsd_l

    def bld_prp_ele_ans_JSON(self,conn,report_id,instance_id,ans_prsd_l,prompt_id,att_form_int = 0):
        prp_ele_ans_JSON_l = []
        for elemnt in ans_prsd_l:
            list_values=i_mstr_api.get_ele_prp_ans(conn=conn,report_id=report_id,
                                                   instance_id=instance_id,prompt_id=prompt_id,att_form_str=elemnt["List"][att_form_int])
            for e in list_values[0]:
                if e["name"] == elemnt["String"]:
                    prp_ele_ans_JSON_l.append(e)
        return prp_ele_ans_JSON_l

    def bld_prp_obj_ans_JSON(self,conn,report_id,instance_id,prompt_id,ans_prsd_l):
        #mapping of the prompt page answers
        #with the data from pa
        prp_obj_ans_JSON_l = []
        for elemnt in ans_prsd_l:
            list_values = i_mstr_api.get_prp_ans(conn=conn, report_id=report_id, instance_id=instance_id, prompt_id=prompt_id)
            for o in list_values:
                if o["name"] == elemnt["String"]:
                    prp_obj_ans_JSON_l.append(o)
        return prp_obj_ans_JSON_l

    def bld_val_prp_JSON(self,prompt_id,dataType,val_str):
        #parsing of the date / times prompts
        #regional settings will be a challange
        post_str="T00:00:00.000+0000"
        #val_prp_JSON_d=  {"id":prompt_id,"type": "VALUE","answers": ""}
        if dataType=="DATE" and len(val_str)>0:
            datetime_object = datetime.strptime(val_str, '%d.%m.%Y')
            val_str=str(datetime_object.date())+post_str
        else:
            val_str=val_str
        return val_str

i_parse_pa=parse_pa()
class parse_att_exp_prp():
    #expression prompts are the most complicated prompts
    #to prompts. Supported compare logigs:
    # "excatly"
    def bld_prp_exp_ans_JSON(self,conn,report_id,instance_id,prompt_id,pa_ans_prsd_l,hier_att_df):

        prp_obj_ans_JSON_l = []
        prp_ans_base_l = i_mstr_api.get_prp_ans(conn=conn, report_id=report_id, instance_id=instance_id,
                                             prompt_id=prompt_id)
        prp_exp_ans_JSON_l=[]
        if prp_ans_base_l[0][0]["type"] == "metric":
            pass
            #for elemnt in ans_prsd_l:
            #    self.pa_parse_metric_ans(list_values,elemnt )
        elif prp_ans_base_l[0][0]["type"] == "attribute":
            att_exp_ans_l=self.pa_parse_exp_ans(prp_ans_base_l, pa_ans_prsd_l )
            att_GUID_exp_ans_l=self.add_att_GUID(prompt_id=prompt_id, att_exp_ans_l=att_exp_ans_l, hier_att_df=hier_att_df)
            prp_exp_ans_JSON_l=i_prompts.bld_expr_prp_answ(prompt_id=prompt_id, att_exp_ans_l=att_GUID_exp_ans_l)
            return prp_exp_ans_JSON_l

        elif prp_ans_base_l[0][0]["type"] == "xxxxxx":
            att_exp_ans_l=self.pa_parse_exp_ans(prp_ans_base_l, pa_ans_prsd_l )
            att_GUID_exp_ans_l=self().add_att_GUID(prompt_id=prompt_id, att_exp_ans_l=att_exp_ans_l, hier_att_df=hier_att_df)
            prp_exp_ans_JSON_l = i_prompts.bld_expr_prp_answ(prompt_id=prompt_id, att_exp_ans_l=att_GUID_exp_ans_l)

            return prp_exp_ans_JSON_l

        return prp_exp_ans_JSON_l

    def pa_parse_exp_ans(self,prp_ans_base_l, pa_ans_prsd_l ):
        exp_ans_l=[]
        for ans in pa_ans_prsd_l:
            #pa concats mutiple filter expression with "And"
            pa_exp_ans_l = ans["String"].split(" And ")
            for pa_exp_ans in pa_exp_ans_l:
                exp_ans_l.append(self.pa_split_exp_ans(pa_exp_ans))
        return exp_ans_l

    def pa_split_exp_ans(self, pa_exp_ans):
        #split pa ans in parts
        #name | operator | value
        #operaters like between, isNull and others are not supported
        exp_ans_d = {}
        if len(pa_exp_ans)>0:
            pa_exp_ans=parse_pa().rem_braket(pa_exp_ans)
            pa_exp_ans=parse_pa().rem_curly(pa_exp_ans)
            split_operator_val_l = pa_exp_ans.strip().split(" ")
            exp_ans_d["att_name"]=split_operator_val_l[0]
            exp_ans_d["att_form_name"]=parse_pa().rem_braket(split_operator_val_l[1])
            exp_ans_d["operator"]=split_operator_val_l[2]
            exp_ans_d["val"]=split_operator_val_l[3]

        return exp_ans_d

    def add_att_GUID(self, prompt_id, att_exp_ans_l, hier_att_df):
        #in PA only the names of attributes (forms) are logged
        # to answer Prompt over REST we need to pass the GUID
        mapping_d={}
        prp_att_df=hier_att_df[hier_att_df["hier_name"]=="System Hierarchy"]

        att_GUID_exp_ans_l=[]

        for ans in att_exp_ans_l:
            if len(ans)>0:
                att_df = prp_att_df[(prp_att_df["att_name"] == ans["att_name"]) & (prp_att_df["att_form_name"] == ans["att_form_name"]) ]
                if att_df.empty:
                    raise {"err_msg":"no attribute /form found"}

                att_GUID_exp_ans_l.append({"prompt_id": prompt_id
                                           ,"att_id":att_df["att_id"].values[0]
                                           ,"att_name":prp_att_df["att_name"].values[0]
                                           ,"att_form_id":att_df["att_form_id"].values[0]
                                           ,"data_type":att_df["att_form_data_type"].values[0]
                                           ,"filter_val_l":ans["val"]
                                           ,"operator": ans["operator"]
                                           })
        return att_GUID_exp_ans_l
i_pa_parse_prp=pa_parse_prp()
i_parse_att_exp_prp=parse_att_exp_prp()

class run_prp_ans_bld():
    #this class controls the parsing of the
    # pa answers and the generation of the prompts answer JSON
    # of a certain mstr job
    def bld_pa_job_prp_JSON(self,conn,action_prp_l,pa_raw_data_df,hier_att_df,report_id,instance_id ):
        prompt_ans_JSON_l = []
        #print("1")
        print(prompt_ans_JSON_l)
        for p in action_prp_l:
            #the first step in parsing is
            #to check the prompt type
            #print({"prompt_id":p["id"],"prompt_type":p["type"]})
            pa_ele_prp_row_ans_str = i_parse_pa.get_pa_prp_row_ans(pa_raw_data_df, prompt_id=p["id"])
            pa_ans_prsd_l = i_pa_parse_prp.pa_parse_ele_ans(pa_raw_ans_str=pa_ele_prp_row_ans_str)

            if p["type"] == "ELEMENTS":
                #pa_ans_prsd_l = i_pa_parse_prp.pa_parse_ele_ans(pa_raw_ans_str=pa_ele_prp_row_ans_str)
                prp_ele_ans_JSON_l = i_pa_parse_prp.bld_prp_ele_ans_JSON(conn=conn, report_id=report_id,
                                                                     instance_id=instance_id, prompt_id=p["id"],
                                                                     ans_prsd_l=pa_ans_prsd_l)

                prompt_ans_JSON_l.append(i_prompts.frame_prp_ans(prompt_id=p["id"],prp_type="ELEMENTS",
                                                                 prp_ans_JSON_l=prp_ele_ans_JSON_l)
                                         )
            elif p["type"] == "VALUE":
                #print(p)
                prp_val_ans_JSON=i_pa_parse_prp.bld_val_prp_JSON(prompt_id=p["id"],
                                                                 dataType=p["dataType"],
                                                                 val_str=pa_ele_prp_row_ans_str)


                prompt_ans_JSON_l.append(i_prompts.frame_prp_ans(prompt_id=p["id"],prp_type="VALUE",
                                                                 prp_ans_JSON_l=prp_val_ans_JSON)
                                         )
            elif p["type"] == "OBJECTS":
                prp_obj_ans_JSON_l = i_pa_parse_prp.bld_prp_obj_ans_JSON(conn=conn, report_id=report_id,
                                                                     instance_id=instance_id, prompt_id=p["id"],
                                                                     ans_prsd_l=pa_ans_prsd_l)

                prompt_ans_JSON_l.append(i_prompts.frame_prp_ans(prompt_id=p["id"],prp_type="OBJECTS",
                                                                 prp_ans_JSON_l=prp_obj_ans_JSON_l)
                                        )

            elif p["type"] == "EXPRESSION":
                prp_exp_ans_JSON_l = i_parse_att_exp_prp.bld_prp_exp_ans_JSON(conn=conn, report_id=report_id,
                                                                     instance_id=instance_id, prompt_id=p["id"],
                                                                     pa_ans_prsd_l=pa_ans_prsd_l,
                                                                     hier_att_df=hier_att_df)

                prompt_ans_JSON_l.append(prp_exp_ans_JSON_l)

        print(prompt_ans_JSON_l)
        return prompt_ans_JSON_l

"""
class parse_exp_prp():

    def pa_parse_metric_ans(self,list_values,elemnt ):
        pa_exp_ans_l = elemnt["String"].split(" And ")
        for pa_exp_ans in pa_exp_ans_l :
            metric_ans_d=self.pa_split_exp_ans(pa_exp_ans)
            self.bld_metric_exp_prp_JSON(metric_ans_d)


    def pa_split_exp_ans(self, pa_exp_ans):
        #split pa ans in parts
        #name
        #operator
        #value
        #operaters like between, isNull and others are not supported
        exp_ans_d={}
        pa_exp_ans=pa_exp_ans[1:][:-1][1:]
        split_metric_l=pa_exp_ans.split("}")
        exp_ans_d["metric_name"]=split_metric_l[0]
        split_operator_val_l=split_metric_l[1].strip().split(" ")
        exp_ans_d["operator"]=split_operator_val_l[0]
        exp_ans_d["val"]=split_operator_val_l[1]
        return exp_ans_d

    def pa_rest_operator_trans(self,operator):
        operator_trans_d={"greater":">"}

    def bld_metric_exp_prp_JSON(self,prompt_id,metric_id,operator,level_att="default"):
        prp_metric_ans=  {
                            "id": prompt_id,
                            "type": "EXPRESSION",
                            "answers": {
                              "expression": {
                                "operator": "And",
                                "operands": [
                                  {
                                    "operator": operator,
                                    "operands": [
                                      {
                                        "type": "metric",
                                        "id": metric_id
                                      },
                                      {
                                        "type": "constant",
                                        "dataType": "Numeric",
                                        "value": "200"
                                      }
                                    ],
                                    "level": {
                                      "type": level_att
                                    }
                                  }
                                ]
                              }
                            }
                          }

"""