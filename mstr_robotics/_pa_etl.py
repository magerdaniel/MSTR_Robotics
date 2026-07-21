from datetime import datetime

from mstr_robotics._connectors import MstrApi
from mstr_robotics.read_out_prj_obj import IoAttributes, ReadOutHierarchy
from mstr_robotics.report import Prompts, Rep

i_rep = Rep()
i_prompts = Prompts()
i_mstr_api = MstrApi()
i_io_attributes = IoAttributes()
i_read_out_hierarchy = ReadOutHierarchy()


class ParsePa:
    stop_job_exe = False

    def rem_braket(self, exp):
        # attribute forms are marked with brakets in PA
        # with them we indentify them
        if exp[:1] == "(":
            exp = exp.replace("(", "")
            exp = exp.replace(")", "")
        return exp

    def rem_curly(self, att):
        # used for multiple prompts to distinct multiple answers
        if att[:1] == "{":
            att = att.replace("{", "")
            att = att.replace("}", "")
        return att

    def get_pa_prp_row_ans(self, pa_raw_data_d, prompt_id):
        # select the answer for a certain prompt
        # for a report for a certain report job
        for p_ans_row in pa_raw_data_d.iterrows():
            if p_ans_row[1]["Prompt@GUID"] == prompt_id:
                prp_ans = p_ans_row[1]["Prompt_Answer@ID"]
        return prp_ans

    def get_pa_prp_id_l(self, get_pa_raw_data_df):
        id_l = []
        for prp_l in get_pa_raw_data_df.iterrows():
            id_l.append(prp_l[1]["Prompt@GUID"])
        return list(dict.fromkeys(id_l))


class PaParsePrp:
    # this class handels the extraction, parsing
    # and enrichment of element, object and value prompts
    def pa_parse_ele_ans(self, pa_raw_ans_str):
        # parses multiple elements from a comma separated
        # string to a list of elements
        # the attribute form values, for compount keys of attributes
        # are
        em_ans_ele_l = pa_raw_ans_str.split(",")
        ans_prsd_l = []
        for element in em_ans_ele_l:
            ans_prsd_l.append({"List": element.split(":"), "String": element})
        return ans_prsd_l

    def bld_prp_ele_ans_JSON(self, conn, report_id, instance_id, ans_prsd_l, prompt_id, att_form_int=0):
        prp_ele_ans_JSON_l = []
        for elemnt in ans_prsd_l:
            list_values = i_mstr_api.get_ele_prp_ans(
                conn=conn,
                report_id=report_id,
                instance_id=instance_id,
                prompt_id=prompt_id,
                att_form_str=elemnt["List"][att_form_int],
            )
            for e in list_values[0]:
                if e["name"] == elemnt["String"]:
                    prp_ele_ans_JSON_l.append(e)
        return prp_ele_ans_JSON_l

    def bld_prp_obj_ans_JSON(self, conn, report_id, instance_id, prompt_id, ans_prsd_l):
        # mapping of the prompt page answers
        # with the data from pa
        prp_obj_ans_JSON_l = []
        for elemnt in ans_prsd_l:
            list_values = i_mstr_api.get_prp_ans(
                conn=conn, report_id=report_id, instance_id=instance_id, prompt_id=prompt_id
            )
            for o in list_values:
                if o["name"] == elemnt["String"]:
                    prp_obj_ans_JSON_l.append(o)
        return prp_obj_ans_JSON_l

    def bld_val_prp_JSON(self, prompt_id, dataType, val_str):
        # parsing of the date / times prompts
        # regional settings will be a challange
        post_str = "T00:00:00.000+0000"
        # val_prp_JSON_d=  {"id":prompt_id,"type": "VALUE","answers": ""}
        if dataType == "DATE" and len(val_str) > 0:
            datetime_object = datetime.strptime(val_str, "%d.%m.%Y")
            val_str = str(datetime_object.date()) + post_str
        else:
            val_str = val_str
        return val_str


i_parse_pa = ParsePa()


class ParseAttExpPrp:
    # expression prompts are the most complicated prompts
    # to prompts. Supported compare logigs:
    # "excatly"
    def __init__(self):
        self._sys_hier_att_df = None

    def _get_sys_hier_att_df(self, conn):
        # System Hierarchy attributes are read out live once per session
        # and cached; this replaces the former "System hier_att" cube
        if self._sys_hier_att_df is None:
            self._sys_hier_att_df = i_read_out_hierarchy.read_out_sys_hier(conn=conn)
        return self._sys_hier_att_df

    def bld_prp_exp_ans_JSON(self, conn, report_id, instance_id, prompt_id, pa_ans_prsd_l):

        prp_ans_base_l = i_mstr_api.get_prp_ans(
            conn=conn, report_id=report_id, instance_id=instance_id, prompt_id=prompt_id
        )
        prp_exp_ans_JSON_l = []
        if prp_ans_base_l[0][0]["type"] == "metric":
            pass
            # for elemnt in ans_prsd_l:
            #    self.pa_parse_metric_ans(list_values,elemnt )
        elif prp_ans_base_l[0][0]["type"] == "attribute":
            att_exp_ans_l = self.pa_parse_exp_ans(prp_ans_base_l, pa_ans_prsd_l)
            att_GUID_exp_ans_l = self.add_att_GUID(conn=conn, prompt_id=prompt_id, att_exp_ans_l=att_exp_ans_l)
            prp_exp_ans_JSON_l = i_prompts.bld_expr_prp_answ(prompt_id=prompt_id, att_exp_ans_l=att_GUID_exp_ans_l)
            return prp_exp_ans_JSON_l

        elif prp_ans_base_l[0][0]["type"] == "xxxxxx":
            att_exp_ans_l = self.pa_parse_exp_ans(prp_ans_base_l, pa_ans_prsd_l)
            att_GUID_exp_ans_l = self.add_att_GUID(conn=conn, prompt_id=prompt_id, att_exp_ans_l=att_exp_ans_l)
            prp_exp_ans_JSON_l = i_prompts.bld_expr_prp_answ(prompt_id=prompt_id, att_exp_ans_l=att_GUID_exp_ans_l)

            return prp_exp_ans_JSON_l

        return prp_exp_ans_JSON_l

    def pa_parse_exp_ans(self, prp_ans_base_l, pa_ans_prsd_l):
        exp_ans_l = []
        for ans in pa_ans_prsd_l:
            # pa concats mutiple filter expression with "And"
            pa_exp_ans_l = ans["String"].split(" And ")
            for pa_exp_ans in pa_exp_ans_l:
                exp_ans_l.append(self.pa_split_exp_ans(pa_exp_ans))
        return exp_ans_l

    def pa_split_exp_ans(self, pa_exp_ans):
        # split pa ans in parts
        # name | operator | value
        # operaters like between, isNull and others are not supported
        exp_ans_d = {}
        if len(pa_exp_ans) > 0:
            pa_exp_ans = ParsePa().rem_braket(pa_exp_ans)
            pa_exp_ans = ParsePa().rem_curly(pa_exp_ans)
            split_operator_val_l = pa_exp_ans.strip().split(" ")
            exp_ans_d["att_name"] = split_operator_val_l[0]
            exp_ans_d["att_form_name"] = ParsePa().rem_braket(split_operator_val_l[1])
            exp_ans_d["operator"] = split_operator_val_l[2]
            exp_ans_d["val"] = split_operator_val_l[3]

        return exp_ans_d

    def add_att_GUID(self, conn, prompt_id, att_exp_ans_l):
        # in PA only the names of attributes (forms) are logged
        # to answer Prompts over REST we need to pass the GUIDs
        # the attribute id is resolved by name over the System Hierarchy
        # read-out, the form id / data type come from the attribute
        # definition (IoAttributes.read_att_form_exp)
        prp_att_df = self._get_sys_hier_att_df(conn=conn)

        att_GUID_exp_ans_l = []
        att_form_cache_d = {}

        for ans in att_exp_ans_l:
            if len(ans) > 0:
                att_df = prp_att_df[prp_att_df["att_name"] == ans["att_name"]]
                if att_df.empty:
                    raise ValueError(f"no attribute found for '{ans['att_name']}'")
                att_id = att_df["att_id"].values[0]

                # read the attribute definition once per attribute
                if att_id not in att_form_cache_d:
                    att_form_cache_d[att_id] = i_io_attributes.read_att_form_exp(conn=conn, att_id_l=[att_id])[
                        "all_att_maps_l"
                    ]

                form_d_l = [f for f in att_form_cache_d[att_id] if f["form_name"] == ans["att_form_name"]]
                if not form_d_l:
                    raise ValueError(f"no form '{ans['att_form_name']}' found for attribute '{ans['att_name']}'")
                form_d = form_d_l[0]

                att_GUID_exp_ans_l.append(
                    {
                        "prompt_id": prompt_id,
                        "att_id": att_id,
                        "att_name": ans["att_name"],
                        "att_form_id": form_d["form_id"],
                        "form_data_type": form_d["REST_form_type"],
                        "filter_val_l": ans["val"],
                        "operator": ans["operator"],
                    }
                )
        return att_GUID_exp_ans_l


i_pa_parse_prp = PaParsePrp()
i_parse_att_exp_prp = ParseAttExpPrp()


class RunPrpAnsBld:
    # this class controls the parsing of the
    # pa answers and the generation of the prompts answer JSON
    # of a certain mstr job
    def bld_pa_job_prp_JSON(self, conn, action_prp_l, pa_raw_data_df, report_id, instance_id):
        prompt_ans_JSON_l = []
        # print("1")
        print(prompt_ans_JSON_l)
        for p in action_prp_l:
            # the first step in parsing is
            # to check the prompt type
            # print({"prompt_id":p["id"],"prompt_type":p["type"]})
            pa_ele_prp_row_ans_str = i_parse_pa.get_pa_prp_row_ans(pa_raw_data_df, prompt_id=p["id"])
            pa_ans_prsd_l = i_pa_parse_prp.pa_parse_ele_ans(pa_raw_ans_str=pa_ele_prp_row_ans_str)

            if p["type"] == "ELEMENTS":
                # pa_ans_prsd_l = i_pa_parse_prp.pa_parse_ele_ans(pa_raw_ans_str=pa_ele_prp_row_ans_str)
                prp_ele_ans_JSON_l = i_pa_parse_prp.bld_prp_ele_ans_JSON(
                    conn=conn, report_id=report_id, instance_id=instance_id, prompt_id=p["id"], ans_prsd_l=pa_ans_prsd_l
                )

                prompt_ans_JSON_l.append(
                    i_prompts.frame_prp_ans(prompt_id=p["id"], prp_type="ELEMENTS", prp_ans_JSON_l=prp_ele_ans_JSON_l)
                )
            elif p["type"] == "VALUE":
                # print(p)
                prp_val_ans_JSON = i_pa_parse_prp.bld_val_prp_JSON(
                    prompt_id=p["id"], dataType=p["dataType"], val_str=pa_ele_prp_row_ans_str
                )

                prompt_ans_JSON_l.append(
                    i_prompts.frame_prp_ans(prompt_id=p["id"], prp_type="VALUE", prp_ans_JSON_l=prp_val_ans_JSON)
                )
            elif p["type"] == "OBJECTS":
                prp_obj_ans_JSON_l = i_pa_parse_prp.bld_prp_obj_ans_JSON(
                    conn=conn, report_id=report_id, instance_id=instance_id, prompt_id=p["id"], ans_prsd_l=pa_ans_prsd_l
                )

                prompt_ans_JSON_l.append(
                    i_prompts.frame_prp_ans(prompt_id=p["id"], prp_type="OBJECTS", prp_ans_JSON_l=prp_obj_ans_JSON_l)
                )

            elif p["type"] == "EXPRESSION":
                prp_exp_ans_JSON_l = i_parse_att_exp_prp.bld_prp_exp_ans_JSON(
                    conn=conn,
                    report_id=report_id,
                    instance_id=instance_id,
                    prompt_id=p["id"],
                    pa_ans_prsd_l=pa_ans_prsd_l,
                )

                prompt_ans_JSON_l.append(prp_exp_ans_JSON_l)

        print(prompt_ans_JSON_l)
        return prompt_ans_JSON_l
