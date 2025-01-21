from mstr_robotics._helper import msic
from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics.mstr_classes import mstr_global,md_searches
from mstr_robotics.report import cube,rep,prompts
from mstr_robotics._connectors import mstr_api
from mstrio.api import facts, attributes, user_hierarchies,tables, filters, metrics,cubes
import pandas as pd

i_mstr_global=mstr_global()
i_md_searches=md_searches()
i_df_helper=df_helper()
i_msic=msic()
i_cube=cube()
i_rep=rep()
i_prompts=prompts()
i_mstr_api=mstr_api()


class read_out_hierarchy():

    def __init__(self):
        self.run_prop_d={}

    def _read_hier_in_prp(self, conn):
        #reads out hierarchies used in prompts
        #currently not used
        all_proj_prp_rep_l=prompts().get_project_prp(conn=conn)
        proj_prp_hier_l = []
        for p in all_proj_prp_rep_l:
            try:
                if p["expressionType"] == "hierarchy":
                    for hier in p["question"]["predefinedObjects"]:
                        proj_prp_hier_d=self.run_prop_d.copy()
                        proj_prp_hier_d["project_id"]=conn.headers["X-MSTR-ProjectID"]
                        proj_prp_hier_d["prompt_id"]= p["information"]["objectId"]
                        proj_prp_hier_d["object_id"]= hier["objectId"]
                        proj_prp_hier_d["subtype"]= hier["subType"]
                        proj_prp_hier_d["object_name"]= hier["name"]
                        proj_prp_hier_l.append(proj_prp_hier_d.copy() )
            except:
                print()

        return proj_prp_hier_l

    def read_out_hier_att_df(self, conn, proj_hier_l) :
        all_hier_att_col=self.run_prop_d.keys()
        all_hier_att_col.extend(["project_id","hier_id","hier_subType","hier_name","att_id","att_name",
                          "att_form_id","att_form_name","att_form_data_type"])
        all_hier_att_l=[]

        for hier in proj_hier_l:

            hier_att_l=i_mstr_api.get_hier_att(conn=conn,
                                         hier_id=hier["id"]
                                                    )

            for att in hier_att_l[0]["attributes"]:
                for af in att["forms"]:
                    run_prop_val=list(self.run_prop_d.keys().values())
                    ahl_l=run_prop_val
                    ahl_l.extend([conn.headers["X-MSTR-ProjectID"],
                                           hier["id"],
                                           hier["subtype"],
                                           hier["name"],
                                           att["id"],
                                           att["name"],
                              af["id"],af["name"],af["dataType"]])
                    all_hier_att_l.append(ahl_l)

        return pd.DataFrame(all_hier_att_l, columns=all_hier_att_col)

    def read_out_sys_hier(self, conn):

        proj_hier_l = i_md_searches.search_for_type_l(conn=conn, obj_l=["14"],
                                                      name="System Hierarchy",
                                                      info_level="base")
        print("WWWW " + str(proj_hier_l ))
        sys_hier_att_df = self.read_out_hier_att_df(conn=conn
                                                , proj_hier_l=proj_hier_l)
        return sys_hier_att_df

    def read_out_hier_df(self, conn, proj_hier_l=None):
        #reads out hierarchies of a project or provided in a list
        #proj_hier_l = None
        if proj_hier_l==None:

            proj_hier_l = i_md_searches.search_for_type_l(conn=conn, obj_l=["14"],
                                                      name="System Hierarchy")

        hier_att_df = self.read_out_hier_att_df(conn=conn
                                                         , proj_hier_l=proj_hier_l)

        return hier_att_df

class read_table_def():

    def __init__(self):
        self.columns = ["project_id", "table_id", "logical_table_name", "physical_table_name", "column_id", "column_name"]
        self.run_prop_d={}

    def get_tbl_col(self, base_url, conn, table_id):
        inst_u = f"{base_url}/api/model/tables/{table_id}"
        cols = conn.get(inst_u).json()
        tabl_name=cols["information"]["name"]
        tblrow, prjtblcol = [], []


        for col in cols["physicalTable"]["columns"]:
            all_col_d = self.run_prop_d
            all_col_d["project_id"] = conn.headers["X-MSTR-ProjectID"]
            all_col_d["table_id"] =table_id
            all_col_d["table_name"] = tabl_name
            all_col_d["physicalTable_id"] =cols["physicalTable"]["information"]["objectId"]
            all_col_d["physicalTable_name"] = cols["physicalTable"]["information"]["name"]
            all_col_d["column_id"] = col["information"]["objectId"]
            #print(col["information"]["name"])
            all_col_d["column_name"] = col["information"]["name"]
            all_col_d["column_versionId"] = col["information"]["versionId"]
            all_col_d["column_dateModified"] = col["information"]["dateModified"]
            all_col_d["column_dateCreated"] = col["information"]["dateCreated"]
            all_col_d["column_subType"] = col["information"]["subType"]
            all_col_d["column_dataType"] = col["dataType"]["type"]
            all_col_d["column_precision"] = col["dataType"]["precision"]
            all_col_d["column_scale"] = col["dataType"]["scale"]
            prjtblcol.append(all_col_d.copy())

        return prjtblcol

    def runreadout(self,conn,*args,**kwargs):
        #define target table for data upload
        #do the readout
        tbl_id_l=[]
        all_tables_l= i_mstr_api.get_prj_tbl(conn).json()["tables"]
        #read out all project tables
        for t in all_tables_l:
            tbl_id_l.append({"prj":conn.headers["X-MSTR-ProjectID"],"tbl_id":t["information"]["objectId"]})
        tbl_col_l=[]
        #read out columns of tables
        for t in tbl_id_l:
            t_cols_l=self.get_tbl_col(base_url=conn.base_url,conn=conn,
                                           table_id=t["tbl_id"])
            tbl_col_l.extend(t_cols_l.copy())

        return tbl_col_l

class io_facts():
    def __init__(self):
        self.i_read_fact=facts.read_fact
        self.run_prop_d={}
    def read_fact_exp(self,conn,fact_id_l):

        all_fact_maps_l = []
        for fact_id in fact_id_l:
            fact=self.i_read_fact(connection=conn, id=fact_id,show_expression_as="tokens").json()
            # print(facts)
            all_fact_maps_d = self.run_prop_d
            all_fact_maps_d["project_id"] = conn.headers["X-MSTR-ProjectID"]
            all_fact_maps_d["fact_id"] = fact["id"]
            all_fact_maps_d["fact_name"] = fact["name"]
            all_fact_maps_d["fact_versionId"] = fact["versionId"]
            all_fact_maps_d["fact_dateCreated"] = fact["dateCreated"]
            all_fact_maps_d["fact_subType"] = fact["subType"]
            all_fact_maps_d["fact_dataType"] = fact["dataType"]["type"]
            all_fact_maps_d["fact_precision"] = fact["dataType"]["precision"]
            all_fact_maps_d["fact_scale"] = fact["dataType"]["scale"]
            for f in fact["expressions"]:
                all_fact_maps_d["fact_expressionId"] = f["expressionId"]
                all_fact_maps_d["fact_expression_text"] = f["expression"]["text"]
                for c in f["expression"]["tokens"]:
                    if "target" in list(c.keys()) and c["type"] == "column_reference":
                        all_fact_maps_d["column_id"] = c["target"]["objectId"]
                        all_fact_maps_d["column_versionId"] = c["target"]["versionId"]
                        all_fact_maps_d["column_dateCreated"] = c["target"]["dateCreated"]
                        all_fact_maps_d["column_dateModified"] = c["target"]["dateModified"]
                        all_fact_maps_d["column_name"] = c["target"]["name"]
                        for table in f["tables"]:
                            all_fact_maps_d["table_id"] = table["objectId"]
                            all_fact_maps_d["table_subType"] = table["subType"]
                            all_fact_maps_l.append(all_fact_maps_d.copy())

        return all_fact_maps_l

class io_attributes():

    def __init__(self):
        self.i_read_att=attributes.get_attribute
        self.run_prop_d={}
    def read_att_form_exp(self,conn,att_id_l,*args,**kwargs):
        all_att_maps_d = self.run_prop_d.copy()
        all_att_maps_l = []

        for att_id in att_id_l:
            #print(att_id)
            att=self.i_read_att(connection=conn,id=att_id,show_expression_as="tokens").json()
            key_form_l = self.get_att_key_form_l(att)
            for form in att["forms"]:
                if "isFormGroup" not in list(form.keys()):
                    all_att_maps_d["project_id"] = conn.headers["X-MSTR-ProjectID"]
                    all_att_maps_d["attribute_id"] = att_id
                    all_att_maps_d["attribute_name"] =att["name"]
                    all_att_maps_d["form_id"] = form["id"]
                    all_att_maps_d["form_name"] = form["name"]
                    #all_att_maps_d["description"] = form["description"]
                    all_att_maps_d["form_category"] = form["category"]
                    all_att_maps_d["form_type"] = form["type"]
                    all_att_maps_d["displayFormat"] = form["displayFormat"]
                    all_att_maps_d["form_dataType"] = form["dataType"]["type"]
                    all_att_maps_d["REST_form_type"] = i_prompts.get_exp_prp_data_type(baseFormType=form["dataType"]["type"])
                    all_att_maps_d["form_precision"] = form["dataType"]["precision"]
                    all_att_maps_d["form_scale"] = form["dataType"]["scale"]

                    if form["id"] in key_form_l:
                        all_att_maps_d["key_form"]=True
                    else:
                        all_att_maps_d["key_form"]=False

                    all_att_maps_d["display_form_nr"]=0
                    display_form_nr=1
                    for rd_form in att["displays"]["reportDisplays"]:
                        if form["id"] == rd_form["id"]:
                            all_att_maps_d["display_form_nr"]=display_form_nr
                        else:
                            display_form_nr+=1


                    all_att_maps_d["browse_form_nr"] = 0
                    browse_form_nr = 1
                    for bd_form in att["displays"]["browseDisplays"]:
                        if form["id"] == bd_form["id"]:
                            all_att_maps_d["browse_form_nr"] = browse_form_nr
                        else:
                            browse_form_nr += 1

                    for e in form["expressions"]:
                        # print(e)
                        all_att_maps_d["form_expressionId"] = e["expressionId"]
                        all_att_maps_d["form_expressionText"] = e["expression"]["text"]
                        # all_att_maps_d["expression_text"]=e["expression"]["text"]
                        for tok in e["expression"]["tokens"]:
                            if "target" in list(tok.keys()):
                                if "column" == tok["target"]["subType"]:
                                    all_att_maps_d["exp_value"] = tok["value"]
                                    all_att_maps_d["column_id"] = tok["target"]["objectId"]
                                    all_att_maps_d["column_name"] = tok["target"]["name"]
                                    all_att_maps_d["column_versionId"] = tok["target"]["versionId"]
                                    all_att_maps_d["column_dateCreated"] = tok["target"]["dateCreated"]
                                    all_att_maps_d["column_dateModified"] = tok["target"]["dateModified"]
                                    all_att_maps_l.append(all_att_maps_d.copy())
                                    for table in e["tables"]:
                                        all_att_maps_d["table_id"] = table["objectId"]
                                        all_att_maps_d["table_subType"] = table["subType"]
                                        all_att_maps_l.append(all_att_maps_d.copy())

        return all_att_maps_l

    def get_att_key_form_l(self,att):
        key_form_l = []
        try:
            PK_form_id = att["keyForm"]["id"]
            for form in att["forms"]:
                # print(form)
                if PK_form_id == form["id"]:
                    if "childForms" in form.keys():
                        for child in form["childForms"]:
                            key_form_l.append(form["id"])
        except Exception as err:
            print(err)
        return key_form_l

class read_schema():

    def __init__(self):
        self.io_attributes=io_attributes()
        self.io_facts = io_facts()
        self.read_table_def=read_table_def()

    def set_run_prop_d(self, run_prop_d):
        self.io_attributes.run_prop_d=run_prop_d
        self.io_facts.run_prop_d=run_prop_d
        self.read_table_def.run_prop_d=run_prop_d

    def table_mappings(self,conn,*args,**kwargs):

        att_l=i_md_searches.search_for_type_l(conn,obj_l=["12"] )
        att_id_l=i_md_searches._exclude_derrived_att(att_l)

        fact_l=i_md_searches.search_for_type_l(conn,obj_l=["13"] )
        fact_id_l=i_msic.get_key_form_dict_l(dict_l=fact_l)

        att_form_exp_l=self.io_attributes.read_att_form_exp(conn=conn,att_id_l=att_id_l)
        att_form_exp_df=pd.DataFrame.from_dict(att_form_exp_l)

        fact_exp_l=self.io_facts.read_fact_exp(conn=conn,fact_id_l=fact_id_l)
        fact_exp_df = pd.DataFrame.from_dict(fact_exp_l)

        table_read_out_l = self.read_table_def.runreadout(conn=conn, run_prop_d={})
        table_df = pd.DataFrame.from_dict(table_read_out_l)

        tbl_att_fct_df = pd.merge(table_df[["project_id","table_id", "column_id","physicalTable_id","physicalTable_name"]], att_form_exp_df[["project_id", 'table_id', "column_id", "attribute_id", "form_expressionId"]], on=['project_id', 'table_id', 'column_id'], how='left')
        i_df_helper.clean_double_col(df=tbl_att_fct_df)

        tbl_att_fct_df = pd.merge(tbl_att_fct_df, fact_exp_df[["project_id", "column_id","fact_id","fact_expressionId"]], on=['project_id', 'column_id'], how='left')
        i_df_helper.clean_double_col(df=tbl_att_fct_df)

        tbl_att_fct_df["phys_tbl_col_id"] = tbl_att_fct_df["physicalTable_id"] + "_" + tbl_att_fct_df["column_id"]

        schema_df_d={"tbl_att_fct_df":tbl_att_fct_df,"att_form_exp_df":att_form_exp_df, "fact_exp_df":fact_exp_df, "table_df":table_df}

        return schema_df_d

class read_gen():

    def chk_by_obj_type(self,conn,obj_type_l,mtdi_id=None,*args,**kwargs):

        obj_l=i_md_searches.search_for_type_l(conn=conn,obj_l=obj_type_l)

        obj_l =i_md_searches.search_for_used_in_obj_direct(conn=conn, obj_l=obj_l, dpn_fg=True,
                                       *args, **kwargs)


        obj_depn_df=pd.DataFrame.from_dict(obj_l)
        return obj_depn_df

    def get_obj_def(self,conn, object_id, obj_type=None, obj_sub_type=None):
        obj_def = {}
        print(obj_type)
        if str(obj_type) == "12":

            obj_def = attributes.get_attribute(connection=conn, id=object_id, show_expression_as="tokens").json()

        elif str(obj_type) == "13":

            obj_def = facts.read_fact(connection=conn, id=object_id, show_expression_as="tokens").json()

        elif str(obj_type) == "14":

            obj_def = user_hierarchies.get_user_hierarchy(connection=conn, id=object_id).json()

        elif str(obj_type) == "15":

            obj_def = tables.get_table(connection=conn, id=object_id).json()

        elif str(obj_sub_type) == "256":

            obj_def = filters.get_filter(connection=conn, id=object_id, show_expression_as="tokens").json()

        elif str(obj_type) == "4":

            obj_def = metrics.get_metric(connection=conn, id=object_id, show_expression_as="tokens").json()

        elif str(obj_sub_type) == "257":

            obj_def = i_mstr_api.get_custom_group(conn=conn, custom_group_id=object_id, show_expression_as="tokens")

        elif str(obj_type) == "47":

            obj_def = i_mstr_api.get_consolidation(conn=conn, consolidation_id=object_id)

        elif str(obj_type) == "10":

            obj_def = i_mstr_api.get_prompt_def(conn=conn, prompt_id=object_id)

        elif str(obj_sub_type) == "14081":

            obj_def = i_mstr_api.get_dossier_def(conn=conn, dossier_id=object_id)


        elif str(obj_sub_type) in ["768", "769", "770", "771", "772", "773", "774", "775", "777", "778"]:
            # REPORT_GRID = 768
            # REPORT_GRAPH = 769
            # REPORT_ENGINE = 770
            # REPORT_TEXT = 771
            # REPORT_DATAMART = 772
            # REPORT_BASE = 773
            # REPORT_GRID_AND_GRAPH = 774
            # REPORT_NON_INTERACTIVE = 775
            # INCREMENTAL_REFRESH_REPORT = 777
            # REPORT_TRANSACTION = 778

            obj_def = i_mstr_api.get_report_all_def(conn=conn, report_id=object_id)

        elif str(obj_sub_type) in ["776", "779", "780"]:
            # OLAP_CUBE = 776
            # SUPER_CUBE = 779
            # SUBER_CUBE_IRR = 780

            obj_def = cubes.cube_definition(connection=conn, id=object_id)

        else:
            print("Element not foundasasas")
            print("object_id: " + str(object_id))
            print("obj_type: " + str(obj_type))
            print("obj_sub_type: " + str(obj_sub_type))
        return obj_def

"""
    def _chk_by_obj_type(self,conn,obj_type_l,cube_to_loop_d=None,mtdi_id=None,*args,**kwargs):

        obj_l=i_md_searches.run_search(conn=conn
                                            ,obj_l=obj_type_l
                                            ,input_type ="type_l",
                                            *args,**kwargs)


        #obj_l = i_md_searches.bld_objType_l(obj_l)
        obj_l = i_md_searches.run_depn_search(conn=conn
                                              , obj_l=obj_l
                                              #, obj_l= ['4C05177011D3E877C000B3B2D86C964F;4', 'C6B60AF341D38FF8DC6F3AA8D11D3115;55']
                                              , input_type="used_in_obj_direct"
                                              ,cube_to_loop_d=cube_to_loop_d
                                              ,mtdi_id=mtdi_id
                                              ,*args, **kwargs)

        obj_depn_df=pd.DataFrame.from_dict(obj_l)
        return obj_depn_df
"""
class read_prompts():

    def __init__(self):
        self.prompt_def_l = []
        self.prompt_type_l = []
        self.prompt_obj_l = []
        self.prompt_ele_l = []
        self.prompt_keys_l = []
        self.prp_search_obj_l = []
        self.run_prop_d={}
    def get_project_prp(self,conn):
        all_prompt_rep = i_md_searches.search_by_type(conn=conn, obj_type_l=["10"])
        prp_id_l = i_msic.get_key_form_dict_l(dict_l=all_prompt_rep.json())
        all_proj_prp_rep_l = self.get_prompt_def(conn=conn, prp_id_l=prp_id_l)
        return all_proj_prp_rep_l

    def get_prompt_def(self, conn, prp_id_l):
        prompt_info_l=[]
        for prompt_id in prp_id_l:
            prompt_info_l.append(i_mstr_api.get_prompt_def(conn=conn,prompt_id=prompt_id))
        return prompt_info_l

    def read_obj_prp_def(self,conn,prompt_def_d):
        p_obj_row_d = {}
        p_obj_row_d["project_id"] = conn.project_id
        p_obj_row_d["prompt_id"] = prompt_def_d['information']["objectId"]
        p_obj_row_d["prp_subType"] = prompt_def_d['information']["subType"]

        if "predefinedObjects" in prompt_def_d["question"].keys():
            for obj in prompt_def_d["question"]["predefinedObjects"]:
                #print(obj)
                p_obj_row_d["object_id"] = obj["objectId"]
                p_obj_row_d["obj_type"] = obj["subType"]
                p_obj_row_d["obj_prp_ans"] = i_prompts.bld_obj_prp_json( object_id=obj["objectId"],
                                                                         object_type=obj["subType"])
                p_obj_row_d["key"] = obj["name"]

                self.prompt_obj_l.append(p_obj_row_d.copy())

        else:
            # print(prompt_def_d)
            p_obj_row_d["object_id"] = prompt_def_d["question"]["search"]["objectId"]
            p_obj_row_d["prp_subType"] = prompt_def_d["question"]["search"]["subType"]
            self.prompt_obj_l.append(p_obj_row_d.copy())
            self.prp_search_obj_l.append({"id": p_obj_row_d["prompt_id"], "type": 10}.copy())

    def _add_att_to_obj_l(self,conn,p_row_d):
        p_obj_row_d={}
        p_obj_row_d["project_id"] = conn.project_id
        p_obj_row_d["prompt_id"]=p_row_d["prompt_id"]
        p_obj_row_d["prp_subType"] = p_row_d["prp_subType"]
        p_obj_row_d["object_id"]   = p_row_d["object_id"]
        p_obj_row_d["obj_type"]    = "attribute"
        p_obj_row_d["obj_prp_ans"] = i_prompts.bld_obj_prp_json(object_id=p_row_d["object_id"],
                                                                object_type="attribute")
        p_obj_row_d["key"]         = p_row_d["key"]
        self.prompt_obj_l.append(p_obj_row_d.copy())


    def read_ele_prp_def(self,conn,prompt_def_d):
        p_row_d={}
        p_row_d["project_id"] = conn.project_id
        p_row_d["prompt_id"] = prompt_def_d['information']["objectId"]
        p_row_d["prp_subType"] = prompt_def_d['information']["subType"]
        p_row_d["object_id"] = prompt_def_d['question']["attribute"]["objectId"]
        p_row_d["key"] = prompt_def_d['question']["attribute"]["name"]
        p_row_d["listAllElements"] = prompt_def_d['question']["listAllElements"]

        self._add_att_to_obj_l(conn,p_row_d)

        if "filter" in prompt_def_d["question"].keys():
            p_row_d["filter_id"] = prompt_def_d['question']["filter"]["objectId"]
        else:
            p_row_d["filter_id"] = None

        self.prompt_ele_l.append(p_row_d.copy())

    def obj_prp_search_dpn(self,conn, prp_idType_l):
        #print(prp_idType_l)
        prp_obj_rel_l = []
        prp_rep_l = i_md_searches.search_for_used_in_obj_direct(conn=conn, obj_l=prp_idType_l, count_only_fg=False)
        prp_id_l=i_msic.get_key_form_dict_l(prp_idType_l)
        for dpn in prp_rep_l:
            # print(prp["id"])
            if dpn["dpn_type"]=="3":
                if dpn["id"] in prp_id_l:
                    try:
                        # print(prp["dpn_id"])
                        instance_id = i_rep.open_Instance(conn=conn, report_id=dpn["dpn_id"])
                        prp_l = i_mstr_api.get_prp_ans(conn=conn, report_id=dpn["dpn_id"], instance_id=instance_id,
                                                       prompt_id=dpn["id"])
                        for obj in prp_l:
                            prp_obj_rel_d = {}
                            prp_obj_rel_d["project_id"] = conn.project_id
                            prp_obj_rel_d["prompt_id"] = dpn["id"]
                            prp_obj_rel_d["object_id"] = obj["id"]
                            prp_obj_rel_d["obj_type"]  = obj["type"]
                            prp_obj_rel_d["obj_prp_ans"] = i_prompts.bld_obj_prp_json(object_id=obj["id"],
                                                                                    object_type=obj["type"])
                            prp_obj_rel_d["key"] = obj["name"]
                            prp_obj_rel_l.append(prp_obj_rel_d.copy())

                        prp_id_l.remove(dpn["id"])
                    except Exception as err:
                        print("ERRR")
                        print(err)
                        print(dpn)
                        print("RRRRRRRR")

        return prp_obj_rel_l

    def run_read_out_prp_def(self, conn,prompts_l=None):
        self.prompt_def_l = []
        self.prompt_obj_l = []
        self.prompt_exp_l = []
        self.prompt_ele_l = []
        self.prp_search_obj_l = []
        self.prp_search_exp_l=[]

        if prompts_l ==None:
            prompts_l = i_md_searches.search_for_type_l(conn=conn, obj_l=10)

        for p in prompts_l:

            prompt_def_d = i_mstr_api.get_prompt_def(conn, prompt_id=p["id"])
            p_row_d = {}

            if "information" in prompt_def_d.keys():
                p_row_d["project_id"] = conn.project_id
                p_row_d["prompt_id"] = prompt_def_d['information']["objectId"]
                p_row_d["version_id"] = prompt_def_d['information']["versionId"]
                p_row_d["prp_subType"] = prompt_def_d['information']["subType"]
                p_row_d["promt_name"] = prompt_def_d['information']["name"]
                p_row_d["date_created"] = prompt_def_d['information']["dateCreated"]
                p_row_d["date_modified"] = prompt_def_d['information']["dateModified"]
                p_row_d["primaryLocale"] = prompt_def_d['information']["primaryLocale"]
                self.prompt_def_l.append(p_row_d.copy())

                if "prompt_objects" == prompt_def_d['information']["subType"]:
                    self.read_obj_prp_def(conn,prompt_def_d)

                if "prompt_expression" == prompt_def_d['information']["subType"]:
                    self.read_obj_prp_def(conn,prompt_def_d)

                if "prompt_elements" == prompt_def_d['information']["subType"]:
                    self.read_ele_prp_def(conn,prompt_def_d)

                if "prompt_date" == prompt_def_d['information']["subType"]:
                    pass

                if "prompt_big_decimal" == prompt_def_d['information']["subType"]:
                    pass
                if "prompt_double" == prompt_def_d['information']["subType"]:
                    pass

                if "prompt_string" == prompt_def_d['information']["subType"]:
                    pass

        # p=i_msic.rem_dbl_in_l(list_l=prompt_type_l)
        # unique_list_of_keys = list(set(tuple(keys) for keys in prompt_keys_l))
        if len(self.prp_search_obj_l)>0:
            #here we add objects found be searches used in prompts
            prompt_search_l = self.obj_prp_search_dpn(conn=conn, prp_idType_l=self.prp_search_obj_l)
            self.prompt_obj_l.extend(prompt_search_l)


        return {"prompt_def_l": self.prompt_def_l,"prompt_ele_l": self.prompt_ele_l
                ,"prompt_obj_l": self.prompt_obj_l,"prompt_exp_l": self.prp_search_exp_l}

    def add_prp_def_to_cube(self,conn,cube_name="prompt_def",cube_id=None,cube_folder_id=None,force=False):

        prompt_def_df = pd.DataFrame(self.prompt_def_l)
        prompt_ele_df = pd.DataFrame(self.prompt_ele_l)
        prompt_obj_df = pd.DataFrame(self.prompt_obj_l)


        prompt_d_l = [{"df": prompt_ele_df, "tbl_name": "prompt_ele_df", "update_policy": "Replace"}]
        prompt_d_l.extend({"df": prompt_obj_df, "tbl_name": "prompt_obj_df", "update_policy": "Replace"})
        prompt_d_l.extend({"df": prompt_def_df, "tbl_name": "prompt_def_df", "update_policy": "Replace"})



        i_cube.upload_cube_mult_table(conn=conn, tbl_upd_dict=prompt_d_l, mtdi_id=cube_id
                                      , cube_name=cube_name, folder_id=cube_folder_id, force=force)


class read_report():

    def __init__(self):
        self.run_prop_d={}
    def read_grid_metrics(self,conn,obj, row_col_fg, row_col_nr):
        metrics_l = []
        for met in obj["elements"]:
            met_d = self.run_prop_d.copy()
            met_d["project_id"] = conn.project_id
            met_d["object_id"] = met["id"]
            met_d["object_name"] = met["name"]
            met_d["type"] = met["type"]
            met_d["row_col_fg"] = row_col_fg
            met_d["row_col_nr"] = row_col_nr
            met_d["form_id"] = ""
            met_d["form_name"] = ""
            metrics_l.append(met_d.copy())
            row_col_nr +=1
        return metrics_l

    def read_grid_others(self,conn,obj, row_col_fg, row_col_nr):
        obj_d = self.run_prop_d.copy()
        obj_d["project_id"] = conn.project_id
        obj_d["object_id"] = obj["id"]
        obj_d["object_name"] = obj["name"]
        obj_d["type"] = obj["type"]
        obj_d["row_col_fg"] = row_col_fg
        obj_d["row_col_nr"] = row_col_nr
        obj_d["form_id"] = ""
        obj_d["form_name"] = ""
        return obj_d



    def read_grid_attribute(self,conn, obj, row_col_fg, row_col_nr):
        att_l=[]
        att_d = self.run_prop_d.copy()
        att_d["project_id"] = conn.project_id
        att_d["object_id"] = obj["id"]
        att_d["object_name"] = obj["name"]
        att_d["type"] = obj["type"]
        att_d["row_col_fg"] = row_col_fg
        att_d["row_col_nr"] = row_col_nr
        for form in obj["forms"]:
            att_d["form_id"] = form["id"]
            att_d["form_name"] = form["name"]
            att_l.append(att_d.copy())
        return att_l

       #return {"vis_att_l": vis_att_l, "vis_met_l": vis_met_l}


    def read_out_grid(self,conn, grid_definition):
        grid_cont_d={}
        grid_met_l=[]
        grid_att_l = []
        grid_obj_l = []
        row_nr = 1
        col_nr = 1
        if "rows" in grid_definition:

            for row in grid_definition["rows"]:
                if row["type"] == "templateMetrics":
                    grid_met_l=self.read_grid_metrics(conn=conn,obj=row, row_col_fg="row", row_col_nr=row_nr)
                elif row["type"] == "attribute":
                    grid_att_l.extend(self.read_grid_attribute(conn=conn,obj=row, row_col_fg="row", row_col_nr=row_nr))

                else:
                    grid_obj_l.append((self.read_grid_others(conn=conn,obj=row, row_col_fg="row", row_col_nr=row_nr)))

            row_nr+=1
        if "columns" in grid_definition:

            for col in grid_definition["columns"]:
                if col["type"] == "templateMetrics":
                    grid_met_l=self.read_grid_metrics(conn=conn,obj=col, row_col_fg="col", row_col_nr=col_nr)
                elif col["type"] == "attribute":
                    grid_att_l.extend(self.read_grid_attribute(conn=conn,obj=col,row_col_fg="col", row_col_nr=col_nr))
                else:
                    grid_obj_l.append((self.read_grid_others(conn=conn, obj=col, row_col_fg="col", row_col_nr=col_nr)))

            col_nr += 1


        grid_obj_l.extend(grid_met_l)
        grid_obj_l.extend(grid_att_l)
        grid_obj_l.extend(grid_obj_l)
        return grid_obj_l


    def read_avail_obj(self,conn,avail_obj):
        rep_d = {}
        rep_att_l = []
        rep_met_l = []
        rep_obj_l = []

        for k in avail_obj.keys():
            for obj in avail_obj[k]:
                if k == "attributes":
                    rep_att_l.extend(self.read_rep_attribute(conn=conn,att=obj))
                elif k == "metrics":
                    rep_met_l = self.read_rep_metrics(conn=conn,met_l=avail_obj[k])
                else:
                    rep_obj_l.append(self.read_rep_others(conn=conn,obj=obj))

        #rep_obj_d = {"rep_att_l": rep_att_l, "rep_met_l": rep_met_l, "rep_obj_l": rep_obj_l}

        rep_obj_l.extend(rep_att_l)
        rep_obj_l.extend(rep_met_l)
        rep_obj_l.extend(rep_obj_l)

        return rep_obj_l


    def read_rep_attribute(self,conn,att):
        att_l=[]
        att_d = self.run_prop_d.copy()
        att_d["object_id"] = att["id"]
        att_d["object_name"] = att["name"]
        att_d["type"] = att["type"]
        for form in att["forms"]:
            att_d["form_id"] = form["id"]
            att_d["form_name"] = form["name"]
            att_l.append(att_d.copy())
        return att_l

    def read_rep_metrics(self,conn,met_l):
        metric_l = []
        for met in met_l:
            met_d = self.run_prop_d.copy()
            met_d["object_id"] = met["id"]
            met_d["object_name"] = met["name"]
            met_d["type"] = met["type"]
            met_d["form_id"] = ""
            met_d["form_name"] = ""
            metric_l.append(met_d)
        return metric_l


    def read_rep_others(self,conn,obj):
        obj_d = self.run_prop_d.copy()
        obj_d["project_id"] = conn.project_id
        obj_d["object_id"] = obj["id"]
        obj_d["object_name"] = obj["name"]
        obj_d["type"] = obj["type"]
        obj_d["form_id"] = ""
        obj_d["form_name"] = ""
        return obj_d


    def read_grid_pageBy(self,conn,pageBy_l):
        rep_page_l = []
        page_nr=0
        for att in pageBy_l:
            #rep_page_l.append(self.read_rep_attribute(conn=conn,att=att))
            rep_page_l.extend(self.read_grid_attribute(conn, obj=att, row_col_fg="pageBy", row_col_nr=page_nr))
            page_nr +=1

        return rep_page_l

    def read_rep_quick_filt(self,conn,report_id,show_advanced_properties="false"):
        rep_def=i_rep.get_report_all_def(conn=conn,report_id=report_id,show_advanced_properties=show_advanced_properties)
        filter_text=""
        report_limit_text=""
        try:
            filter_text=str(rep_def["dataSource"]["filter"]["text"])
        except:
            pass
        if "dataTemplate" in rep_def["dataSource"].keys():
            for u in rep_def["dataSource"]["dataTemplate"]['units']:
                if u["type"]=="metrics":
                    try:
                        report_limit_text=str(u["limit"]["text"])
                    except:
                        pass

        rep_head_d=i_mstr_global.get_object_info_d(conn=conn,object_id=report_id,type="3")
        #rep_head_d.rename(columns={'id': 'report_id'}, inplace=True)
        rep_head_d.pop("project_id")
        rep_head_d.pop("id")
        rep_head_d = {**self.run_prop_d, **rep_head_d}
        rep_head_d["report_limit_text"]=report_limit_text
        rep_head_d["filter_text"]=filter_text

        return rep_head_d


    def read_reports(self,conn,report_id_l,load_d={}):
        rep_head_l=[]
        rep_avail_obj_l=[]
        rep_grid_obj_l=[]
        for report_id in report_id_l:
            load_d["project_id"]=conn.project_id
            load_d["report_id"]=report_id
            self.run_prop_d=load_d.copy()
            rep_def=i_rep.get_report_def(conn=conn,report_id=report_id).json()
            grid_obj_d=self.read_out_grid(conn=conn,grid_definition=rep_def["definition"]["grid"])
            pageBy_obj_d=self.read_grid_pageBy(conn=conn,pageBy_l=rep_def["definition"]["grid"]["pageBy"])
            avail_obj_d=self.read_avail_obj(conn=conn,avail_obj=rep_def["definition"]["availableObjects"])

            rep_grid_obj_l.extend(grid_obj_d)
            rep_grid_obj_l.extend(pageBy_obj_d)
            rep_avail_obj_l.extend(avail_obj_d)
            rep_head_d=self.read_rep_quick_filt(conn=conn,report_id=report_id,show_advanced_properties="false")
            rep_head_l.append(rep_head_d)

        rep_def_df= self.bld_rep_def_df(rep_head_l,rep_grid_obj_l=rep_grid_obj_l,rep_avail_obj_l=rep_avail_obj_l)

        return rep_def_df

    def bld_rep_def_df(self,rep_head_l, rep_grid_obj_l, rep_avail_obj_l):

        rep_head_df = pd.DataFrame(rep_head_l)
        rep_grid_obj_df = pd.DataFrame(rep_grid_obj_l)
        rep_avail_obj_df = pd.DataFrame(rep_avail_obj_l)
        rep_head_df.rename(columns={'type': 'rep_type','type_bez': 'rep_type_bez',
                                    'subtype': 'rep_subtype'}, inplace=True)

        rep_avail_obj_df.rename(columns={'type': 'obj_type'}, inplace=True)

        rep_def_df = pd.merge(rep_head_df[['project_id', 'report_id', 'version', 'name', 'path_name', 'path_ids',
                                           'rep_type', 'rep_type_bez', 'rep_subtype', 'owner_id', 'owner_name', 'date_modified',
                                           'date_created', 'report_limit_text', 'filter_text']],
                              rep_avail_obj_df[['project_id', 'report_id', 'object_id', 'object_name', 'obj_type',
                                                'form_id', 'form_name']],
                              left_on=['project_id', 'report_id'], right_on=['project_id', 'report_id'], how='inner')

        rep_def_df = i_df_helper.clean_double_col(df=rep_def_df)
        rep_def_df.reset_index()

        rep_def_df = pd.merge(rep_def_df[['project_id', 'report_id', 'version', 'name', 'path_name', 'path_ids',
                                          'rep_type', 'rep_type_bez', 'rep_subtype', 'owner_id', 'owner_name',
                                          'date_modified', 'date_created', 'report_limit_text', 'filter_text',
                                          'object_id', 'object_name','obj_type', 'form_id', 'form_name']],
                              rep_grid_obj_df[['project_id', 'report_id', 'object_id', 'object_name',
                                               'row_col_fg', 'row_col_nr', 'form_id', 'form_name']],
                              left_on=['project_id', 'report_id', 'object_id', 'form_id'],
                              right_on=['project_id', 'report_id', 'object_id', 'form_id'], how='left')
        rep_def_df = i_df_helper.clean_double_col(df=rep_def_df)
        return rep_def_df