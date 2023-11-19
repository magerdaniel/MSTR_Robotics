from mstr_robotics._helper import msic
from mstr_robotics.mstr_pandas import df_helper
from mstr_robotics.mstr_classes import mstr_global,md_searches
from mstr_robotics.report import cube
from mstr_robotics._connectors import mstr_api
from mstrio.api import facts
from mstrio.api import attributes
import pandas as pd

i_mstr_global=mstr_global()
i_md_searches=md_searches()
i_df_helper=df_helper()
i_msic=msic()
i_cube=cube()
i_mstr_api=mstr_api()

class read_out_hierarchy():

    def _read_hier_in_prp(self, conn):
        #reads out hierarchies used in prompts
        #currently not used
        all_proj_prp_rep_l=prompts().get_project_prp(conn=conn)
        proj_prp_hier_l = []
        for p in all_proj_prp_rep_l:
            try:
                if p["expressionType"] == "hierarchy":
                    for hier in p["question"]["predefinedObjects"]:
                        proj_prp_hier_l.append({"project_id": conn.headers["X-MSTR-ProjectID"],
                                                "prompt_id": p["information"]["objectId"],
                                                "object_id": hier["objectId"],
                                                "subtype": hier["subType"],
                                                "object_name": hier["name"]
                                                }
                                               )
            except:
                print()

        return proj_prp_hier_l

    def read_out_hier_att_df(self, conn, proj_hier_l) :
        all_hier_att_col=["project_id","hier_id","hier_subType","hier_name","att_id","att_name",
                          "att_form_id","att_form_name","att_form_data_type"]
        all_hier_att_l=[]
        for hier in proj_hier_l:

            hier_att_l=i_mstr_api.get_hier_att(conn=conn,
                                         hier_id=hier["id"]
                                                    )

            for att in hier_att_l[0]["attributes"]:
                for af in att["forms"]:
                    all_hier_att_l.append([conn.headers["X-MSTR-ProjectID"],
                                           hier["id"],
                                           hier["subtype"],
                                           hier["name"],
                                           att["id"],
                                           att["name"],
                              af["id"],af["name"],af["dataType"]])

        return pd.DataFrame(all_hier_att_l, columns=all_hier_att_col)

    def read_out_sys_hier(self, conn):

        proj_hier_l = i_md_searches.search_for_type_l(conn=conn, obj_l=["14"],
                                                      name="System Hierarchy")
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

    def get_tbl_col(self, base_url, conn, table_id, run_prop_d):
        inst_u = f"{base_url}/api/model/tables/{table_id}"
        cols = conn.get(inst_u).json()
        tabl_name=cols["information"]["name"]
        tblrow, prjtblcol = [], []
        all_call_d= {}

        for col in cols["physicalTable"]["columns"]:

            all_call_d["project_id"] = conn.headers["X-MSTR-ProjectID"]
            all_call_d["table_id"] =table_id
            all_call_d["table_name"] = tabl_name
            all_call_d["physicalTable_id"] =cols["physicalTable"]["information"]["objectId"]
            all_call_d["physicalTable_name"] = cols["physicalTable"]["information"]["name"]
            all_call_d["column_id"] = col["information"]["objectId"]
            #print(col["information"]["name"])
            all_call_d["column_name"] = col["information"]["name"]
            all_call_d["column_versionId"] = col["information"]["versionId"]
            all_call_d["column_dateModified"] = col["information"]["dateModified"]
            all_call_d["column_dateCreated"] = col["information"]["dateCreated"]
            all_call_d["column_subType"] = col["information"]["subType"]
            all_call_d["column_dataType"] = col["dataType"]["type"]
            all_call_d["column_precision"] = col["dataType"]["precision"]
            all_call_d["column_scale"] = col["dataType"]["scale"]
            prjtblcol.append(all_call_d.copy())

        return prjtblcol

    def get_prj_tbl(self,conn):
        inst_u = f"{conn.base_url}/api/model/tables"
        r = conn.get(inst_u)
        return r

    def runreadout(self,conn,run_prop_d={},*args,**kwargs):
        #define target table for data upload
        tbl_name = "all_mppd_tbl_col"
        #initialize column order for pandas dataframe
        #load_col_ord_l=list(run_prop_d.keys())


        #do the readout
        tbl_id_l=[]
        all_tables_l= self.get_prj_tbl(conn).json()["tables"]
        #read out all project tables
        for t in all_tables_l:
            tbl_id_l.append({"prj":conn.headers["X-MSTR-ProjectID"],"tbl_id":t["information"]["objectId"]})
        tbl_col_l=[]
        #read out columns of tables
        for t in tbl_id_l:
            t_cols_l=self.get_tbl_col(base_url=conn.base_url,conn=conn,
                                           table_id=t["tbl_id"],run_prop_d=run_prop_d)
            tbl_col_l.extend(t_cols_l.copy())

        return tbl_col_l

class io_facts():
    def __init__(self):
        self.i_read_fact=facts.read_fact

    def read_fact_exp(self,conn,fact_id_l):
        all_fact_maps_d = {}
        all_fact_maps_l = []
        for fact_id in fact_id_l:
            fact=self.i_read_fact(connection=conn, id=fact_id,show_expression_as="tokens").json()
            # print(facts)
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

                        all_fact_maps_l.append(all_fact_maps_d.copy())

        return all_fact_maps_l

class io_attributes():

    def __init__(self):
        self.i_read_att=attributes.get_attribute

    def read_att_form_exp(self,conn,att_id_l,*args,**kwargs):
        all_att_maps_d = {}
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
                    all_att_maps_d["form_precision"] = form["dataType"]["precision"]
                    all_att_maps_d["form_scale"] = form["dataType"]["scale"]

                    if form["id"] in key_form_l:
                        all_att_maps_d["key_form"]=True
                    else:
                        all_att_maps_d["key_form"]=False
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

    def table_mappings(self,conn,run_prop_d,*args,**kwargs):
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
        schema_df_d={"tbl_att_fct_df":tbl_att_fct_df,"att_form_exp_df":att_form_exp_df, "fact_exp_df":fact_exp_df, "table_df":table_df}
        return schema_df_d

class chk_usage():

    def chk_by_obj_type(self,conn,obj_type_l,cube_to_loop_d=None,mtdi_id=None,*args,**kwargs):

        obj_l=i_md_searches.search_for_type_l(conn=conn,obj_l=obj_type_l)

        obj_l =i_md_searches.search_for_used_in_obj_direct(conn=conn, obj_l=obj_l, dpn_fg=True,
                                      cube_to_loop_d=cube_to_loop_d, mtdi_id=mtdi_id,*args, **kwargs)


        obj_depn_df=pd.DataFrame.from_dict(obj_l)
        return obj_depn_df

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
class prompts():

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