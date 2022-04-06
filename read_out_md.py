from mstrio.connection import Connection
from mstrio.project_objects.datasets import SuperCube
from datetime import datetime
import cubeload
import pandas as pd
import openpyxl
import json
import numpy as np

##mtdi_structure_json = '{"name": "MD_ANALYTICS_CUBE","description": "My first time","folderId": "09988C274CEA09ABB1DE66B511521CCF","tables": [{"name": "all_mppd_tbl_col","columnHeaders": [{"name": "project_guid","dataType": "STRING"},{"name": "table_guid","dataType": "STRING"},{"name": "logical_table_name","dataType": "STRING"},{"name": "physical_table_name","dataType": "STRING"},{"name": "column_guid","dataType": "STRING"},{"name": "column_name","dataType": "STRING"}]}],"attributes": [{"name": "project_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "project_guid"}]}]},{"name": "table_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "table_guid"}]}]},{"name": "logical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "logical_table_name"}]}]},{"name": "physical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "physical_table_name"}]}]},{"name": "column_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_guid"}]}]},{"name": "column_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_name"}]}]}]}'
##mtdi_upload_session_body ='{"tables": [{"name": "all_mppd_tbl_col","updatePolicy": "REPLACE","orientation": "ROW"}]}'
#Neu
columns = ["project_guid", "table_guid", "logical_table_name", "physical_table_name", "column_guid", "column_name"]
def get_tbl_col(base_url,conn, prj_id, tbl_guid,load_vars):
    inst_u = f"{base_url}/api/model/tables/{tbl_guid}"
    headers = {'X-MSTR-ProjectID': prj_id}
    cols = conn.get(inst_u)
    tblrow,prjtblcol = [],[]

    if "facts" in cols.json():
        for f in cols.json()["facts"]:
            tblrow=_add_tbl_col_(cols=cols,load_vars=load_vars,prj_id=prj_id)
            #fact expression
            tblrow.append(f["expression"]["text"])
            #fact name
            tblrow.append(f["information"]["name"])
            #fact object ID
            tblrow.append(f["information"]["objectId"])
            #object type
            tblrow.append(f["information"]["subType"])
            prjtblcol.append(tblrow)

    if "attributes" in cols.json():
        for a in cols.json()["attributes"]:
            tblrow=_add_tbl_col_(cols=cols,load_vars=load_vars,prj_id=prj_id)
            #Attributeformexpression
            tblrow.append(a["forms"][0]["expression"]["text"])
            #Attribute Name
            tblrow.append(a["information"]["name"])
            #Attribute Name
            tblrow.append(a["information"]["objectId"])
            #Object type
            tblrow.append(a["information"]["subType"])
            prjtblcol.append(tblrow)

    return prjtblcol

def _add_tbl_col_(cols=None,load_vars=None,prj_id=None):
    tblrow = []
    tblrow.extend(list(load_vars.values()))
    tblrow.append(prj_id)
    tblrow.append(cols.json()["information"]["objectId"])
    tblrow.append(cols.json()["information"]["name"])
    t_name=cols.json()["physicalTable"]["information"]["name"]
    tblrow.append(cols.json()["physicalTable"]["information"]["name"])
    return tblrow

def get_prj_tbl(conn):
    inst_u = f"{conn.base_url}/api/model/tables"
    r = conn.get(inst_u)
    return r

def runreadout(conn=None,conn_cube_load=None,cube_project_id=None,cube_name=None,folder_id=None,
                    updatePolicy="REPLACE",mtdi_id=None,project=None,en_v="xxxxx"):
    cube_load = cubeload.load_cube(conn)
    #define target table for data upload
    tbl_name = "all_mppd_tbl_col"
    #
    load_col_ord_l=["env","base_url","username","user_id","load_ts"]
    #define globals
    load_vars={}
    load_vars["env"]=en_v
    load_vars["base_url"]=conn.base_url
    load_vars["username"]=conn.username
    load_vars["user_id"]=conn.user_id
    load_vars["load_ts"]= str(datetime.now().timestamp())
    #do the readout
    tbl=[]
    conn.project_id=project
    r= get_prj_tbl(conn)
    load_col_ord_l.append("project_guid")
    for t in r.json()["tables"]:
        #print(t["information"]["name"])
        tbl.append({"prj":project,"tbl_guid":t["information"]["objectId"]})
    tblcol=[]
    for t in tbl:
        #don't forget to add your globals
        #print(t["tbl_guid"])
        tblcol.extend(get_tbl_col(conn.base_url,conn,t["prj"],t["tbl_guid"],load_vars))
    #colums added by get_tbl_col
    load_col_ord_l.extend(["table_guid","logical_table_name","physical_table_name","expression","object_name","object_guid","object_type"])
    #np_tblcol=np.array(tblcol)
    load_df = pd.DataFrame(tblcol, columns=load_col_ord_l)
    if mtdi_id ==None:
        ds = SuperCube(connection=conn_cube_load, name=cube_name)
        ds.add_table(name=tbl_name, data_frame=load_df, update_policy="replace")
        ds.create(folder_id=folder_id)
        mtdi_id=ds.id
    else:
        ds = SuperCube(connection=conn_cube_load, id=mtdi_id)
        ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
        ds.update()
        mtdi_id=ds.id
    return ds.id

def runreadout_loop(conn=None,conn_cube_load=None,cube_project_id=None,cube_name=None,folder_id=None,
                    updatePolicy="REPLACE",mtdi_id=None,env_project_l=None):
    for en_v in env_project_l:
        for project in en_v["projects"]:
           conn.headers["X-MSTR-ProjectID"]=project
           mtdi_id=runreadout(conn=conn,conn_cube_load=conn_cube_load,mtdi_id=mtdi_id,cube_project_id=cube_project_id,folder_id=folder_id
                           , cube_name=cube_name,en_v=en_v["env"],project=project,updatePolicy=updatePolicy )


def get_conn(base_url=None, username=None,password=None,project_id=None):
    #in mstrio connenction the parameter ['Content-type'] is not set to "application/json"
    #thus we need our own conn to eensure all functions of this
    conn = Connection(base_url=base_url,username=username,
                                password=password,project_id=project_id)
    conn.headers['Content-type']="application/json"
    return conn