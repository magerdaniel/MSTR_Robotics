from mstrio import connection
from datetime import datetime
import cubeload
import pandas as pd
import openpyxl
import json

##mtdi_structure_json = '{"name": "MD_ANALYTICS_CUBE","description": "My first time","folderId": "09988C274CEA09ABB1DE66B511521CCF","tables": [{"name": "all_mppd_tbl_col","columnHeaders": [{"name": "project_guid","dataType": "STRING"},{"name": "table_guid","dataType": "STRING"},{"name": "logical_table_name","dataType": "STRING"},{"name": "physical_table_name","dataType": "STRING"},{"name": "column_guid","dataType": "STRING"},{"name": "column_name","dataType": "STRING"}]}],"attributes": [{"name": "project_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "project_guid"}]}]},{"name": "table_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "table_guid"}]}]},{"name": "logical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "logical_table_name"}]}]},{"name": "physical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "physical_table_name"}]}]},{"name": "column_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_guid"}]}]},{"name": "column_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_name"}]}]}]}'
##mtdi_upload_session_body ='{"tables": [{"name": "all_mppd_tbl_col","updatePolicy": "REPLACE","orientation": "ROW"}]}'
#Neu
columns = ["project_guid", "table_guid", "logical_table_name", "physical_table_name", "column_guid", "column_name"]
def get_tbl_col(base_url,conn, prj_id, tbl_guid,load_ts):
    inst_u = f"{base_url}/api/model/tables/{tbl_guid}"
    headers = {'X-MSTR-ProjectID': prj_id}
    cols = conn.get(inst_u)
    tblrow,prjtblcol = [],[]
    for c in cols.json()["physicalTable"]["columns"]:
        tblrow=[]
        tblrow.append(load_ts)
        tblrow.append(prj_id)
        tblrow.append(cols.json()["information"]["objectId"])
        tblrow.append(cols.json()["information"]["name"])
        tblrow.append(cols.json()["physicalTable"]["information"]["name"])
        tblrow.append(c["information"]["objectId"])
        tblrow.append(c["information"]["name"])
        prjtblcol.append(tblrow)
    return prjtblcol

def get_prj_tbl(conn):
    inst_u = f"{conn.base_url}/api/model/tables"
    r = conn.get(inst_u)
    return r

def runreadout(conn=None,chkprj_l=None,fld_id=None,updatePolicy="REPLACE",mtdi_id=None,cube_name=None):
    cube_load = cubeload.load_cube(conn)
    #define globals
    load_ts= datetime.now()

    #define target table for data upload
    tbl_name = "all_mppd_tbl_col1"

    #do the readout
    tbl=[]
    for p in chkprj_l:
        conn.project_id=p
        r= get_prj_tbl(conn)
        for t in r.json()["tables"]:
            tbl.append({"prj":p,"tbl_guid":t["information"]["objectId"]})
    tblcol=[]
    for t in tbl:
        #don't forget to add your globals
        tblcol.extend(get_tbl_col(conn.base_url,conn,t["prj"],t["tbl_guid"],load_ts))
    #finish read out

    #create cubee if not proided
    if not mtdi_id:
        p_cbe_def = pd.read_excel("TBL_COL_CUBE.xlsx", sheet_name="MyCube")
        #create cube
        mtdi_id=cube_load.create_cube(p_cbe_def=p_cbe_def, cube_name=cube_name, fld_id=fld_id)
        #the first upload must use Replace
        cube_load.trigger_upload(conn=conn, mtdi_id=mtdi_id, tbl_name=tbl_name, tbl_data=tblcol,
                                 updatePolicy="REPLACE")
    else:
        #upload the data to existing cube
        cube_load.trigger_upload(conn=conn, mtdi_id=mtdi_id,
                                 tbl_name=tbl_name,tbl_data=tblcol,
                                 updatePolicy=updatePolicy)

def get_conn(base_url=None, username=None,password=None,project_id=None):
    conn= connection.Connection(base_url=base_url,username=username,
                                password=password,project_id=project_id)
    conn.headers['Content-type']="application/json"
    return conn



