from mstrio import connection
import cubeload
import md_analytics_conf
import json

##mtdi_structure_json = '{"name": "MD_ANALYTICS_CUBE","description": "My first time","folderId": "09988C274CEA09ABB1DE66B511521CCF","tables": [{"name": "all_mppd_tbl_col","columnHeaders": [{"name": "project_guid","dataType": "STRING"},{"name": "table_guid","dataType": "STRING"},{"name": "logical_table_name","dataType": "STRING"},{"name": "physical_table_name","dataType": "STRING"},{"name": "column_guid","dataType": "STRING"},{"name": "column_name","dataType": "STRING"}]}],"attributes": [{"name": "project_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "project_guid"}]}]},{"name": "table_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "table_guid"}]}]},{"name": "logical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "logical_table_name"}]}]},{"name": "physical_table_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "physical_table_name"}]}]},{"name": "column_guid","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_guid"}]}]},{"name": "column_name","attributeForms": [{"category": "ID","expressions": [{"tableName": "all_mppd_tbl_col","columnName": "column_name"}]}]}]}'
##mtdi_upload_session_body ='{"tables": [{"name": "all_mppd_tbl_col","updatePolicy": "REPLACE","orientation": "ROW"}]}'

def get_tbl_col(base_url,conn, prj_id, tbl_guid):
    inst_u = f"{base_url}/api/model/tables/{tbl_guid}"
    headers = {'X-MSTR-ProjectID': prj_id}
    cols = conn.get(inst_u)
    tblrow,prjtblcol = [],[]
    for c in cols.json()["physicalTable"]["columns"]:
        tblrow=[]
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

def runreadout(conn=None,chkprj_l=None,fld_id=None):
    mtdi_structure_json = md_analytics_conf.cbeloadmappedcol["mtdi_structure_json"]
    mtdi_structure_json = mtdi_structure_json.replace("xxx_folder_guid_xxx",fld_id)
    mtdi_upload_session_body = md_analytics_conf.cbeloadmappedcol["mtdi_upload_session_body"]
    cube=cubeload.load_cube()

    tbl=[]
    for p in chkprj_l:
        conn.project_id=p
        r= get_prj_tbl(conn)
        for t in r.json()["tables"]:
            tbl.append({"prj":p,"tbl_guid":t["information"]["objectId"]})
    tblcol=[]
    for t in tbl:
        tblcol.extend(get_tbl_col(conn.base_url,conn,t["prj"],t["tbl_guid"]))
    cube.setsessvar(conn)
    json_data1= cube.precbeld(tblcol=tblcol)
    cube.main(mtdi_structure_json,mtdi_upload_session_body,json_data1)

def get_conn(base_url=None, username=None,password=None,project_id=None):
    conn= connection.Connection(base_url=base_url,username=username,password=password,project_id=project_id)
    conn.headers['Content-type']="application/json"
    return conn

def main():
    base_url = 'http://85.214.60.83:8080/MicroStrategyLibrary/api/'
    conn = get_conn(base_url=base_url, username="Administrator", password="IBCS",
                    project_name="REGAM Testautomatisation")


##main()
"""
print(cols.json()["physicalTable"]["information"]["name"])
print(cols.json()["physicalTable"]["tableName"])
print(cols.json()["physicalTable"]["columns"])
print(cols.json()["physicalTable"]["columns"][0]["information"]["name"])
"""

