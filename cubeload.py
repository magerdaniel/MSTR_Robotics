import requests
import base64
import pandas as pd
import json
from mstrio.api import datasets


# just remember to change push_data() function: <json=json_data> to <data=json_data>

class cube_def:

    def get_cbe_tbl_col(self,conn=None,mtdi_id=None):
        ds=datasets
        col = ds.dataset_definition(connection=conn, id=mtdi_id, fields=("columns"))
        t_col={}
        if col.ok:
            for c in col.json()["result"]["definition"]["availableObjects"]["columns"]:
                t_col=self._add_to_t_col_(t_col=t_col,c=c)
        return t_col

    def _add_to_t_col_(self,t_col=None,c=None):
        col_def={}
        col_def["columnId"] = c["columnId"]
        col_def["columnName"] = c["columnName"]
        col_def["dataType"] = self._get_dataType_desc_(c["dataType"])
        for tc in t_col:
            if tc==c["tableName"]:
                t_col[c["tableName"]].append(col_def)
                return t_col

        t_col[c["tableName"]]=[]
        t_col[c["tableName"]].append(col_def)
        return t_col

    def _get_dataType_desc_(self,dataType):
        if dataType==33:
            return "STRING"

    def get_tbl_col_names(self, conn=None, mtdi_id=None, tbl_name=None):
        tbl_col_d=self.get_cbe_tbl_col(conn=conn,mtdi_id=mtdi_id)
        t_col_l=[]
        for c in tbl_col_d[tbl_name]:
            t_col_l.append(c["columnName"])
        return t_col_l

    def build_full_upld_sess_body(self,conn=None,mtdi_id=None,tbl_name=None,updatePolicy=None):
        cube_tbl_col = self.get_tbl_col_names(conn=conn,mtdi_id=mtdi_id,tbl_name=tbl_name)
        mtdi_upload_session_body = '{"tables": [{"name": "'+tbl_name+'","updatePolicy": "' + updatePolicy + '","orientation": "ROW","columnHeaders":[' + self._list_to_quote_str_(cube_tbl_col) + ']}]}'

        return mtdi_upload_session_body

    def _list_to_quote_str_(self,list_var):
        quote_str = ""
        for s in list_var:
            quote_str += '"' + s + '",'
        return quote_str[:-1]

class create_JSON_def:

    i_tbl = 0
    i_tblcol = 1
    i_coltyp = 2
    i_objtyp = 3
    i_obj = 4
    i_attform = 5
    i_attformtype=6

    def _addnwtbl_(self,t_str=None, t_row=None, t_lstrow=None):
        if t_row != t_lstrow:
            # add a new table into the cube
            t_str += '{"data":"e30=","name":"' + str(t_row) + '","columnHeaders":['
        return t_str

    def _addnwcol_(self,t_str=None, c_row=None, c_lstrow=None, cd_row=None):
        if c_row != c_lstrow:
            t_str += '{"name":"' + c_row + '","dataType":"' + cd_row + '"},'
        return t_str

    def _addatt_(self,a_str=None, att_row=None, att_lstrow=None):
        if att_row != att_lstrow:
            if a_str[-1] == ",":
                a_str = a_str[:-1] + "]}]},"
            a_str += '{"name":"' + att_row + '","attributeForms":['
        return a_str

    def _addm_(self,m_str=None, m_row=None, m_lstrow=None):
        if m_row != m_lstrow:
            if m_str[-1] == ",":
                m_str = m_str[:-1] + "]}]},"
            m_str += '{"name":"' + m_row + '","expressions":['
        return m_str

    def _addattfrom_(self,a_str=None, attfrom_row=None, attform_lstrow=None, attfromtype=None):
        if attfrom_row != attform_lstrow:
            if a_str[-1] == ",":
                a_str = a_str[:-1] + "]},"

            a_str += '{"name":"'+attfrom_row+'","category": "'+attfromtype+'","expressions":['
        return a_str

    def _setplast_(self,p_cbe_def=None):

        p_last = p_cbe_def.copy(deep=True)
        p_last.iat[0, self.i_tbl] = "zz67z_table_name"
        p_last.iat[0, self.i_tblcol] = "zz67z_column_name"
        p_last.iat[0, self.i_coltyp] = "zz67z_data_type"
        p_last.iat[0, self.i_objtyp] = "zz67z_object_type"
        p_last.iat[0, self.i_obj] = "zz67z_object_name"
        p_last.iat[0, self.i_attform] = "zz67z_attribute_form_name"
        p_last.iat[0,self.i_attformtype]="zz67z_attribute_form_nametype"

        return p_last.iloc[0]

    def cbe_tbl_def(self,p_cbe_def=None):
        tbl_col = p_cbe_def[["table_name", "column_name", "data_type"]]
        tbl_col_u = tbl_col.drop_duplicates()
        t_last = self._setplast_(p_cbe_def)
        t_str = '"tables":['
        for t in tbl_col_u.iterrows():
            # print(t[1])
            t_str = self._addnwtbl_(t_str, t[1][self.i_tbl], t_last[1][self.i_tbl])
            t_str = self._addnwcol_(t_str, t[1][self.i_tblcol], t_last[1][self.i_tblcol], t[1][self.i_coltyp])
            t_last = t

        return t_str[:-1] + ' ]}]'

    def cbe_m_def(self,p_cbe_def=None):
        obj = p_cbe_def[["table_name", "column_name", "data_type", "object_type", "object_name"]]
        m_sort = obj.sort_values(by=['object_type', 'object_name'])
        m_last = self._setplast_(p_cbe_def)
        m_str = '"metrics":['
        for m in m_sort.iterrows():
            if m[1][self.i_objtyp] == "metrics":
                m_str = self._addm_(m_str, m[1][self.i_obj], m_last[1][self.i_obj])
                m_str += '{"tableName":"' + m[1][self.i_tbl] + '","columnName":"' + m[1][self.i_tblcol] + '"},'
            m_last = m
        return m_str[:-1] + ' ]}]}]'

    def cbe_att_def(self,p_cbe_def=None):
        obj = p_cbe_def[["table_name", "column_name", "data_type", "object_type", "object_name", "attribute_form_name", "attribute_form_type"]]
        obj_sort = obj.sort_values(by=['object_type', 'object_name', 'attribute_form_name', "attribute_form_type"])
        a_last = self._setplast_(p_cbe_def)
        a_str = '"attributes":['
        for a in obj_sort.iterrows():
            if a[1][self.i_objtyp] == "attributes":
                a_str = self._addatt_(a_str, a[1][self.i_obj], a_last[1][self.i_obj])
                a_str = self._addattfrom_(a_str=a_str,attfrom_row= a[1][self.i_attform],attform_lstrow= a_last[1][self.i_attform],attfromtype=a[1][self.i_attformtype])
                a_str += '{"tableName":"' + a[1][self.i_tbl] + '","columnName":"' + a[1][self.i_tblcol] + '"},'
            a_last = a
        return a_str[:-1] + ' ]}]}]'


    def get_JSON(self,p_cbe_def=None,cube_name=None,fld_id=None):
        c_str = '{"name": "'+ cube_name+ '", "description": "Cube with categories data","folderId": "'+fld_id+'",'
        t_str = self.cbe_tbl_def(p_cbe_def)
        a_str = self.cbe_att_def(p_cbe_def)
        m_str = self.cbe_m_def(p_cbe_def)
        str = c_str + t_str

        if len(m_str) > 20:
            str +=',' + m_str
        if len(a_str) > 20:
            str +=',' + a_str

        return  str +'}'

class load_cube:

    def __init__(self,conn):
        self.authToken = conn.headers["X-MSTR-AuthToken"]
        self.base_url=conn.base_url
        conn.headers['Content-type']="application/json"
        self.conn=conn
        self.ds=datasets


    def create_cube_mtdi(self, mtdi_structure_json=None):
        #self.ds.create_dataset(connection=self.conn,body=mtdi_structure_json)
        r = self.conn.post(self.base_url + "/api/datasets/models", data=mtdi_structure_json)
        return r.json()['id']

    def upload_session(self, mtdiguid=None, mtdi_upload_session_body=None):
        #self.ds.upload_session(connection=self.conn,id=mtdiguid, body=mtdi_upload_session_body)

        upload_session_url = self.base_url + "/api/datasets/" + mtdiguid + "/uploadSessions"
        r=self.conn.post(upload_session_url, data=mtdi_upload_session_body)
        #r = requests.post(upload_session_url, headers=self.headers, data=mtdi_upload_session_body, cookies=self.cookies)
        print("Creating new upload session...")
        if r.ok:
            return r
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
            return r

    def push_data(self, mtdi_id=None, uploadSession=None, json_data=None):
        push_url=self.base_url+'/api/datasets/'+mtdi_id+ '/uploadSessions/'+uploadSession.json()['uploadSessionId']
        return self.conn.put(push_url,json=json_data)

    def publish_cube(self,mtdi_id=None, UploadSession=None):
        publish_url = self.base_url+"/api/datasets/" + mtdi_id + "/uploadSessions/" +UploadSession.json()['uploadSessionId'] + "/publish"
        return self.conn.post(publish_url)

    def prepare_data(self, tbl_data=None,tbl_name=None, columns=None):
        df = pd.DataFrame(tbl_data, columns=columns)
        tbl_data_json = df.to_json(orient='values', date_format='iso')
        tbl_data_encoded = base64.b64encode(bytes(tbl_data_json.__str__(), 'utf-8')).decode('ascii')
        json_data={}
        json_data["tableName"]=tbl_name
        json_data["index"]=1
        json_data["data"]=tbl_data_encoded
        return json_data

    def create_cube(self,p_cbe_def=None, cube_name=None, fld_id=None):
        mtdi_structure_json=create_JSON_def().get_JSON(p_cbe_def = p_cbe_def, cube_name = cube_name, fld_id = fld_id)
        mtdi_id = self.create_cube_mtdi(mtdi_structure_json)
        return mtdi_id

    def upload_data(self,mtdi_id=None,mtdi_upload_session_body=None,json_data=None):
        print("Loading cube: "+ mtdi_id)
        UploadSession = self.upload_session(mtdi_id,mtdi_upload_session_body)
        print("UploadSession is: " + str(UploadSession.json()))
        push_status=self.push_data(mtdi_id, UploadSession, json_data)
        if push_status.ok:
            print("data is uploaded")
        else:
            print("upload faild")
        publish_stauts=self.publish_cube(mtdi_id, UploadSession)
        if publish_stauts.ok:
            print("Cube is published")
        else:
            print("Cube is publishing failed")
        return

    def trigger_upload(self,conn=None, mtdi_id=None, tbl_name=None,tbl_data=None,updatePolicy=None):
        columns = cube_def().get_tbl_col_names(conn=conn, mtdi_id=mtdi_id, tbl_name=tbl_name)
        json_data=self.prepare_data(tbl_data=tbl_data,tbl_name=tbl_name, columns=columns)
        mtdi_upload_session_body=cube_def().build_full_upld_sess_body(conn=conn,mtdi_id=mtdi_id,tbl_name=tbl_name, updatePolicy=updatePolicy)
        self.upload_data(mtdi_id=mtdi_id, mtdi_upload_session_body=mtdi_upload_session_body, json_data=json_data)