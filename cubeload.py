import requests
import base64
import pandas as pd
import json


# just remember to change push_data() function: <json=json_data> to <data=json_data>
class load_cube:

    def login(base_url, api_login, api_password):
        print("Getting token...")
        data_get = {'username': api_login,
                    'password': api_password,
                    'loginMode': 1}
        r = requests.post(base_url + 'auth/login', data=data_get)
        if r.ok:
            authToken = r.headers['X-MSTR-AuthToken']
            cookies = dict(r.cookies)
            print("\nToken: " + authToken)
            return authToken, cookies
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    def set_headers(self):
        headers = {'X-MSTR-AuthToken': self.authToken,
                   'Content-Type': 'application/json',  # IMPORTANT!
                   'Accept': 'application/json',
                   'X-MSTR-ProjectID': self.project_id}
        return headers


    def create_cube_mtdi(self, mtdi_structure_json):
        print("Creating new cube...")
        inst_u = f"{self.conn.base_url}/api/datasets/models"
        r = self.conn.post(inst_u,data=mtdi_structure_json)
        ## r = requests.post(self.base_url + "datasets/models", headers=self.conn.session.headers, data=json.dumps(mtdi_structure_json), cookies=self.cookies)
        if r.ok:
            print("\nCube CREATED successfully")
            print("Cube ID:     " + r.json()['id'])
            print("Cube Name:   " + r.json()['name'])
            print("HTTP Status Code: " + str(r.status_code) + "    ||    Error: " + str(r.raise_for_status()))
            print("Remember to copy and note down Cube ID (dataset ID). Enter this value in 'Parameters' section")
            return r.json()['id']
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    def upload_session(self, mtdiguid, mtdi_upload_session_body):
        upload_session_url = self.base_url + "/api/datasets/" + mtdiguid + "/uploadSessions"
        r=self.conn.post(upload_session_url, data=mtdi_upload_session_body)
        #r = requests.post(upload_session_url, headers=self.headers, data=mtdi_upload_session_body, cookies=self.cookies)
        print("Creating new upload session...")
        if r.ok:
            uploadSession = r.json()['uploadSessionId']
            print("\nUpload Session ID:     " + uploadSession)
            print("HTTP Status Code: " + str(r.status_code) + "    ||    Error: " + str(r.raise_for_status()))
            return uploadSession
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    def push_data(self, mtdi_id, uploadSession, json_data):
        push_url = self.conn.base_url + "/api/datasets/" + mtdi_id + "/uploadSessions/" + uploadSession
        r = self.conn.put(push_url, json=json_data)
        print("Pushing data...")
        if r.ok:
            print("\nData PUSHED successfully...")
            print("HTTP Status Code: " + str(r.status_code) + "    ||    Error: " + str(r.raise_for_status()))
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    def publish_cube(self,mtdi_id, UploadSession):
        publish_url = self.base_url + "/api/datasets/" + mtdi_id + "/uploadSessions/" + UploadSession + "/publish"
        r = self.conn.post(publish_url)
        print("Publishing cube...")
        if r.ok:
            print("\nCube PUBLISHED successfully...")
            print("HTTP Status Code: " + str(r.status_code) + "    ||    Error: " + str(r.raise_for_status()))
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))

    def precbeld(self,tblcol=None):
        df = pd.DataFrame(tblcol, columns=["project_guid","table_guid","logical_table_name","physical_table_name","column_guid","column_name"])
        tblcol_data_json = df.to_json(orient='values', date_format='iso')
        tblcol_encoded = base64.b64encode(bytes(tblcol_data_json.__str__(), 'utf-8')).decode('ascii')
        json_data1 = {"tableName": "all_mppd_tbl_col", "index": 1, "data": tblcol_encoded}
        return json_data1

    def setsessvar(self,conn):
        self.authToken = conn.headers["X-MSTR-AuthToken"]
        self.base_url=conn.base_url
        self.headers=conn.headers
        self.conn=conn

    def main(self,mtdi_structure_json,mtdi_upload_session_body,json_data1):

        mtdi_id = self.create_cube_mtdi(mtdi_structure_json)
        UploadSession = self.upload_session(mtdi_id,mtdi_upload_session_body)
        self.push_data(mtdi_id, UploadSession, json_data1)
        self.publish_cube(mtdi_id, UploadSession)
