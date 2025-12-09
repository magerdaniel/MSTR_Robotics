import pandas as pd
import json
from mstrio.project_objects.datasets import super_cube
from mstrio.connection import Connection

with open('D:\\shared_drive\\Python\\mstr_robotics\\mstr_robotics\\user_d.json', 'r') as openfile:
    user_d = json.load(openfile)
username = user_d["username"]
password =user_d["password"]
server="85.214.60.83"
port="8080"
project_id="B7CA92F04B9FAE8D941C3E9B7E0CD754" #Turtorial
folder_id="D3C7D461F69C4610AA6BAA5EF51F4125" #\Public Objects\Reports
cube_name="mtdi_cube"
data_d_l=[{"stay":"hotel","drink":"beer"},
{"stay":"house","drink":"wine"}]



cube_upload_param={}
base_url= "http://" + server + ":" + port + "/MicroStrategyLibrary/api"

conn = Connection(base_url=base_url, project_id=project_id,username=username,password=password)
conn.headers['Content-type'] = "application/json"
conn.select_project(project_id)

def cube_upload(conn, load_df, tbl_name, updatePolicy="REPLACE",
                folder_id=None, cube_name=None,mtdi_id=None,force=False):
    if mtdi_id == None or mtdi_id =="":
        ds = super_cube.SuperCube(connection=conn, name=cube_name)
        ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
        ds.create(folder_id=folder_id,force=force)
    else:
        ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
        ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
        ds.update()
    return ds.id


data_df=pd.DataFrame.from_dict(data_d_l)
cube_upload(conn, load_df=data_df, tbl_name="data_df", updatePolicy="REPLACE",
                folder_id=folder_id, cube_name=cube_name,mtdi_id=None,force=True)