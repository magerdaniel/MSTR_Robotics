import read_out_md as read
import pandas as pd
import openpyxl
import md_compare

run_type="check"
base_url="https://mstr.dwapplications.com:8443/MicroStrategyLibrary/api/"

env_project_l=[{"env":"prod","base_url":"https://mstr.dwapplications.com:8443/MicroStrategyLibrary/api/",
                "projects":["A10C8E64CE4FD26ED37D93BB7A1B4928"]}]
cube_project_id="5B30C5AB11E9C111DD5D0080EFA5420F"
username='hacker_admin'
password='h4cker@!'


conn_cube_load = read.get_conn(base_url, username=username, password=password, project_id=cube_project_id)
#daniels folder in tutorial
folder_id = '75028E138C4733B6C4DFE9964AE0F125'

#read out projects
if run_type=="read":
    cube_name = 'test_1'
    updatePolicy="upsert"
    mtdi_id = "E94516ED3C4E8F8BFB0DE297C0CC85C2"
    #mtdi_id=None
    #open a new connection (mstrio)
    conn=read.get_conn(base_url, username=username, password=password, project_id=cube_project_id)
    #call read out function
    read.runreadout_loop(conn=conn,conn_cube_load=conn_cube_load, cube_project_id=cube_project_id, cube_name=cube_name, folder_id=folder_id,
                         updatePolicy=updatePolicy,env_project_l=env_project_l,mtdi_id=mtdi_id)

if run_type=="check":
    comp=md_compare
    update_policy="Replace"
    read_mtdi_id = "E94516ED3C4E8F8BFB0DE297C0CC85C2"

    diff_d=comp.get_cube_data(conn=conn_cube_load,project_id=cube_project_id,mtdi_id=read_mtdi_id)
    cube_name = 'compare_diff'

    write_cube_mtdi_id="52E6738D3E4F0C33B1B5FCA41039E731"
    write_cube_mtdi_id=None
    write_cube_mtdi_id=comp.upload_data(conn=conn_cube_load,
                     project_id=cube_project_id,
                     mtdi_id=write_cube_mtdi_id,
                     tbl_upd_dict=diff_d,
                     cube_name=cube_name,
                     folder_id=folder_id,
                     update_policy=update_policy)
    print("cube " +write_cube_mtdi_id + " loaded")
    pass