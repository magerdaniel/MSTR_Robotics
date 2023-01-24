import mig_create_short_cut

mig = mig_create_short_cut.open_conn()
mig_rep= mig_create_short_cut.get_change_log()
mig_search= mig_create_short_cut.searches()
mig_short_cut= mig_create_short_cut.bld_short_cuts()
cube_it= mig_create_short_cut.cubes()
username = "Administrator"
password ="Administrator"
server="85.214.60.83"
port="8080"



cube_upload_param={}
# conn_det=None
chg_log_from_date = "2023-01-14"
chg_log_to_date = "2023-01-22"
short_cut_proj_id = "B19DEDCC11D4E0EFC000EB9495D0F44F" #E
short_cut_folder_id = "E9E592074122ADC61CF2AFA10941EFBD"
migration_folder_id = "E586B4F645D1C3ADC066A2B93804FF0A"
mtdi_id = ""  # "E37952C94F2195BF15BF6B953C84AD58"

chg_log_rep_proj_id = "B19DEDCC11D4E0EFC000EB9495D0F44F"
chg_log_report_id = "454B29DF4872B9C74785A5A0A1A5FFB0"
chg_log_from_date_prompt_id = "EB9094EC49C2D048FD9765B59E96CA31"
chg_log_to_date_prompt_id = "16C4D8A0477EE34A0ACC88AD6F90ECC8"
chg_log_proj_prompt_id = "35F616224B5A80B5FDCA6BA77BC799F9"
chg_log_obj_prompt_id = "F21128A94659B9EC5CA6EEA1653B1680"

base_url= "http://" + server + ":" + port + "/MicroStrategyLibrary/api"

cube_name = "TestCube_ds"

conn_det = {"username": username, "password": password,
            "base_url": "http://"+ server + ":" +port+"/MicroStrategyLibrary/api" }

change_log_report={"chg_log_rep_proj_id":chg_log_rep_proj_id
                   ,"chg_log_report_id":chg_log_report_id
                   ,"chg_log_from_date_prompt_id":chg_log_from_date_prompt_id
                   ,"chg_log_to_date_prompt_id":chg_log_to_date_prompt_id
                   ,"chg_log_proj_prompt_id":chg_log_proj_prompt_id
                   , "migration_folder_id": migration_folder_id
                   , "chg_log_obj_prompt_id": chg_log_obj_prompt_id
                   }

change_query={"chg_log_from_date":chg_log_from_date
              ,"chg_log_to_date":chg_log_to_date
              ,"short_cut_proj_id":short_cut_proj_id
              ,"short_cut_folder_id": short_cut_folder_id
              ,"mtdi_id":None
              ,"cube_name":"TestCube"}


conn=mig.login(base_url=base_url,username=username,password=password,project_id=chg_log_rep_proj_id)
""" 
res=mig_rep.chk_chg_log_cols(conn=conn,report_id="454B29DF4872B9C74785A5A0A1A5FFB0")
print(res)

#pass over the parameters
mig_rep.set_md_rep_params(conn=conn,change_query=change_query,change_log_report=change_log_report)
#get the changed objetcs from change logs
#for a project and timeframe
ch_df=mig_rep.get_mig_obj_logs(conn=conn,proect_id=short_cut_proj_id,from_date=chg_log_from_date,
                               to_date=chg_log_to_date)

#pd_df=mig_rep.get_mig_obj_logs(conn=conn, proect_id=, from_date=chg_log_from_date,to_date=)
#reads out objects from pandas data frame
#df_obj=mig_rep.obj_id_from_df_l(pd_df,obj_col_ind=7,type_col_ind=10)

#create short cuts for change logs at level of user & object

short_cut_df= mig_rep.bld_change_log_shortcut_df(conn=conn,change_log_df=ch_df)
df_key_cols_d={"object_id":"OBJECT_ID",
                           "object_type":"OBJECT_TYPE",
                           "object_name":"NEW_OBJECT_NAME",
                           "object_desc":"NEW_OBJECT_DESC"}
mig_short_cut.run_short_cut_build(conn=conn, project_id=change_query["short_cut_proj_id"],
                                  short_cut_df=short_cut_df,folder_id=change_query["short_cut_folder_id"],df_key_cols_d=df_key_cols_d)
"""
#mig_short_cut.run_short_cut_build(conn=conn, project_id=change_query["short_cut_proj_id"],
#                                  short_cut_df=short_cut_df,folder_id=change_query["short_cut_folder_id"],short_cut_col=short_cut_col)

#export pandas change list to cube
#pls note, that the structure of the cube
#is defined by creating the cube
#the cube
"""
cube_upload_param["load_df"]=ch_df
cube_upload_param["updatePolicy"]="REPLACE" # Replace will delete and refresh content, will "ADD" will add your new objects to the cube
cube_upload_param["cube_name"]="Change_log_cube"
cube_upload_param["tbl_name"] ="changes"
cube_upload_param["migration_folder_id"]=change_log_report["migration_folder_id"]
#when you run the script the first time, you'll recive die GUID of the cube.
#By puting this number here you can reload the cube and build dossiers on top of it
cube_upload_param["mtdi_id"]=None

cube_it.up_load_to_cube(conn=conn,cube_upload_param=cube_upload_param)
"""
#

obj_id_l=['335FFA9640B5F1C1E0C0F3A469E627A8;55','59D59D7741F61B68480572B9B7A05709;4','C6B60AF341D38FF8DC6F3AA8D11D3115;55']
#obj_id_l=['335FFA9640B5F1C1E0C0F3A469E627A8;55','59D59D7741F61B68480572B9B7A05709;4','C6B60AF341D38FF8DC6F3AA8D11D3115;55'] #Dossier & metric "hire date"
used_obj_dict_l=mig_search.used_by_obj_l_rec(conn=conn,project_id=change_log_report["chg_log_rep_proj_id"]
                          ,obj_id_l=obj_id_l)
#export search result to cube
search_df=mig_search.bld_df_from_search_result(conn=conn,search_l=used_obj_dict_l)


cube_upload_param["load_df"]=search_df
cube_upload_param["updatePolicy"]="REPLACE" # Replace will delete and refresh content, will "ADD" will add your new objects to the cube
cube_upload_param["cube_name"]="Object_dependencies"
cube_upload_param["tbl_name"] ="obj"
cube_upload_param["migration_folder_id"]=change_log_report["migration_folder_id"]
#when you run the script the first time, you'll recive die GUID of the cube.
#By puting this number here you can reload the cube and build dossiers on top of it
cube_upload_param["mtdi_id"]=None
#conn.headers["X-MSTR-ProjectID"] = chg_log_rep_proj_id
cube_it.up_load_to_cube(conn=conn,cube_upload_param=cube_upload_param)
print(conn)

