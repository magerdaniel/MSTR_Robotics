import json
import yaml
import sys
import os
from pathlib import Path
from mstr_robotics.report import cube
from mstr_robotics.prepare_AI_data import parse_json
from mstr_robotics.mstr_classes import mstr_global
#from mstr_robotics.json_checksum_handler import json_checksum_handler
from mstr_robotics.json_compare import JSONComparator,compare_mstr_objects
from mstr_robotics.read_out_prj_obj import read_gen
from mstr_robotics.redis_db import  redis_bi_analysis
from mstr_robotics._helper import msic
from mstr_robotics._connectors import mstr_api
from mstr_robotics.prepare_AI_data import export_mstr_md,mstr_to_json
from mstr_robotics.redis_db import redis_mstr_json
from mstr_robotics.redis_db import fetch_it_all

import pandas as pd


i_mstr_global=mstr_global()
i_msic=msic()
i_read_gen=read_gen()
i_parse_json=parse_json()
#i_json_checksum_handler=json_checksum_handler()
i_mstr_to_json=mstr_to_json()
i_mstr_api=mstr_api()
i_redis_mstr_json=redis_mstr_json()

i_compare_mstr_objects=compare_mstr_objects()
i_JSONComparator=JSONComparator()

ENV_DIR = Path(sys.prefix)
os.chdir(ENV_DIR)

with open("..\\config\\mstr_redis_y.yml", 'r') as openfile:
    mstr_redis_y = yaml.safe_load(openfile)

with open("..\\config\\user_d.json", 'r') as openfile:
    user_d = json.load(openfile)
conn_params =  user_d["conn_params"]

class run_compare():

    def __init__(self, conn, redis_config=None, selected_redis_env=None):
        """Initialize run_compare with MSTR connection and optional Redis configuration

        Args:
            conn: MSTR connection object
            redis_config: Optional Redis configuration dictionary. If None, loads from file.
            selected_redis_env: Optional Redis environment name (e.g., 'redis_dev', 'redis_test').
                               If None, defaults to 'redis_dev'.
        """
        # Load Redis configuration
        if redis_config is None:
            # Fallback: Load from file system (backward compatibility)
            with open("..\\config\\mstr_redis_y.yml", 'r') as openfile:
                mstr_redis_y = yaml.safe_load(openfile)
        else:
            # Use provided configuration
            mstr_redis_y = redis_config

        # Load user configuration (still from file)
        with open("..\\config\\user_d.json", 'r') as openfile:
            user_d = json.load(openfile)

        # Select Redis environment
        if selected_redis_env is None:
            selected_redis_env = "redis_dev"  # Default environment

        
        self.redis_con_d = mstr_redis_y["redis_env_d"][selected_redis_env]
        self.project_prefix = mstr_redis_y["project_prefix"]
        self.prefix_map = mstr_redis_y["prefix_map"]
        self.searches_used_in_prp_d_l = mstr_redis_y["searches_used_in_prp_d_l"]
        self.conn = conn
        print(self.redis_con_d)

        self.i_redis_bi_analysis = redis_bi_analysis(
                                host=self.redis_con_d["host"],
                                port=self.redis_con_d["port"],
                                password=self.redis_con_d["password"],
                                username=self.redis_con_d["username"],
                                decode_responses=self.redis_con_d["decode_responses"]
                                )

    def search_update(self,conn,search_id):
        all_obj_d_l={}
        search_result = i_mstr_api.run_mstr_search(conn=conn,
                            search_id=search_id)
        if search_result["totalItems"] > 0:
            all_obj_d_l=search_result["result"]
        return all_obj_d_l

    def shortcut_folder(self,conn,project_id,folder_id):
        conn.select_project(project_id)
        all_obj_d_l=i_mstr_global.get_obj_from_sh_fold(conn,folder_id=folder_id)
        return all_obj_d_l

    def update_searches(self,searches_used_in_prp_d_l):
        i_redis_mstr_json.run_searches_redis(conn=self.conn
                                            ,i_redis_bi_analysis=self.i_redis_bi_analysis
                                            ,prefix_map=self.prefix_map
                                            ,env_prefix=self.env_prefix
                                            ,searches_used_in_prp_d_l=searches_used_in_prp_d_l)   

    def run_comparison(self,play_compare_d):
        #set variables
        org_project_id=play_compare_d["org_project_id"]
        comp_project_id=play_compare_d["comp_project_id"]
        obj_list_type = list(play_compare_d["obj_list_type"].keys())[0]
        recursive_fg=play_compare_d["recursive_fg"] 

        org_prefix=self.project_prefix[org_project_id]
        comp_prefix=self.project_prefix[comp_project_id]

        all_obj_d_l=[]
        if play_compare_d["run_daily_update_fg"]:

            
            for pre in self.project_prefix:
                env_prefix=self.project_prefix[pre]
                self.conn.select_project(pre)
                update_obj_d_l=self.search_update(self.conn, search_id=play_compare_d["run_daily_update_search_id"])

                err_d_l=i_redis_mstr_json.save_obj_json_to_redis(i_redis_bi_analysis=self.i_redis_bi_analysis                                                    
                                                                    ,conn=self.conn
                                                                    ,prefix_map=self.prefix_map
                                                                    , all_obj_d_l=update_obj_d_l
                                                                    , env_prefix=env_prefix)
            

        self.conn.select_project(comp_project_id)
        if obj_list_type=="search_id":
            
            all_obj_d_l=self.search_update(self.conn, search_id=play_compare_d["obj_list_type"]["search_id"])
        elif obj_list_type=="shorcut_folder_id":
            all_obj_d_l=self.shortcut_folder(self.conn, self.project_prefix, play_compare_d["obj_list_type"]["shorcut_folder_id"])

        elif obj_list_type=="obj_d_l":
            all_obj_d_l=play_compare_d["obj_list_type"]["obj_d_l"]




        org_root_object_id_l=[]
        comp_root_object_id_l=[]

        for obj in all_obj_d_l:
            obj_id=obj["id"]
            self.conn.select_project(org_project_id)
            org_root_object_id_l.append(i_redis_mstr_json.bld_redis_key(self.conn,obj_id,org_prefix))
            self.conn.select_project(comp_project_id)
            comp_root_object_id_l.append(i_redis_mstr_json.bld_redis_key(self.conn,obj_id,comp_prefix))

        i_fetch_it_all=fetch_it_all(self.i_redis_bi_analysis)
        all_org_objects_df = pd.DataFrame(i_fetch_it_all.fetch_all_objects_recursively(root_object_l=org_root_object_id_l,recursive_fg=recursive_fg))
        all_comp_objects_df = pd.DataFrame(i_fetch_it_all.fetch_all_objects_recursively(root_object_l=comp_root_object_id_l,recursive_fg=recursive_fg))
        #print(all_org_objects_df.head(2))
        diff_d_l=i_compare_mstr_objects.compare_objects( all_org_objects_df, all_comp_objects_df)
        
        if len(diff_d_l) > 0:
            diff_d=pd.DataFrame(diff_d_l)
            diff_d["no_number_path"] = diff_d["path_list"].apply(i_JSONComparator.remove_bracket_numbers)

            for col in diff_d.columns:
                diff_d[col] = diff_d[col].astype('string')

            dict_list = diff_d.to_dict('records')
            diff_df=pd.DataFrame(dict_list)


            self.conn.select_project(org_project_id)
            org_obj_prop_d_l=i_mstr_api.get_proj_obj_by_id_l(conn=self.conn, obj_id_l=diff_df['org_obj_id'].tolist())
            org_obj_prop_df = pd.DataFrame(org_obj_prop_d_l)
            
            self.conn.select_project(comp_project_id)
            comp_obj_prop_d_l=i_mstr_api.get_proj_obj_by_id_l(conn=self.conn, obj_id_l=diff_df['comp_obj_id'].tolist())
            comp_obj_prop_df = pd.DataFrame(comp_obj_prop_d_l)
            df=pd.merge(diff_df,
                org_obj_prop_df,
                left_on=["org_obj_id"],
                right_on=["id"],
                how="inner"
        )   
            df=pd.merge(df,
                comp_obj_prop_df,
                left_on=["comp_obj_id"],
                right_on=["id"],
                how="inner"
                )

            print(df.columns)
            # Group by org_obj_key and aggregate both json_key_path and diff_type
            org_obj_diff_path_df = df.groupby(
                ['org_obj_key','comp_obj_id', 'id_x','id_y']).agg({
                'name_x': max,
                'obj_type_x': max,
                'subtype_x': max,
                'dateCreated_x': max,
                'dateModified_x': max,
                'version_x': max,
                'owner_x': max,
                'fold_path_x': max,
                'name_y': max,
                'obj_type_y': max,
                'subtype_y': max,
                'dateCreated_y': max,
                'dateModified_y': max,
                'version_y': max,
                'owner_y': max,
                'fold_path_y': max,
                'json_key_path': list,
                'no_number_path': list,
                'diff_type': list,
                }).reset_index()

            obj_d_l=[]

            for i,obj in org_obj_diff_path_df.iterrows():
                # Keep the full key including prefix (e.g., "mstr_dev:METRIC:12345")

                obj_d= {'org_obj_key': obj['org_obj_key'],
                        "org_obj_info":{
                        "id": obj['id_x'],
                        'name': obj['name_x'],
                        "obj_type": obj['obj_type_x'],
                        "subtype": obj['subtype_x'],
                        "dateCreated": obj['dateCreated_x'],
                        "dateModified": obj['dateModified_x'],
                        "version": obj['version_x'],
                        "owner": obj['owner_x'],
                        "fold_path": obj['fold_path_x']},
                        "comp_obj_info":{
                        "id": obj['id_y'],
                        'name': obj['name_y'],
                        "obj_type": obj['obj_type_y'],
                        "subtype": obj['subtype_y'],
                        "dateCreated": obj['dateCreated_y'],
                        "dateModified": obj['dateModified_y'],
                        "version": obj['version_y'],
                        "owner": obj['owner_y'],
                        "fold_path": obj['fold_path_y']},
                        "json_key_path": obj['json_key_path'],
                        'no_number_path': obj['no_number_path'],
                        'diff_types': obj['diff_type']
                        }  # Add diff_types list
                obj_d_l.append(obj_d.copy())
  

            return obj_d_l


if __name__ == "__main__":
    with open('..\\config\\First_comp_run.yml', 'r') as openfile:
        play_compare_y = yaml.safe_load(openfile)

    #run_comparison(conn, play_compare_y)
    
