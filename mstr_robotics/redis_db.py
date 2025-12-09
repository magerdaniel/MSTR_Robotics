from numpy import str_
from mstrio.api.browsing import get_objects_from_quick_search
from datetime import datetime
import redis
import json
import os
from mstr_robotics._helper import msic,str_func
from mstr_robotics.prepare_AI_data import mstr_to_json
from mstr_robotics._connectors import mstr_api
from mstr_robotics.read_out_prj_obj import read_gen
from mstr_robotics.json_compare import json_checksum_handler
from typing import List, Optional
from collections import deque
i_msic=msic()
i_str_func=str_func()
i_mstr_to_json=mstr_to_json()
i_mstr_api=mstr_api()
i_read_gen=read_gen()
i_json_checksum_handler=json_checksum_handler()

class redis_bi_analysis():
 
    def __init__(self, username, host: str = 'localhost', port: int = 14995, db: int = 0, 
                 password: Optional[str] = None, decode_responses: bool = True):
        """
        Initialize Redis connection
        
        Args:
            host: Redis server host (update with your server details)
            port: Redis server port (update with your server details) 
            db: Database number
            password: Redis password if required
            decode_responses: Whether to decode responses to strings
        """
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            decode_responses=decode_responses
        ) 

   

    def emergency_flush_db(self, confirm_phrase: str = None) -> dict:
        """
        EMERGENCY: Flush entire database (fastest but nuclear option)
        
        Args:
            confirm_phrase: Must type "FLUSH_ALL_DATA" to proceed
            
        Returns:
            Dictionary with results
        """
        print("JJJJJ")
        if confirm_phrase != "FLUSH_ALL_DATA":
            return {
                "status": "error",
                "message": "Must provide confirm_phrase='FLUSH_ALL_DATA' to proceed"
            }
        
        try:
            self.redis_client.flushdb()
            return {
                "status": "success",
                "message": "Database flushed successfully",
                "deleted_count": "all"
            }
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to flush database: {e}"
            }
    
    def fetch_key_list(self,key_l):
        key_val_d_l=[]
        try:
            for key in key_l:
                value = self.fetch_key_value(key)
                if len(value)>2:
                    key_val_d_l.append({"key": key, "value": value})
        except Exception as e:
            print(f"Error fetching keys: {e}")
        return key_val_d_l

    def scan_all_keys(self, pattern: str = "*", batch_size: int = 1000) -> List[str]:
        """
        Scan and return all keys matching pattern using SCAN (production-safe)
        
        Args:
            pattern: Key pattern to match (default: all keys)
            batch_size: Number of keys to scan per iteration
            
        Returns:
            List of all matching keys
        """
        all_keys = []
        cursor = 0
        
        print(f"🔍 Scanning for keys matching pattern: '{pattern}'")
        
        while True:
            cursor, keys = self.redis_client.scan(cursor=cursor, match=pattern, count=batch_size)
            all_keys.extend(keys)
            
            if cursor == 0:  # Scan complete
                break
                
            print(f"   Found {len(all_keys)} keys so far...")
        
        print(f" Scan complete. Found {len(all_keys)} total keys")
        return all_keys



    def upload_key_value(self, key: str, value, data_type: str = "json") -> dict:

        try:
            if data_type == "json":
                # Store as JSON (Redis 6.2+ with RedisJSON)
                self.redis_client.json().set(key, '$', value)

            if data_type == "set":
                # Store as set
                self.redis_client.sadd(key, *value)

            return {
                "status": "success",
                "message": f"Successfully uploaded key: {key}",
                "key": key,
                "data_type": data_type
            }
            
        except Exception as e:
            print(f"Error uploading key '{key}': {e}")
            return {
                "status": "error",
                "message": f"Failed to upload key '{key}': {e}",
                "key": key
            }

    def fetch_key_value(self, key: str, data_type: str = "json") -> dict:
        """
        Args:
            key: Redis key name to fetch
            data_type: Expected data type ("json", "string", "hash")
            
        Returns:
            Dictionary with fetch results and value
        """
        try:
            # Check if key exists
            if not self.redis_client.exists(key):
                return {
                    "status": "error",
                    "message": f"Key '{key}' does not exist",
                    "key": key,
                    "value": None
                }
            
            if data_type == "json":

                value = self.redis_client.json().get(key, '$')

                if isinstance(value, list) and len(value) > 0:
                    value = value[0]
            elif data_type == "string":

                raw_value = self.redis_client.get(key)
                try:
                    value = json.loads(raw_value)
                except json.JSONDecodeError:
                    value = raw_value  # Return as string if not valid JSON
            elif data_type == "hash":

                value = self.redis_client.hgetall(key)
            else:
                return {
                    "status": "error",
                    "message": f"Unsupported data_type: {data_type}",
                    "key": key,
                    "value": None
                }
            
            return {
                "status": "success",
                "message": f"Successfully fetched key: {key}",
                "key": key,
                "data_type": data_type,
                "value": value
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to fetch key '{key}': {e}",
                "key": key,
                "value": None
            }
        
    def bulk_upload_json_files(self,redis,folder_path, batch_size=1):
        error_files_l = []
        files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            #print(batch)
            pipe = redis.pipeline()
            try:
                for filename in batch:
                    
                    filepath = os.path.join(folder_path, filename)
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Store as JSON (Redis 6.2+) or serialized string
                    key = f"file:{filename.replace('.json', '')}"
                    pipe.json().set(key, '$', data)  # or pipe.set(key, json.dumps(data))
                
                pipe.execute()
                print(f"Uploaded batch {i//batch_size + 1}/{(len(files) + batch_size - 1)//batch_size}")
            except Exception as e:

                print(f"Error uploading batch {i//batch_size + 1}: {e}")
                error_files_l.append(key)
                # On error, move the file to the error folder
                error_folder = os.path.join(os.path.dirname(folder_path), "redis_upload_errors")
                os.makedirs(error_folder, exist_ok=True)
                
                for filename in batch:
                    try:
                        source_path = os.path.join(folder_path, filename)
                        dest_path = os.path.join(error_folder, filename)
                        if os.path.exists(source_path):
                            os.rename(source_path, dest_path)
                            print(f"Moved error file {filename} to {error_folder}")
                    except Exception as move_error:
                        print(f"Failed to move error file {filename}: {move_error}")



        return error_files_l
    
    def build_subtype_map(self):
        all_values_l=[]
        all_keys = self.scan_all_keys("*")
        for key in all_keys:
            result = self.fetch_key_value(key=key)
            if result['status'] == 'success':
                k_v={"r_key":key,"r_value":result['value']}
                all_values_l.append(k_v.copy() ) # This is the actual value
        subtype_l=[]
        for k in all_values_l:
            if "information" in k["r_value"].keys():
                d={"prefix":k["r_key"].split(":")[0],"c_subtype":k["r_value"]["information"]["subType"]}
                subtype_l.append(d.copy())
            else:
                #print(k.keys())
                if "subtype" in  k["r_value"].keys():
                    d={"prefix":k["r_key"].split(":")[0],"c_subtype":k["r_value"]["subtype"]}
                    subtype_l.append(d.copy())
                pass

        map_mstr_types_d={}
        for st in i_msic.rem_dbl_dict_in_l(subtype_l):

            map_mstr_types_d[st["c_subtype"]]=st["prefix"]
        return map_mstr_types_d
  

class fetch_it_all:

    def __init__(self, i_redis_bi_analysis):
        self.i_redis_bi_analysis=i_redis_bi_analysis

    def fetch_all_objects_recursively(self, root_object_l, recursive_fg=True, batch_size=100):
        """
        Optimized recursive fetch using Redis pipelining to reduce network round-trips
        
        Args:
            root_object_l: List of initial object keys to start with
            recursive_fg: Whether to recursively fetch child objects
            batch_size: Number of keys to fetch per pipeline batch (default: 100)
            
        Returns:
            List of all object definitions (dictionaries)
        """
        
        all_objects = []
        
        for root_object_key in root_object_l:
            objects_to_process = deque([root_object_key])  # O(1) operations instead of O(n)
            visited_objects = set()
            
            while objects_to_process:
                # Collect a batch of keys to fetch
                batch_keys = []
                while objects_to_process and len(batch_keys) < batch_size:
                    current_key = objects_to_process.popleft()
                    
                    if current_key not in visited_objects:
                        visited_objects.add(current_key)
                        batch_keys.append(current_key)
                
                if not batch_keys:
                    continue
                
                # Fetch batch using pipeline (single network round-trip)
                try:
                    pipe = self.i_redis_bi_analysis.redis_client.pipeline()
                    
                    for key in batch_keys:
                        pipe.json().get(key, '$')
                    
                    results = pipe.execute()
                    
                    # Process batch results
                    for i, current_key in enumerate(batch_keys):
                        try:
                            obj_def = results[i]
                            
                            # Handle Redis JSON response format
                            if isinstance(obj_def, list) and len(obj_def) > 0:
                                obj_def = obj_def[0]
                            
                            if obj_def is None:
                                print(f"Warning: Key {current_key} returned None")
                                continue
                            
                            # Add current object to results
                            all_objects.append({
                                'root_obj_key': root_object_key,
                                'root_obj_id': i_str_func.get_after_last_colon(root_object_key),
                                'obj_key': current_key,
                                'obj_id': i_str_func.get_after_last_colon(current_key),
                                'checksum_full': obj_def.get("checksum_full"),
                                'definition': obj_def
                            })
                            
                            # Extract child object references
                            if recursive_fg:
                                child_keys = self.extract_child_object_keys(obj_def)
                                
                                # Add child keys to processing queue
                                for child_key in child_keys:
                                    if child_key not in visited_objects:
                                        objects_to_process.append(child_key)
                        
                        except Exception as e:
                            print(f"Error processing object {current_key}: {e}")
                            continue
                            
                except Exception as e:
                    print(f"Error fetching batch: {e}")
                    continue
                
                # Optional: Progress tracking
                if len(all_objects) % 500 == 0 and len(all_objects) > 0:
                    print(f"Processed {len(all_objects)} objects so far...")
        
        return all_objects

    def extract_child_object_keys(self, obj_def):
        """
        Extract child object keys from the object definition
        
        Args:
            obj_def: Object definition dictionary
            
        Returns:
            List of child object keys
        """
        child_keys = []
        
        try:
            # Navigate to child_obj_d_l.prefix_child_d_l

            if 'child_obj_d_l' in obj_def:
                child_obj_d_l = obj_def['child_obj_d_l']
                
                if isinstance(child_obj_d_l, dict) and 'prefix_child_d_l' in child_obj_d_l:
                    prefix_child_d_l = child_obj_d_l['prefix_child_d_l']
                    
                    for child_ref in prefix_child_d_l:
                        child_keys.append(child_ref["redis_key"])
                        if child_ref["redis_key"].find("DBTABLE") != -1:
                            pass

        except Exception as e:
            print(f"Error extracting child keys: {e}")
        
        return child_keys

class redis_mstr_json:
    # in this class I bundle all methods related to Redis and MSTR JSON handling

    def bld_redis_key(self,conn,object_id,env_prefix):
        
        search_d={
                "projectIdAndObjectIds": [
                                    {"projectId": conn.project_id,
                                    "objectIds": [object_id]
                                    }]
                }
        obj_properties_d_l = get_objects_from_quick_search(connection=conn,body=search_d).json()
        key=None
        for obj_properties_d in obj_properties_d_l["result"]:
            subtype_text=i_read_gen.find_type_subtype(obj_properties_d["subtype"])
    #
            redis_obj_prefix=self.get_redis_prefix(subtype_text=subtype_text)
            key=f"{env_prefix}:{redis_obj_prefix}:{obj_properties_d["id"]}"
            #obj_def_j=i_redis_bi_analysis.fetch_key_value( key=key)
            #redis_obj_l.append(obj_def_j.copy()["value"])
        return key

    def bld_redis_child_key(self,  child_obj_d_l,env_prefix=""):
        #bld the redis key
        if env_prefix!="":
            env_prefix=env_prefix+":"

        prefix_child_obj_d_l = []
        for child in child_obj_d_l["prefix_child_d_l"]:
            new_child=child.copy()
            new_child["redis_key"] = f"{env_prefix}{child['prefix']}:{child['key']}"
            prefix_child_obj_d_l.append(new_child)
        child_obj_d_l["prefix_child_d_l"] = prefix_child_obj_d_l

        return child_obj_d_l
    
    def run_searches_redis(self,conn,i_redis_bi_analysis
                            ,prefix_map,env_prefix,searches_used_in_prp_d_l):
        #searches need's to be refreshed frequently 
        # and the outcame need to be stored in the NoSQL / Keyvalue db
        for s in searches_used_in_prp_d_l:

            search_result = i_mstr_api.run_mstr_search(conn=conn,
                                    search_id=s["search_id"])
            obj_def={"id":s["search_id"],
                     "search_id":s["search_id"],
                        "obj_type":39
                    }
            obj_def=i_mstr_to_json.create_obj_def_search_rel(obj_def=obj_def
                                                            ,search_obj_d_l=search_result["result"]
                                                            ,prefix_map=prefix_map)
            
            obj_def = i_json_checksum_handler.add_checksums_to_object(obj_data=obj_def)
            prefix=str(i_read_gen.find_type_subtype(type_sub_type_int=39))
            obj_def["child_obj_d_l"]=self.bld_redis_child_key(env_prefix=env_prefix,
                                                                    child_obj_d_l =obj_def["child_obj_d_l"])
            i_redis_bi_analysis.upload_key_value(key=f"{env_prefix}:{prefix}:{s["search_id"]}", value=obj_def)

    def save_obj_json_to_redis(self,conn, i_redis_bi_analysis, all_obj_d_l,prefix_map, env_prefix):
        #this method handels the read out of object on a given list of objects
        err_d_l = []
        for o in all_obj_d_l:
            try:
                # Get object definition
                obj_def = i_mstr_to_json.prepare_obj_def(conn=conn, obj_def_d=o)

                        
                obj_def= i_mstr_to_json.child_object_handler(conn=conn,obj_def=obj_def,
                                                            obj_type=o["type"],prefix_map=prefix_map)

                obj_def = i_json_checksum_handler.add_checksums_to_object(obj_data=obj_def)
                prefix=str(i_read_gen.find_type_subtype(type_sub_type_int=o["type"]))

                obj_def["child_obj_d_l"]=self.bld_redis_child_key(env_prefix=env_prefix,child_obj_d_l =obj_def["child_obj_d_l"])
                obj_def["obj_uploaded"]=datetime.now().isoformat()
                i_redis_bi_analysis.upload_key_value(key=f"{env_prefix}:{prefix}:{o["id"]}", value=obj_def)
                print(f"Uploaded object definition for {o['id']}")
            
            except Exception as e:
                err_d_l.append({
                    "id": o["id"],
                    "type": o["type"], 
                    "subtype": o["subtype"],
                    "name": o["name"],
                    "error": str(e)
                    })
            
        return err_d_l
    
    def get_redis_prefix(self, subtype_text):
        prefix_map={'report_grid_and_graph': 'REPORT_DEFINITION',
                    'prompt_double': 'PROMPT',
                    'prompt_expression': 'PROMPT',
                    'agg_metric': 'AGG_METRIC',
                    'report_engine': 'REPORT_DEFINITION',
                    'prompt_objects': 'PROMPT',
                    'report_grid': 'REPORT_DEFINITION',
                    'report_graph': 'REPORT_DEFINITION',
                    'prompt_elements': 'PROMPT',
                    'prompt_big_decimal': 'PROMPT',
                    'prompt_string': 'PROMPT',
                    'consolidation_element': 'CONSOLIDATION_ELEMENT',
                    "14081": 'DOCUMENT_DEFINITION',
                    'report_writing_document':'DOCUMENT_DEFINITION',
                    'consolidation': 'CONSOLIDATION',
                    'custom_group': 'FILTER',
                    'prompt_date': 'PROMPT',
                    'attribute':'ATTRIBUTE',
                    'metric': 'METRIC',
                    'filter': 'FILTER',
                    'fact': 'FACT',
                    'logical_table':'DBTABLE',
                    'search':'SEARCH',
                    'dimension_user':'DIMENSION'}
        
        prefix=prefix_map.get(subtype_text.lower(), "")
        if prefix:
            return prefix

        return subtype_text

