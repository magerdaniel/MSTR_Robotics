from mstr_robotics.report import cube,rep
#from mstr_robotics.json_analyser import parse_json
from mstr_robotics._helper import msic
from mstr_robotics._connectors import mstr_api
from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics.read_out_prj_obj import io_attributes,read_gen
from mstrio.api.browsing import get_objects_from_quick_search
#from mstr_robotics.json_checksum_handler import json_checksum_handler
from mstr_robotics.json_compare import json_checksum_handler
import re
from datetime import datetime
import copy



io_att=io_attributes()

i_cube=cube()
i_msic=msic()
i_mstr_global=mstr_global()
i_rep=rep()
i_mstr_api=mstr_api()
i_read_gen=read_gen()
i_json_checksum_handler=json_checksum_handler()



class export_mstr_md():

    def read_out_prj_by_type(self,conn,obj_types_d=None):
    
        if obj_types_d is None:
            obj_types_d={"FILTER": 1,"REPORT_DEFINITION": 3,"METRIC": 4,"BASE_FORMULAR":7,"PROMPT": 10,"ATTRIBUTE": 12,"FACT": 13,
                    "DIMENSION": 14,"TABLE": 15,"DATAMART_REPORT": 16,"DBLOGIN": 30,"DBCONNECTION": 31,"PROJECT": 32,"USER": 34,
                    "USERGROUP": 34,"ROLE_TRANSFORMATION": 43,"CONSOLIDATION": 47,"CONSOLIDATION_ELEMENT": 48,"DBTABLE": 53,"DOCUMENT_DEFINITION": 55,
                    "DRILL_MAP": 56,"SECURITY_FILTER": 58}
        #obj_types_d={"ATTRIBUTE": 4}
        all_obj_d_l=[]
        for obj_type in obj_types_d:
            obj_d_l=self.search_obj_by_type(conn, obj_type_id=obj_types_d[obj_type])
            for obj in obj_d_l:
                obj_d={"id":obj["id"],"type":obj["type"],"subtype":obj["subtype"],"name":obj["name"]}
                all_obj_d_l.append(obj_d.copy())
        return all_obj_d_l

    def search_obj_by_type(self,conn, obj_type_id):
        url = f'{conn.base_url}/api/metadataSearches/results?pattern=4&domain=2&scope=rooted&type={obj_type_id}&visibility=ALL'
        search_inst_resp = conn.post(url)
        
        if search_inst_resp.status_code == 200:
            search_id=search_inst_resp.json()["id"]
            url = f'{conn.base_url}/api/metadataSearches/results?searchId={search_id}&offset=0&limit=-1' 
            search_resp = conn.get(url)
            if search_resp.status_code == 200:
                obj_d_l=search_resp.json()
                print(len(obj_d_l))
                return obj_d_l

class sort_mstr_json:
    # within the exported MD definitions there are sometimes lists, where the order does not matter
    # i.e. parent child relationships. In some of those cases, MSTR sorts the order randomly
    # to simplify comparison, we sort those lists by certain keys
    
    def sort_json_lists_by_keys(self, json_obj,obj_type, sort_keys=["name","text","predicateText","value",
                                                            "expressionId", "objectId", "id", "predicateId"]):
        """
        Recursively sort lists of dictionaries in JSON by specified keys
        
        Args:
            json_obj: JSON object to process
            sort_keys: List of keys to use for sorting (in priority order)
            
        Returns:
            JSON object with sorted lists
        """
       
        # Create a deep copy to avoid modifying the original
        sorted_json = copy.deepcopy(json_obj)
        
        def get_sort_value(item, sort_keys):
            """Get the value to sort by from an item"""
            if not isinstance(item, dict):
                return ""
            
            # Try each sort key in order of priority
            for key in sort_keys:
                if key in item:
                    value = item[key]
                    # Convert to string for consistent sorting
                    return str(value) if value is not None else ""
            
            # If no sort keys found, return empty string
            return ""
        
        def sort_recursive(obj, current_path=[]):
            """Recursively process the JSON structure with path tracking"""
            if isinstance(obj, dict):
                # First, recursively process all values in the dictionary
                
                for key, value in obj.items():
                    # Build new path for this key
                    new_path = current_path + [key]
                    obj[key] = sort_recursive(value, new_path)
            
            elif isinstance(obj, list):
                # First, recursively process each item in the list
                for i, item in enumerate(obj):
                    # Include list index in path
                    item_path = current_path + [f"[{i}]"]
                    obj[i] = sort_recursive(item, item_path)
                
                # Then, check if this is a list of dictionaries and sort it

                if obj and all(isinstance(item, dict) for item in obj):
                    # Check if any dictionary has the sort keys               
                    has_sort_keys = any(
                        any(key in item for key in sort_keys) 
                        for item in obj if isinstance(item, dict)
                    )
                    try:
                        if has_sort_keys:
                            obj.sort(key=lambda x: get_sort_value(x, sort_keys))                            
                        
                        elif item_path[0] == "relationships" and str(self.obj_type) == "12":
                            obj = sorted(obj, key=lambda cp: cp["parent"]["objectId"] + cp["child"]["objectId"]).copy()
                        elif item_path[0] == "metricSubtotals" and str(self.obj_type) == "4":
                            obj = sorted(obj, key=lambda cp: cp["definition"]["objectId"]).copy()


                    except Exception as e:
                        print(f"Warning: Could not sort list at path {e}")

            return obj
        
        # Start recursion with empty path
        self.obj_type=obj_type
        sort_recursive(sorted_json,  current_path=[])
        return sorted_json
    

class mstr_to_json:
    def __init__(self):
        self.i_parse_json=parse_json()
        pass

    def add_ai_obj_to_l(self,obj):
        obj_d_l=[]
        if obj["type"]=="metrics":
            for m in obj["elements"]:
                obj_d_l.append({"object_id":m["id"],"type":m["subType"]})
        else:
            obj_d_l.append({"object_id":obj["id"],"type":obj["type"]})
        return obj_d_l

    def create_obj_def_search_rel(self,obj_def, search_obj_d_l,prefix_map):
        # prompts can be based on searches
        # while the REST API does not support to read out the definitions
        # we can read out the output of the search
        # to identify the relevant objects
        new_obj_d_l=[]
        no_prefix_child_d_l=[]

        for obj in search_obj_d_l:
            child_d={}
            child_d["keyPath"] =[]
            child_d["key_name"] ="id"
            child_d["key"]=obj["id"]
            child_d["org_sub"]=obj["subtype"]
            child_d["subType"]=str(i_read_gen.find_type_subtype(type_sub_type_int=obj["subtype"])).lower()
            type_prefix = prefix_map.get(str(child_d["subType"]), "") if prefix_map else ""
            if type_prefix:
                child_d["prefix"] = type_prefix
            new_obj_d_l.append(child_d.copy())

        obj_def["child_obj_d_l"]= {"prefix_child_d_l":new_obj_d_l,"no_prefix_child_d_l":no_prefix_child_d_l}

        return obj_def

    def child_object_handler(self,conn, obj_def,obj_type,prefix_map):
        # Extract child objects from the object definition
        # and handle expetions for certain objects
        # and clear their role for AI
        child_obj_d_l = self.i_parse_json.extract_specific_key_value_pairs(data=obj_def,prefix_map=prefix_map)
        obj_def["child_obj_d_l"] = child_obj_d_l
        
        if obj_type==3:
            obj_def["ai_grid_objects"] = self.parse_template_obj(obj_def_d=obj_def)
            obj_def["ai_report_filter"] = self.report_filter_objects(obj_def_d=obj_def
                                                                     ,prefix_map=prefix_map)
        elif obj_type==55:
            obj_def=self.add_dataset_obj_type(conn,obj_def)
        elif obj_type==10:
            pass
        # outcome are object with which exist as standalone objects (prefix)
        # and sub objects which do not exist as standalone objects (no_prefix)
        prefix_child_d_l=[]
        no_prefix_child_d_l=[]
        for child in obj_def["child_obj_d_l"]:
            if "prefix" in child.keys():
                if child["key"] not in i_msic.get_key_form_dict_l(dict_l=prefix_child_d_l,key="key"):
                    prefix_child_d_l.append(child.copy())
            else:
                if child["key"] not in i_msic.get_key_form_dict_l(dict_l=no_prefix_child_d_l,key="key"):
                    no_prefix_child_d_l.append(child.copy())

        obj_def["child_obj_d_l"]= {"prefix_child_d_l":prefix_child_d_l,"no_prefix_child_d_l":no_prefix_child_d_l}

        return obj_def

    def replace_random_id(self, obj_def,key_to_replace, path_l=[]):
        """
        Replace ID values only at specified paths to reduce noise in object comparisons
        
        Args:
            obj_def: JSON object to process
            path_l: List of paths where IDs should be replaced. Examples:
                    ["dataSource.filter.tree.children[].id", 
                    "dataSource.dataTemplate.units[].limit.tree.children[].id",
                    "grid.viewTemplate.rows.units[].elements[].id"]
        """
        
        def path_matches(current_path, target_paths):
            """Check if current path matches any target path"""
            if not target_paths:
                return False
                
            current_path_str = ".".join(current_path)
            
            for target_path in target_paths:
                # Replace [] wildcards with regex pattern
                #target_pattern = target_path.replace("[]", r"\[\d+\]")
                #target_pattern = target_pattern.replace(".", r"\.")
                #target_pattern = f"^{target_pattern}$"
                if re.match(target_path, current_path_str):
                    return True
            
            return False
    
        def replace_ids_recursive(obj, key_to_replace,current_path=[]):
            """Recursively process the JSON structure"""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = current_path + [key]
                    
                    # Check if we should replace the ID at this path
                    print(new_path)
                    if key == key_to_replace and path_matches(new_path, path_l):
                        obj[key] = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                    else:
                        # Continue recursively
                        replace_ids_recursive(value, key_to_replace, new_path)

            elif isinstance(obj, list):
                for index, item in enumerate(obj):
                    new_path = current_path + [f"[{index}]"]
                    replace_ids_recursive(item, key_to_replace, new_path)

        replace_ids_recursive(obj_def,key_to_replace)
        return obj_def

    def add_dataset_obj_type(self,conn,obj_def):
        # if a report or cube is used as a data set in an dashboard
        # there is no subtype in the JSON file, thus we need to figure it out
        new_child_obj_d_l = []
        child_obj_d_l=obj_def["child_obj_d_l"]
        for child_obj in child_obj_d_l:
            if len(child_obj["keyPath"]) ==3 and child_obj["keyPath"][0]== "datasets":
                n_child_obj=child_obj.copy()

                search_d={
                        "projectIdAndObjectIds": [
                                            {
                                            "projectId": conn.project_id,
                                            "objectIds": [child_obj["key"]]
                                            }
                                        ]
                                        }
                dataset_obj = get_objects_from_quick_search(connection=conn,body=search_d).json()
                if dataset_obj["result"][0]["subtype"] in [776]:
                    n_child_obj["subType"]="report_grid"
                    n_child_obj["prefix"]="REPORT_DEFINITION"
                else:
                    n_child_obj["subtype"]="report_grid"
                    n_child_obj["prefix"]="REPORT_DEFINITION"

                new_child_obj_d_l.append(n_child_obj)
            else:
                new_child_obj_d_l.append(child_obj.copy())

        obj_def["child_obj_d_l"]=new_child_obj_d_l
        return obj_def

    def parse_template_obj(self, obj_def_d):
        # for AI Chats we need to know, the content of the 
        # visualisations of dashboards
        view_template = obj_def_d["grid"]["viewTemplate"]
        grid_object_d_l = []

        if len(view_template["rows"]) > 0:
            for obj in view_template["rows"]["units"]:
                grid_object_d_l.extend(self.add_ai_obj_to_l(obj))
        if len(view_template["columns"]) > 0:
            for obj in view_template["columns"]["units"]:
                grid_object_d_l.extend(self.add_ai_obj_to_l(obj))
        if len(view_template["pageBy"]) > 0:
            for obj in view_template["pageBy"]["units"]:
                grid_object_d_l.extend(self.add_ai_obj_to_l(obj))
        return grid_object_d_l

    def report_filter_objects(self,obj_def_d,prefix_map):
        # for AI Chats we need to know, how reports
        # are beeing filtered
        self.i_parse_json.path_fg=True
        self.i_parse_json.json_type="rep_filter"
        self.i_parse_json.obj_path=[]
        self.i_parse_json.filter_def_l = []
        child_d_l = self.i_parse_json.extract_specific_key_value_pairs(
                                        data=obj_def_d["dataSource"]["filter"]
                                        ,path_fg=False
                                        ,json_type="rep_filter"
                                        ,prefix_map=prefix_map)
        prompt_element_d_l=[]
        child_d_l=i_msic.rem_dbl_dict_in_l(child_d_l)
        ai_filter_d_l=[]

        for c in child_d_l:
            ai_filt_d={}
            if c["subType"]=="prompt_elements":
                ai_filt_d["filter_type"]="element_prompt"
                prompt_element_d_l.append({"id":c["key"],"subType":c["subType"]}.copy())
        if prompt_element_d_l:
            ai_filt_d["filter_type"]="element_prompt"
            ai_filt_d["element_prompt_d_l"]=prompt_element_d_l

        for filt in self.i_parse_json.filter_def_l:
            ai_filt_d={}
            if filt["type"]=="predicate_element_list":
                ai_filt_d["filter_type"]="predicate_element_list"
                ai_filt_d["attribute_name"]=filt["predicateTree"]["attribute"]["name"]
                ai_filt_d["attribute_id"]=filt["predicateTree"]["attribute"]["objectId"]
                element_l=[]
                #print(filt["predicateTree"]["attribute"])
                for ele in filt["predicateTree"]["elements"]:
                    element_l.append(ele["display"])
                ai_filt_d["ai_text"]=element_l
            elif filt["type"]=="predicate_form_qualification":
                ai_filt_d["filter_type"]="predicate_form_qualification"
                ai_filt_d["attribute_id"]=filt["predicateTree"]["attribute"]["objectId"]
                ai_filt_d["attribute_form_id"]=filt["predicateTree"]["form"]["objectId"]
                ai_filt_d["attribute_name"]=filt["predicateTree"]["attribute"]["name"]
                ai_filt_d["ai_text"]=filt["predicateText"]
            elif filt["type"]=="predicate_metric_qualification":
                ai_filt_d["filter_type"]="predicate_metric_qualification"
                ai_filt_d["ai_text"]=filt["predicateText"]
                ai_filt_d["filter_type"]=filt["predicateTree"]["metric"]["objectId"]
                ai_filt_d["filter_type"]=filt["predicateTree"]["metric"]["name"]

                print(filt)
            ai_filter_d_l.append(ai_filt_d.copy())

    def prepare_obj_def(self,conn,obj_def_d):
        # Prepare the object definition for AI processing
        try:
            # Get object definition
            if obj_def_d["type"]!=39:  # search objects
                obj_def = i_read_gen.get_obj_def(
                    conn=conn,
                    object_id=obj_def_d["id"],
                    obj_type=obj_def_d["type"],                
                    obj_sub_type=obj_def_d["subtype"]
                    )
                
                if "information" in obj_def.keys():
                    obj_def["name"]=obj_def["information"].get("name", "")
                    obj_def["id"]=obj_def["information"].get("objectId", "")
                if  "error" not in obj_def.keys():
                    if obj_def_d["subtype"]=="256":
                        obj_def = self.replace_random_id(obj_def,key_to_replace="predicateId" ,path_l=["qualification"])
                    if obj_def_d["subtype"]=="257":
                        obj_def = self.replace_random_id(obj_def,key_to_replace="id", path_l=["elements"])
                    if obj_def_d["type"] not in ["55","3"]:
                        obj_def = sort_mstr_json().sort_json_lists_by_keys(json_obj=obj_def,obj_type=obj_def_d["type"])

                    if str(obj_def_d["subtype"])=="768":
                        child_l=obj_def["dataSource"]["filter"]["tree"]["children"]
                        
                        # Add filt_obj and sort by objectId
                        for child in child_l:
                            child["filt_obj"] = child["predicateTree"]
                        
                        child_l = sorted(child_l, key=lambda cp: cp["predicateTree"][list(cp["predicateTree"].keys())[0]].get("objectId", "") if cp.get("predicateTree") else "").copy()
                        obj_def["dataSource"]["filter"]["tree"]["children"]=child_l
            else:
                obj_def={}
        
        except Exception as err:
            print(f"Error preparing objefdfddct definition: {err}")

        return obj_def

class parse_json():
    def __init__(self):
        self.filter_def_l = []
        self.obj_path=[]
    def extract_specific_key_value_pairs(self, data, target_keys=["id", "key", "objectId"], all_pairs=None,
                                        path_fg=True, json_type="all", prefix_map=None):
        """
        Recursively extract specific key-value pairs from a nested JSON structure
        """
        if all_pairs is None:
            all_pairs = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                # Build readable path using object names when available

                self.obj_path.append(key)
                
                #harmonize subType 
                if "subType" in data.keys():
                    subType = data["subType"]
                elif "subtype" in data.keys():
                    subType = data["subtype"]
                #i.e. metrics appear as type
                elif "type" in data.keys():
                    subType = data["type"]
                else:
                    subType = None

                #keys or better mstr guids appear as id, objectId or key in JSON files
                if key in target_keys:
                    pair_info = {}
                    if path_fg:
                        pair_info["keyPath"] = self.obj_path.copy()  # Use copy to preserve current path
                    pair_info["key_name"] = key
                    pair_info["key"] = value
                    pair_info["subType"] = subType
                    type_prefix = prefix_map.get(str(subType), "") if prefix_map else ""
                    if type_prefix:
                        pair_info["prefix"] = type_prefix

                    all_pairs.append(pair_info)
                    
                # this pass is only relevant, if the JSON files is marked as filter
                # by setting the class variable before execution
                if json_type == "rep_filter" and key == "predicateTree":
                    if set(["attribute","metric"]).intersection(set(data["predicateTree"].keys())):
                        self.filter_def_l.append(data)
                
                # Recursively process nested structures (only if not primitive)
                if not isinstance(value, (str, int, float, bool, type(None))):
                    self.extract_specific_key_value_pairs(value, target_keys, all_pairs, path_fg, json_type, prefix_map)
                
                # Remove current key from path after processing (backtrack)
                self.obj_path.pop()

        elif isinstance(data, list):
            # Process each item in the list
            for index, item in enumerate(data):
                # Add list index to path
                self.obj_path.append(f"[{index}]")
                
                # Recursively process list items
                if not isinstance(item, (str, int, float, bool, type(None))):
                    self.extract_specific_key_value_pairs(item, target_keys, all_pairs, path_fg, json_type, prefix_map)
                
                # Remove list index from path after processing (backtrack)
                self.obj_path.pop()

        return all_pairs
    
class map_objects():

    def zzz_get_doss_rep_prp(self,conn, object_l):
        prp_rep_l = []
        prp_rep_err_l = []
        for rep_dos in object_l:
            try:
                if rep_dos["type"] == 3:
                    for prp in i_rep.get_rep_prp_l(conn=conn, report_id=rep_dos["id"]):
                        rep_prp_d = {}
                        rep_prp_d["project_id"]=conn.project_id
                        rep_prp_d["rep_dos_id"] = rep_dos["id"]
                        rep_prp_d["type"] = rep_dos["type"]
                        rep_prp_d["subtype"] = rep_dos["subtype"]
                        rep_prp_d["prompt_id"] = prp.id
                        prp_rep_l.append(rep_prp_d.copy())
                elif rep_dos["type"] == 55:
                    for prp in i_mstr_api.get_dossier_prp_l(conn=conn, dossier_id=rep_dos["id"]):
                        rep_prp_d = {}
                        rep_prp_d["project_id"]=conn.project_id
                        rep_prp_d["rep_dos_id"] = rep_dos["id"]
                        rep_prp_d["type"] = rep_dos["type"]
                        rep_prp_d["subtype"] = rep_dos["subtype"]
                        rep_prp_d["prompt_id"] = prp["id"]
                        prp_rep_l.append(rep_prp_d.copy())

            except Exception as e:
                prp_rep_err_d = {"rep_dos_id": rep_dos["id"], "rep_dos_type":rep_dos["type"], "err_msg": str(e)}
                prp_rep_err_l.append(prp_rep_err_d.copy())

        all_rep_prp_d = {"prp_rep_l": prp_rep_l, "prp_rep_err_l": prp_rep_err_l}
        return all_rep_prp_d

class clean_mstr_ids:
    #there are a bunch of id's in object definitions, that are changing as soon as the object
    # is beeing saved. This causes trouble, as soon as we want to compare objects using their
    # definitions n JSON

    def __init__(self, conn):
        pass

    def clean_json(self, json_obj_def):
        # Implement cleaning logic for JSON object here
        
        if "information" in json_obj_def.keys():
            obj_type= json_obj_def["information"]["type"]
            obj_subtype=json_obj_def["information"]["subtype"]
        else:
            obj_type= json_obj_def["type"]
            obj_subtype=json_obj_def["subtype"]

        if obj_subtype in [257]:
            for unit_no, unit in enumerate(json_obj_def["dataSource"]["dataTemplate"]["units"]):
                for child_no, child in enumerate(unit["limit"]["tree"]["children"]):
                    if "predicateId" in child:
                        del json_obj_def["dataSource"]["dataTemplate"]["units"][unit_no]["limit"]["tree"]["children"][child_no]
                

            for child_no, child in enumerate(json_obj_def["dataSource"]["filter"]["tree"]["children"]):
                if "predicateId" in child:
                    del json_obj_def["dataSource"]["filter"]["tree"]["children"][child_no]

        return json_obj_def

class zzz_redis_mstr_json:
    # in this class I bundle all methods related to Redis and MSTR JSON handling

    def bld_redis_key(self,  child_obj_d_l,env_prefix=""):
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
            obj_def=mstr_to_json().create_obj_def_search_rel(obj_def=obj_def
                                                            ,search_obj_d_l=search_result["result"]
                                                            ,prefix_map=prefix_map)
            
            obj_def = i_json_checksum_handler.add_checksums_to_object(obj_data=obj_def)
            prefix=str(i_read_gen.find_type_subtype(type_sub_type_int=39))
            obj_def["child_obj_d_l"]=self.bld_redis_key(env_prefix=env_prefix,
                                                                    child_obj_d_l =obj_def["child_obj_d_l"])
            i_redis_bi_analysis.upload_key_value(key=f"{env_prefix}:{prefix}:{s["search_id"]}", value=obj_def)

    def save_obj_json_to_redis(self,conn, i_redis_bi_analysis, all_obj_d_l,prefix_map, env_prefix):
        #this method handels the read out of object on a given list of objects
        err_d_l = []
        for o in all_obj_d_l:
            try:
                # Get object definition
                obj_def = mstr_to_json().prepare_obj_def(conn=conn, obj_def_d=o)

                        
                obj_def= mstr_to_json().child_object_handler(conn=conn,obj_def=obj_def,
                                                            obj_type=o["type"],prefix_map=prefix_map)

                obj_def = i_json_checksum_handler.add_checksums_to_object(obj_data=obj_def)
                prefix=str(i_read_gen.find_type_subtype(type_sub_type_int=o["type"]))

                obj_def["child_obj_d_l"]=self.bld_redis_key(env_prefix=env_prefix,child_obj_d_l =obj_def["child_obj_d_l"])
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
