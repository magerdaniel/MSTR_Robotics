from mstrio.api.browsing import get_objects_from_quick_search
import re
import ast
import pandas as pd
import copy
from typing import  Any, Dict, List, Set, Union
from html import escape
import json
import hashlib
from pathlib import Path
from mstr_robotics.read_out_prj_obj import read_gen
#from mstr_robotics.prepare_AI_data import redis_mstr_json
#from mstr_robotics.redis_db import redis_mstr_json
from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics._helper import msic
from mstr_robotics._connectors import mstr_api
from mstr_robotics.mstr_classes import mstr_global


import pandas as pd

i_read_gen=read_gen()

#i_redis_mstr_json=redis_mstr_json()
i_mstr_global=mstr_global()
i_msic=msic()

class JSONComparator:
    def __init__(self):
        self.differences = []
        self.comp_det_d = {}


    def remove_bracket_numbers(self,path_list):
        # Check for NaN/None FIRST - use 'is None' or check scalar NaN
        if path_list is None or (isinstance(path_list, float) and pd.isna(path_list)):
            return path_list
        
        if isinstance(path_list, str):
            try:
                path_list = ast.literal_eval(path_list)
            except (ValueError, SyntaxError):
                return path_list
        
        # If it's not a list, try to convert it
        if not isinstance(path_list, list):
            try:
                path_list = list(path_list)
            except (TypeError, ValueError):
                return path_list
        
        return [elem for elem in path_list if not re.match(r'^\[\d+\]$', str(elem))]


    def remove_no_interest_fields(self, json_obj, remove_key_l=["child_obj_d_l","checksum_obj_def","checksum_full","destinationFolderId",
                                            "ai_grid_objects","ai_report_filter","versionId","dateModified","dateCreated","obj_uploaded","uploaded"],
                                json_path=None):
        """
        Remove specified fields from JSON object recursively at all nested levels
        
        Args:
            json_obj: JSON object to clean
            remove_key_l: List of keys to remove
            json_path: Optional path constraint - only remove keys if they are under this path
                    Format: "dataSource.filter.tree" or ["dataSource", "filter", "tree"]
            
        Returns:
            Cleaned JSON object
        """
        
        # Create a deep copy to avoid modifying the original
        cleaned_json = copy.deepcopy(json_obj)
        
        # Convert json_path to list if it's a string
        if json_path is not None:
            if isinstance(json_path, str):
                target_path = json_path.split('.')
            else:
                target_path = json_path
        else:
            target_path = None
        
        def remove_keys_recursive(obj, current_path=[]):
            if isinstance(obj, dict):
                # Check if we should remove keys at this level
                should_remove = True
                if target_path is not None:
                    # Only remove if we're under the specified path
                    if len(current_path) >= len(target_path):
                        # Check if current path starts with target path
                        should_remove = current_path[:len(target_path)] == target_path
                    else:
                        should_remove = False
                
                # Remove keys from current level if conditions are met
                if should_remove:
                    keys_to_remove = [key for key in obj.keys() if key in remove_key_l]
                    for key in keys_to_remove:
                        del obj[key]
                
                # Recursively process remaining values
                for key, value in obj.items():
                    new_path = current_path + [key]
                    #print(new_path)
                    remove_keys_recursive(value, new_path)
                    
            elif isinstance(obj, list):
                # Process each item in the list
                for index, item in enumerate(obj):
                    new_path = current_path + [f"[{index}]"]
                    remove_keys_recursive(item, new_path)
        
        remove_keys_recursive(cleaned_json)
        return cleaned_json

    def clean_json(self, json_obj_def):
        # Implement cleaning logic for JSON object here
        obj_subtype=""
        if "information" in json_obj_def.keys():
            obj_subtype=json_obj_def["information"]["subType"]
            self.comp_det_d["name"] = json_obj_def["information"]["name"]
        elif "subType" in json_obj_def.keys():
            obj_subtype = json_obj_def["subType"]
            self.comp_det_d["name"] = json_obj_def["name"]
        elif "subtype" in json_obj_def.keys():
            obj_subtype = json_obj_def["subtype"]
            self.comp_det_d["name"] = json_obj_def["name"]
        #i.e. metrics appear as type
        elif "type" in json_obj_def.keys():
            obj_subtype = json_obj_def["type"]
            self.comp_det_d["name"] = json_obj_def["name"]
        else:
            print("no subtype in " + str(json_obj_def))
        
        self.comp_det_d["obj_subtype"] = i_read_gen.child_obj_type_handler(subtype=str(obj_subtype))

        if str(obj_subtype) in ["257","custom_group"]:
            json_obj_def = self.remove_no_interest_fields(json_obj_def, 
                                                        remove_key_l=["predicateId","id"],
                                                        json_path="elements")
        elif str(obj_subtype) in ["report_grid"]:
            # FIX: Assign the result back to json_obj_def
            json_obj_def = self.remove_no_interest_fields(json_obj_def, 
                                                        remove_key_l=["predicateId"],
                                                        json_path="dataSource")
        #else:
        #    print(obj_subtype)

        return json_obj_def

    def compare_json_data(self, comp_det_d:dict, json1: Any, json2: Any) -> List[Dict]:
        """
        Compare two JSON data structures
        
        Args:
            json1: First JSON data structure
            json2: Second JSON data structure
            
        Returns:
            List of dictionaries containing differences
        """
        self.differences = []
        self.comp_det_d = comp_det_d
        
        json1 = self.remove_no_interest_fields(json1).copy()
        json2 = self.remove_no_interest_fields(json2).copy()
        json1 = self.clean_json(json1)
        json2 = self.clean_json(json2)
        self._compare_recursive(json1, json2, [])
        return self.differences
    
    def _compare_recursive(self, obj1: Any, obj2: Any, path: List[str]):
        """
        Recursively compare two objects and track differences
        
        Args:
            obj1: First object to compare
            obj2: Second object to compare
            path: Current path in the JSON structure
        """
        # Handle None cases
        if obj1 is None and obj2 is None:
            return
        elif obj1 is None:
            self._add_difference(path, "only_in_org", None, obj2)
            return
        elif obj2 is None:
            self._add_difference(path, "only_in_comp", obj1, None)
            return
        
        # Handle different types
        if type(obj1) != type(obj2):
            self._add_difference(path, "different", obj1, obj2)
            return
        
        # Handle dictionaries
        if isinstance(obj1, dict):
            self._compare_dicts(obj1, obj2, path)
        
        # Handle lists
        elif isinstance(obj1, list):
            self._compare_lists(obj1, obj2, path)
        
        # Handle primitive types
        else:
            if obj1 != obj2:
                self._add_difference(path, "different", obj1, obj2)
    
    def _compare_dicts(self, dict1: Dict, dict2: Dict, path: List[str]):
        """Compare two dictionaries"""
        # Check for only_in_org keys in dict2
        for key in dict1:
            new_path = path + [key]
            if key not in dict2:
                self._add_difference(new_path, "only_in_org", dict1[key], None)
            else:
                self._compare_recursive(dict1[key], dict2[key], new_path)
        
        # Check for extra keys in dict2
        for key in dict2:
            if key not in dict1:
                new_path = path + [key]
                self._add_difference(new_path, "only_in_comp", None, dict2[key])
    
    def _compare_lists(self, list1: List, list2: List, path: List[str]):
        """Compare two lists"""
        max_len = max(len(list1), len(list2))
        
        for i in range(max_len):
            new_path = path + [f"[{i}]"]
            
            if i >= len(list1):
                self._add_difference(new_path, "only_in_comp", None, list2[i])
            elif i >= len(list2):
                self._add_difference(new_path, "only_in_org", list1[i], None)
            else:
                self._compare_recursive(list1[i], list2[i], new_path)
    
    def _add_difference(self, path: List[str], diff_type: str, value1: Any, value2: Any):
        """
        Add a difference to the results
        
        Args:
            path: Path to the difference
            diff_type: Type of difference (only_in_org, only_in_comp, different)
            value1: Value from first JSON
            value2: Value from second JSON
        """
        path_str = self._format_path(path)

        difference = self.comp_det_d.copy()
        difference["json_key_path"] = path_str
        difference["path_list"] = path.copy()
        difference["diff_type"] = diff_type
        difference["value1"] = value1
        difference["value2"] = value2 
        if diff_type:      
            self.differences.append(difference)
    
    def _format_path(self, path: List[str]) -> str:
        """Format path list into a readable string with proper handling of keys with spaces

        Examples:
            ['chapters', '[0]', 'pages'] -> 'chapters[0].pages'
            ['advancedProperties', 'VLDB Select'] -> 'advancedProperties["VLDB Select"]'
        """
        if not path:
            return "root"

        result = ""
        for part in path:
            if part.startswith("[") and part.endswith("]"):
                # This is a list index [0], append directly (no dot before)
                result += part
            else:
                # Check if key contains spaces or special characters that need quoting
                if " " in part or any(c in part for c in ['.', '(', ')', '-', '/', '\\']):
                    # Use bracket notation for keys with spaces/special chars
                    if result:  # Add to existing path
                        result += f'["{part}"]'
                    else:  # First element
                        result = f'["{part}"]'
                else:
                    # Regular key without special characters
                    if result and not result.endswith(']'):
                        # Add dot separator if previous wasn't a bracket
                        result += f'.{part}'
                    elif result and result.endswith(']'):
                        # Previous was a bracket, add dot then key
                        result += f'.{part}'
                    else:
                        # First element
                        result = part

        return result
    
class compare_mstr_objects():
    """
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
            redis_obj_prefix=i_redis_mstr_json.get_redis_prefix(subtype_text=subtype_text)
            key=f"{env_prefix}:{redis_obj_prefix}:{obj_properties_d["id"]}"
            #obj_def_j=i_redis_bi_analysis.fetch_key_value( key=key)
            #redis_obj_l.append(obj_def_j.copy()["value"])
        return key

    def zzz_fetch_comp_objdef_d_l(self,conn,i_redis_bi_analysis,comp_json_d_l,redis_mstr_d):

        comp_result_list=[]

        for json_comp in comp_json_d_l:
            n_json_comp=json_comp.copy()
            project_id=json_comp["org_j_f"]["project_id"]
            conn.select_project(project_id)
            object_id=[json_comp["org_j_f"]["object_id"]]
            env_prefix=redis_mstr_d["project_prefix"][project_id]       
            n_json_comp["redis_key"]=self.bld_redis_key(conn,object_id,env_prefix)
            n_json_comp["org_obj_def_j"]=i_redis_bi_analysis.fetch_key_value( key=n_json_comp["redis_key"])
            
            project_id=json_comp["comp_j_f"]["project_id"]
            conn.select_project(project_id)
            object_id=[json_comp["comp_j_f"]["object_id"]]
            env_prefix=redis_mstr_d["project_prefix"][project_id]
            n_json_comp["redis_key"]=self.bld_redis_key(conn,object_id,env_prefix)
            n_json_comp["comp_obj_def_j"]=self.bld_redis_key(conn,object_id,env_prefix)
            
            comp_result_list.append(n_json_comp.copy())

        return comp_result_list

    def zzz_fetch_obj_id_from_sh_folder(conn,env,org_project_id,short_cut_folder_id):
        conn.select_project(org_project_id)
        #env_prefix=redis_mstr_d["project_prefix"][project_id]
        all_obj_d_l=i_mstr_global.get_obj_from_sh_fold(conn,folder_id=short_cut_folder_id)
        all_obj_d_l=i_msic.get_key_form_dict_l(dict_l=all_obj_d_l)

        comp_json_d_l=[]
        for o in all_obj_d_l:
            comp_json_d={}
            comp_json_d["org_j_f"]={}
            comp_json_d["org_j_f"]["env"]=env
            comp_json_d["org_j_f"]["project_id"]=org_project_id
            comp_json_d["org_j_f"]["object_id"]=o

            comp_json_d["comp_j_f"]={}
            comp_json_d["comp_j_f"]["env"]=env
            comp_json_d["comp_j_f"]["project_id"]=org_project_id
            comp_json_d["comp_j_f"]["object_id"]=o
            comp_json_d_l.append(comp_json_d.copy())
        
        return comp_json_d_l
    
    """
    def compare_objects(self, all_org_objects_df, all_comp_objects_df):
        df=pd.merge(all_org_objects_df,
                all_comp_objects_df,
                on=["root_obj_id","obj_id"],
                how="outer"
        )
        df_filtered = df.query('checksum_full_x != checksum_full_y')
        diff_d_l=[]
        print(df_filtered.columns)
        for i,comp in df_filtered.iterrows():
            comp_det_d={}
            comp_det_d["org_root_obj_key"]=comp["root_obj_key_x"]
            comp_det_d["org_obj_id"]=comp["obj_id"]
            comp_det_d["comp_obj_id"]=comp["obj_id"]
            comp_det_d["org_obj_key"]=comp["obj_key_x"]
            comp_det_d["comp_root_obj_key"]=comp["root_obj_key_y"]
            comp_det_d["comp_obj_key"]=comp["obj_key_y"]
            comp_det_d["org_obj_def"]=comp["definition_x"]
            comp_det_d["comp_obj_def"]=comp["definition_y"]
            differences=None
            if (isinstance(comp_det_d["org_obj_def"] , dict)
                and isinstance(comp_det_d["comp_obj_def"] , dict)):
                differences = JSONComparator().compare_json_data(comp_det_d=comp_det_d,
                                                            json1=comp["definition_x"] 
                                                        , json2=comp["definition_y"] )
            elif (isinstance(comp_det_d["org_obj_def"] , dict)
                  and not isinstance(comp_det_d["comp_obj_def"] , dict)):
                differences = [{"json_key_path":"json_key_path","path_list":"path_list","diff_type":"Compare OBJECT is missing","value1":"value1",	"value2":"value2"}]
            elif (not isinstance(comp_det_d["org_obj_def"] , dict)
                  and isinstance(comp_det_d["comp_obj_def"] , dict)):
                differences = [{"json_key_path":"json_key_path","path_list":"path_list","diff_type":"Aditional OBJECT","value1":"value1",	"value2":"value2"}]
            if len (differences)>0:

                diff_d_l.extend(differences)
            #diff_d_l.append(comp_det_d)
        return diff_d_l

class json_checksum_handler:
    """
    A class to handle JSON checksums with filtering capabilities for MicroStrategy objects.
    """

    def filter_json_keys(self, data: Any, ignore_keys: Set[str]) -> Any:
        """
        Recursively filter out specified keys from JSON data structure.

        Args:
            data: JSON data (dict, list, or primitive)
            ignore_keys: Set of keys to ignore (including all their children)

        Returns:
            Filtered data structure
        """
        # If it's a dictionary, filter out ignored keys
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # Skip this key if it's in the ignore list (skips entire subtree)
                if key not in ignore_keys:
                    # Recursively process the value (in case it has nested structures)
                    result[key] = self.filter_json_keys(value, ignore_keys)
            return result

        # If it's a list, recursively process each item
        elif isinstance(data, list):
            result = []
            for item in data:
                result.append(self.filter_json_keys(item, ignore_keys))
            return result

        # If it's a primitive (string, number, bool, None), return as-is
        else:
            return data

    def json_checksum(self, data: Union[Dict, List, str, Path], ignore_keys: List[str] = None, algorithm: str = "sha256") -> str:
        """
        Generate a checksum for JSON data with option to ignore specific keys.

        Args:
            data: JSON data as dict/list, JSON string, or file path
            ignore_keys: List of keys to ignore (including all their children)
            algorithm: Hash algorithm (md5, sha1, sha256, sha512)

        Returns:
            Hexadecimal checksum string

        Example:
            >>> handler = json_checksum_handler()
            >>> data = {"name": "John", "age": 30, "metadata": {"created": "2024-01-01"}}
            >>> checksum = handler.json_checksum(data, ignore_keys=["metadata"])
            >>> print(checksum)
        """
        # Convert ignore_keys to set for faster lookup
        ignore_keys_set = set(ignore_keys) if ignore_keys else set()

        # Load data if it's a file path or string
        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.exists() and path.is_file():
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                # Try to parse as JSON string
                data = json.loads(data)

        # Filter out ignored keys
        filtered_data = self.filter_json_keys(data, ignore_keys_set)

        # Convert to canonical JSON string (sorted keys, no whitespace)
        canonical_json = json.dumps(filtered_data, sort_keys=True, ensure_ascii=False)

        # Generate checksum
        hash_func = hashlib.new(algorithm)
        hash_func.update(canonical_json.encode('utf-8'))

        return hash_func.hexdigest()

    def generate_object_checksums(self, obj_data: Dict, ignore_keys: List[str] = None) -> Dict[str, str]:
        """
        Generate checksums for MicroStrategy object data with different levels of detail.
        
        Args:
            obj_data: MicroStrategy object data
            ignore_keys: Keys to ignore when generating checksums
            
        Returns:
            Dictionary with different checksum types

        Example:
            >>> handler = json_checksum_handler()
            >>> checksums = handler.generate_object_checksums(mstr_object_data)
            >>> print(checksums["checksum_no_timestamps"])
        """
        default_ignore = ["dateModified", "dateCreated", "version", "checksum_obj_def", "checksum_obj_ACL", "obj_uploaded"]
        if ignore_keys:
            ignore_keys = list(set(default_ignore + ignore_keys))
        else:
            ignore_keys = default_ignore
            
        return {
            "checksum_full": self.json_checksum(obj_data),
            "checksum_definition_only": self.json_checksum(obj_data.get("definition", {}), ignore_keys=ignore_keys),
            #"checksum_acl_only": self.json_checksum(obj_data.get("acl", {}), ignore_keys=ignore_keys)
        }

    def add_checksums_to_object(self, obj_data: Dict, ignore_keys: List[str] = None) -> Dict:
        """
        Add checksum fields to a MicroStrategy object data structure.
        
        Args:
            obj_data: MicroStrategy object data (will be modified)
            ignore_keys: Additional keys to ignore beyond defaults
            
        Returns:
            Modified object data with checksum fields added
        """
        checksums = self.generate_object_checksums(obj_data, ignore_keys)
        
        # Add checksums to the object data
        obj_data["checksum_obj_def"] = checksums["checksum_definition_only"]
        #obj_data["checksum_obj_ACL"] = checksums["checksum_acl_only"]
        obj_data["checksum_full"] = checksums["checksum_full"]
        #obj_data["checksum_no_timestamps"] = checksums["checksum_no_timestamps"]
        
        return obj_data