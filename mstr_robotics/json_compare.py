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
#from mstr_robotics.prepare_ai_data import redis_mstr_json
#from mstr_robotics.redis_db import redis_mstr_json
from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics._helper import msic


import pandas as pd

i_read_gen=read_gen()

#i_redis_mstr_json=redis_mstr_json()
i_mstr_global=mstr_global()
i_msic=msic()


# ==================== UTILITY FUNCTIONS ====================

def remove_after_last_dot_if_bracket(text):
    """Remove everything after the last dot if preceded by a bracket

    Args:
        text: String to process

    Returns:
        Processed string
    """
    if isinstance(text, str):
        last_dot = text.rfind('.')
        if last_dot > 0 and text[last_dot - 1] == ']':
            return text[:last_dot]
    return text


# ==================== JSON PATH UTILITIES ====================

class JSONPathHelper:
    """Helper class for JSON path operations"""

    @staticmethod
    def parse_path_string(path_str):
        """Parse dot notation path string into list

        Args:
            path_str: Path string in dot notation

        Returns:
            List of path components

        Examples:
            'chapters[0].pages[1].name' -> ['chapters', 0, 'pages', 1, 'name']
            'advancedProperties.vldbProperties["VLDB Select"]' -> ['advancedProperties', 'vldbProperties', 'VLDB Select']
        """

        parts = re.split(r'\.', path_str)
        result = []

        for part in parts:
            if '[' in part:
                # Match both numeric indices [0] and quoted keys ["key name"]
                matches = re.findall(r'([^\[\]]+)|\[(\d+)\]|\["([^"]+)"\]', part)
                for match in matches:
                    if match[0]:  # Regular key name
                        result.append(match[0])
                    elif match[1]:  # Numeric index [0]
                        result.append(int(match[1]))
                    elif match[2]:  # Quoted key ["key name"]
                        result.append(match[2])
            else:
                if part:  # Only append non-empty parts
                    result.append(part)

        return result

    @staticmethod
    def get_nested_value(data, path):
        """Navigate to a nested value using a path list

        Args:
            data: The JSON data to navigate
            path: List of keys/indices to navigate (e.g., ['advancedProperties', 'vldbProperties', 'VLDB Select'])

        Returns:
            The value at the path, or None if path doesn't exist
        """
        current = data
        for key in path:
            if isinstance(current, dict):
                if key in current:
                    current = current[key]
                else:
                    return None
            elif isinstance(current, list) and isinstance(key, int):
                if key < len(current):
                    current = current[key]
                else:
                    return None
            else:
                return None
        return current

    @staticmethod
    def extract_all_paths(data, max_depth=3, current_path=None, current_depth=0, path_labels=None):
        """Recursively extract all paths from JSON up to max_depth

        Args:
            data: JSON data to extract paths from
            max_depth: Maximum depth to traverse
            current_path: Current path being built (for recursion)
            current_depth: Current recursion depth
            path_labels: Display labels for path components

        Returns:
            List of tuples: (numeric_path, display_path)
        """
        if current_path is None:
            current_path = []
        if path_labels is None:
            path_labels = []

        paths = []

        if current_depth >= max_depth:
            return paths

        if isinstance(data, dict):
            for key, value in data.items():
                new_path = current_path + [key]
                new_labels = path_labels + [key]

                if isinstance(value, (dict, list)):
                    numeric_path = ".".join(str(p) if not isinstance(p, int) else f"[{p}]" for p in new_path)
                    numeric_path = numeric_path.replace(".[", "[")

                    display_path = ".".join(str(p) if not isinstance(p, int) else f"[{p}]" for p in new_labels)
                    display_path = display_path.replace(".[", "[")

                    paths.append((numeric_path, display_path))
                    paths.extend(JSONPathHelper.extract_all_paths(value, max_depth, new_path, current_depth + 1, new_labels))

        elif isinstance(data, list) and len(data) > 0:
            for idx in range(min(3, len(data))):
                new_path = current_path + [idx]
                value = data[idx]

                label = str(idx)
                if isinstance(value, dict):
                    if "name" in value:
                        label = str(value['name'])[:30]
                    elif "text" in value:
                        label = str(value['text'])[:30]
                    else:
                        keys = sorted(value.keys())
                        if keys:
                            first_val = value[keys[0]]
                            if isinstance(first_val, str):
                                label = first_val[:30]

                new_labels = path_labels + [label]

                if isinstance(value, (dict, list)):
                    numeric_path = ".".join(str(p) if not isinstance(p, int) else f"[{p}]" for p in new_path)
                    numeric_path = numeric_path.replace(".[", "[")

                    display_path = ".".join(str(p) if not isinstance(p, int) else f"[{p}]" for p in new_labels)
                    display_path = display_path.replace(".[", "[")

                    paths.append((numeric_path, display_path))
                    paths.extend(JSONPathHelper.extract_all_paths(value, max_depth, new_path, current_depth + 1, new_labels))

        return paths


class JSONFilterUtils:
    """Utility class for JSON filtering and comparison operations"""

    @staticmethod
    def get_content_hash(obj, ignore_keys=None):
        """Calculate hash of an object ignoring specified keys

        Args:
            obj: Object to hash
            ignore_keys: Keys to ignore in comparison

        Returns:
            MD5 hash of the object
        """
        if ignore_keys is None:
            ignore_keys = ["predicateId", "versionId", "dateModified", "dateCreated"]

        def remove_ignored_keys(obj, ignore_keys):
            if isinstance(obj, dict):
                return {k: remove_ignored_keys(v, ignore_keys)
                       for k, v in obj.items()
                       if k not in ignore_keys}
            elif isinstance(obj, list):
                return [remove_ignored_keys(item, ignore_keys) for item in obj]
            else:
                return obj

        cleaned = remove_ignored_keys(obj, ignore_keys)
        try:
            json_str = json.dumps(cleaned, sort_keys=True, default=str)
            return hashlib.md5(json_str.encode()).hexdigest()
        except:
            return hashlib.md5(str(cleaned).encode()).hexdigest()

    @staticmethod
    def build_path_str(path_list):
        """Build path string from path list

        Args:
            path_list: List of path components

        Returns:
            Dot-notation path string
        """
        result = []
        for p in path_list:
            if isinstance(p, int):
                if result:
                    result[-1] = result[-1] + f'[{p}]'
                else:
                    result.append(f'[{p}]')
            else:
                result.append(str(p))
        return '.'.join(result)

    @staticmethod
    def has_relevant_child_path(current_path_str, filter_paths):
        """Check if any filter path contains this path as a prefix

        Args:
            current_path_str: Current path string
            filter_paths: List of filter paths

        Returns:
            True if any filter path starts with current path
        """
        if not current_path_str:
            return True

        return any(
            p.startswith(current_path_str + '.') or
            p.startswith(current_path_str + '[') or
            p == current_path_str
            for p in filter_paths
        )

    @staticmethod
    def filter_json_by_paths(obj, filter_paths, current_path=[], ignore_keys=None):
        """Create a filtered copy of JSON object that only includes specified paths

        Args:
            obj: JSON object to filter
            filter_paths: List of paths to include
            current_path: Current path in recursion
            ignore_keys: Keys to ignore

        Returns:
            Filtered JSON object
        """
        if ignore_keys is None:
            ignore_keys = ["predicateId", "versionId", "dateModified", "dateCreated"]

        current_path_str = JSONFilterUtils.build_path_str(current_path)

        if current_path_str and not JSONFilterUtils.has_relevant_child_path(current_path_str, filter_paths):
            return None

        if isinstance(obj, dict):
            filtered = {}
            for key, value in obj.items():
                if key in ignore_keys:
                    continue
                new_path = current_path + [key]
                filtered_value = JSONFilterUtils.filter_json_by_paths(value, filter_paths, new_path, ignore_keys)
                if filtered_value is not None:
                    filtered[key] = filtered_value
            return filtered if filtered else None

        elif isinstance(obj, list):
            filtered = []
            for i, item in enumerate(obj):
                new_path = current_path + [i]
                filtered_value = JSONFilterUtils.filter_json_by_paths(item, filter_paths, new_path, ignore_keys)
                if filtered_value is not None:
                    filtered.append(filtered_value)
            return filtered if filtered else None
        else:
            return obj

    @staticmethod
    def extract_different_value_paths(diff_paths, diff_types, obj_def_1, obj_def_2):
        """Extract paths that have different values (excluding ignored keys)

        The diff_types parameter is used to filter differences by type.
        It distinguishes between paths that have "different" values vs other types
        of differences (like missing keys, added keys, etc.). This allows showing
        only the differences where values actually differ, excluding differences
        that are only due to ignored keys or list ordering.

        Args:
            diff_paths: List of difference paths
            diff_types: List of difference types
            obj_def_1: First object
            obj_def_2: Second object

        Returns:
            List of paths with different values
        """
        different_value_paths = []

        for path, dtype in zip(diff_paths, diff_types):
            parsed_path = JSONPathHelper.parse_path_string(path)
            val1 = JSONPathHelper.get_nested_value(obj_def_1, parsed_path)
            val2 = JSONPathHelper.get_nested_value(obj_def_2, parsed_path)

            if dtype == "different":
                if isinstance(val1, list) and isinstance(val2, list):
                    try:
                        hashes1 = set(JSONFilterUtils.get_content_hash(item) for item in val1)
                        hashes2 = set(JSONFilterUtils.get_content_hash(item) for item in val2)
                        if hashes1 != hashes2:
                            different_value_paths.append(path)
                    except:
                        different_value_paths.append(path)
                elif val1 is not None and val2 is not None:
                    try:
                        if JSONFilterUtils.get_content_hash(val1) != JSONFilterUtils.get_content_hash(val2):
                            different_value_paths.append(path)
                    except:
                        different_value_paths.append(path)
                else:
                    different_value_paths.append(path)

        return different_value_paths


# ==================== JSON COMPARISON CLASSES ====================

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