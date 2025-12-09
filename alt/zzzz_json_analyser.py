import json
from typing import Any, Dict, List, Set, Union
from pathlib import Path
import json
import hashlib
from typing import Any, Dict, List, Set, Union



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

    def compare_json_data(self, data1: Any, data2: Any, ignore_keys: List[str] = None, algorithm: str = "sha256") -> Dict[str, Any]:
        """
        Compare two JSON data structures using checksums.

        Args:
            data1: First JSON data structure
            data2: Second JSON data structure
            ignore_keys: List of keys to ignore in comparison
            algorithm: Hash algorithm to use

        Returns:
            Dictionary with comparison results
        """
        checksum1 = self.json_checksum(data1, ignore_keys, algorithm)
        checksum2 = self.json_checksum(data2, ignore_keys, algorithm)

        return {
            "are_equal": checksum1 == checksum2,
            "data1_checksum": checksum1,
            "data2_checksum": checksum2,
            "algorithm": algorithm,
            "ignored_keys": ignore_keys or []
        }

    def compare_json_files(self, file1: Union[str, Path], file2: Union[str, Path], ignore_keys: List[str] = None, algorithm: str = "sha256") -> Dict[str, Any]:
        """
        Compare two JSON files using checksums.

        Args:
            file1: Path to first JSON file
            file2: Path to second JSON file
            ignore_keys: List of keys to ignore in comparison
            algorithm: Hash algorithm to use

        Returns:
            Dictionary with comparison results

        Example:
            >>> handler = json_checksum_handler()
            >>> result = handler.compare_json_files("file1.json", "file2.json", ignore_keys=["timestamp"])
            >>> print(result["are_equal"])
        """
        checksum1 = self.json_checksum(file1, ignore_keys, algorithm)
        checksum2 = self.json_checksum(file2, ignore_keys, algorithm)

        return {
            "are_equal": checksum1 == checksum2,
            "file1_checksum": checksum1,
            "file2_checksum": checksum2,
            "algorithm": algorithm,
            "ignored_keys": ignore_keys or []
        }


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
        default_ignore = ["dateModified", "dateCreated", "version", "checksum_obj_def", "checksum_obj_ACL"]
        if ignore_keys:
            ignore_keys = list(set(default_ignore + ignore_keys))
        else:
            ignore_keys = default_ignore
            
        return {
            "checksum_full": self.json_checksum(obj_data),
            "checksum_no_timestamps": self.json_checksum(obj_data, ignore_keys=ignore_keys),
            "checksum_definition_only": self.json_checksum(obj_data.get("definition", {}), ignore_keys=ignore_keys),
            "checksum_acl_only": self.json_checksum(obj_data.get("acl", {}), ignore_keys=ignore_keys)
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
        obj_data["checksum_obj_ACL"] = checksums["checksum_acl_only"]
        obj_data["checksum_full"] = checksums["checksum_full"]
        obj_data["checksum_no_timestamps"] = checksums["checksum_no_timestamps"]
        
        return obj_data

    def verify_object_integrity(self, obj_data: Dict, expected_checksum: str, checksum_type: str = "checksum_no_timestamps") -> Dict[str, Any]:
        """
        Verify the integrity of a MicroStrategy object against an expected checksum.
        
        Args:
            obj_data: MicroStrategy object data
            expected_checksum: Expected checksum value
            checksum_type: Type of checksum to verify against
            
        Returns:
            Dictionary with verification results
        """
        checksums = self.generate_object_checksums(obj_data)
        current_checksum = checksums.get(checksum_type)
        
        return {
            "is_valid": current_checksum == expected_checksum,
            "expected_checksum": expected_checksum,
            "current_checksum": current_checksum,
            "checksum_type": checksum_type,
            "all_checksums": checksums
        }


if __name__ == "__main__":
    # Example usage
    handler = json_checksum_handler()
    
    sample_data = {
        "name": "Product",
        "version": "1.0.0",
        "metadata": {
            "created": "2024-01-01",
            "modified": "2024-01-02",
            "author": "John"
        },
        "data": {
            "items": [1, 2, 3],
            "count": 3
        }
    }

    # Generate checksum ignoring metadata
    checksum1 = handler.json_checksum(sample_data, ignore_keys=["metadata", "version"])
    print(f"Checksum (ignoring metadata, version): {checksum1}")

    # Generate full checksum
    checksum2 = handler.json_checksum(sample_data)
    print(f"Full checksum: {checksum2}")

    # Generate MicroStrategy object checksums
    mstr_checksums = handler.generate_object_checksums(sample_data)
    print(f"MicroStrategy checksums: {mstr_checksums}")