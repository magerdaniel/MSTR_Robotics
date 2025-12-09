import json
import hashlib
from typing import Any, Dict, List, Set, Union
from pathlib import Path


def filter_json_keys(data: Any, ignore_keys: Set[str]) -> Any:
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
                result[key] = filter_json_keys(value, ignore_keys)
        return result

    # If it's a list, recursively process each item
    elif isinstance(data, list):
        result = []
        for item in data:
            result.append(filter_json_keys(item, ignore_keys))
        return result

    # If it's a primitive (string, number, bool, None), return as-is
    else:
        return data


def json_checksum(
    data: Union[Dict, List, str, Path],
    ignore_keys: List[str] = None,
    algorithm: str = "sha256"
) -> str:
    """
    Generate a checksum for JSON data with option to ignore specific keys.

    Args:
        data: JSON data as dict/list, JSON string, or file path
        ignore_keys: List of keys to ignore (including all their children)
        algorithm: Hash algorithm (md5, sha1, sha256, sha512)

    Returns:
        Hexadecimal checksum string

    Example:
        >>> data = {"name": "John", "age": 30, "metadata": {"created": "2024-01-01"}}
        >>> checksum = json_checksum(data, ignore_keys=["metadata"])
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
    filtered_data = filter_json_keys(data, ignore_keys_set)

    # Convert to canonical JSON string (sorted keys, no whitespace)
    canonical_json = json.dumps(filtered_data, sort_keys=True, ensure_ascii=False)

    # Generate checksum
    hash_func = hashlib.new(algorithm)
    hash_func.update(canonical_json.encode('utf-8'))

    return hash_func.hexdigest()


def compare_json_files(
    file1: Union[str, Path],
    file2: Union[str, Path],
    ignore_keys: List[str] = None,
    algorithm: str = "sha256"
) -> Dict[str, Any]:
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
        >>> result = compare_json_files("file1.json", "file2.json", ignore_keys=["timestamp"])
        >>> print(result["are_equal"])
    """
    checksum1 = json_checksum(file1, ignore_keys, algorithm)
    checksum2 = json_checksum(file2, ignore_keys, algorithm)

    return {
        "are_equal": checksum1 == checksum2,
        "file1_checksum": checksum1,
        "file2_checksum": checksum2,
        "algorithm": algorithm,
        "ignored_keys": ignore_keys or []
    }


def batch_checksum(
    file_paths: List[Union[str, Path]],
    ignore_keys: List[str] = None,
    algorithm: str = "sha256"
) -> Dict[str, str]:
    """
    Generate checksums for multiple JSON files.

    Args:
        file_paths: List of JSON file paths
        ignore_keys: List of keys to ignore
        algorithm: Hash algorithm to use

    Returns:
        Dictionary mapping file paths to their checksums

    Example:
        >>> files = ["file1.json", "file2.json", "file3.json"]
        >>> checksums = batch_checksum(files, ignore_keys=["timestamp", "version"])
        >>> for file, checksum in checksums.items():
        ...     print(f"{file}: {checksum}")
    """
    results = {}

    for file_path in file_paths:
        path = Path(file_path)
        try:
            checksum = json_checksum(path, ignore_keys, algorithm)
            results[str(path)] = checksum
        except Exception as e:
            results[str(path)] = f"ERROR: {str(e)}"

    return results


if __name__ == "__main__":
    # Example usage
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
    checksum1 = json_checksum(sample_data, ignore_keys=["metadata", "version"])
    print(f"Checksum (ignoring metadata, version): {checksum1}")

    # Generate full checksum
    checksum2 = json_checksum(sample_data)
    print(f"Full checksum: {checksum2}")
