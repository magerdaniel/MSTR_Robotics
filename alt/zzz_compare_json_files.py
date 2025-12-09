import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from deepdiff import DeepDiff
from dataclasses import dataclass
import re

@dataclass
class DiffResult:
    """Data class to store diff analysis results"""
    id: str
    name: str
    subType: str
    key_path_to_diff: str
    original_value: Any
    comparison_value: Any
    diff_val: str
    path_to_org_file: str
    path_to_compare_file: str

class JSONDiffAnalyzer:
    """
    A class to analyze differences between JSON files using DeepDiff
    and output results in tabular format.
    """
    
    def __init__(self, directory_path: str, comparison_directory_path: Optional[str] = None):
        """
        Initialize the analyzer with the directory containing JSON files.
        
        Args:
            directory_path: Path to directory containing JSON files
            comparison_directory_path: Optional path to second directory for cross-folder comparison
        """
        self.directory_path = Path(directory_path)
        self.comparison_directory_path = Path(comparison_directory_path) if comparison_directory_path else None
        self.results: List[DiffResult] = []
        
    def load_json_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Load and parse a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Parsed JSON data as dictionary
            
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If file is not valid JSON
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in file {file_path}: {str(e)}", e.doc, e.pos)
    
    def get_json_files(self, directory: Optional[Path] = None) -> List[Path]:
        """
        Get all JSON files from the directory.
        
        Args:
            directory: Optional directory path, defaults to self.directory_path
            
        Returns:
            List of Path objects for JSON files
        """
        target_dir = directory or self.directory_path
        
        if not target_dir.exists():
            raise FileNotFoundError(f"Directory not found: {target_dir}")
        
        json_files = list(target_dir.glob("*.json"))
        if not json_files:
            raise ValueError(f"No JSON files found in {target_dir}")
        
        return sorted(json_files)
    
    def extract_id_name_from_json(self, data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extract ID and name from JSON data.
        
        Args:
            data: JSON data dictionary
            
        Returns:
            Tuple of (id, name)
        """
        # Common fields that might contain ID
        id_fields = ['id', 'ID', '_id', 'identifier', 'key']
        # Common fields that might contain name
        name_fields = ['name', 'title', 'label', 'description']
        
        extracted_id = "unknown"
        extracted_name = "unknown"
        
        # Try to find ID
        for field in id_fields:
            if field in data:
                extracted_id = str(data[field])
                break
        
        # Try to find name
        for field in name_fields:
            if field in data:
                extracted_name = str(data[field])
                break
        
        return extracted_id, extracted_name
    
    def parse_diff_path(self, path: str) -> str:
        """
        Parse DeepDiff path format to a more readable format.
        
        Args:
            path: DeepDiff path string
            
        Returns:
            Cleaned path string
        """
        # Remove root[] wrapper and clean up the path
        cleaned = re.sub(r"root\['([^']+)'\]", r"\1", path)
        cleaned = re.sub(r"\[(\d+)\]", r"[\1]", cleaned)
        cleaned = cleaned.replace("']['", ".")
        return cleaned
    
    def analyze_diff_type(self, diff_type: str, path: str, details: Any) -> str:
        """
        Analyze the type of difference and create a description.
        
        Args:
            diff_type: Type of difference from DeepDiff
            path: Path where difference occurred
            details: Details of the difference
            
        Returns:
            Description of the difference
        """
        if diff_type == "values_changed":
            old_val = details.get('old_value', 'N/A')
            new_val = details.get('new_value', 'N/A')
            return f"Value changed from '{old_val}' to '{new_val}'"
        elif diff_type == "dictionary_item_added":
            return f"New item added: {details}"
        elif diff_type == "dictionary_item_removed":
            return f"Item removed: {details}"
        elif diff_type == "iterable_item_added":
            return f"Array item added: {details}"
        elif diff_type == "iterable_item_removed":
            return f"Array item removed: {details}"
        elif diff_type == "type_changes":
            old_type = type(details.get('old_value', '')).__name__
            new_type = type(details.get('new_value', '')).__name__
            return f"Type changed from {old_type} to {new_type}"
        else:
            return f"Change type: {diff_type}"
    
    def compare_files(self, file1: Path, file2: Path) -> List[DiffResult]:
        """
        Compare two JSON files and return diff results.
        
        Args:
            file1: Path to original file
            file2: Path to comparison file
            
        Returns:
            List of DiffResult objects
        """
        # Load JSON files
        data1 = self.load_json_file(file1)
        data2 = self.load_json_file(file2)
        
        # Extract ID and name from original file
        file_id, file_name = self.extract_id_name_from_json(data1)
        
        # Perform deep diff
        diff = DeepDiff(data1, data2, ignore_order=False, verbose_level=2)
        
        results = []
        
        # Process different types of changes
        for change_type, changes in diff.items():
            if isinstance(changes, dict):
                # Handle dictionary-type changes
                for path, details in changes.items():
                    clean_path = self.parse_diff_path(path)
                    
                    if change_type == "values_changed":
                        original_val = details.get('old_value', '')
                        comparison_val = details.get('new_value', '')
                    else:
                        original_val = details if change_type in ['dictionary_item_removed', 'iterable_item_removed'] else ''
                        comparison_val = details if change_type in ['dictionary_item_added', 'iterable_item_added'] else ''
                    
                    diff_description = self.analyze_diff_type(change_type, path, details)
                    
                    result = DiffResult(
                        id=file_id,
                        name=file_name,
                        subType=change_type,
                        key_path_to_diff=clean_path,
                        original_value=original_val,
                        comparison_value=comparison_val,
                        diff_val=diff_description,
                        path_to_org_file=str(file1),
                        path_to_compare_file=str(file2)
                    )
                    results.append(result)
            
            elif isinstance(changes, set):
                # Handle set-type changes (like iterable_item_added/removed)
                for item in changes:
                    clean_path = self.parse_diff_path(str(item)) if hasattr(item, '__str__') else str(item)
                    
                    result = DiffResult(
                        id=file_id,
                        name=file_name,
                        subType=change_type,
                        key_path_to_diff=clean_path,
                        original_value='',
                        comparison_value=item,
                        diff_val=f"{change_type}: {item}",
                        path_to_org_file=str(file1),
                        path_to_compare_file=str(file2)
                    )
                    results.append(result)
        
        return results
    
    def analyze_directory(self, comparison_strategy: str = "pairwise") -> pd.DataFrame:
        """
        Analyze all JSON files in the directory.
        
        Args:
            comparison_strategy: "pairwise" (compare each file with every other),
                               "sequential" (compare each file with the next),
                               or "cross_folder" (compare matching files between two folders)
            
        Returns:
            Pandas DataFrame with diff results
        """
        self.results = []
        
        if comparison_strategy == "cross_folder":
            if not self.comparison_directory_path:
                raise ValueError("comparison_directory_path must be provided for cross_folder strategy")
            
            # Get files from both directories
            original_files = self.get_json_files(self.directory_path)
            comparison_files = self.get_json_files(self.comparison_directory_path)
            
            # Create dictionaries with filenames as keys
            original_dict = {f.name: f for f in original_files}
            comparison_dict = {f.name: f for f in comparison_files}
            
            # Find matching files and compare them
            for filename in original_dict.keys():
                if filename in comparison_dict:
                    try:
                        results = self.compare_files(original_dict[filename], comparison_dict[filename])
                        self.results.extend(results)
                    except Exception as e:
                        print(f"Error comparing {filename}: {e}")
                else:
                    print(f"Warning: {filename} not found in comparison directory")
        
        else:
            # Original logic for single directory comparisons
            json_files = self.get_json_files()
            
            if len(json_files) < 2:
                raise ValueError("Need at least 2 JSON files for comparison")
            
            if comparison_strategy == "pairwise":
                # Compare every file with every other file
                for i, file1 in enumerate(json_files):
                    for j, file2 in enumerate(json_files):
                        if i < j:  # Avoid duplicate comparisons
                            try:
                                results = self.compare_files(file1, file2)
                                self.results.extend(results)
                            except Exception as e:
                                print(f"Error comparing {file1.name} with {file2.name}: {e}")
            
            elif comparison_strategy == "sequential":
                # Compare each file with the next one
                for i in range(len(json_files) - 1):
                    try:
                        results = self.compare_files(json_files[i], json_files[i + 1])
                        self.results.extend(results)
                    except Exception as e:
                        print(f"Error comparing {json_files[i].name} with {json_files[i + 1].name}: {e}")
        
        # Convert results to DataFrame
        if self.results:
            df = pd.DataFrame([
                {
                    'id': result.id,
                    'name': result.name,
                    'subType': result.subType,
                    'key_path_to_diff': result.key_path_to_diff,
                    'original_value': result.original_value,
                    'comparison_value': result.comparison_value,
                    'diff_val': result.diff_val,
                    'path_to_org_file': result.path_to_org_file,
                    'path_to_compare_file': result.path_to_compare_file
                }
                for result in self.results
            ])
        else:
            # Return empty DataFrame with correct columns if no diffs found
            df = pd.DataFrame(columns=[
                'id', 'name', 'subType', 'key_path_to_diff', 'original_value',
                'comparison_value', 'diff_val', 'path_to_org_file', 'path_to_compare_file'
            ])
        
        return df
    
    def save_results(self, output_path: str, format: str = "csv") -> None:
        """
        Save analysis results to file.
        
        Args:
            output_path: Path where to save the results
            format: Output format ("csv", "excel", "json")
        """
        if not self.results:
            raise ValueError("No analysis results available. Run analyze_directory() first.")
        
        df = pd.DataFrame([
            {
                'id': result.id,
                'name': result.name,
                'subType': result.subType,
                'key_path_to_diff': result.key_path_to_diff,
                'original_value': result.original_value,
                'comparison_value': result.comparison_value,
                'diff_val': result.diff_val,
                'path_to_org_file': result.path_to_org_file,
                'path_to_compare_file': result.path_to_compare_file
            }
            for result in self.results
        ])
        
        if format.lower() == "csv":
            df.to_csv(output_path, index=False, encoding='utf-8')
        elif format.lower() == "excel":
            df.to_excel(output_path, index=False, engine='openpyxl')
        elif format.lower() == "json":
            df.to_json(output_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        print(f"Results saved to {output_path}")


def run_json_diff_analysis(original_folder: str, comparison_folder: str) -> pd.DataFrame:
    try:
        # Initialize with both folders
        analyzer = JSONDiffAnalyzer(original_folder, comparison_folder)

        # Compare matching files between folders
        results_df = analyzer.analyze_directory(comparison_strategy="cross_folder")

        analyzer.save_results("D:/shared_drive/Python/mstr_robotics/json_compare/json_diff_analysis.csv", format="csv")
        pd.read_csv("D:/shared_drive/Python/mstr_robotics/json_compare/json_diff_analysis.csv")
        
        return results_df
    
    except Exception as e:
        print(f"Error during analysis: {e}")
    
# Example usage
if __name__ == "__main__":
    # Initialize analyzer
    original_folder = "D:/shared_drive/Python/mstr_robotics/json_compare/system1_json"
    comparison_folder = "D:/shared_drive/Python/mstr_robotics/json_compare/system2_json"

    run_json_diff_analysis(original_folder=original_folder, comparison_folder=comparison_folder)
