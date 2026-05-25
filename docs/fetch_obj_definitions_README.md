# Object Definition Fetcher Tool

A tool for fetching MicroStrategy object definitions by providing a list of GUIDs.

## Overview

This tool automatically:
1. Takes a list of object GUIDs
2. Retrieves type and subtype information for each object
3. Fetches the complete object definition using the appropriate API endpoint
4. Handles errors gracefully
5. Optionally saves results to JSON files

## Installation

The tool is part of the `mstr_robotics` package. No additional installation needed.

## Quick Start

```python
from mstr_robotics.fetch_obj_definitions import fetch_object_definitions

# Your list of GUIDs
guid_list = [
    '002A171A4498279551A3E1BACD888AAD',
    '005775B34E14BCE596C6088743A3E6A6',
    '005B9AA84DF6D55531C141ABB5FE6A4F'
]

# Fetch definitions
results = fetch_object_definitions(conn, guid_list, save_to_file=True)

# Check results
print(f"Successfully fetched: {results['metadata']['successful_count']} objects")
print(f"Errors: {results['metadata']['error_count']}")
```

## Usage

### Basic Usage

```python
from mstr_robotics.fetch_obj_definitions import fetch_object_definitions

results = fetch_object_definitions(
    conn=conn,                          # Your MicroStrategy connection
    guid_list=['GUID1', 'GUID2', ...],  # List of object GUIDs
    save_to_file=False,                 # Optional: save to JSON files
    output_dir="./obj_definitions"      # Optional: output directory
)
```

### Advanced Usage with Class

```python
from mstr_robotics.fetch_obj_definitions import ObjectDefinitionFetcher

# Create fetcher instance
fetcher = ObjectDefinitionFetcher()

# Fetch definitions
results = fetcher.fetch_definitions(
    conn=conn,
    guid_list=guid_list,
    save_to_file=True,
    output_dir="./my_definitions"
)

# Get summary of object types
fetcher.get_type_summary()
```

## Return Value

The function returns a dictionary with the following structure:

```python
{
    'successful': [
        {
            'object_id': 'GUID',
            'object_type': 'type_number',
            'object_subtype': 'subtype_number',
            'definition': { ... }  # Full object definition
        },
        ...
    ],
    'errors': [
        {
            'object_id': 'GUID',
            'object_type': 'type_number',
            'object_subtype': 'subtype_number',
            'error': 'error message'
        },
        ...
    ],
    'not_mapped': [
        {
            'object_id': 'GUID',
            'obj_type': 'type_number',
            'obj_sub_type': 'subtype_number'
        },
        ...
    ],
    'metadata': {
        'project_id': 'PROJECT_GUID',
        'total_requested': 10,
        'successful_count': 8,
        'error_count': 1,
        'not_mapped_count': 1,
        'timestamp': '2026-04-18T...'
    }
}
```

## Supported Object Types

The tool supports fetching definitions for:

- **Attributes** (type 12, subtype 3072)
- **Facts** (type 13, subtype 3328)
- **Metrics** (subtype 1024)
- **Filters** (subtype 256)
- **Tables** (type 15, subtypes 3840, 3842)
- **Hierarchies** (type 14)
- **Reports** (subtypes 768-778)
- **Cubes** (subtypes 776, 779, 780)
- **Dashboards** (subtype 14081)
- **Transformations** (type 43)
- **Security Filters** (type 58)
- **Prompts** (type 10)
- **Custom Groups** (subtype 257)
- **Consolidations** (type 47)
- **Derived Elements** (type 48)
- And more...

## File Output

When `save_to_file=True`, the tool creates:

1. **all_results_TIMESTAMP.json** - Complete results including metadata and errors
2. **GUID.json** - Individual object definition files (one per successful fetch)
3. **errors_TIMESTAMP.json** - Error details (if any errors occurred)

## Examples

### Example 1: Fetch from a list

```python
guid_list = [
    '002A171A4498279551A3E1BACD888AAD',
    '005775B34E14BCE596C6088743A3E6A6'
]

results = fetch_object_definitions(conn, guid_list)

for obj in results['successful']:
    obj_name = obj['definition'].get('information', {}).get('name', 'Unknown')
    print(f"{obj['object_id']}: {obj_name}")
```

### Example 2: Read GUIDs from file

```python
# Read GUIDs from text file (one per line)
with open('guid_list.txt', 'r') as f:
    guid_list = [line.strip() for line in f if line.strip()]

results = fetch_object_definitions(
    conn=conn,
    guid_list=guid_list,
    save_to_file=True,
    output_dir="./batch_fetch"
)
```

### Example 3: Process specific object types

```python
results = fetch_object_definitions(conn, guid_list)

# Extract attribute information
for obj in results['successful']:
    if obj['object_subtype'] == '3072':  # Attribute
        definition = obj['definition']
        name = definition.get('information', {}).get('name')
        forms = definition.get('forms', [])
        print(f"Attribute: {name}")
        for form in forms:
            print(f"  - Form: {form.get('name')}")
```

### Example 4: Handle errors

```python
results = fetch_object_definitions(conn, guid_list)

if results['errors']:
    print("The following objects had errors:")
    for error in results['errors']:
        print(f"  {error['object_id']}: {error['error']}")

if results['not_mapped']:
    print("\nThe following object types are not yet supported:")
    for obj in results['not_mapped']:
        print(f"  Type: {obj['obj_type']}, Subtype: {obj['obj_sub_type']}")
```

## Integration with Existing read_gen Class

This tool is built on top of the existing `read_gen` class from `read_out_prj_obj.py`. It uses:

- `read_gen.get_proj_obj_def_by_id_l()` - Main method for fetching definitions
- `read_gen.get_obj_def()` - Fetches individual object definitions
- `mstr_api.get_proj_obj_by_id_l()` - Gets object type/subtype information

## Error Handling

The tool handles errors at multiple levels:

1. **Metadata retrieval errors** - If object type/subtype cannot be determined
2. **Definition fetch errors** - If the API call fails for a specific object
3. **Unmapped types** - Objects whose type/subtype combination is not yet supported

All errors are captured in the results dictionary for review.

## Performance Considerations

- The tool fetches objects sequentially to avoid overwhelming the API
- Progress is printed to console for monitoring
- For large batches, consider running in smaller chunks

## See Also

- `examples/example_fetch_definitions.py` - Complete working examples
- `read_out_prj_obj.py` - Underlying implementation details
