"""
Example: How to use the fetch_obj_definitions tool
"""
from mstr_robotics.fetch_obj_definitions import fetch_object_definitions, ObjectDefinitionFetcher
# from mstr_robotics._connectors import mstr_api  # Uncomment when you have your connection setup

# Example 1: Quick usage with the convenience function
def example_quick_fetch():
    """Simple example using the convenience function"""
    
    # Your list of GUIDs
    guid_list = [
        '002A171A4498279551A3E1BACD888AAD',
        '005775B34E14BCE596C6088743A3E6A6',
        '005B9AA84DF6D55531C141ABB5FE6A4F',
        # Add more GUIDs here...
    ]
    
    # Fetch definitions (replace 'conn' with your actual connection object)
    # results = fetch_object_definitions(conn, guid_list, save_to_file=True)
    
    # Access results
    # print(f"Successfully fetched: {results['metadata']['successful_count']} objects")
    # print(f"Errors: {results['metadata']['error_count']}")
    
    # Access individual definitions
    # for obj in results['successful']:
    #     obj_id = obj['object_id']
    #     obj_name = obj['definition'].get('information', {}).get('name', 'Unknown')
    #     print(f"{obj_id}: {obj_name}")


# Example 2: Using the class for more control
def example_detailed_fetch():
    """Example using the ObjectDefinitionFetcher class directly"""
    
    guid_list = [
        '002A171A4498279551A3E1BACD888AAD',
        '005775B34E14BCE596C6088743A3E6A6',
    ]
    
    # Create fetcher instance
    fetcher = ObjectDefinitionFetcher()
    
    # Fetch definitions
    # results = fetcher.fetch_definitions(
    #     conn=conn,  # Your connection object
    #     guid_list=guid_list,
    #     save_to_file=True,
    #     output_dir="./my_definitions"
    # )
    
    # Get type summary
    # fetcher.get_type_summary()
    
    # Access specific results
    # for error in results['errors']:
    #     print(f"Error for {error['object_id']}: {error['error']}")
    
    # for obj in results['not_mapped']:
    #     print(f"Not mapped - Type: {obj['obj_type']}, Subtype: {obj['obj_sub_type']}")


# Example 3: Reading GUIDs from a file
def example_fetch_from_file():
    """Example reading GUIDs from a text file"""
    
    # Read GUIDs from file (one per line)
    with open('guid_list.txt', 'r') as f:
        guid_list = [line.strip() for line in f if line.strip()]
    
    # Fetch definitions
    # results = fetch_object_definitions(
    #     conn=conn,
    #     guid_list=guid_list,
    #     save_to_file=True,
    #     output_dir="./batch_definitions"
    # )


# Example 4: Processing results
def example_process_results():
    """Example of processing the results"""
    
    guid_list = ['002A171A4498279551A3E1BACD888AAD']
    
    # results = fetch_object_definitions(conn, guid_list)
    
    # Group by object type
    # by_type = {}
    # for obj in results['successful']:
    #     obj_type = obj['object_type']
    #     if obj_type not in by_type:
    #         by_type[obj_type] = []
    #     by_type[obj_type].append(obj)
    
    # print(f"Found {len(by_type)} different object types")
    
    # Extract specific information from attributes
    # for obj in results['successful']:
    #     if obj['object_subtype'] == '3072':  # Attribute
    #         definition = obj['definition']
    #         name = definition.get('information', {}).get('name')
    #         forms = definition.get('forms', [])
    #         print(f"Attribute: {name}, Forms: {len(forms)}")


if __name__ == "__main__":
    print("Examples for using fetch_obj_definitions tool")
    print("=" * 60)
    print("\nUncomment the function calls below and provide your connection object")
    print("to run the examples:")
    print("\n1. example_quick_fetch()")
    print("2. example_detailed_fetch()")
    print("3. example_fetch_from_file()")
    print("4. example_process_results()")
