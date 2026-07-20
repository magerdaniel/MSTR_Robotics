"""
Script to load MicroStrategy data to Microsoft Fabric Lakehouse
Supports multiple tables using Delta Lake format via Spark or direct file writes
"""

import pandas as pd
import json
import yaml
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
from mstrio.connection import Connection
from mstr_robotics import report
from mstr_robotics._connectors import mstr_api
from mstr_robotics._paths import USER_CONFIG

import platform

# Check if Spark is available and usable on this platform
SPARK_AVAILABLE = False
if platform.system() != 'Windows':
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        SPARK_AVAILABLE = True
    except ImportError:
        print("Warning: PySpark not available. Will use file-based loading only.")
else:
    print("Note: Running on Windows - using Delta Lake native writer (deltalake library).")

# Check if deltalake library is available for proper Delta table writes
DELTALAKE_AVAILABLE = False
try:
    from deltalake import write_deltalake, DeltaTable
    DELTALAKE_AVAILABLE = True
    print("Delta Lake native writer available - tables will have proper Delta metadata.")
except ImportError:
    print("Warning: deltalake library not available. Install with: pip install deltalake")
    print("Falling back to Parquet mode (may not be recognized as Delta tables).")


class FabricLakehouseLoader:
    """Loader class for Microsoft Fabric Lakehouse"""

    def __init__(self, lakehouse_path: str, workspace_id: Optional[str] = None,
                 lakehouse_id: Optional[str] = None, use_spark: bool = True):
        """
        Initialize Fabric Lakehouse connection

        Args:
            lakehouse_path: Local or OneLake path to lakehouse
                Examples:
                - Local/mounted: "C:/Users/user/OneLake/workspace/lakehouse.Lakehouse/Tables"
                - OneLake ABFS: "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables"
            workspace_id: Optional workspace GUID for API operations
            lakehouse_id: Optional lakehouse GUID for API operations
            use_spark: Whether to use Spark for Delta table operations (recommended)
        """
        self.lakehouse_path = lakehouse_path
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.use_spark = use_spark and SPARK_AVAILABLE
        self.spark = None

        # Ensure path exists if local
        if not lakehouse_path.startswith('abfss://'):
            Path(lakehouse_path).mkdir(parents=True, exist_ok=True)

    def connect(self):
        """Initialize Spark session for Delta operations"""
        if self.use_spark and not self.spark:
            try:
                # Configure Spark with Delta Lake
                builder = SparkSession.builder \
                    .appName("FabricLakehouseLoader") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
                print("Successfully initialized Spark session with Delta Lake support")
                return self.spark
            except Exception as e:
                print(f"Error initializing Spark: {e}")
                print("Falling back to file-based operations")
                self.use_spark = False
        return None

    def disconnect(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            self.spark = None
            print("Stopped Spark session")

    def write_delta_table_spark(self, table_name: str, data: Any, columns: List[str],
                                mode: str = "overwrite"):
        """
        Write Delta table using Spark (recommended for Lakehouse)

        Args:
            table_name: Name of the Delta table
            data: Numpy array, pandas DataFrame, or list of lists
            columns: List of column names
            mode: Write mode - "overwrite", "append", "ignore", or "error"
        """
        if not self.spark:
            self.connect()

        try:
            # Convert to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)

            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)

            # Write as Delta table
            table_path = os.path.join(self.lakehouse_path, table_name)

            spark_df.write \
                .format("delta") \
                .mode(mode) \
                .save(table_path)

            print(f"Successfully wrote {len(df)} rows to Delta table: {table_name}")
            print(f"Table location: {table_path}")

        except Exception as e:
            print(f"Error writing Delta table with Spark: {e}")
            raise

    def write_delta_table_native(self, table_name: str, data: Any, columns: List[str],
                                 mode: str = "overwrite"):
        """
        Write Delta table using deltalake library (works on Windows)

        Args:
            table_name: Name of the Delta table
            data: Numpy array, pandas DataFrame, or list of lists
            columns: List of column names
            mode: Write mode - "overwrite" or "append"
        """
        try:
            # Convert to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)

            # Create table directory
            table_path = os.path.join(self.lakehouse_path, table_name)

            # Write Delta table with proper metadata
            write_deltalake(
                table_path,
                df,
                mode=mode,
                engine='pyarrow'
            )

            print(f"Successfully wrote {len(df)} rows to Delta table: {table_name}")
            print(f"Table location: {table_path}")

        except Exception as e:
            print(f"Error writing Delta table: {e}")
            raise

    def write_parquet_table(self, table_name: str, data: Any, columns: List[str],
                           mode: str = "overwrite"):
        """
        Write table as Parquet files (fallback when Delta not available)

        Args:
            table_name: Name of the table
            data: Numpy array, pandas DataFrame, or list of lists
            columns: List of column names
            mode: Write mode - "overwrite" or "append"
        """
        try:
            # Convert to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)

            # Create table directory
            table_path = os.path.join(self.lakehouse_path, table_name)
            os.makedirs(table_path, exist_ok=True)

            # Write as Parquet
            if mode == "overwrite":
                # Clear existing files
                for file in Path(table_path).glob("*.parquet"):
                    file.unlink()

                parquet_file = os.path.join(table_path, f"{table_name}.parquet")
                df.to_parquet(parquet_file, index=False, engine='pyarrow')
            elif mode == "append":
                # Append with timestamped filename
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                parquet_file = os.path.join(table_path, f"{table_name}_{timestamp}.parquet")
                df.to_parquet(parquet_file, index=False, engine='pyarrow')

            print(f"Successfully wrote {len(df)} rows to Parquet table: {table_name}")
            print(f"Table location: {table_path}")

        except Exception as e:
            print(f"Error writing Parquet table: {e}")
            raise

    def load_table(self, table_name: str, data: Any, columns: List[str],
                   mode: str = "overwrite"):
        """
        Load data to Lakehouse table (Delta or Parquet)

        Args:
            table_name: Name of the target table
            data: Numpy array, pandas DataFrame, or list of lists
            columns: List of column names
            mode: Write mode - "overwrite" or "append"
        """
        if self.use_spark:
            # Use Spark for Delta (Linux/Mac)
            self.write_delta_table_spark(table_name, data, columns, mode)
        elif DELTALAKE_AVAILABLE:
            # Use native Delta writer (Windows-compatible)
            self.write_delta_table_native(table_name, data, columns, mode)
        else:
            # Fallback to Parquet only
            self.write_parquet_table(table_name, data, columns, mode)


def load_mstr_reports_to_fabric(mstr_conn: Connection, fabric_loader: FabricLakehouseLoader,
                                 report_configs: List[Dict[str, Any]]):
    """
    Load multiple MicroStrategy reports to Fabric Lakehouse

    Args:
        mstr_conn: MicroStrategy connection object
        fabric_loader: FabricLakehouseLoader instance (already connected)
        report_configs: List of dictionaries with report configurations
            Example: [
                {
                    'report_id': 'C4FF6CF34933EF4B3B1D798D02D4FB36',
                    'table_name': 'mstr_sales_data',
                    'columns': ['Region', 'Category', 'Call_Center', 'Last_Name', 'First_Name'],
                    'mode': 'overwrite',  # or 'append'
                    'prompts': None  # Optional: prompt answers if report has prompts
                }
            ]
    """
    i_rep = report.rep()

    for config in report_configs:
        report_id = config['report_id']
        table_name = config['table_name']
        columns = config['columns']
        mode = config.get('mode', 'overwrite')
        prompts = config.get('prompts', None)

        print(f"\n{'='*60}")
        print(f"Processing report: {report_id}")
        print(f"Target table: {table_name}")
        print(f"Mode: {mode}")
        print(f"{'='*60}")

        try:
            # Open instance
            instance_id = i_rep.open_Instance(conn=mstr_conn, report_id=report_id)

            # Handle prompts if provided
            if prompts:
                prompt_answ = json.dumps(prompts)
                i_rep.set_inst_prompt_ans(
                    conn=mstr_conn,
                    report_id=report_id,
                    instance_id=instance_id,
                    prompt_answ=prompt_answ
                )

            # Get report data
            report_data = i_rep.report_dict(
                conn=mstr_conn,
                report_id=report_id,
                instance_id=instance_id
            )

            print(f"Retrieved {len(report_data)} rows from MicroStrategy")

            # Load to Fabric Lakehouse
            fabric_loader.load_table(
                table_name=table_name,
                data=report_data,
                columns=columns,
                mode=mode
            )

            print(f"Successfully loaded {table_name}")

        except Exception as e:
            print(f"Error processing report {report_id}: {e}")
            continue


# Example usage
if __name__ == "__main__":

    # 1. Load MicroStrategy configuration
    config_path = USER_CONFIG
    with open(config_path, 'r') as file:
        user_d = yaml.safe_load(file)

    # 2. Connect to MicroStrategy
    base_url = "http://217.154.213.84:8080/MicroStrategyLibrary"
    username = user_d["conn_params"]["username"]
    password = user_d["conn_params"]["password"]
    project_id = "B7CA92F04B9FAE8D941C3E9B7E0CD754"

    mstr_conn = Connection(
        base_url=base_url,
        username=username,
        password=password,
        project_id=project_id
    )
    mstr_conn.headers['Content-type'] = "application/json"

    # 3. Configure Fabric Lakehouse path
    # Option 1: Local OneLake mount path (if OneLake is mounted locally)
    lakehouse_path = "C:\\Users\\danie\\OneLake - Microsoft\\MSTR_db\\dans_lake.Lakehouse\\Tables"

    # Option 2: ABFS path for direct cloud access
    # lakehouse_path = "abfss://YourWorkspace@onelake.dfs.fabric.microsoft.com/YourLakehouse.Lakehouse/Tables"

    # Optional: Workspace and Lakehouse IDs for API operations
    workspace_id = None  # "YOUR-WORKSPACE-GUID"
    lakehouse_id = None  # "YOUR-LAKEHOUSE-GUID"

    # 4. Initialize Fabric Lakehouse loader
    fabric_loader = FabricLakehouseLoader(
        lakehouse_path=lakehouse_path,
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
        use_spark=False  # Windows doesn't support local Spark - use Parquet mode
    )
    # No need to connect() for Parquet mode
    # fabric_loader.connect()

    try:
        # 5. Define reports to load
        report_configs = [
            {
                'report_id': 'C4FF6CF34933EF4B3B1D798D02D4FB36',
                'table_name': 'pa_rep_data',
                'columns': ['Region', 'Category', 'Call_Center', 'Last_Name', 'First_Name'],
                'mode': 'overwrite'
            },
            # Add more reports here as needed
            # {
            #     'report_id': 'ANOTHER_REPORT_ID',
            #     'table_name': 'another_table',
            #     'columns': ['col1', 'col2', 'col3'],
            #     'mode': 'overwrite',  # or 'append'
            #     'prompts': {"prompts": [{"id": "PROMPT_ID", "type": "ELEMENTS", "answers": [...]}]}
            # }
        ]

        # 6. Load all reports to Fabric Lakehouse
        load_mstr_reports_to_fabric(mstr_conn, fabric_loader, report_configs)

    finally:
        # 7. Clean up connections
        fabric_loader.disconnect()
        print("\nAll operations completed!")
