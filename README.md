#Overview

The `RawCsvToBronzeDelta` class facilitates the end-to-end processing of raw CSV files in Azure Data Lake Storage (ADLS), storing them in Delta format. The workflow includes reading a CSV file from ADLS, displaying it for verification, and writing it to a Delta table with optional upsert functionality to avoid duplicating records. This class is designed to simplify data ingestion tasks in Spark environments.

#Class Definition

```python
from delta.tables import DeltaTable

class RawCsvToBronzeDelta:
    """Processes raw CSV files and stores them in Delta format in Azure Data Lake Storage (ADLS)."""
    ...
```

##Key Attributes

*   `storage_account_name (str)`: Name of the Azure storage account.
*   `container_name (str)`: Name of the container in the storage account.
*   `input_file_path (str)`: Path for reading the CSV file from ADLS.
*   `output_file_path (str)`: Path for writing the Delta table in ADLS.
-------------------------------------------------------------------------------------------------

## Initialization
###Constructor: `__init__`
Sets up the class by constructing paths for reading and writing data.
```python
def __init__(self, storage_account_name, container_name, input_folder, output_folder, sub_folder=""):
    """
    Initializes paths for reading and writing data in Delta table format.

    Parameters:
        storage_account_name (str): Azure storage account name.
        container_name (str): Container name in the storage account.
        input_folder (str): Folder path for input CSV files.
        output_folder (str): Folder path for output Delta tables.
        sub_folder (str): Optional sub-folder for more granular data organization.
    """
```
####Example:
```python
processor = RawCsvToBronzeDelta(
    storage_account_name="my_storage_account",
    container_name="my_container",
    input_folder="input_data",
    output_folder="bronze_data"
)
```
-------------------------------------------------------------------------------------------------
## Methods
### `_construct_path`
Constructs a path for a folder (with optional sub-folder) within the ADLS structure.
```python
def _construct_path(self, folder, sub_folder=""):
    """
    Constructs the full ADLS path for input/output folders.

    Parameters:
        folder (str): Main folder path.
        sub_folder (str): Optional sub-folder.

    Returns:
        str: Full ADLS path.
    """
```
#### Example
Return: `abfss://my_container@my_storage_account.dfs.core.windows.net/input_data/`

### `log`
Logs actions for tracking file processing stages.
```python
def log(self, file_name, message):
    """
    Logs a message related to the current processing file.

    Parameters:
        file_name (str): Name of the file being processed.
        message (str): Message to log.
    """
```
### `read_raw_csv`
Reads a CSV file from ADLS into a Spark DataFrame with the specified formatting options.
```python
def read_raw_csv(self, file_name):
    """
    Reads a raw CSV file from ADLS into a Spark DataFrame.

    Parameters:
        file_name (str): CSV file name.

    Returns:
        DataFrame: Spark DataFrame containing the CSV data.
    """
```
####Options Explained:
*   `header="true"`: Assumes the CSV has a header row.
*   `multiLine="true"`: Allows for multi-line values in cells.
*   `mode="PERMISSIVE"`: Loads rows even if they have formatting issues.

### `display_data`
Displays the loaded DataFrame for visual verification. Useful for a quick visual inspection to confirm data accuracy.
```python
def display_data(self, df, file_name):
    """
    Displays the DataFrame content for verification.

    Parameters:
        df (DataFrame): DataFrame to display.
        file_name (str): Name of the file being processed.
    """
```

#### `_upsert_to_delta`
Performs an upsert (merge) operation to update existing rows with matching primary keys and insert new rows.
```python
def _upsert_to_delta(self, df, primary_key, file_name):
    """
    Upserts the DataFrame to the Delta table by merging on the primary key.

    Parameters:
        df (DataFrame): DataFrame to upsert.
        primary_key (str): Column name to use as the primary key.
        file_name (str): Name of the file being processed.
    """
```
* Merge Condition: The `primary_key` column is used to match records between the source (CSV) and target (Delta table).

### `write_to_delta`
Writes the DataFrame to Delta format with either "overwrite" or "upsert" modes. Optionally, partitions data by specified columns.
```python
def write_to_delta(self, df, mode="overwrite", primary_key=None, partition_cols=None, file_name=""):
    """
    Writes DataFrame to Delta table, with optional upsert and partitioning.

    Parameters:
        df (DataFrame): DataFrame to write.
        mode (str): Write mode, either "overwrite" or "upsert".
        primary_key (str): Primary key column for upsert operations.
        partition_cols (list): Columns for partitioning the Delta table.
        file_name (str): Name of the file being processed.
    """
```
*   **Modes**:
    *   `"overwrite"`: Replaces the entire Delta table with new data.
    *   `"upsert"`: Updates existing data while preserving existing rows.
*   **Partitioning**: Enhances query performance by dividing data into logical segments.

### `process_file`
Orchestrates the entire process of reading, displaying, and writing the CSV to Delta, following the specified mode and options.
``` python
def process_file(self, file_name, mode="overwrite", primary_key=None, partition_cols=None, sub_folder=""):
    """
    Runs the complete CSV-to-Delta workflow.

    Parameters:
        file_name (str): Name of the CSV file to process.
        mode (str): Write mode, either "overwrite" or "upsert".
        primary_key (str): Primary key column for upsert.
        partition_cols (list): Columns to partition Delta table by.
        sub_folder (str): Optional sub-folder for organized file storage.
    """
```
