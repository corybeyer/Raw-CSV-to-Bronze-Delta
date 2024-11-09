class raw_csv_to_bronze_delta:
    def __init__(self, storage_account_name, container_name, input_folder, output_folder):
        """
        Initializes the RawToBronze class with paths for reading and writing files within Azure Storage.

        Args:
        - storage_account_name (str): Azure storage account name.
        - container_name (str): Container name in the Azure storage account.
        - input_folder (str): Folder name containing the input raw files.
        - output_folder (str): Folder name for output Delta files in the Bronze layer.
        """
        # Store the storage account name for Azure authentication
        self.storage_account_name = storage_account_name
        # Store the container name where files are stored within the storage account
        self.container_name = container_name
        
        # Construct the path for the input files by combining the storage account, container name, and input folder
        # abfss:// is the protocol for accessing Azure Blob Storage from Spark
        self.input_file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{input_folder}/"
        
        # Construct the output path similarly, pointing to where the processed Bronze files will be stored
        self.output_file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{output_folder}/"

    def read_raw_csv(self, file_name):
        """
        Reads the raw CSV file from Azure storage.

        Args:
        - file_name (str): The name of the CSV file to be read.
        
        Returns:
        - df (DataFrame): Spark DataFrame containing the raw CSV data.
        """
        # Log message to indicate start of file reading
        print(f"[{file_name}] Reading raw CSV file from {self.input_file_path}{file_name}...")
        
        # Use Spark to read the CSV file, configuring options for handling special cases in CSV parsing
        df = (
            spark.read.format("csv")
            .option("header", "true")  # First row of the file is treated as a header
            .option("quote", "\"")     # Specifies the quote character for quoted fields
            .option("escape", "\"")    # Specifies the escape character to handle quotes within fields
            .option("multiLine", "true")  # Allows parsing multi-line fields in CSV
            .option("mode", "PERMISSIVE")  # Silently ignores corrupt records, allowing for lenient reading
            .load(f"{self.input_file_path}{file_name}")  # Load the file from the specified path
        )
        
        # Log message to confirm successful file read
        print(f"[{file_name}] Raw CSV file read successfully.")
        return df

    def display_data(self, df, file_name):
        """
        Displays the DataFrame for visual verification of data.

        Args:
        - df (DataFrame): The DataFrame to display.
        - file_name (str): The name of the file for logging.
        """
        # Log message for data display action
        print(f"[{file_name}] Displaying data for verification.")
        
        # Display DataFrame in notebook or console for visual inspection to verify data content and structure
        display(df)

    def define_output_path(self, file_name):
        """
        Defines the output path for the Delta table.

        Args:
        - file_name (str): The name of the CSV file being processed.
        
        Returns:
        - bronze_output_path (str): Path where the Delta table will be saved.
        """
        # Remove file extension from file name to create a folder name for the output path
        bronze_output_path = f"{self.output_file_path}{file_name.split('.')[0]}"
        
        # Log the defined output path for debugging and verification
        print(f"[{file_name}] Defined Bronze output path: {bronze_output_path}")
        return bronze_output_path

    def write_to_delta(self, df, output_path, partition_cols=None, mode="overwrite", file_name=None):
        """
        Writes the DataFrame to Delta format at the specified output path, with optional partitioning.

        Args:
        - df (DataFrame): The DataFrame to write.
        - output_path (str): The path where the Delta table will be saved.
        - partition_cols (list, optional): Columns to partition the Delta table by; default is None.
        - mode (str): Write mode, either "overwrite" or "append".
        - file_name (str): The name of the file for logging.
        """
        # Check if partition columns were specified, affecting data organization in the Delta table
        if partition_cols:
            # Log action: writing with partitioning for optimized storage
            print(f"[{file_name}] Writing data to Bronze Delta table with partitioning by {partition_cols} in '{mode}' mode...")
            
            # Write to Delta format, partitioning the data for efficient query performance
            df.write.format("delta") \
                .mode(mode) \
                .partitionBy(*partition_cols) \
                .save(output_path)
        else:
            # Log action: writing without partitioning
            print(f"[{file_name}] Writing data to Bronze Delta table without partitioning in '{mode}' mode...")
            
            # Write to Delta format without any partitioning
            df.write.format("delta") \
                .mode(mode) \
                .save(output_path)
        
        # Confirm the data was successfully written to the specified Delta location
        print(f"[{file_name}] Data written to Bronze Delta table at {output_path} successfully.")

    def create_bronze_delta_table(self, file_name, partition_cols=None, mode="overwrite"):
        """
        Processes a raw CSV file by chaining the steps to read, display, and write it as a Delta table.

        Args:
        - file_name (str): The name of the CSV file to be processed.
        - partition_cols (list, optional): Columns to partition the Delta table by; default is None.
        - mode (str): Write mode, either "overwrite" or "append".
        """
        # Start processing for the specified file, logging the beginning of the process
        print(f"[{file_name}] Starting Bronze processing.")
        
        # Step 1: Read the CSV file into a DataFrame
        df = self.read_raw_csv(file_name)
        
        # Step 2: Display the data in the DataFrame for visual verification of content
        self.display_data(df, file_name)
        
        # Step 3: Define the output path to save the Bronze Delta table based on the input file name
        bronze_output_path = self.define_output_path(file_name)
        
        # Step 4: Write the DataFrame to Delta format, partitioned if specified, otherwise in default configuration
        self.write_to_delta(df, bronze_output_path, partition_cols, mode, file_name)

    def processor(self, file_name, partition_cols=None, mode="overwrite"):
        """
        Chains the methods to process the raw CSV file and save it as a Delta table in the Bronze layer.

        Args:
        - file_name (str): The name of the CSV file to be processed.
        - partition_cols (list, optional): List of columns to partition the Delta table by; default is None.
        - mode (str): Write mode, either "overwrite" or "append".
        """
        # Log initiation of the processing workflow, specifying the write mode
        print(f"[{file_name}] Initiating processing workflow for Bronze layer with '{mode}' mode.")
        
        # Run the full workflow from reading to writing the file into the Bronze layer Delta table
        self.create_bronze_delta_table(file_name, partition_cols, mode)
        
        # Log successful completion of the workflow for tracking and confirmation
        print(f"[{file_name}] Processing workflow completed.")
