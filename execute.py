# Example execution
# Initialize the BronzeFileProcessor class with Azure storage details, specifying input and output folders
bronze_processor = raw_csv_to_bronze_delta(storage_account_name="stkdieusnp03", 
                                       container_name="kdianalytics-dev03",
                                       input_folder="microsoft-planner-data/report_data/",
                                       output_folder="microsoft-planner-data/bronze_deltas")

# Process files by calling the process_file method, specifying any needed partitioning details
bronze_processor.processor("kdi_msplanner_assignee.csv", partition_cols= None)
bronze_processor.processor("kdi_msplanner_buckets.csv", partition_cols= None)
bronze_processor.processor("kdi_msplanner_plans.csv", partition_cols= None)
bronze_processor.processor("kdi_msplanner_checklist.csv", partition_cols= None)
bronze_processor.processor("kdi_msplanner_sharepoint.csv", partition_cols= None)
bronze_processor.processor("kdi_msplanner_tasks.csv", partition_cols= None)
