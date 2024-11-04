# AWS Glue Job Script Documentation

This document provides an overview and explanation of a Python script used to perform data ingestion from AWS Glue tables, handle change data capture (CDC), and save processed data to Amazon S3 in Parquet format. The script is designed to support both full and incremental (CDC) loads using `updated_at` timestamps. It leverages AWS Glue, PySpark, and Boto3 for data management and transformations.

## Overview

The script performs the following key tasks:

1. Sets up a logging framework to capture runtime information and issues.
2. Initializes the Glue context and Spark session for processing.
3. Retrieves necessary parameters to specify Athena database details, output paths, and other configurations.
4. Defines helper functions for managing primary keys, tracking CDC, and fetching metadata from S3.
5. Iterates over all tables in the specified Athena database to perform data ingestion, deduplication, and CDC management.
6. Saves the processed data to Amazon S3 in Parquet format and updates CDC tracker files for future incremental loads.

## Detailed Explanation

### Imports and Setup

The script begins by importing essential libraries and modules:

- **`sys`**: For handling system arguments.
- **`logging`**: To configure and capture logs.
- **`boto3`**: For AWS SDK to interact with AWS services.
- **`pandas`** and **`StringIO`**: For handling data transformations in memory.
- **`pyspark`** and **`awsglue`**: To interact with Spark and Glue for data processing.
- **`datetime`**: For handling date and time formatting.

These libraries allow the script to interact with S3, Glue, and other AWS services while managing data transformations and timestamp tracking.

### Logging Configuration

The script configures logging to capture messages at the INFO level, with timestamps and log levels included in each message. This setup ensures that the script provides detailed information on each operation, aiding in debugging and monitoring.

### Glue and Spark Context Initialization

The script initializes the Glue context and Spark session:

- **Glue Context**: Enables interaction with AWS Glue to read and process data from Glue tables.
- **Spark Session**: Allows PySpark to perform distributed data processing.
- **Glue Job**: Manages the lifecycle of the Glue job, including committing the job status at the end.

This setup prepares the environment for running transformations on data from Glue tables.

### Parameter and Argument Retrieval

The script retrieves job parameters provided at runtime, which include:

- **`JOB_NAME`**: The name of the Glue job.
- **`athena_database_name`**: The Athena (Glue) database name containing the tables.
- **`output_path`**: The S3 path where processed data will be saved.
- **`cdc_path`**: S3 path for storing CDC tracker files.
- **`load_type`**: Indicates whether to perform a full or incremental load.
- **`primary_key_s3_prefix`**: S3 path prefix to locate primary key information files.

These parameters define the data sources, destinations, and configuration options for the Glue job.

### Helper Functions

The script defines several helper functions to simplify operations:

1. **Fetch Latest Primary Key CSV from S3**: This function retrieves the latest primary key file from the specified S3 path and loads it as a DataFrame. This file is used to determine the primary key for deduplication.

2. **Get Primary Key for a Table**: Given a table name, this function fetches the corresponding primary key from the DataFrame containing primary key data. If no primary key is found, a warning is logged, and deduplication is skipped.

3. **Get CDC Tracker Path**: This function constructs the S3 path to the CDC tracker file for each table. The tracker file records the last processed timestamp for incremental loading.

4. **Get Last Processed Timestamp**: This function retrieves the last processed timestamp from the CDC tracker file. If no tracker file exists, it defaults to a full load.

5. **Update Last Processed Timestamp**: After processing each table, this function updates the CDC tracker file with the latest processed timestamp. This ensures that future runs will only process new or updated records.

### Processing Each Table in the Database

The script iterates through all tables in the specified Athena database and performs the following steps for each table:

1. **List Tables**: Uses the Glue client to list all tables in the database, fetching metadata for each table.

2. **Process Each Table**: For each table, the script performs data loading, deduplication, and CDC tracking:

   - **Column Check**: Verifies that the `updated_at` column exists, as it is essential for CDC. If absent, the script logs a warning and skips CDC for that table.
   - **Primary Key Check**: Retrieves the primary key for the table to perform deduplication. If no primary key is defined, deduplication is skipped.
   - **Load Type**:
     - **Full Load**: If `load_type` is "full" or no CDC tracker exists, the script loads all records and deduplicates them using the primary key.
     - **Incremental Load (CDC)**: If a CDC tracker file exists, the script filters records based on the last processed timestamp and deduplicates the new records.
   - **Data Writing**: Writes the deduplicated data to the specified S3 path in Parquet format.
   - **CDC Tracker Update**: Updates the CDC tracker file with the latest `updated_at` timestamp, preparing for future incremental loads.

### Job Commit

At the end of the process, the script commits the Glue job, marking it as complete.

## Error Handling

The script incorporates error handling through log warnings and default behaviors:

- If a primary key is missing, deduplication is skipped, and a warning is logged.
- If a CDC tracker file is not found, the script defaults to a full load.

These error-handling mechanisms help the script to handle various scenarios gracefully, ensuring reliable execution.

## Sample Usage

To execute this Glue job, provide the required parameters through the AWS Glue Console or CLI. Specify the necessary values for `athena_database_name`, `output_path`, `cdc_path`, `load_type`, and other parameters to configure the job correctly.

## Logging and Debugging

The script provides detailed logging for each operation, including table processing, deduplication, and CDC handling. This information can be invaluable for debugging and performance monitoring, as it allows users to trace the workflow step-by-step.

---

This Glue job script automates the ingestion, CDC tracking, and deduplication of data from an Athena database, using S3 for CDC tracking and primary key management.
