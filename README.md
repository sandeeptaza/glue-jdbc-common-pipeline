# AWS Glue Job: CDC with PostgreSQL via JDBC

## Author: Sandeep R Diddi

### Date: 2024-10-24

## Overview

This AWS Glue job processes multiple tables from a PostgreSQL database connected via JDBC. It supports **Change Data Capture (CDC)** using the `updated_at` column and logs the processing results. The logs are stored in memory during job execution and are uploaded to an S3 bucket after completion.

The script processes tables in batches to avoid memory overflow issues. It tracks the maximum `updated_at` timestamp for each table and stores this information in a CSV file in S3, enabling efficient CDC in subsequent runs by processing only updated rows.

## Key Features

- **PostgreSQL via JDBC**: The job connects to a PostgreSQL database and processes tables using AWS Glue's dynamic frame API.
- **CDC Logic**: Uses the `updated_at` column to capture changes and only processes new or updated records in subsequent runs.
- **In-memory Logging**: Logs are stored in memory during the job execution and uploaded to S3 at the end.
- **Batch Processing**: Tables are processed in batches to avoid memory issues and ensure efficient execution.
- **S3 Integration**: The job reads from and writes to S3, including logging and tracking CDC timestamps.

## Requirements

1. **AWS Glue**: This job is written for the AWS Glue environment.
2. **S3 Buckets**: You will need S3 buckets for:
   - Storing CDC timestamp CSV (`max_timestamps.csv`).
   - Storing logs after job completion.
3. **PostgreSQL Database**: The script assumes a PostgreSQL database that has been crawled into the Glue Data Catalog.

## Parameters

The Glue job accepts the following parameters:

- `JOB_NAME`: The name of the Glue job.
- `database_name`: The name of the database in the Glue Data Catalog to process.
- `s3_output_path`: The S3 path where the processed data will be stored.
- `environment`: The environment label (e.g., `dev`, `prod`).
- `csv_file_name`: The S3 location of the `max_timestamps.csv` file used for CDC.
- `log_folder_path`: The S3 path where logs will be uploaded after job completion.
- `load_type`: Defines whether the job is running in **full load** (`full`) mode or **CDC** (`cdc`) mode.

## Script Flow

1. **Retrieve Table Names**: The script fetches the list of tables from the specified database in the Glue Data Catalog.
2. **CDC Timestamp Tracking**: 
   - For each table, the script reads the maximum `updated_at` timestamp from a CSV file stored in S3. 
   - If this is the first run for the table, it processes all records (full load). In subsequent runs, only rows with an `updated_at` greater than the previously recorded timestamp are processed.
3. **Process Tables in Batches**: Tables are processed in batches to avoid memory overflows.
4. **Store Processed Data**: The data is written to S3 in Parquet format, partitioned by the `updated_at` date.
5. **Update CDC Tracking**: After processing each table, the maximum `updated_at` timestamp is updated in the CSV file in S3.
6. **Log Upload**: In-memory logs are uploaded to S3 after the job completes.

# Script Flow

### 1. Retrieve Table Names
- The script fetches the list of tables from the specified database in the Glue Data Catalog.

### 2. CDC Timestamp Tracking
- For each table, the script reads the maximum `updated_at` timestamp from a CSV file stored in S3.
- If this is the first run for the table, it processes all records (full load). In subsequent runs, only rows with an `updated_at` greater than the previously recorded timestamp are processed.

### 3. Process Tables in Batches
- Tables are processed in batches to avoid memory overflows.

### 4. Store Processed Data
- The data is written to S3 in Parquet format, partitioned by the `updated_at` date.

### 5. Update CDC Tracking
- After processing each table, the maximum `updated_at` timestamp is updated in the CSV file in S3.

### 6. Log Upload
- In-memory logs are uploaded to S3 after the job completes.

# Script Breakdown

### 1. Initialize AWS Glue Context and Job
- The script initializes the Glue context, Spark context, and job.


# Script Flow

### 1. Retrieve Table Names
- The script fetches the list of tables from the specified database in the Glue Data Catalog.

!![App Screenshot](images/Args1.png)


### 2. CDC Timestamp Tracking
- For each table, the script reads the maximum `updated_at` timestamp from a CSV file stored in S3.
- If this is the first run for the table, it processes all records (full load). In subsequent runs, only rows with an `updated_at` greater than the previously recorded timestamp are processed.

### 3. Process Tables in Batches
- Tables are processed in batches to avoid memory overflows.

### 4. Store Processed Data
- The data is written to S3 in Parquet format, partitioned by the `updated_at` date.

### 5. Update CDC Tracking
- After processing each table, the maximum `updated_at` timestamp is updated in the CSV file in S3.

### 6. Log Upload
- In-memory logs are uploaded to S3 after the job completes.

# Script Breakdown

### 1. Initialize AWS Glue Context and Job
- The script initializes the Glue context, Spark context, and job.


## Usage Instructions

### 1. Setup AWS Glue Job
- Create an AWS Glue Job in the Glue Console and upload this script.
- **Configure the Job**:
  - **Job type**: Spark
  - **IAM Role**: Ensure it has the necessary permissions for Glue, S3, and PostgreSQL.
  - **Number of DPUs**: Adjust based on your dataset size.

### 2. Configure S3 Buckets
- **S3 Output Path**: Set up an S3 path to store processed data in Parquet format.
- **Log Folder**: Set up an S3 path where logs will be uploaded after the job completes.
- **CDC Tracking CSV**: Create an S3 location where the `max_timestamps.csv` file will be stored.
