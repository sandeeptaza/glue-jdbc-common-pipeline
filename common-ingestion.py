# Author: Sandeep R Diddi
# Date: 2024-10-24
# Description: AWS Glue job script to process multiple tables from a PostgreSQL database via JDBC and
#              handles Change Data Capture (CDC) with the 'updated_at' column, and log the 
#              processing results. Logs are handled in memory and uploaded to S3 after job completion if required.

#Change Log 

#28 Oct 2024 : Added Duplicate Logic and handel tables in batch for large tables in DB
import sys
import logging
import boto3
import pandas as pd
from io import StringIO
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define parameters
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'athena_database_name',
        'output_path',
        'cdc_path',
        'load_type',
        'primary_key_s3_prefix'
    ]
)

athena_database_name = args['athena_database_name']
output_path = args['output_path']
cdc_path = args['cdc_path']
load_type = args['load_type'].lower()
primary_key_s3_prefix = args['primary_key_s3_prefix']

# Initialize S3 client
s3_client = boto3.client('s3')

# Helper function to find the latest primary key CSV file in S3
def get_latest_primary_key_csv():
    bucket_name, prefix = primary_key_s3_prefix.replace("s3://", "").split("/", 1)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv")]
    latest_csv = max(csv_files) if csv_files else None
    if latest_csv:
        response = s3_client.get_object(Bucket=bucket_name, Key=latest_csv)
        primary_key_csv_data = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(primary_key_csv_data))
    else:
        raise FileNotFoundError("No primary key CSV file found in specified S3 location.")

# Load primary key information
primary_key_df = get_latest_primary_key_csv()

# Helper function to get the primary key for a table
def get_primary_key(table_name):
    row = primary_key_df[primary_key_df['table_name'] == table_name]
    if not row.empty:
        return row['primary_key_column'].values[0]
    else:
        logger.warning(f"No primary key found for table {table_name}. Skipping de-duplication.")
        return None

# Helper function to get CDC tracker path for each table
def get_cdc_tracker_path(table_name):
    return f"{cdc_path}/{table_name}.txt"

# Helper function to get last processed timestamp for CDC
def get_last_updated_timestamp(table_name):
    cdc_tracker_path = get_cdc_tracker_path(table_name)
    try:
        last_timestamp_df = spark.read.text(cdc_tracker_path)
        return last_timestamp_df.collect()[0][0]
    except Exception as e:
        logger.info(f"No CDC tracker found for {table_name}. Defaulting to full load.")
        return None

# Helper function to update the last processed timestamp for CDC
def update_last_updated_timestamp(table_name, max_timestamp):
    cdc_tracker_path = get_cdc_tracker_path(table_name)
    max_timestamp_str = max_timestamp.strftime("%Y-%m-%d %H:%M:%S") if max_timestamp else "1970-01-01 00:00:00"
    timestamp_df = spark.createDataFrame([(max_timestamp_str,)], ["timestamp"])
    timestamp_df.write.mode("overwrite").text(cdc_tracker_path)
    logger.info(f"Updated CDC tracker for {table_name} with timestamp: {max_timestamp_str}")

# Use Boto3 to list all tables in the specified Athena (Glue) database
glue_client = boto3.client('glue')
tables_response = glue_client.get_tables(DatabaseName=athena_database_name)
table_names = [table['Name'] for table in tables_response['TableList']]

# Process each table in the database
for table_name in table_names:
    logger.info(f"Starting processing for table: {table_name}")
    
    last_updated = get_last_updated_timestamp(table_name)
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=athena_database_name,
        table_name=table_name
    )

    df = dynamic_frame.toDF()
    if 'updated_at' not in df.columns:
        logger.warning(f"Table {table_name} does not have 'updated_at' column. Skipping CDC.")
        continue

    primary_key = get_primary_key(table_name)
    if not primary_key:
        logger.warning(f"Primary key not defined for table {table_name}. Skipping de-duplication.")
        continue

    if load_type == "full" or last_updated is None:
        logger.info(f"Performing full load for table: {table_name}")
        df_deduped = df.dropDuplicates([primary_key])
        df_deduped.write.mode("overwrite").parquet(f"{output_path}/{table_name}/")
        max_timestamp = df_deduped.agg({"updated_at": "max"}).collect()[0][0]
        update_last_updated_timestamp(table_name, max_timestamp)
    else:
        logger.info(f"Performing CDC load for table: {table_name} after {last_updated}")
        df_filtered = df.filter(df['updated_at'] > last_updated)
        df_filtered_deduped = df_filtered.dropDuplicates([primary_key])

        if df_filtered_deduped.count() > 0:
            logger.info(f"Appending {df_filtered_deduped.count()} new records to S3 for table: {table_name}")
            # Write CDC with overwrite mode per partition to ensure duplicates are removed
            df_filtered_deduped.write.mode("overwrite").parquet(f"{output_path}/{table_name}/")
            
            max_timestamp = df_filtered_deduped.agg({"updated_at": "max"}).collect()[0][0]
            update_last_updated_timestamp(table_name, max_timestamp)
        else:
            logger.info(f"No new records for table: {table_name}")

job.commit()
logger.info("Job completed successfully for all tables.")
