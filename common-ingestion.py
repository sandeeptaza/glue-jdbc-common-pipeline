# Author: Sandeep R Diddi
# Date: 2024-10-24
# Description: AWS Glue job script to process multiple tables from a PostgreSQL database via JDBC and
#              handles Change Data Capture (CDC) with the 'updated_at' column, and log the 
#              processing results. Logs are handled in memory and uploaded to S3 after job completion if required.

import sys
import boto3
import csv
import logging
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, max as spark_max, date_format
from datetime import datetime

# Initialize AWS Glue Context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 's3_output_path', 'environment', 'csv_file_name', 'log_folder_path', 'load_type'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set up in-memory log handling using StringIO
log_buffer = StringIO()
logging.basicConfig(stream=log_buffer, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

# Set up S3 client
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Function to get all table names from the Glue Data Catalog
def get_table_names(database_name):
    table_names = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            table_names.append(table['Name'])
            logger.info(f"Retrieved table: {table['Name']}")
    return table_names

# Function to read max timestamp from CSV for each table (stored in S3)
def read_max_timestamp_from_csv(s3_bucket, s3_key, table_name):
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        csv_data = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(csv_data)
        for row in reader:
            if row['table_name'] == table_name:
                return datetime.strptime(row['max_timestamp'], '%Y-%m-%d %H:%M:%S')
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"No max timestamp found for table {table_name}. Treating as first load.")
        return None
    except Exception as e:
        logger.error(f"Error reading max timestamp from CSV for table {table_name}: {e}")
    return None

# Function to write max timestamp to CSV (stored in S3)
def write_max_timestamp_to_csv(s3_bucket, s3_key, table_name, max_timestamp):
    max_timestamp_str = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    try:
        local_csv_file = '/tmp/temp_max_timestamps.csv'

        try:
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            csv_data = response['Body'].read().decode('utf-8').splitlines()
            rows = [row for row in csv.DictReader(csv_data)]
        except s3_client.exceptions.NoSuchKey:
            rows = []

        table_exists = False
        for row in rows:
            if row['table_name'] == table_name:
                row['max_timestamp'] = max_timestamp_str
                table_exists = True
                break
        if not table_exists:
            rows.append({'table_name': table_name, 'max_timestamp': max_timestamp_str})

        with open(local_csv_file, 'w', newline='') as csvfile:
            fieldnames = ['table_name', 'max_timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        s3_client.upload_file(local_csv_file, s3_bucket, s3_key)
        logger.info(f"Uploaded updated max timestamp CSV for table {table_name} to s3://{s3_bucket}/{s3_key}")

    except Exception as e:
        logger.error(f"Error writing max timestamp to CSV for table {table_name}: {e}")

# Function to process each table with error handling and partitioning
def process_table(glueContext, table_name, database_name, output_path):
    try:
        s3_bucket = args['csv_file_name'].replace("s3://", "").split("/")[0]
        s3_key = "/".join(args['csv_file_name'].replace("s3://", "").split("/")[1:])

        logger.info(f"Starting to process table: {table_name}")
        
        max_timestamp = read_max_timestamp_from_csv(s3_bucket, s3_key, table_name)
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx=f"{table_name}_node"
        )
        df = dynamic_frame.toDF()

        if 'updated_at' not in df.columns:
            logger.warning(f"Table {table_name} does not have 'updated_at' column. Skipping.")
            return

        row_count = df.count()
        if row_count == 0:
            logger.info(f"Table {table_name} is empty. Skipping.")
            return

        if args['load_type'] == 'cdc' and max_timestamp:
            df = df.filter(col('updated_at') > max_timestamp)

        df = df.withColumn("partition_date", date_format(col("updated_at"), "yyyy-MM-dd"))
        dynamic_frame_filtered = DynamicFrame.fromDF(df, glueContext, f"{table_name}_filtered")

        output_table_path = f"{output_path}/{table_name}/"
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame_filtered,
            connection_type="s3",
            format="parquet",
            connection_options={"path": output_table_path, "partitionKeys": ["partition_date"]},
            format_options={"compression": "snappy"},
            transformation_ctx=f"{table_name}_s3_write"
        )

        max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
        if max_updated_at:
            write_max_timestamp_to_csv(s3_bucket, s3_key, table_name, max_updated_at)

        logger.info(f"Successfully processed table: {table_name}")

    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")

# Process tables in batches to avoid memory issues
def process_tables_in_batches(table_names, batch_size=10):
    for i in range(0, len(table_names), batch_size):
        batch = table_names[i:i + batch_size]
        for table in batch:
            process_table(glueContext, table, args['database_name'], args['s3_output_path'])

# Get tables from the Glue Data Catalog and process them in batches
database_name = args['database_name']
table_names = get_table_names(database_name)

# Process tables in batches (adjust batch size as needed)
process_tables_in_batches(table_names, batch_size=10)

# Commit Glue job
job.commit()

# Upload logs to S3 from memory buffer
def upload_log_to_s3(bucket, key, log_data):
    try:
        s3_client.put_object(Body=log_data, Bucket=bucket, Key=key)
        logger.info(f"Log file uploaded to S3: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error uploading log file to S3: {e}")

# Upload the in-memory logs to S3
upload_log_to_s3(args['log_folder_path'], f"{args['JOB_NAME']}.log", log_buffer.getvalue())
