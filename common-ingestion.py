# Author: Sandeep R Diddi
# Date: 2024-11-11
# Description: AWS Glue job script to process multiple tables from a PostgreSQL database via JDBC and
#              handles Change Data Capture (CDC) with the 'updated_at' column, and log the 
#              processing results. Logs are handled in memory and uploaded to S3 after job completion if required.

#Change Log: Updating Parallel threads for Large tables 

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
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
        'primary_key_s3_prefix',
        'parallel_tables',      # List of tables to process in parallel
        'chunk_column',         # Column used to create chunks
        'chunk_size',           # Size of each chunk
        'num_chunks'            # Number of chunks for each table
    ]
)

# Set parameters
athena_database_name = args['athena_database_name']
output_path = args['output_path']
cdc_path = args['cdc_path']
load_type = args['load_type'].lower()
primary_key_s3_prefix = args['primary_key_s3_prefix']
parallel_tables = args['parallel_tables'].split(',')  # List of tables to parallelize
chunk_column = args['chunk_column']
chunk_size = int(args['chunk_size'])
num_chunks = int(args['num_chunks'])

# Initialize S3 client
s3_client = boto3.client('s3')

# Helper functions for CDC tracking, primary key handling, etc., remain the same

# Function to generate chunk filters dynamically based on chunk size and chunk column
def generate_chunk_filters(table_name):
    chunk_filters = []
    for i in range(num_chunks):
        start_value = i * chunk_size
        end_value = start_value + chunk_size - 1
        chunk_filters.append(f"{chunk_column} BETWEEN {start_value} AND {end_value}")
    return chunk_filters

# Function to process a chunk of a large table
def process_table_chunk(table_name, chunk_filter):
    try:
        logger.info(f"Starting processing for {table_name} chunk: {chunk_filter}")
        
        last_updated = get_last_updated_timestamp(table_name)
        
        # Read the chunk of data based on the filter
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=athena_database_name,
            table_name=table_name,
            push_down_predicate=chunk_filter  # Apply chunk filter
        )
        
        df = dynamic_frame.toDF()
        primary_key = get_primary_key(table_name)
        
        # Apply CDC and deduplication logic as before
        if load_type == "full" or last_updated is None:
            df_deduped = df.dropDuplicates([primary_key])
            df_deduped.write.mode("overwrite").parquet(f"{output_path}/{table_name}/{chunk_filter}/")
            max_timestamp = df_deduped.agg({"updated_at": "max"}).collect()[0][0]
            update_last_updated_timestamp(table_name, max_timestamp)
        else:
            df_filtered = df.filter(df['updated_at'] > last_updated)
            df_filtered_deduped = df_filtered.dropDuplicates([primary_key])
            
            if df_filtered_deduped.count() > 0:
                df_filtered_deduped.write.mode("append").parquet(f"{output_path}/{table_name}/{chunk_filter}/")
                max_timestamp = df_filtered_deduped.agg({"updated_at": "max"}).collect()[0][0]
                update_last_updated_timestamp(table_name, max_timestamp)
            else:
                logger.info(f"No new records for {table_name} chunk: {chunk_filter}")
                
        logger.info(f"Completed processing for {table_name} chunk: {chunk_filter}")
    except Exception as e:
        logger.error(f"Error processing {table_name} chunk {chunk_filter}: {str(e)}")

# Process each table in parallel by dividing it into chunks
for table_name in parallel_tables:
    chunk_filters = generate_chunk_filters(table_name)  # Generate chunk filters for the table
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_chunk = {executor.submit(process_table_chunk, table_name, chunk): chunk for chunk in chunk_filters}
        
        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Chunk {chunk} for table {table_name} generated an exception: {exc}")

# Sequential processing for other tables
def process_table(table_name):
    try:
        logger.info(f"Starting processing for table: {table_name}")
        
        last_updated = get_last_updated_timestamp(table_name)
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=athena_database_name,
            table_name=table_name
        )

        df = dynamic_frame.toDF()
        primary_key = get_primary_key(table_name)
        
        if load_type == "full" or last_updated is None:
            df_deduped = df.dropDuplicates([primary_key])
            df_deduped.write.mode("overwrite").parquet(f"{output_path}/{table_name}/")
            max_timestamp = df_deduped.agg({"updated_at": "max"}).collect()[0][0]
            update_last_updated_timestamp(table_name, max_timestamp)
        else:
            df_filtered = df.filter(df['updated_at'] > last_updated)
            df_filtered_deduped = df_filtered.dropDuplicates([primary_key])
            
            if df_filtered_deduped.count() > 0:
                df_filtered_deduped.write.mode("append").parquet(f"{output_path}/{table_name}/")
                max_timestamp = df_filtered_deduped.agg({"updated_at": "max"}).collect()[0][0]
                update_last_updated_timestamp(table_name, max_timestamp)
            else:
                logger.info(f"No new records for table: {table_name}")
                
        logger.info(f"Completed processing for table: {table_name}")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {str(e)}")

# Commit job
job.commit()
logger.info("Job completed successfully for all tables.")
