
import os

import boto3
from botocore.client import Config
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType
from pyspark.sql.functions import year, month, dayofmonth, when, col, size, create_map, lit
from pandas.core.api import DataFrame as pd_DataFrame


RUSTFS_ENDPOINT = 'http://telemetry-rustfs:9000' 
ACCESS_KEY = 'rustfsadmin'
SECRET_KEY = 'rustfsadmin'
TEMP_BUCKET_NAME = 'temporary-telelemetry'
PERM_BUCKET_NAME = 'permanent-telemetry'
REGION = 'ap-southeast-2' # RustFS does not validate regions; any value works


def get_files_from_s3():
    files = []
    s3_client = get_s3_client()    
    print(f"s3+client in caller: {s3_client}")
    response = s3_client.list_objects_v2(Bucket=TEMP_BUCKET_NAME)
    print("Objects in bucket:")
    for obj in response.get('Contents', []):
        print(f"- {obj['Key']} ({obj['Size']} bytes)")
        files.append(obj['Key'])
    return files

def update_filename_in_s3(old_key, new_key):
    s3_client = get_s3_client()
    # Copy the object to the new key
    s3_client.copy_object(Bucket=TEMP_BUCKET_NAME, CopySource={'Bucket': TEMP_BUCKET_NAME, 'Key': old_key}, Key=new_key)
    # Delete the old object
    s3_client.delete_object(Bucket=TEMP_BUCKET_NAME, Key=old_key)
    print(f"Renamed {old_key} to {new_key} in bucket {TEMP_BUCKET_NAME}")        


def get_s3_client():    
    # Create an S3 client configured for RustFS
    
    # if env variable DEVELOPMENT_MODE is set to "true", then use the following configuration for the S3 client
    if os.getenv("DEVELOPMENT_MODE") == "true":
        print("DEVELOPMENT_MODE is true, using development configuration for S3 client")
        s3_client = boto3.client(
            's3',
            endpoint_url=RUSTFS_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name=REGION
        )
    else:
        print("DEVELOPMENT_MODE is not true, using default configuration for S3 client")
        # In production, the Glue job will have an IAM role with permissions to access the S3 buckets, so we can use the default configuration which will pick up the credentials from the environment
        s3_client = boto3.client('s3')
    
    
    
    # s3_client = boto3.client(
    #     's3',
    #     endpoint_url=RUSTFS_ENDPOINT,
    #     aws_access_key_id=ACCESS_KEY,
    #     aws_secret_access_key=SECRET_KEY,
    #     config=Config(signature_version='s3v4'),
    #     region_name=REGION
    # )
    # print(f"s3+client: {s3_client}")
    return s3_client

telemetry_event_schema = StructType([
    StructField("application", StringType()),
    StructField("timestamp", StringType()),
    StructField("type", StringType()),
    StructField("data",  MapType(StringType(), StringType()), True)
])

def append_file_contents_to_spark_dataframe(file_key, spark: SparkSession, df=None):
    s3_client = get_s3_client()
    # Get the object from S3
    response = s3_client.get_object(Bucket=TEMP_BUCKET_NAME, Key=file_key)
    # Read the content of the file
    file_content = response['Body'].read().decode('utf-8')
    # Create a Spark DataFrame from the file content
    new_df = spark.read.option("header", "false").schema(telemetry_event_schema).json(spark.sparkContext.parallelize([file_content]))
    # Union the new DataFrame with the existing one
    if df is None:
        return new_df
    else:
        return df.union(new_df)

def create_spark_session():
    spark = SparkSession.builder.appName("S3ToS3ForAnalytics").getOrCreate()
    print(f"Spark session created: {spark}")
    return spark

def embellish_dataframe(df: DataFrame):
    embellish_dataframe = df.withColumn("timestamp_year", year("timestamp")).withColumn("timestamp_month", month("timestamp")).withColumn("timestamp_day", dayofmonth("timestamp"))
    
    default_data_map = create_map(lit("__empty"), lit("true"))
    # set default value for data column to {"__empty": "true"} map if the map has not fields
    embellish_dataframe = embellish_dataframe.withColumn("data", when(col("data").isNull() | (size(col("data")) == 0), default_data_map).otherwise(col("data")))
    return embellish_dataframe

def write_group_to_s3(pandas_df_group: pd_DataFrame):
    print("write_group_to_s3 called with group:")
    application = pandas_df_group['application'].iloc[0]
    timestamp_year = pandas_df_group['timestamp_year'].iloc[0]
    timestamp_month = pandas_df_group['timestamp_month'].iloc[0]
    timestamp_day = pandas_df_group['timestamp_day'].iloc[0]    
    
    output_path = f"application={application}/timestamp_year={timestamp_year}/timestamp_month={timestamp_month}/timestamp_day={timestamp_day}/data.txt"
    
    # write the pandas DataFrame to S3 as parquet file
    local_file_path = f"/tmp/{application}_{timestamp_year}_{timestamp_month}_{timestamp_day}.parquet"
    pandas_df_group.to_parquet(local_file_path, index=False)
    local_csv_file_path = f"/tmp/{application}_{timestamp_year}_{timestamp_month}_{timestamp_day}.csv"
    pandas_df_group.to_csv(local_csv_file_path, index=False)
    s3_client = get_s3_client()
    s3_client.upload_file(local_csv_file_path, PERM_BUCKET_NAME, output_path)
    
    print(f"Wrote data for application={application}, timestamp_year={timestamp_year}, timestamp_month={timestamp_month}, timestamp_day={timestamp_day} to {output_path}")
    return pandas_df_group
    

def process_dataframe(df: DataFrame):
    applicationGroups = df.groupBy("application", "timestamp_year", "timestamp_month", "timestamp_day")
    
    group_schema = telemetry_event_schema.add(StructField("timestamp_year", IntegerType()))
    group_schema = group_schema.add(StructField("timestamp_month", IntegerType()))
    group_schema = group_schema.add(StructField("timestamp_day", IntegerType()))
    applicationGroups.applyInPandas(write_group_to_s3, schema=group_schema).show()
    
def main():
    spark = create_spark_session()
    files = get_files_from_s3()
    df = None
    for file_key in files:
        df = append_file_contents_to_spark_dataframe(file_key, spark, df)
        df.show()
    
    df = embellish_dataframe(df)
    df.show()
    process_dataframe(df)


if __name__ == "__main__":
    main()
