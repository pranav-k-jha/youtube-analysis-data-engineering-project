import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts and the job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the S3 source with partitioned data
s3_source_path = "s3://data-eng-on-youtube-raw-us-east-1-dev/youtube/raw_statistics/"
partition_columns = ["region"]

# Read data from S3 with partitioning
AmazonS3_node1721964934698 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [s3_source_path], "recurse": True, "partitionKeys": partition_columns},
    transformation_ctx="AmazonS3_node1721964934698"
)

# Filter the data based on the region column
filtered_frame = AmazonS3_node1721964934698.toDF().filter(col("region").isin(["ca", "gb", "us"]))
dynamic_filtered_frame = DynamicFrame.fromDF(filtered_frame, glueContext, "dynamic_filtered_frame")

# Drop null fields
dropnullfields3 = DropNullFields.apply(frame=dynamic_filtered_frame, transformation_ctx="dropnullfields3")

# Convert to DataFrame, coalesce, and convert back to DynamicFrame
datasink1 = dropnullfields3.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Write the data back to S3 in Glue Parquet format with partitioning and compression
output_path = "s3://data-eng-on-youtube-cleansed-us-east-1-dev/youtube/raw_statistics/"
AmazonS3_node1721965133914 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": output_path, "partitionKeys": partition_columns},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1721965133914"
)

# Commit the job
job.commit()
