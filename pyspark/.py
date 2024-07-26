import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the data from the S3 bucket
amazon_s3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://data-eng-on-youtube-raw-us-east-1-dev/youtube/raw_statistics/"], "recurse": True},
    transformation_ctx="amazon_s3_node"
)

# Convert DynamicFrame to DataFrame
dataframe = amazon_s3_node.toDF()

# Apply the filter to the DataFrame
filtered_dataframe = dataframe.filter(dataframe["region"].isin("ca", "gb", "us"))

# Convert DataFrame back to DynamicFrame
filtered_dynamic_frame = DynamicFrame.fromDF(filtered_dataframe, glueContext, "filtered_dynamic_frame")

# Write the filtered data back to the S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://data-eng-on-youtube-cleansed-us-east-1-dev/youtube/raw_statistics/", "partitionKeys": ["region"]},
    format_options={"compression": "snappy"},
    transformation_ctx="filtered_dynamic_frame_node"
)

job.commit()
