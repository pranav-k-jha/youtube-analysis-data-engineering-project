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

predicate_pushdown = "region = 'ca' OR region = 'gb' OR region = 'us'"


# Script generated for node Amazon S3
AmazonS3_node1721959861815 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://data-eng-on-youtube-raw-us-east-1-dev/youtube/raw_statistics/"], "recurse": True}, transformation_ctx="AmazonS3_node1721959861815", push_down_predicate = predicate_pushdown)


# Script generated for node Amazon S3
AmazonS3_node1721959904716 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1721959861815, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-eng-on-youtube-cleansed-us-east-1-dev/youtube/raw_statistics/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1721959904716")

job.commit()