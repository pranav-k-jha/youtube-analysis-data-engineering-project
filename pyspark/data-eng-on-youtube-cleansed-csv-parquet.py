import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

predicate_pushdown = "region in ('ca','gb','us')"

# Script generated for node Amazon S3
AmazonS3_node1722017066432 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://data-eng-on-youtube-raw-us-east-1-dev/youtube/raw_statistics/"], "recurse": True}, transformation_ctx="AmazonS3_node1722017066432")

dropnullfields3 = DropNullFields.apply(frame = AmazonS3_node1722017066432, transformation_ctx = "dropnullfields3")

datasink1 = dropnullfields3.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")


# Script generated for node Amazon S3
AmazonS3_node1722017083199 = glueContext.write_dynamic_frame.from_options(frame=df_final_output, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-eng-on-youtube-cleansed-us-east-1-dev/youtube/raw_statistics/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1722017083199")

job.commit()