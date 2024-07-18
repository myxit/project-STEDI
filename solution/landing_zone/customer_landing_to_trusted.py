import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1721072857000 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://tuchka-tuchka-ne-medved/customer/landing/customer-1691348231425.json"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1721072857000",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where sharewithresearchasofdate is not null
"""
SQLQuery_node1721070300283 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AmazonS3_node1721072857000},
    transformation_ctx="SQLQuery_node1721070300283",
)

# Script generated for node Amazon S3
AmazonS3_node1721070307031 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1721070300283,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tuchka-tuchka-ne-medved/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1721070307031",
)

job.commit()