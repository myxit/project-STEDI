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

# Script generated for node customer_trusted
customer_trusted_node1721337946203 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1721337946203",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1721337975384 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1721337975384",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct customer_trusted.* 
from customer_trusted 
join accelerometer_trusted on customer_trusted.email = accelerometer_trusted.user
"""
SQLQuery_node1721338056988 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1721337946203,
        "accelerometer_trusted": accelerometer_trusted_node1721337975384,
    },
    transformation_ctx="SQLQuery_node1721338056988",
)

# Script generated for node customer_curated
customer_curated_node1721338420832 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1721338056988,
    database="stedi-db",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1721338420832",
)

job.commit()
