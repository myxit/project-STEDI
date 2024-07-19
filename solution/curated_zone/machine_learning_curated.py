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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1721345009711 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1721345009711",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1721344962556 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1721344962556",
)

# Script generated for node SQL Query
SqlQuery0 = """
select step_trainer_trusted.* 
from step_trainer_trusted
join accelerometer_trusted on step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timestamp

"""
SQLQuery_node1721345069432 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1721344962556,
        "step_trainer_trusted": step_trainer_trusted_node1721345009711,
    },
    transformation_ctx="SQLQuery_node1721345069432",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721345369835 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1721345069432,
    database="stedi-db",
    table_name="machine_learning_curated",
    transformation_ctx="AWSGlueDataCatalog_node1721345369835",
)

job.commit()