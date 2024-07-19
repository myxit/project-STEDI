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

# Script generated for node customer_curated
customer_curated_node1721324915098 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuchka-tuchka-ne-medved/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1721324915098",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1721324691124 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuchka-tuchka-ne-medved/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1721324691124",
)

# Script generated for node SQL Query
SqlQuery0 = """
select s.* 
from step_trainer_landing as s
join customer_curated as c on c.serialnumber = s.serialnumber
"""
SQLQuery_node1721325073022 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": customer_curated_node1721324915098,
        "step_trainer_landing": step_trainer_landing_node1721324691124,
    },
    transformation_ctx="SQLQuery_node1721325073022",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721347916483 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1721325073022,
    database="stedi-db",
    table_name="step_trainer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1721347916483",
)

job.commit()