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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1721327346641 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1721325073022,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tuchka-tuchka-ne-medved/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1721327346641",
)

job.commit()