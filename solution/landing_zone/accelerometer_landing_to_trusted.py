import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1721256333243 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuchka-tuchka-ne-medved/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1721256333243",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1721256635053 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuchka-tuchka-ne-medved/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1721256635053",
)

# Script generated for node Join
Join_node1721256253335 = Join.apply(
    frame1=customer_trusted_node1721256333243,
    frame2=accelerometer_landing_node1721256635053,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1721256253335",
)

# Script generated for node Drop Fields
DropFields_node1721328115931 = DropFields.apply(
    frame=Join_node1721256253335,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1721328115931",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1721256832987 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1721328115931,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tuchka-tuchka-ne-medved/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1721256832987",
)

job.commit()