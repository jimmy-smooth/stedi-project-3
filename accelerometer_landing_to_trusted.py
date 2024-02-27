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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1708506803113 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1708506803113",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1708506801568 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1708506801568",
)

# Script generated for node SQL Query
SqlQuery1578 = """
select user, timestamp, x, y, z
from accelerometer_landing
inner join customer_trusted on customer_trusted.email = accelerometer_landing.user
"""
SQLQuery_node1708932965601 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1578,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1708506803113,
        "customer_trusted": CustomerTrusted_node1708506801568,
    },
    transformation_ctx="SQLQuery_node1708932965601",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1708506848542 = glueContext.getSink(
    path="s3://stedi-project-3/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1708506848542",
)
AccelerometerTrusted_node1708506848542.setCatalogInfo(
    catalogDatabase="stedi_project_3", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1708506848542.setFormat("json")
AccelerometerTrusted_node1708506848542.writeFrame(SQLQuery_node1708932965601)
job.commit()
