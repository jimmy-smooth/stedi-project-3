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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1708936984617 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1708936984617",
)

# Script generated for node Customer Curated
CustomerCurated_node1708937013905 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1708937013905",
)

# Script generated for node SQL Query
SqlQuery1476 = """
select step_trainer_landing.sensorReadingTime, step_trainer_landing.serialNumber, step_trainer_landing.distanceFromObject
from step_trainer_landing
inner join customer_curated on customer_curated.serialnumber = step_trainer_landing.serialnumber

"""
SQLQuery_node1708937035934 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1476,
    mapping={
        "customer_curated": CustomerCurated_node1708937013905,
        "step_trainer_landing": StepTrainerLanding_node1708936984617,
    },
    transformation_ctx="SQLQuery_node1708937035934",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1708937049571 = glueContext.getSink(
    path="s3://stedi-project-3/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1708937049571",
)
StepTrainerTrusted_node1708937049571.setCatalogInfo(
    catalogDatabase="stedi_project_3", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1708937049571.setFormat("json")
StepTrainerTrusted_node1708937049571.writeFrame(SQLQuery_node1708937035934)
job.commit()
