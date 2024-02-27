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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1708938049948 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1708938049948",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1708938050717 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_3",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1708938050717",
)

# Script generated for node SQL Query
SqlQuery1600 = """
select distinct *
from step_trainer_trusted
inner join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1708938053264 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1600,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1708938050717,
        "step_trainer_trusted": StepTrainerTrusted_node1708938049948,
    },
    transformation_ctx="SQLQuery_node1708938053264",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1708938054689 = glueContext.getSink(
    path="s3://stedi-project-3/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1708938054689",
)
MachineLearningCurated_node1708938054689.setCatalogInfo(
    catalogDatabase="stedi_project_3", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1708938054689.setFormat("json")
MachineLearningCurated_node1708938054689.writeFrame(SQLQuery_node1708938053264)
job.commit()
