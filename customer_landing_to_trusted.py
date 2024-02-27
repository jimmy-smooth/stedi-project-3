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

# Script generated for node Customer Landing
CustomerLanding_node1708505794494 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-3/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1708505794494",
)

# Script generated for node SQL Query
SqlQuery1355 = """
select * from customer_landing
where shareWithResearchAsOfDate IS NOT NULL
"""
SQLQuery_node1708854179873 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1355,
    mapping={"customer_landing": CustomerLanding_node1708505794494},
    transformation_ctx="SQLQuery_node1708854179873",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1708505819938 = glueContext.getSink(
    path="s3://stedi-project-3/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1708505819938",
)
CustomerTrusted_node1708505819938.setCatalogInfo(
    catalogDatabase="stedi_project_3", catalogTableName="customer_trusted"
)
CustomerTrusted_node1708505819938.setFormat("json")
CustomerTrusted_node1708505819938.writeFrame(SQLQuery_node1708854179873)
job.commit()
