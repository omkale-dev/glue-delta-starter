from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import importlib
from pyspark.sql.functions import col, coalesce, lit
import helper_functions
importlib.reload(helper_functions)

LOCAL = True
job_name = "the_glue_job_one"

source_path = "s3://<bucket>/d_customer/d_customer_two.csv"
mapping_path = f"./jupyter_workspace/{job_name}/the_resources/mapping.csv"
delta_bucket = "s3://<delta-bucket>"


def glue_init():
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    sc = spark.sparkContext
    glueContext = GlueContext(sc)
    # spark = glueContext.spark_session
    job = Job(glueContext)
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    if not LOCAL:
        global source_path, mapping_path
        args = getResolvedOptions(sys.argv, ["source_path"])
        source_path = args["source_path"]
        print(source_path)
        mapping_path = "./mapping.csv"

    return spark, glueContext, job


def extract(glueContext, source_file):
    source_df = helper_functions.get_source_df(glueContext, source_file)
    return source_df


def first_load(full_load_df, delta_bucket):
    full_load_df.write.format("delta").mode(
        "overwrite").save(delta_bucket+"/delta/customer/")


def delta_scd(spark, cdc_df, delta_bucket):
    delta_df = DeltaTable.forPath(spark, delta_bucket + "/delta/customer/")
    # UPSERT process if matches on the condition the update else insert
    # if there is no keyword then create a data set with Insert, Update and Delete flag and do it separately.
    # for delete it has to run in loop with delete condition, this script do not handle deletes.
    delta_df.alias("prev_df").merge(
        source=cdc_df.alias("append_df"), \
        # matching on primarykey
        condition=expr("prev_df.customer_id = append_df.customer_id"))\
        .whenMatchedUpdate(
        set={
            "prev_df.customer_name": col("append_df.customer_name"),
            "prev_df.state": col("append_df.state"),
            "prev_df.is_active": col("append_df.is_active")
        })\
        .whenNotMatchedInsert(
        values={
            "prev_df.customer_id": col("append_df.customer_id"),
            "prev_df.customer_name": col("append_df.customer_name"),
            "prev_df.state": col("append_df.state"),
            "prev_df.is_active": col("append_df.is_active")
        }).execute()
    print("SCD load completed")


# Main
if __name__ == "__main__":
    spark, glueContext, job = glue_init()
    mapping = helper_functions.get_csv_mapping(mapping_path)
    source_df = extract(glueContext, source_path)
    # first_load(source_df,delta_bucket)
    delta_scd(spark, source_df, delta_bucket)
