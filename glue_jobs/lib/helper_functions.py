from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from boto3 import client, session
import os
from botocore.exceptions import ClientError
import json
import csv
from datetime import datetime
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

def get_source_df(glueContext, pvo_file):
    dyf = glueContext.create_dynamic_frame_from_options(
        connection_type="s3", 
        connection_options={"paths": [pvo_file]}, 
        format="csv",
        format_options= {'withHeader': True}
    )
    df = dyf.toDF()
    return df


def get_csv_mapping(mapping_path):
    csv_data = open(mapping_path)
    reader = csv.DictReader(csv_data)
    table_mapping = [row for row in reader]
    return table_mapping