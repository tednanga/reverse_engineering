# Databricks notebook source
import pandas as pd
import numpy as np
import os

import sys
sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

import dataframe_cleaner
import env_config

from SEND_SI.si_functions import create_send_si
from pésee_indiv.functions_pesee_indiv import send_pesee_indiv

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType, DoubleType

# COMMAND ----------

# Adapt the table_name
table_name = "send_si"
full_table_name = env_config.get_full_table_name(table_name)
volume_path = env_config.get_volume_path()
catalog = "studies"
schema = dbName = db = "cern_growth"

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]

# COMMAND ----------

send_si_tables = []
# pesee_send_tables = []
send_si_errors = {}
# send_errors = {}

# COMMAND ----------

for file in files:
    try:
        # 1. 
        data = send_pesee_indiv(file)
        # Add source file for traceability
        data['Sourcefile'] = os.path.basename(file)
        table_si = create_send_si(data)
        send_si_tables.append(table_si)
    except Exception as ex:
        send_si_errors[file] = f"{type(ex).__name__}: {ex}"


# COMMAND ----------

# send_si = pd.concat(send_si_tables)
# Concatenate all results
if send_si_tables:
    send_si = pd.concat(send_si_tables, ignore_index=True)
    dataframe_cleaner.clean_and_normalize(send_si)
else:
    send_si = pd.DataFrame()


# COMMAND ----------

send_si

# COMMAND ----------

if not send_si.empty:
    # full_table_name = f"{catalog}.{dbName}.croissance_recalc"
    df_spark_send_si = spark.createDataFrame(send_si)
    df_spark_send_si.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)