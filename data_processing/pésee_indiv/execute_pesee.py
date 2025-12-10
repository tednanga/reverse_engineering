# Databricks notebook source
import pandas as pd
import numpy as np
import os
import sys

import importlib

sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')


import functions_pesee_agg as fpa
import functions_pesee_indiv as fpi
import dataframe_cleaner

import env_config
# from pésee_indiv.functions_pesee_agg import agg_pesee_indiv
# from pésee_indiv.functions_pesee_indiv import select_pesee_indiv, send_pesee_indiv

importlib.reload(fpa)
importlib.reload(fpi)


# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType, DoubleType

# COMMAND ----------

# Adapt the table_name
table_name = "pesee"
error_table_name = "pesee_errors"
full_pesee_table_name = env_config.get_full_table_name(table_name)
full_error_pesee_table_name = env_config.get_full_table_name(error_table_name)
volume_path = env_config.get_volume_path()

# COMMAND ----------

catalog = "studies"
schema = dbName = db = "cern_growth"

# volume_name = "full_raw"

# COMMAND ----------

volume_path

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]
total_files = len(files)

# COMMAND ----------

pesee_agg_tables = []
pesee_send_tables = []
agg_errors = {}
send_errors = {}

# COMMAND ----------

for file in files:
    try:
        # 1. SELECT/AGG
        indiv = fpi.select_pesee_indiv(file)
        # Add source file for traceability
        indiv['Sourcefile'] = os.path.basename(file)
        pesee_agg = fpa.agg_pesee_indiv(indiv)
        pesee_agg['Sourcefile'] = os.path.basename(file)
        pesee_agg_tables.append(pesee_agg)
    except Exception as ex:
        agg_errors[file] = f"{type(ex).__name__}: {ex}"

    try:
        # 2. SEND
        pesee_send = fpi.send_pesee_indiv(file)
        pesee_send['Sourcefile'] = os.path.basename(file)
        pesee_send_tables.append(pesee_send)
    except Exception as ex:
        send_errors[file] = f"{type(ex).__name__}: {ex}"

# COMMAND ----------

# Concatenate all results
if pesee_agg_tables:
    df_agg = pd.concat(pesee_agg_tables, ignore_index=True)
else:
    df_agg = pd.DataFrame()

if pesee_send_tables:
    df_send = pd.concat(pesee_send_tables, ignore_index=True)
else:
    df_send = pd.DataFrame()

# COMMAND ----------

dataframe_cleaner.clean_and_normalize(df_agg)

# COMMAND ----------

df_agg.head(1)

# COMMAND ----------

dataframe_cleaner.clean_and_normalize(df_send)

# COMMAND ----------

df_send.head(1)

# COMMAND ----------

if not df_agg.empty:
    full_table_name = f"{catalog}.{dbName}.pesee_agg"
    df_spark_df_agg = spark.createDataFrame(df_agg)
    df_spark_df_agg.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)



# COMMAND ----------

if not df_send.empty:
    full_table_name = f"{catalog}.{dbName}.pesee_send"
    df_spark_df_send = spark.createDataFrame(df_send)
    df_spark_df_send.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)

# COMMAND ----------

if agg_errors:
    print("Errors in agg_pesee_indiv:", agg_errors)
if send_errors:
    print("Errors in send_pesee_indiv:", send_errors)

# COMMAND ----------

