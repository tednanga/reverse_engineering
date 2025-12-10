# Databricks notebook source
import pandas as pd
import os

import importlib

import sys
sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

from croissance.functions_croissance_recalc import pipeline_croissance_recalc, pipeline_croissance_recalc_mort_new
from croissance.functions_croissance_extract_calc import extract_croissance_complete

import env_config
import dataframe_cleaner

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType, DoubleType

# COMMAND ----------

volume_path = env_config.get_volume_path()
catalog = "studies"
schema = dbName = db = "cern_growth"

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]

# COMMAND ----------

files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recalculation Croissance based on raw data (Saisie excel files)
# MAGIC ### * old version mortality (Consumption du mort)

# COMMAND ----------

croissance_recalc_tables = []
croissance_recalc_mort_new_tables = []
croissance_recalc_errors = {}
croissance_recalc_mort_new_errors = {}

# COMMAND ----------

for file in files:
    try:
        # 1. croissance_recalc
        croissance_croissance_recalc = pipeline_croissance_recalc(file)
        # Add source file for traceability
        croissance_croissance_recalc['Sourcefile'] = os.path.basename(file)
        croissance_recalc_tables.append(croissance_croissance_recalc)
    except Exception as ex:
        croissance_recalc_errors[file] = f"{type(ex).__name__}: {ex}"

    try:
        # 2. croissance_recalc_mort_new
        croissance_recalc_mort_new = pipeline_croissance_recalc_mort_new(file)
        croissance_recalc_mort_new['Sourcefile'] = os.path.basename(file)
        croissance_recalc_mort_new_tables.append(croissance_recalc_mort_new)
    except Exception as ex:
        croissance_recalc_mort_new_errors[file] = f"{type(ex).__name__}: {ex}"

# COMMAND ----------

croissance_recalc_mort_new_tables

# COMMAND ----------

# Concatenate all results
if croissance_recalc_tables:
    df_croissance_recalc = pd.concat(croissance_recalc_tables, ignore_index=True)
else:
    df_croissance_recalc = pd.DataFrame()

if croissance_recalc_mort_new_tables:
    df_croissance_recalc_mort_new = pd.concat(croissance_recalc_mort_new_tables, ignore_index=True)
else:
    df_croissance_recalc_mort_new = pd.DataFrame()

# COMMAND ----------

dataframe_cleaner.clean_and_normalize(df_croissance_recalc)

# COMMAND ----------

if not df_croissance_recalc.empty:
    full_table_name = f"{catalog}.{dbName}.croissance_recalc"
    df_spark_df_croissance_recalc = spark.createDataFrame(df_croissance_recalc)
    df_spark_df_croissance_recalc.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)

# COMMAND ----------

dataframe_cleaner.clean_and_normalize(df_croissance_recalc_mort_new)

# COMMAND ----------

if not df_croissance_recalc_mort_new.empty:
    full_table_name = f"{catalog}.{dbName}.croissance_recalc"
    df_spark_df_croissance_recalc_mort_new = spark.createDataFrame(df_croissance_recalc_mort_new)
    df_spark_df_croissance_recalc_mort_new.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)