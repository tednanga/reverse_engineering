# Databricks notebook source
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
import os
import time
import sys

import importlib

import numpy as np

sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

import mortality_helper as mh
import save_pandas_to_delta_table as spdt



from pandas import ExcelFile

# from mortalité.functions_general import mortalite_2, mortalite_2V2, process_all_mortalite_files_parallel
# from mortalité.functions_recalc import calc_alternative_mortalite
# from saisie_deux.functions_saisie_raw import create_saisie_raw
# from mortalité.functions_transform import transform_mortalite

import mortalité.functions_general as fg
import mortalité.functions_recalc as fr
import saisie_deux.functions_saisie_raw as sr
import mortalité.functions_transform as ft

import env_config # from env_config import catalog, schema
import logging_config as lc

importlib.reload(fg)
importlib.reload(fr)
importlib.reload(sr)
importlib.reload(ft)
importlib.reload(mh)

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType

# COMMAND ----------

# Adapt the table_name
table_name = "mortality"
error_table_name = "mortality_errors"
full_mortality_table_name = env_config.get_full_table_name(table_name)
full_error_mortality_table_name = env_config.get_full_table_name(error_table_name)
volume_path = env_config.get_volume_path()

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]
total_files = len(files)

# COMMAND ----------

tables, errors = fg.process_all_mortalite_files_parallel(volume_path, files)

# Combine everything into one DataFrame if needed
if tables:
    final_df = pd.concat(tables, ignore_index=True)
    print("Combined all data into final DataFrame.")
else:
    print("No data was processed successfully.")

# Print errors if any
if errors:
    print("Errors encountered:")
    for file, err in errors.items():
        print(f"{os.path.basename(file)} → {err}")


# COMMAND ----------

final_df_clean, final_df_fail = mh.clean_dataframe(final_df)

# COMMAND ----------

schema = StructType([
    StructField("Study", StringType(), True),
    StructField("date du mort", TimestampType(), True),
    StructField("jour du mort", IntegerType(), True),
    StructField("N° parquet", IntegerType(), True),
    StructField("n° régime", IntegerType(), True),
    StructField("poids du mort", FloatType(), True),
    StructField("alt J mort", IntegerType(), True),
    StructField("alt départ", IntegerType(), True),
    StructField("conso du mort", FloatType(), True),
    StructField("Nb animaux", IntegerType(), True),
    StructField("Poids mort après pesée", FloatType(), True),
    StructField("N°Bague", IntegerType(), True),
    StructField("Diagnostic", StringType(), True),
    StructField("period", IntegerType(), True),
    StructField("SourceFile", StringType(), True)
])

# COMMAND ----------

# spark.sql(f"USE {catalog}.{schema}")

# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# spark.sql(f"DROP TABLE IF EXISTS {error_table_name}")

# COMMAND ----------

if final_df_fail is not None and final_df_fail.empty:
    print("No errors found.")
    spdt.save_pandas_to_delta_table(final_df_clean, full_mortality_table_name, schema)
else:
    print("Errors found:")
    spdt.save_pandas_df_to_delta_table(final_df_fail, full_error_mortality_table_name)


# COMMAND ----------

# mortalite_tables = pd.concat(tables)

# COMMAND ----------

# mortalite_tables.to_excel('mortalite_files/StudiesJune2023_mortalité_2_diagnostic.xlsx', index=False, encoding='Windows-1252')

# COMMAND ----------

# MAGIC %md
# MAGIC ## -----------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next cell is only for testing as it doesnt have the mortality

# COMMAND ----------

logger = lc.setup_logger("FileProcessor for mortality recalc")

errors = {}
tables = []

total_files = len(files)
for idx, file in enumerate(files, 1):
    try:
        logger.info(f'[{idx}/{total_files}] Processing file: {file}')
        mortalite = fr.calc_alternative_mortalite(file)
        tables.append(mortalite)
            
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors.update({file:message})
        logger.error(f'[{idx}/{total_files}] Error processing file: {file} -- {message}')

# COMMAND ----------

errors

# COMMAND ----------

mortalite_tables = pd.concat(tables)

# COMMAND ----------

# mortalite_tables.to_excel('mortalite_files/StudiesJune2023_mortalité_2_recalc.xlsx', index=False, encoding='Windows-1252')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create analysis table for mortalité -> visualisation

# COMMAND ----------

logger = lc.setup_logger("FileProcessor for mortality visualization")

errors = {}
tables = []
for idx, file in enumerate(files, 1):
    logger.info(f'[{idx}/{total_files}] Processing file: {file}')
    saisie = sr.create_saisie_raw(file)
    tables.append(saisie)
saisie_all = pd.concat(tables)


# COMMAND ----------

# NB change
# read directly from the table intead of the excel file
mortalite = spark.table(full_mortality_table_name).toPandas()


# COMMAND ----------

mortalite_analysis = ft.transform_mortalite(saisie_all, mortalite)

# COMMAND ----------

# use cleaner to get transform on int type
# Define schema
mortalite_analysis_to_clean_schema = {
    "sstyp": {'type': 'String'},
    "studyid": {'type': 'String'},
    "eupoolid": {'type': 'String'},
    "usubjid": {'type': 'String'},
    "regime": {'type': 'Int64', 'clean_func': mh.extract_digits},
    "phase": {'type': 'Int64'},
    "deathd": {'type': 'String'},
    "adt": {'type': 'datetime64[ns]'},
    "ady": {'type': 'Int64'},
    "aged": {'type': 'Int64'},
    "numd": {'type': 'Int64'},
    "numd_cum": {'type': 'Int64'}
}


# COMMAND ----------

mortalite_analysis_clean, mortalite_analysis_fail = mh.clean_dataframe(mortalite_analysis, mortalite_analysis_to_clean_schema)

# COMMAND ----------

mortalite_analysis_clean.dtypes

# COMMAND ----------

errors

# COMMAND ----------

# Adapt the table_name
table_name = "mortality_vizualisation"
error_table_name = "mortality_vizualisation_errors"
full_mortality_table_name = env_config.get_full_table_name(table_name)
full_error_mortality_table_name = env_config.get_full_table_name(error_table_name)
# volume_path = env_config.get_volume_path()

# COMMAND ----------

mortalite_analysis_fail is not None

# COMMAND ----------

if mortalite_analysis_fail is not None and mortalite_analysis_fail.empty:
    print("No errors found.")
    spdt.save_pandas_to_delta_table(mortalite_analysis_clean, full_mortality_table_name)
else:
    print("Errors found:")
    spdt.save_pandas_df_to_delta_table(mortalite_analysis_fail, full_error_mortality_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dont need the following as we save all earlier in delta

# COMMAND ----------

# col_death = [
#     'SSTYP', 
#     'USUBJID',
#     'PHASE', 
#     'DEATHD', 
#     'ADT', 
#     'ADY', 
#     'AGED', 
#     'NUMD', 
#     'NUMD_CUM'
# ]

# COMMAND ----------

# mortalite_analysis.to_excel('mortalite_files/Volaille_Analysis_Croissance_mortalite.xlsx', index=False)

# COMMAND ----------

# mortalite_analysis[col_death].to_excel('mortalite_files/Volaille_DEATHS_V2.xlsx', index=False)