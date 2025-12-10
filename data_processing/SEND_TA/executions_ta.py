# Databricks notebook source
import pandas as pd
import numpy as np
import re
import os
import sys
import importlib

sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

# from SEND_TA.ta_arm_class import TA_ARM_FEATURES
# from SEND_TA.ta_basics_class import TA_Base
# from SEND_TA.ta_infos_class import TA_Infos
# from SEND_TA.function_send_ta import create_send_ta_product, modification_send_ta
# import ta_arm_class
import ta_basics_class
import ta_infos_class
import function_send_ta 

import env_config
import dataframe_cleaner

# importlib.reload(ta_arm_class)
importlib.reload(ta_basics_class)
importlib.reload(ta_infos_class)
importlib.reload(function_send_ta)

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType, DoubleType
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

volume_path = env_config.get_volume_path()
catalog = "studies"
schema = dbName = db = "cern_growth"
send_ta_full_table_name = f"{catalog}.{dbName}.send_ta"
send_ta_product_full_table_name = f"{catalog}.{dbName}.send_ta_product"

# COMMAND ----------

# MAGIC %md
# MAGIC ## We dont need the following code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test ARM feature for all studies and ARMS

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]

# COMMAND ----------

frames = []
errors = {}
for file in files:
    try:
        # 1. 
        send_ta_inst = ta_infos_class.TA_Infos(file, sheet_name='Infos')
        send_ta = send_ta_inst()
        # Add source file for traceability
        # send_ta['Sourcefile'] = os.path.basename(file)
        frames.append(pd.concat(send_ta).assign(Sourcefile=os.path.basename(file)))
    except Exception as ex:
        errors[file] = f"{type(ex).__name__}: {ex}"


# COMMAND ----------

errors

# COMMAND ----------

send_ta[0].columns

# COMMAND ----------

# send_ta_final = pd.concat(frames)
# Concatenate all results
if frames:
    send_ta_final = pd.concat(frames, ignore_index=True)
    dataframe_cleaner.clean_and_normalize(send_ta_final)
else:
    send_ta_final = pd.DataFrame()

# COMMAND ----------

if not send_ta_final.empty:
    # full_table_name = f"{catalog}.{dbName}.send_ta"
    df_spark_send_ta_final = spark.createDataFrame(send_ta_final)
    df_spark_send_ta_final.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(send_ta_full_table_name)

# COMMAND ----------

# send_ta_final.to_excel('tables_output/SEND_TA_VERSION_2_StudiesCroissanceJune2023.xlsx', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation SEND_TA_PRODUCT (PBi visualisation)

# COMMAND ----------

# send_ta1 = pd.read_excel('tables_output/SEND_TA_VERSION_2_StudiesCroissanceJune2023.xlsx')
# send_ta2 = pd.read_excel('tables_output/SEND_TA_VERSION_2_StudiesCroissanceJune2023_new.xlsx')
try:
    df_send_ta = spark.table(send_ta_full_table_name)
except:
    df_send_ta = None

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

if df_send_ta is not None:
    send_ta_product = function_send_ta.create_send_ta_product(df_send_ta)
    dataframe_cleaner.clean_and_normalize(send_ta_product)
else:
    send_ta_product = None

# COMMAND ----------

if not send_ta_product.empty:
    # full_table_name = f"{catalog}.{dbName}.send_ta"
    df_spark_send_ta_product = spark.createDataFrame(send_ta_product)
    df_spark_send_ta_product.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(send_ta_product_full_table_name)

# COMMAND ----------

# send_ta_product.to_excel('tables_output/SEND_TA_PRODUCT_StudiesJune2023.xlsx', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation TREATMENT Feature for EUPOOLS from SEND_TA

# COMMAND ----------

# send_ta_treatment = function_send_ta.modification_send_ta('tables_output/SEND_TA_StudiesJune2023.xlsx')

# COMMAND ----------

# send_ta_treatment[['STUDYID', 'REGIME', 'TRAITEMENT']]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

frames = []
errors = {}
for file in files:
        try:
            send_ta_inst = ta_infos_class.TA_Infos(file, sheet_name='Infos')
            arms = send_ta_inst()
            res= []
            for arm in arms:
                data = arm.loc[['Régimes', 'ARM', 'Items', 'Base Aliment']]
                res.append(data)
            table = (pd.concat(res)
                    .reset_index()
                    .assign(idx = lambda df: df.groupby('Essai n°')['Essai n°'].rank('first').astype(int))
                    .pivot(index='idx', columns='Essai n°', values='TAVAL')
                    .rename_axis('', axis='columns')
                    .assign(
                        STUDYID = ta_basics_class.TA_Base.change_study(send_ta_inst.data.columns[1])
                        )
                    .set_index('STUDYID')
                    .reset_index()
                    )
            frames.append(table)
            frames.append(pd.concat(send_ta).assign(Sourcefile=os.path.basename(file)))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors.update({file:message})
            print('error!')
    

# COMMAND ----------

send_ta.columns

# COMMAND ----------

list(arms[0].keys())

# COMMAND ----------

errors

# COMMAND ----------

df = pd.concat(frames).to_excel('../SEND_TA/ARM_FEATURES_Studies_CroissanceJune2023.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

send_ta_tb.create_feed_period_frame()

# COMMAND ----------

send_ta_tb.tb_TFP

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Check all entries for Base Aliment (unmodified Infos sheet) Croissance studies

# COMMAND ----------



# COMMAND ----------

len(files_r)

# COMMAND ----------

errors = {}
dicts_ba = []
for file_r in files_r:
    try:
        inst = SEND_TA_Table(path_file=os.path.join('../files_raw', file_r), sheet_name='Infos')
        inst.upload_file()
        table = inst.get_aliment_tbl()
        dicts_ba.append(table)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors.update({file_r:message})
        print('error!')

# COMMAND ----------

errors

# COMMAND ----------

ba_studies = pd.concat(dicts_ba, ignore_index=True)

# COMMAND ----------

ba_studies

# COMMAND ----------

ba_studies.to_excel('BaseAlimentItems_Studies_CroissanceJune2024.xlsx', index=False)