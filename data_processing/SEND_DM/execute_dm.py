# Databricks notebook source
import pandas as pd
import numpy as np
import os
import sys
import importlib

sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

import dataframe_cleaner
import functions_dm_general as f_dm_g

import env_config
import dataframe_cleaner

from pésee_indiv.functions_pesee_indiv import send_pesee_indiv
# from SEND_DM.functions_dm_general import process_pesee, process_ts, process_send_dm

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType, DoubleType

# COMMAND ----------

# Adapt the table_name
table_name = "send_dm"
error_table_name = "send_dm_errors"
full_pesee_table_name = env_config.get_full_table_name(table_name)
full_error_pesee_table_name = env_config.get_full_table_name(error_table_name)
volume_path = env_config.get_volume_path()
catalog = "studies"
schema = dbName = db = "cern_growth"

# COMMAND ----------

# path_files = '../files_raw'
# path_file = '../files_raw_new'
files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]
total_files = len(files)

# COMMAND ----------

files

# COMMAND ----------

# files = [file for file in os.listdir(path_file) if '.xlsx' in file]

# COMMAND ----------

files

# COMMAND ----------

# MAGIC %md
# MAGIC ### SEND_SI / SEND_TS tables upload

# COMMAND ----------

def read_table(path):
    return pd.read_excel(path)

# COMMAND ----------

send_ts = pd.read_excel('../SEND_Tables/SEND_TS_StudiesJune23.xlsx')
send_si = pd.read_excel('../SEND_Tables/SEND_SI_StudiesJune23.xlsx')

# COMMAND ----------

# use function for new files
# path is different for original dev
#------------------------------------------

path_send_ts = '../Tables_files_new/SEND_TS_StudiesFeb23.xlsx'
path_send_si = '../Tables_files_new/SEND_SI_StudiesFeb23.xlsx'
send_ts = read_table(path_send_ts)
send_si = read_table(path_send_si)

# COMMAND ----------

errors = {}
frames = []
for file in files:
    print(file)
    try:      
        data = send_pesee_indiv(os.path.join(path_file, file))
        send_dm = process_send_dm(process_pesee(data), process_ts(send_ts), send_si)
        frames.append(send_dm)
        
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors.update({file:message})
        print('error!') 

# COMMAND ----------

errors

# COMMAND ----------

send_dm = pd.concat(frames)

# COMMAND ----------

send_dm.STUDYID.value_counts()

# COMMAND ----------

# send_dm.to_excel('../SEND_Tables/SEND_DM_USUBJID_FALSE_StudiesJune23.xlsx', index=False)

# COMMAND ----------

send_dm.to_excel('../Tables_files_new/SEND_DM_USUBJID_FALSE_StudiesFeb23.xlsx', index=False)