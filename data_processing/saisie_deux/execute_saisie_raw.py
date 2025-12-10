# Databricks notebook source
# /Workspace/Growth_studies/02-Data_processing/saisie_deux/execute_saisie_raw
import pandas as pd
import numpy as np
import os
import sys
import importlib

sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

import mortality_helper as mh
import save_pandas_to_delta_table as spdt

sys.path.insert(0, '../')

# from saisie_deux.functions_saisie_raw import create_saisie_raw
import functions_saisie_raw as fsr
importlib.reload(fsr)
from pyspark.sql.types import *

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, FloatType

# COMMAND ----------

catalog = "studies"
schema = dbName = db = "cern_growth"
table_name = "saisie_deux"  
error_table_name = "saisie_deux_errors"
full_error_table_name = f"{catalog}.{db}.{error_table_name}"
full_table_name = f"{catalog}.{db}.{table_name}"

volume_name = "raw"
volume_path = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

files = [os.path.join(volume_path, f) for f in os.listdir(volume_path) if f.endswith(".xlsx")]
total_files = len(files)

# COMMAND ----------

#  mod Nadji
# errors = {}
# tables = []
tables, errors = fsr.create_saisie_raw(files)

if tables:
    saisie_raw = pd.concat(tables)
    print("Data process successfuly")
else:
    print("No Data process successfuly")

# Print errors if any
if errors:
    print("Errors encountered:")
    for file, err in errors.items():
        print(f"{os.path.basename(file)} → {err}")

# for file in files:
#     # if file == 'SAIS19-001-FEEv.xlsx':
#     print(file)
#     try:
#         saisie_raw = fsr.create_saisie_raw(os.path.join(volume_path, file))
#         print('---------------------------------------')
#         tables.append(saisie_raw)
        
#         # name = file.split('.')[0]
#         # saisie_raw.to_excel(f'../files_processed/{name}.xlsx', index=False, sheet_name='saisie_raw')
#         display(saisie_raw)
            
#     except Exception as ex:
#         template = "An exception of type {0} occurred. Arguments:{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         errors.update({file:message})
#         print('error!')   

# COMMAND ----------

saisie_deux_schema = {
    "period": {'type': 'Int64', 'clean_func': mh.extract_digits}
}

# COMMAND ----------

saisie_raw_clean, saisie_raw_fail = mh.clean_dataframe(saisie_raw, saisie_deux_schema)

# COMMAND ----------

saisie_raw_clean = saisie_raw_clean.astype({
        'dose': 'str'
        , 'lot': 'str'
     })

# COMMAND ----------

if saisie_raw_fail is not None and saisie_raw_fail.empty:
    print("No errors found.")
    spdt.save_pandas_to_delta_table(saisie_raw_clean, full_table_name)
else:
    print("Errors found:")
    spdt.save_pandas_df_to_delta_table(saisie_raw_fail, full_error_table_name)

# COMMAND ----------

# cols_ordered = [
#     'Essai', 'Parquet', 'Bloc', 'régime', 'Aliment', 'Base', 'Sexe', 'Traitement',
#     'Dose', 'Lot', 'Nb Animaux départ', 'period', 'Date début', 'Date fin',
#     'Tare caisse 1', 'Tare caisse 2', 'Tare caisse 3', 'Poids caisse 1 fin',
#     'Poids caisse 2 fin', 'Poids caisse 3 fin', 'Alt départ brut miettes',
#     'Alt fin brut miettes', 'Alt départ brut granulés', 'Alt fin brut granulés fin',
#     'Alt départ brut 1 début', 'Alt brut 1 granulés fin', 'Alt départ brut 2 début',
#     'Alt brut 2 granulés fin', 'Apport 1', 'Apport 2', 'Apport 3', 'Apport 4',
#     'Apport 5', 'Apport 6'
# ]


# COMMAND ----------

# for col in cols_ordered:
#     if col not in saisie_raw_studies.columns:
#         saisie_raw_studies[col] = None

# # Reorder columns
# saisie_raw_studies = saisie_raw_studies[cols_ordered]

# COMMAND ----------

# saisie_raw_studies[cols_ordered].to_excel('saisie_files/StudiesJune23_saisie_raw.xlsx', index=False)