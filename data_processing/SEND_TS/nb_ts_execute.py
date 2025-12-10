# Databricks notebook source
import pandas as pd
import numpy as np
import os
import importlib

import sys
sys.path.insert(0, '../')


sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

# import functions to process raw data
#--------------------------------------
# from SEND_TS.ts_functions_triskel import extract_triskel, create_triskel
# from SEND_TS.ts_functions_saisie import extract_saisie
# from SEND_TS.ts_functions_infos import upload_info, process_infos
# from SEND_TS.ts_general import change_study
import ts_functions_triskel, ts_functions_saisie, ts_functions_infos, ts_general
import env_config
import dataframe_cleaner

importlib.reload(env_config)

# COMMAND ----------

path_code_list = env_config.get_volume_path("raw", "studies", "cern_growth", "code_list", "SEND Tables for protocol data.xlsx")
path_triskel = env_config.get_volume_path("raw", "studies", "cern_growth", "triskel", "List of finalized growth studies 2019 to 2022.xlsx") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate TS Data from triskel

# COMMAND ----------

triskel_ts = ts_functions_triskel.extract_triskel(path_code_list, path_triskel)

# COMMAND ----------

triskel_ts.head(), triskel_ts.shape

# COMMAND ----------

triskel_ts.STUDYID.unique()

# COMMAND ----------

path_files = env_config.get_volume_path("raw", "studies", "cern_growth", "studies")
# path_files = '../files_raw_new'

# COMMAND ----------

files = [os.path.join(path_files, f) for f in os.listdir(path_files) if f.endswith(".xlsx")]

# COMMAND ----------

studies = []
errors = {}
for file in files:
    try:
        study = pd.read_excel(file, sheet_name='Infos').set_index('Essai n°').columns[0]
        studies.append(study)
    except Exception as ex:
        errors[file] = f"{type(ex).__name__}: {ex}"


# COMMAND ----------

errors

# COMMAND ----------

# select only the studies from Barbara (June23...)
#-------------------------------------------------
triskel_ts = triskel_ts[triskel_ts.STUDYID.isin([ts_general.change_study(s) for s in studies])]

# COMMAND ----------

triskel_ts.empty

# COMMAND ----------

# MAGIC %md
# MAGIC #### In case of no Data, create manually minimal Table (SPECIES) => otherwise DM crash

# COMMAND ----------

frames = []
errors = {}
for file in files:
    try:
        # path_file = os.path.join(path_files, file)
        frame = ts_functions_triskel.create_triskel(file)
        frames.append(frame)
    except Exception as ex:
        errors[file] = f"{type(ex).__name__}: {ex}"

# COMMAND ----------

errors

# COMMAND ----------

triskel_ts = pd.concat(frames)

# COMMAND ----------

triskel_ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate TS Data from Saisie raw data

# COMMAND ----------

# run iteration over each file
tables = []
errors = {}
for file in files:
    try:
        table = ts_functions_saisie.extract_saisie(os.path.join(path_files, file), path_code_list)
        tables.append(table)
        
    except Exception as ex:
        errors[file] = f"{type(ex).__name__}: {ex}"

# COMMAND ----------

ts_saisie = pd.concat(tables, axis=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data from Info sheet

# COMMAND ----------

files.sort()

# COMMAND ----------

ts_frames = []
errors = {}
for n, file in enumerate(files):    
    try:
        if n in [1]:
            idx = 1
        if n in [1, 2, 10, 13, 14, 18, 19]:
            idx = 2
        else:
            idx = 0
        data = ts_functions_infos.upload_info(file, idx)
        frame = ts_functions_infos.process_infos(data, path_code_list)
        ts_frames.append(frame)
        
    except Exception as ex:
        errors[file] = f"{type(ex).__name__}: {ex}"

# COMMAND ----------

errors

# COMMAND ----------

ts_infos_complete = pd.concat(ts_frames, axis=0)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine all three tables -> saisie, triskel, info

# COMMAND ----------

final_ts = pd.concat(
    [
        ts_infos_complete,
        ts_saisie,
        triskel_ts
        ],
    axis=0,
    ignore_index=True
    )

# COMMAND ----------

dataframe_cleaner.clean_and_normalize(final_ts)

# COMMAND ----------

if not final_ts.empty:
    full_table_name = env_config.get_full_table_name("send_ts")
    df_spark_final_ts = spark.createDataFrame(final_ts)
    df_spark_final_ts.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)

# COMMAND ----------

# Volaille_Studies = create_volaille_studies('Tables_output/SEND_Tables/SEND_TS_StudiesMay24.xlsx')

# COMMAND ----------

# final_ts.sort_values(by=['STUDYID'], ascending=True).to_excel('../Tables_files_new/SEND_TS_StudiesFeb23.xlsx', index=False, encoding='Windows-1252')