# Databricks notebook source
import pandas as pd
import os
import sys
import re
import importlib

sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

from SEND_FW.functions_send_fw_mortalite import create_send_fw_mortalite
from saisie_deux.functions_saisie_raw import create_saisie_raw
from SEND_FW.functions_send_fw_saisie import create_send_fw_saisie
# import functions_send_fw_mortalite
# import functions_saisie_raw
# import functions_send_fw_saisie

import env_config
import dataframe_cleaner

# importlib.reload(env_config)

# COMMAND ----------

mortalite = spark.table('df_spark_df_croissance_recalc')

# COMMAND ----------

mortalite.info()

# COMMAND ----------

path_file_mortalité = '../whole_workflow/Tables_output/StudiesMay24_mortalité_2_diagnostic.xlsx'

# COMMAND ----------

send_fw_mortalite = create_send_fw_mortalite(path_file_mortalité)

# COMMAND ----------

files = os.listdir('../whole_workflow/Studies_input')

# COMMAND ----------

saisie_raw_frames = []
errors = {}
for file in files:
#     if file != 'SAIS21-110-SDPv.xlsx' and file != 'SAIS22-156-PRL.xlsx':
        print(file)
        try:
            saisie_raw = create_saisie_raw(file_path=os.path.join('../whole_workflow/Studies_input', file))
            saisie_raw_frames.append(saisie_raw)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors.update({file:message})
            print('error!')

# COMMAND ----------

saisie_raw_complete = pd.concat(saisie_raw_frames)

# COMMAND ----------

saisie_raw_complete.info()

# COMMAND ----------

sorted([col for col in saisie_raw_complete.columns if 'Alt' in col])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creation SEND_FW

# COMMAND ----------

send_fw_saisies = []
errors = {}
for file in files:
    # if file == 'SAIS19-240-TDQv.xlsx':
        print(file)
        try:
            saisie_raw = create_saisie_raw(file_path=os.path.join('../whole_workflow/Studies_input', file))
            send_fw = create_send_fw_saisie(saisie_raw)
            send_fw_saisies.append(send_fw)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors.update({file:message})
            print('error!')

# COMMAND ----------

send_fw_final = pd.concat((pd.concat(send_fw_saisies), send_fw_mortalite))

# COMMAND ----------

with pd.ExcelWriter('Table_output/SEND_FW_CroissanceStudiesJuly2024.xlsx', mode='a', if_sheet_exists='replace') as writer:
    send_fw_final.to_excel(writer, index=False, sheet_name='SEND_FW_Croissance')

# COMMAND ----------

send_fw = pd.read_excel('Table_output/SEND_FW_CroissanceStudiesJuly2024.xlsx', sheet_name='SEND_FW_Croissance')

# COMMAND ----------

send_fw.STUDYID.unique(), send_fw.STUDYID.nunique()

# COMMAND ----------

send_fw.FWTESTCD.unique()

# COMMAND ----------

send_fw.info()