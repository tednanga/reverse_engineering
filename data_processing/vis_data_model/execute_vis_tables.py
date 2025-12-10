# Databricks notebook source
import pandas as pd
import re
import os
import sys

sys.path.insert(0, '../')

from vis_data_model.functions_send_treatment import create_data_send_treatment, modification_send_tx
from vis_data_model.functions_EUPOOLS import creation_volaille_eupools, modification_send_fp, mofification_send_ts
from vis_data_model.functions_studies import create_volaille_studies
from vis_data_model.functions_transform import transform_croissance_calc, new_frames, series_phases, create_volaille_analysis

# COMMAND ----------



# COMMAND ----------

# file_path_SEND_TS = '../SEND_Tables/SEND_TS_StudiesJune23.xlsx'
# file_path_SEND_FP = '../SEND_Tables/SEND_FP_StudiesJune23.xlsx'
# file_path_SEND_DM = '../SEND_Tables/SEND_DM_USUBJID_FALSE_StudiesJune23.xlsx'
# file_path_SEND_TX = '../SEND_Tables/SEND_TX_StudiesJune23.xlsx'

file_path_SEND_TS = '../Tables_files_new/SEND_TS_StudiesFeb23.xlsx'
file_path_SEND_FP = '../Tables_files_new/SEND_FP_StudiesFeb23.xlsx'
file_path_SEND_DM = '../Tables_files_new/SEND_DM_USUBJID_FALSE_StudiesFeb23.xlsx'
file_path_SEND_TX = '../Tables_files_new/SEND_TX_StudiesFeb23.xlsx'

# COMMAND ----------

DATA_SEND_TREATMENT = create_data_send_treatment(file_path_SEND_TS, file_path_SEND_TX)

# COMMAND ----------

DATA_SEND_TREATMENT.to_excel('VIS_MODEL_TABLES/DATA_SEND_TREATMENT_check.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

Volaille_Eupools = creation_volaille_eupools(
    file_path_SEND_TS,
    file_path_SEND_TX,
    file_path_SEND_FP,
    file_path_SEND_DM
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### do a quick control for the equality of Volaille_EUPOOLS in PBi model

# COMMAND ----------

def set_controls(item):
    if 'Positive' in item or 'Control' in item:
        return 'Positive Control'
    elif 'Negative' in item:
        return 'Negative Control'
    else:
        return 'Study'

# COMMAND ----------

Volaille_Eupools = Volaille_Eupools.assign(Controls = lambda df: df['TRAITEMENT'].apply(set_controls))

# COMMAND ----------

Volaille_Eupools.Controls.value_counts()

# COMMAND ----------

Volaille_Eupools.to_excel('VIS_MODEL_TABLES/Volaille_EUPOOLS_V2.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation Volaille Studies Table

# COMMAND ----------

# for new studies to test workflows
#-----------------------------------

file_path_SEND_TSv = '../Tables_files_new/SEND_TS_StudiesFeb23.xlsx'
Volaille_Studies = create_volaille_studies(file_path_SEND_TSv)

# COMMAND ----------

Volaille_Studies.to_excel('../Tables_files_new/Volaille_Studies.xlsx', index=False)

# COMMAND ----------

# Volaille_Studies = create_volaille_studies(file_path_SEND_TS)

# COMMAND ----------

# Volaille_Studies.to_excel('VIS_MODEL_TABLES/Volaille_Studies.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation Volaille_Analysis table

# COMMAND ----------

# path_table = '../croissance/tables_croissance/Croissance_extract_StudiesJune2023.xlsx'
path_table = '../Tables_files_new/Croissance_extract_StudiesFeb2023.xlsx'

# COMMAND ----------

table = pd.read_excel(path_table)

# COMMAND ----------



# COMMAND ----------

df_calcul = transform_croissance_calc(table)

# COMMAND ----------

# cols_analysis = [
#     'EUPOOLID',
#     'APHASE', 
#     'AGE_ST', 
#     'AGE_EN', 
#     'NUMST', 
#     'NUMEN', 
#     'BW_AVG', 
#     'BG_AVG', 
#     'FI_AVG', 
#     'IC'
# ]

# COMMAND ----------

# df_calcul[cols_analysis].to_excel('VIS_MODEL_TABLES/Volaille_Analysis.xlsx', index=False)

# COMMAND ----------

# df_calcul[cols_analysis].to_excel('../Tables_files_new/Volaille_Analysis.xlsx', index=False)
df_calcul.to_excel('../Tables_files_new/Volaille_Analysis.xlsx', index=False)

# COMMAND ----------

df_calcul

# COMMAND ----------

analysis = df_calcul

collected_frames = []
errors = {}
for eupool in analysis.EUPOOLID.unique():
    
    try:
        frame = analysis[analysis.EUPOOLID == eupool]
        if len(frame) > 2:
            phases = series_phases(frame)
            frame_new = new_frames(phases, frame)
            collected_frames.append(frame_new)
        else:
            collected_frames.append(frame)
            
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors.update({eupool:message})
        print('error!')

# COMMAND ----------

errors

# COMMAND ----------

analysis_new = pd.concat(collected_frames, ignore_index=True)

# COMMAND ----------

analysis_new = (analysis_new
                .assign(
                    STUDYID = lambda df: df['EUPOOLID'].str.split('_').str[0]
                )
                .sort_values(by=['STUDYID', 'EUPOOLID', 'APHASE'], ascending=[True, True, True])
               )

# COMMAND ----------

analysis_new

# COMMAND ----------

analysis_newV2 = create_volaille_analysis(df_calcul)

# COMMAND ----------

analysis_newV2

# COMMAND ----------

# analysis_new.to_excel('VIS_MODEL_TABLES/Volaille_Analysis_more_periods.xlsx', index=False)

# COMMAND ----------

analysis_new.to_excel('../Tables_files_new/Volaille_Analysis_more_periods.xlsx', index=False)