# Databricks notebook source
# /Workspace/Growth_studies/02-Data_processing/SEND_FP/execute_fp
import pandas as pd
import os

import sys
sys.path.insert(0, '../')
sys.path.append('../../__ressources/config')
sys.path.append('../../__ressources/helpers/')

from SEND_FP.functions_fp import create_send_fp, upload_info
import helper

# COMMAND ----------

import pyspark.sql.functions as F
# import log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# COMMAND ----------

catalog = "studies"
schema = dbName = db = "cern_growth"

volume_name = "full_raw"

# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

volume_path

# COMMAND ----------

schema = StructType([
    StructField("DOMAIN", StringType(), True),
    StructField("STUDYID", StringType(), True),
    StructField("FPSEQ", LongType(), True),
    StructField("FPCD", StringType(), True),
    StructField("FPTXT", StringType(), True),  # stays in schema even if empty!
    StructField("FEEDPH", StringType(), True),
    StructField("ARMCD", StringType(), True),
    StructField("ARM", StringType(), True),
    StructField("FPSTDTC", TimestampType(), True),
    StructField("FPENDTC", TimestampType(), True),
    StructField("FPSTDY", LongType(), True),
    StructField("FPENDY", LongType(), True),
    StructField("FPDUR", StringType(), True),
    StructField("FPUPDES", StringType(), True)
])


# COMMAND ----------

path_files = volume_path
# path_files = '../files_raw'
# path_files = '../files_raw_new'

# COMMAND ----------

files = [file for file in os.listdir(path_files) if '.xlsx' in file]

# COMMAND ----------

files = sorted(files)

# COMMAND ----------

toto = ['SAIS19-001-FEEv.xlsx', 'SAIS19-036-DIG.xlsx', 'SAIS19-041-FRE.xlsx', 'SAIS19-077-HBNv.xlsx', 'SAIS19-203-MTO.xlsx', 'SAIS19-208-ECO.xlsx', 'SAIS19-224-MTOv.xlsx', 'SAIS19-228-SSE.xlsx', 'SAIS19-240-TDQv.xlsx', 'SAIS20-013-PHYv.xlsx', 'SAIS20-014-PHYv.xlsx', 'SAIS20-015-PHYv.xlsx', 'SAIS20-021-MET.xlsx', 'SAIS20-038-CIN.xlsx', 'SAIS20-111-SED.xlsx', 'SAIS20-137-METv.xlsx', 'SAIS20-151-MET.xlsx', 'SAIS20-193-PALv.xlsx', 'SAIS20-202-ERUv.xlsx', 'SAIS21-094-PHYv.xlsx', 'SAIS21-109-MEIv.xlsx', 'SAIS21-110-SDPv_brut ne pas toucher .xlsx', 'SAIS21-125-ENIv.xlsx', 'SAIS22-047-MET.xlsx', 'SAIS22-049-MET.xlsx', 'SAIS22-067-ENS.xlsx', 'SAIS22-073-PHY.xlsx', 'SAIS22-089-ROC.xlsx', 'SAIS22-165-PHY.xlsx', 'SAIS23-022-PRO.xlsx', 'SAIS23-024-PRO.xlsx', 'SAIS23-025-ROC.xlsx', 'SAIS23-027-ENS.xlsx', 'SAIS23-037-CER.xlsx', 'SAIS23-040-ROC.xlsx', 'SAIS23-053-MEIv.xlsx', 'SAIS23-075-BUV.xlsx', 'SAIS23-081-PRL.xlsx', 'SAIS24-006-MEI.xlsx', 'SAIS24-035-BUV.xlsx']

cleaned_toto = [f.replace('SAIS', '').replace('.xlsx', '') for f in toto]
display(cleaned_toto)

# COMMAND ----------

files

# COMMAND ----------

errors = {}
frames = []
for n, file in enumerate(files):
    print(file)
    try:
        if n in [1]:
            idx = 1
        if n in [1, 2, 10, 13, 14, 19]:
            idx = 2
        else:
            idx = 0
        
        data = upload_info(os.path.join(path_files, file), idx)
        data_fp = create_send_fp(data)
        frames.append(data_fp)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors.update({file:message})
        print(message)    
    # except Exception as ex:
    #     template = "An exception of type {0} occurred. Arguments:{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     errors.update({file:message})
    #     print('error!') 

# COMMAND ----------

send_fp = pd.concat(frames, ignore_index=True)

# COMMAND ----------

# send_fp.to_excel('../SEND_Tables/SEND_FP_StudiesJune23.xlsx', index=False)

# COMMAND ----------

# Convert the Pandas DataFrame to a Spark DataFrame
send_spark_df = spark.createDataFrame(send_fp,  schema=schema)

# Construct the table name using sanitized identifiers.
table_name = "cern_fp"
full_table_name = f"{catalog}.{db}.{table_name}"

# COMMAND ----------

# Drop the tabla and create a new one
# Use/enable only with caution
spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

# COMMAND ----------

# Write the Spark DataFrame to a Delta table (using overwrite mode)
send_spark_df.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(full_table_name)