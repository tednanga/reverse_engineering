# Databricks notebook source
import pandas as pd
import numpy as np
import os
import re

# COMMAND ----------

files = os.listdir('../whole_workflow/Studies_input')

# COMMAND ----------

files

# COMMAND ----------

errors = {}
bagues = {}
studies_bague = []
for file in files:
    print(file)
    try:
        pesee_indiv = pd.read_excel(
            os.path.join('../whole_workflow/Studies_input', file),
            sheet_name='Pesées individuelles',
            header=1
            )
        bagues_columns = re.findall(r'N° Bague \d', ' '.join(pesee_indiv.columns.tolist()))
        bagues.update({file.split('.')[0]:bagues_columns})
        if not np.all(pesee_indiv['N° Bague 1'].isna()) or not np.all(pesee_indiv['N° Bague 2'].isna()):
            studies_bague.append(file)
            
    except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors.update({file:message})
            print('error!')
    

# COMMAND ----------

errors

# COMMAND ----------

bagues

# COMMAND ----------

studies_bague

# COMMAND ----------



# COMMAND ----------

pesee_indiv = pd.read_excel()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

