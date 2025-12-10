# Databricks notebook source
import pandas as pd
import os

import sys
sys.path.insert(0, '../')

from whole_workflow.select_files import FileSelect

# COMMAND ----------

path_current_vis = '../../DataManagement/studies_Barbara/Studies_June23_results/vis'

# COMMAND ----------

files_current_vis = [f for f in os.listdir(path_current_vis) if '.xlsx' in f and '_STD' not in f and 'Standard' not in f]

# COMMAND ----------

files_current_vis

# COMMAND ----------

vis_files = FileSelect(files_current_vis)

# COMMAND ----------

len(vis_files), vis_files[-1], vis_files(vis_files[2])

# COMMAND ----------

files_new = os.listdir('Tables_output/PowerBI_vis_model')

# COMMAND ----------

files_new

# COMMAND ----------

check_update = {}
for f_new in files_new:
    vis_f_cur = vis_files(f_new)
    data_new = pd.read_excel(os.path.join('Tables_output/PowerBI_vis_model', f_new))
    data = pd.read_excel(os.path.join(path_current_vis, vis_f_cur))
    data_up = pd.concat((data, data_new))
    # include fresh index for STUDYID -> order in visual axes
    if vis_f_cur == 'Volaille_Studies':
        data_up = data_up.assign(STUDY_idx = range(len(data_up)))
    data_up.to_excel(os.path.join('Tables_output/PowerBI_update_tables', vis_f_cur), index=False)
    check_update.update({vis_f_cur:{f_new:len(data_new), vis_f_cur:len(data), 'update':(len(data_up))}})
    

# COMMAND ----------

check_update

# COMMAND ----------



# COMMAND ----------

