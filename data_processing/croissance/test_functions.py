# Databricks notebook source
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, '../')

from croissance.functions_croissance_recalc import agg_mortalite, calculation_1st, calculation_2nd_V2, calculation_fin, pipeline_croissance_recalc
from mortalité.functions_general import mortalite_2
from saisie_deux.functions_saisie_raw import create_saisie_raw
from pésee_indiv.functions_pesee_agg import agg_pesee_indiv
from pésee_indiv.functions_pesee_indiv import select_pesee_indiv

# COMMAND ----------

pesee_agg = agg_pesee_indiv(select_pesee_indiv('../files_raw/SAIS19-001-FEEv.xlsx'))

# COMMAND ----------

saisie = create_saisie_raw('../files_raw/SAIS19-001-FEEv.xlsx')

# COMMAND ----------

mortalite_agg = agg_mortalite(mortalite_2('../files_raw/SAIS19-001-FEEv.xlsx'))

# COMMAND ----------

mortalite_agg.period.unique()

# COMMAND ----------

join = pd.merge(
        saisie,
        mortalite_agg,
        on=['Essai', 'period', 'Parquet'],
        how='left'
        )

# COMMAND ----------

join[['period', 'Parquet', 'mort_total']]

# COMMAND ----------

calculation_1st(join)[['period', 'Parquet', 'mort_total']]

# COMMAND ----------

calc = calculation_1st(join)

# COMMAND ----------

calculation_2nd_V2(calc, pesee_agg)[['period', 'Parquet', 'mort_total']]

# COMMAND ----------

calculation_fin(calculation_2nd_V2(calc, pesee_agg))[['period', 'Parquet', 'Nb Animaux mort_pesée']]

# COMMAND ----------

pipeline_croissance_recalc('../files_raw/SAIS19-001-FEEv.xlsx')[['period', 'Parquet', 'Nb Animaux mort_pesée']]