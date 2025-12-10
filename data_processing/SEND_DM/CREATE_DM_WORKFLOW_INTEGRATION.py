# Databricks notebook source
import pandas as pd
import os

# COMMAND ----------

studies = [file for file in os.listdir('../SEND_SI') if 'output' in file]

# COMMAND ----------

studies

# COMMAND ----------

data = pd.read_excel(os.path.join('../SEND_SI', studies[1]))

# COMMAND ----------

pesee_indiv = (data
                 .rename(
                    columns={
                        # 'N° Bague début':'ID',
                        'Date_naissance':'BRTHDTC',
                        'Essai':'STUDYID'
                        }               
                    )
                 .groupby(['STUDYID', 'Parquet', 'N° Animal']).agg({'Traitement':'first', 'Date':'min', 'régime':'first', 'BRTHDTC':'first'}).reset_index()
                )

# COMMAND ----------

pesee_indiv.query('Parquet == 24').shape

# COMMAND ----------



# COMMAND ----------

ts = pd.read_excel('../whole_workflow/Tables_output/SEND_Tables/SEND_TS_StudiesMay24.xlsx')

# COMMAND ----------

cols = ['AGE', 'NUMANIEU', 'EXPSTDTC', 'STRAIN', 'SPECIES', 'SSTYP']
data_mod = (ts
            .assign(
                pivot_col = lambda df: df['TSPARMCD'] + df['TSSEQ'].apply(lambda x: str(x))
            )
            .pivot(
                index='STUDYID', 
                columns='pivot_col',
                values='TSVAL'
            )
        )
cols_sel = [c if cols[idx] in c else None for idx in range(len(cols)) for c in data_mod.columns]
result_ts = (data_mod[[c for c in cols_sel if c]]
             .rename(columns={c:c.replace('1', '') for c in data_mod.columns})
             .reset_index()
             .rename_axis(columns='')
             )

# COMMAND ----------

result_ts

# COMMAND ----------

# MAGIC %md
# MAGIC ### new function -> modification N° Bague début (deletion in Pesée individuelles(2))

# COMMAND ----------

dm_new = (pesee_indiv
.merge(
   result_ts,
   on=['STUDYID'],
   how='left'
   )
.rename(
    columns={
        'AGE':'AGE_TS',
        'EXPSTDTC':'RFSTDTC',
        'Traitement':'ARM'
        }
    )
.assign(
   DOMAIN = "DM",
   SITEID = 'CERN',
   USUBJID2 = lambda df: df.groupby(['STUDYID', 'Parquet', 'Date'])['Parquet'].rank('first').astype(int),
   SUBJID = lambda df: df['USUBJID2'].astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
   EUPOOLID = lambda df: df['STUDYID'] + '_CERN_' + df['Parquet'].astype(int).apply(lambda x: (3 - len(str(x))) * '0' + str(x)),
   USUBJID = lambda df: df['EUPOOLID'] + '_' + df['USUBJID2'].astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
   RFSTDAT = pd.Series(dtype='object'),
   RFSTTIM = pd.Series(dtype='object'),
   AGE = lambda df: (pd.to_datetime(df['RFSTDTC']) - pd.to_datetime(df['BRTHDTC'])).dt.days + 1,
   ARMCD = lambda df: 'R' + df['régime'].astype(int).astype(str),
   SETCD = lambda df: df['régime'].astype(int).astype(str),
   BRTHDTC = lambda df: df['BRTHDTC'].astype(str),
   )      
)[[
    'STUDYID', 'BRTHDTC', 'DOMAIN', 'AGEU','SBSTRAIN', 'SPECIES', 'STRAIN', 'SITEID',
    'SUBJID', 'EUPOOLID', 'USUBJID', 'RFSTDAT', 'RFSTTIM', 'RFSTDTC', 'AGE', 'ARMCD', 'ARM', 'SETCD'
]]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

