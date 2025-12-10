# Databricks notebook source
import pandas as pd
import os
import sys
sys.path.insert(0, '../')


####### IMPORT FUNCTIONS FOR DATA TRANSFORMATION ##########

#----------------Croissance extraction (no recalculation)-------------------------#
from croissance.functions_croissance_extract_calc import extract_croissance_complete

#----------------mortalité extraction (no recalculation)-------------------------#
from mortalité.functions_general import mortalite_2V2
from saisie_deux.functions_saisie_raw import create_saisie_raw
from mortalité.functions_transform import transform_mortalite

#----------------Tables for Power BI Front End-------------------------#
from vis_data_model.functions_transform import transform_croissance_calc, create_volaille_analysis
from vis_data_model.functions_studies import create_volaille_studies
from vis_data_model.functions_EUPOOLS import creation_volaille_eupools, creation_volaille_eupools_TA
from vis_data_model.functions_send_treatment import create_data_send_treatment, create_data_send_treatment_TA
from vis_data_model.functions_send_product import create_product

#----------------SEND Tables-------------------------#
from SEND_TS.ts_functions_triskel import extract_triskel, create_triskel
from SEND_TS.ts_functions_saisie import extract_saisie
from SEND_TS.ts_functions_infos import upload_info, process_infos
from SEND_TS.ts_general import change_study
from SEND_FP.functions_fp import create_send_fp
from SEND_SI.si_functions import create_send_si
from pésee_indiv.functions_pesee_indiv import send_pesee_indiv
from SEND_DM.functions_dm_general import process_pesee, process_ts, process_send_dm
# from SEND_TX_rep.send_tx_rep_functions import create_send_tx_rep, upload_info
# from SEND_TX_rev.functions_tx_infos import create_send_tx
from SEND_TA.ta_infos_class import TA_Infos
from SEND_TA.function_send_ta import create_send_ta_product, modification_send_ta

# COMMAND ----------

def read_table(path):
    return pd.read_excel(path)

# COMMAND ----------

path_studies = 'Studies_input'

# COMMAND ----------

files = os.listdir(path_studies)

# COMMAND ----------

files

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the whole workflow for creation of SEND and Vis tables
# MAGIC #### ----------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### Croissance / Volaille_Analysis

# COMMAND ----------

croissance_tab = []
volaille_analysis_tab = []
errors_cr = {}
for file in files:
    print(file)
    try:
        # extract croissance from Saisie
        # create Volaille_analysis Tables
        #------------------------------------------------------------------------
        croissance = extract_croissance_complete(os.path.join(path_studies, file))
        croissance_tab.append(croissance)
        volaille_analysis = transform_croissance_calc(croissance)
        volaille_analysisV2 = create_volaille_analysis(volaille_analysis)
        volaille_analysis_tab.append(volaille_analysisV2)
    
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors_cr.update({file:message})
        print('error!')

# COMMAND ----------

croissance_concat = pd.concat(croissance_tab)
volaille_analysis_concat = pd.concat(volaille_analysis_tab)

# COMMAND ----------

croissance_concat.to_excel('Tables_output/Croissance_extract_StudiesMay24.xlsx', index=False)
volaille_analysis_concat.to_excel('Tables_output/PowerBI_vis_model/Volaille_Analysis_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND_TS / Volaille_Studies

# COMMAND ----------

path_code_list = os.path.join('../files_raw/code_list/SEND Tables for protocol data.xlsx')
path_triskel = os.path.join('../SEND_TS/meta_data_tables/List of finalized growth studies 2019 to 2022.xlsx')

# COMMAND ----------

triskels = []
saisie_data = []
infos_data = []
errors_ts = {}
for n, file in enumerate(files):
    print(file)
    try:
        triskel = extract_triskel(path_code_list, path_triskel)
        study = pd.read_excel(os.path.join(path_studies, file), sheet_name='Infos').set_index('Essai n°').columns[0]
        triskel_ts = triskel[triskel.STUDYID.isin([change_study(study)])]
        if triskel_ts.empty:
            triskel_ts = create_triskel(os.path.join(path_studies, file))
        triskels.append(triskel_ts)
        #---------------Info from Saisie---------------------------------------#
        saisie = extract_saisie(os.path.join(path_studies, file), path_code_list)
        saisie_data.append(saisie)
        #---------------Info from Infos---------------------------------------#
        if n in [1]:
            idx = 1
        if n in [1, 2, 10, 13, 14, 19]:
            idx = 2
        else:
            idx = 0
        data_info = upload_info(os.path.join(path_studies, file), idx)
        frame_info = process_infos(data_info, path_code_list)
        infos_data.append(frame_info)
        
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors_ts.update({file:message})
        print('error!')

# COMMAND ----------

errors_ts

# COMMAND ----------

final_ts = pd.concat(
    [   pd.concat(infos_data),
        pd.concat(saisie_data),
        pd.concat(triskels)
        ],
    axis=0,
    ignore_index=True
    )

# COMMAND ----------

final_ts.to_excel('Tables_output/SEND_Tables/SEND_TS_StudiesMay24.xlsx', index=False)

# COMMAND ----------

Volaille_Studies = create_volaille_studies('Tables_output/SEND_Tables/SEND_TS_StudiesMay24.xlsx')

# COMMAND ----------

Volaille_Studies.to_excel('Tables_output/PowerBI_vis_model/Volaille_Studies_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND_FP Tables

# COMMAND ----------

frames_fp = []
errors_fp = {}
for n, file in enumerate(files):
    print(file)
    try:
        if n in [1]:
            idx = 1
        if n in [1, 2, 10, 13, 14, 19]:
            idx = 2
        else:
            idx = 0
        data = upload_info(os.path.join(path_studies, file), idx)
        data_fp = create_send_fp(data)
        frames_fp.append(data_fp)
        
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors_fp.update({file:message})
        print('error!') 

# COMMAND ----------

errors_fp

# COMMAND ----------

send_fp = pd.concat(frames_fp)

# COMMAND ----------

send_fp

# COMMAND ----------

send_fp.to_excel('Tables_output/SEND_Tables/SEND_FP_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND_SI Tables

# COMMAND ----------

frames_si = []
errors_si = {}
for file in files:
    # if file == 'SAIS22-156-PRL.xlsx':
        print(file)
        try:
            data = send_pesee_indiv(os.path.join(path_studies, file))
            table_si = create_send_si(data)
            frames_si.append(table_si)
            
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors_si.update({file:message})
            print('error!') 

# COMMAND ----------

errors_si

# COMMAND ----------

send_si = pd.concat(frames_si)

# COMMAND ----------

send_si.to_excel('Tables_output/SEND_Tables/SEND_SI_Studies_New.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND DM Tables

# COMMAND ----------

path_send_si = [file for file in os.listdir('Tables_output/SEND_Tables') if '_SI_' in file][1]
path_send_ts = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TS_' in file][0]

# COMMAND ----------

path_send_si, path_send_ts

# COMMAND ----------

send_ts = read_table(os.path.join('Tables_output/SEND_Tables', path_send_ts))
send_si = read_table(os.path.join('Tables_output/SEND_Tables', path_send_si))

# COMMAND ----------

frames_dm = []
errors_dm = {}
for file in files:
    # if file == 'SAIS22-156-PRL.xlsx':
        print(file)
        try:      
            data = send_pesee_indiv(os.path.join(path_studies, file))
            send_dm = process_send_dm(process_pesee(data), process_ts(send_ts), send_si)
            frames_dm.append(send_dm)
            
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors_dm.update({file:message})
            print('error!') 

# COMMAND ----------

errors_dm

# COMMAND ----------

send_dm = pd.concat(frames_dm)

# COMMAND ----------

send_dm.to_excel('Tables_output/PowerBI_vis_model/SEND_DM_USUBJID_FALSE_Studies_New.xlsx', index=False)

# COMMAND ----------

send_dm.to_excel('Tables_output/SEND_Tables/SEND_DM_USUBJID_FALSE_Studies_New.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND_TA Tables (! replacement SEND_TX)

# COMMAND ----------

frames_ta = []
errors_ta = {}
for file in files:
    print(file)
    try:
        path_file = os.path.join(path_studies, file)
        send_ta_inst = TA_Infos(path_file=path_file, sheet_name='Infos')
        send_ta = send_ta_inst()
        frames_ta.append(pd.concat(send_ta))
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors_ta.update({file:message})
        print('error!')

# COMMAND ----------

errors_ta

# COMMAND ----------

send_ta_final = pd.concat(frames_ta)

# COMMAND ----------

send_ta_final.to_excel('Tables_output/SEND_Tables/SEND_TA_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND_TA_PRODUCT Table

# COMMAND ----------

path_send_ta = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TA_' in file][0]

# COMMAND ----------

path_send_ta

# COMMAND ----------

send_ta_product = create_send_ta_product(os.path.join('Tables_output/SEND_Tables', path_send_ta))

# COMMAND ----------

send_ta_product.to_excel('Tables_output/PowerBI_vis_model/SEND_TA_PRODUCT_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND TX Tables with iteration over periods => SEND_TX_Product vis

# COMMAND ----------

# path_code_list = '../files_raw/code_list/SEND Tables for protocol data.xlsx'

# COMMAND ----------

# frames_tx_rep = []
# errors_tx_rep = {}
# for n, file in enumerate(files):
#     print(file)
    
#     try:
#         # if n in [1]:
#         #     idx = 1
#         # if n in [1, 2, 10, 13, 14, 18]:
#         #     idx = 2
#         # else:
#         idx = 0
        
#         data = upload_info(os.path.join(path_studies, file), idx)
#         frame = create_send_tx_rep(data, path_code_list)
#         frames_tx_rep.append(frame)
            
            
#     except Exception as ex:
#         template = "An exception of type {0} occurred. Arguments:{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         errors_tx_rep.update({file:message})
#         print('error!')

# COMMAND ----------

# errors_tx_rep

# COMMAND ----------

# send_tx = pd.concat(frames_tx_rep)

# COMMAND ----------

# send_tx.to_excel('Tables_output/SEND_Tables/SEND_TX_StudiesFeb24_V2.xlsx', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND TX Table without amplification periods => for Volaille_EUPOOLS

# COMMAND ----------

# frames_tx_rev = []
# errors_tx_rev = {}
# for n, file in enumerate(files):
#     print(file)
    
#     try:
#         # if n in [1]:
#         #     idx = 1
#         # if n in [1, 2, 10, 13, 14, 18]:
#         #     idx = 2
#         # else:
#         idx = 0
        
#         data = upload_info(os.path.join(path_studies, file), idx)
#         frame = create_send_tx(data, path_code_list)
#         frames_tx_rev.append(frame)
            
            
#     except Exception as ex:
#         template = "An exception of type {0} occurred. Arguments:{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         errors_tx_rev.update({file:message})
#         print('error!')

# COMMAND ----------

# errors_tx_rev

# COMMAND ----------

# send_tx_rev = pd.concat(frames_tx_rev)

# COMMAND ----------

# send_tx_rev.to_excel('Tables_output/SEND_Tables/SENT_TX_StudiesFeb24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Volaille_EUPOOLS Table

# COMMAND ----------

# path_send_fp = [file for file in os.listdir('Tables_output/SEND_Tables') if '_FP_' in file][0]
# path_send_ts = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TS_' in file][0]
path_send_dm = [file for file in os.listdir('Tables_output/SEND_Tables') if '_DM_' in file][1]
# path_send_tx = [file for file in os.listdir('Tables_output') if '_TX_' in file][0]
path_send_ta = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TA_' in file][0]

# COMMAND ----------

path_send_dm, path_send_ta

# COMMAND ----------

Volaille_Eupools = creation_volaille_eupools_TA(
    os.path.join('Tables_output/SEND_Tables', path_send_ta),
    os.path.join('Tables_output/SEND_Tables', path_send_dm)
)

# COMMAND ----------

# Volaille_Eupools = creation_volaille_eupools(
#     os.path.join('Tables_output/SEND_Tables', path_send_ts),
#     os.path.join('Tables_output/SEND_Tables', 'SENT_TX_StudiesFeb24.xlsx'),
#     os.path.join('Tables_output/SEND_Tables', path_send_fp),
#     os.path.join('Tables_output/PowerBI_vis_model', path_send_dm)
#     )

# COMMAND ----------

# def set_controls(item):
#     if 'Positive' in item:
#         return 'Positive Control'
#     elif 'Negative' in item:
#         return 'Negative Control'
#     else:
#         return 'Study'

# COMMAND ----------

# Volaille_Eupools = Volaille_Eupools.assign(Controls = lambda df: df['TRAITEMENT'].apply(set_controls))

# COMMAND ----------

Volaille_Eupools.to_excel('Tables_output/PowerBI_vis_model/Volaille_EUPOOLS_StudiesMay24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA_SEND_TREATMENT Table -> important vis in Power BI

# COMMAND ----------

path_send_ts = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TS_' in file][0]
path_send_ta = [file for file in os.listdir('Tables_output/SEND_Tables') if '_TA_' in file][0]

# COMMAND ----------

path_send_ts, path_send_ta

# COMMAND ----------

# DATA_SEND_TREATMENT = create_data_send_treatment(
#     os.path.join('Tables_output/SEND_Tables', path_send_ts), 
#     os.path.join('Tables_output/SEND_Tables', 'SENT_TX_StudiesFeb24.xlsx'),
#     )

# COMMAND ----------

DATA_SEND_TREATMENT = create_data_send_treatment_TA(
    os.path.join('Tables_output/SEND_Tables', path_send_ts), 
    os.path.join('Tables_output/SEND_Tables', path_send_ta)
)

# COMMAND ----------

DATA_SEND_TREATMENT.to_excel('Tables_output/PowerBI_vis_model/SEND_TREATMENTS_StudiesMay25.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA_SEND_PRODUCT Table => Visualisation TESTPROD (use SEND_TX_rep)

# COMMAND ----------

# send_product = create_product('Tables_output/SEND_Tables/SEND_TX_StudiesFeb24_V2.xlsx')

# COMMAND ----------

# send_product.to_excel('Tables_output/PowerBI_vis_model/SEND_TX_PRODUCT_StudiesFeb24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Mortalité Data Tables

# COMMAND ----------

tables_mort = []
errors_mort = {}
for file in files:
    print(file)
    try:
        mortalite = mortalite_2V2(os.path.join(path_studies, file))
        print('---------------------------------------')
        tables_mort.append(mortalite)
        
        name = file.split('.')[0]
        mortalite.to_excel(f'Studies_processed/{name}.xlsx', index=False, sheet_name='mortalité_diagnostic')
            
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        errors_mort.update({file:message})
        print('error!')  

# COMMAND ----------

errors_mort

# COMMAND ----------

mortalite_tables = pd.concat(tables_mort)

# COMMAND ----------

mortalite_tables.to_excel('Tables_output/StudiesMay24_mortalité_2_diagnostic.xlsx', index=False, encoding='Windows-1252')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SEND DEATHS Table -> Vis Power BI

# COMMAND ----------

tables_saisie = []
errors_death = {}

for file in files:
    saisie = create_saisie_raw(os.path.join(path_studies, file))
    tables_saisie.append(saisie)
saisie_all = pd.concat(tables_saisie)

mortalite = pd.read_excel('Tables_output/StudiesMay24_mortalité_2_diagnostic.xlsx')
mortalite_analysis = transform_mortalite(saisie_all, mortalite)

# COMMAND ----------

col_death = [
    'SSTYP', 
    'USUBJID',
    # 'EUPOOLID',
    'APHASE', 
    'DEATHD', 
    'ADT', 
    'ADY', 
    'AGED', 
    'NUMD', 
    'NUMD_CUM'
]

# COMMAND ----------

mortalite_analysis[col_death].to_excel('Tables_output/PowerBI_vis_model/Volaille_DEATHS_StudiesJune24.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------

