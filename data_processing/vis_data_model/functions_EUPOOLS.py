import pandas as pd
import numpy as np
import os
import re

from vis_data_model.functions_send_treatment import modification_send_tx
from SEND_TA.function_send_ta import modification_send_ta

col_fin = [
    'AGE',
    'AGEU',
    'ARM',
    'ARMCD',
    'BRTHDTC',
    'EUPOOLID',
    'P1_FPENDTC',
    'P1_FPSTDTC',
    'P2_FPENDTC',
    'P2_FPSTDTC',
    'P3_FPENDTC',
    'P3_FPSTDTC',
    'RFSTDTC',
    'SBSTRAIN',
    'SPECIES',
    'SSTYP',
    'STRAIN',
    'STUDYID',
    'USUBJID'
]

def modification_send_fp(SEND_FP):
    
    # original scripts plus results for each step:
    # Jupyter notebook: >CREATION_VOLAILLE_EUPOOLS_TREATMENT_TABLE_VIS<
    #------------------------------------------------------------------
    fp_mod = (SEND_FP
              .pivot(index='STUDYID', columns='FPCD', values=['FPSTDTC', 'FPENDTC']).reset_index()
              )
    
    # dict for rename of columns in fp_mod
    #-------------------------------------
    dict_rename = {
        c:'_'.join(c[::-1])
        if 'STUDYID' not in c 
        else c[0] 
        for c in fp_mod.columns
        }
    
    fp_mod.columns = list(dict_rename.values())
    
    # |   STUDYID    | P1_FPSTDTC | P1_FPENDTC | ........ | 
    # ------------------------------------------------------
    # | ------------ |   -------  |   ------   |    --    |   
    # ------------------------------------------------------
    return fp_mod


def mofification_send_ts(SEND_TS):
    
    # original scripts plus results for each step:
    # Jupyter notebook: >CREATION_VOLAILLE_EUPOOLS_TREATMENT_TABLE_VIS<
    #------------------------------------------------------------------    
    cols_ts = ['STUDYID', 'EXPSTDTC', 'NUMANIEU', 'SSTYP']
    
    ts_mod = (SEND_TS
              .assign(
                  TSPARMCD_SEQ = lambda df: df['TSPARMCD'] + '_' + df['TSSEQ'].astype(str)
                  )
              .pivot(
                  index='STUDYID',
                  columns='TSPARMCD_SEQ',
                  values='TSVAL'
                  )
              .reset_index()
              )
    
    cols_sel = [c if cols_ts[idx] in c else None for idx in range(len(cols_ts)) for c in ts_mod.columns]
    ts_mod = ts_mod[[c for c in cols_sel if c]]
    
    ts_fin = (ts_mod
              .rename(columns={c:c.split('_')[0] for c in ts_mod.columns})
              .rename(columns={'STUDYID':'STUDYID_TS'})               
              .rename_axis(columns='')
              )
    
    # |  STUDYID_TS  | EXPSTDTC   | NUMANIEU   | SSTYP    | 
    # ------------------------------------------------------
    # | ------------ |   -------  |   ------   |    --    |   
    # ------------------------------------------------------
    return ts_fin


def creation_volaille_eupools(file_path_SEND_TS, file_path_SEND_TX, file_path_SEND_FP, file_path_SEND_DM):
    
    SEND_DM = pd.read_excel(file_path_SEND_DM)
    SEND_FP = pd.read_excel(file_path_SEND_FP)
    SEND_TS = pd.read_excel(file_path_SEND_TS)
    SEND_TX = pd.read_excel(file_path_SEND_TX)
    
    mod_fp = modification_send_fp(SEND_FP)
    mod_ts = mofification_send_ts(SEND_TS)
    mod_tx = modification_send_tx(SEND_TX)
    
    mod_tx = (mod_tx
              .drop(['DIETCONU', 'DIETCON'], axis=1)
              .rename(
                  columns={
                      'DIETCONU_str':'DIETCONU', 
                      'DIETCON_str':'DIETCON'
                      }
                  )
            #    .assign(
            #        TRAITEMENT = lambda df: df['TRAITEMENT'].apply(lambda x: re.sub(r'\s-\sP\d+D', '', x))
            #        )
              )[['STUDYID', 'REGIME', 'TRAITEMENT']]
    
    merge_dm_ts = pd.merge(
        SEND_DM,
        mod_ts,
        left_on='STUDYID',
        right_on='STUDYID_TS',
        how='left'
        )
    
    merge_2 = pd.merge(
        merge_dm_ts,
        mod_fp,
        on='STUDYID',
        how='left'
        )[col_fin]
    
    eupools = (merge_2
               .groupby(['STUDYID', 'EUPOOLID']).agg({
                        c:'first'
                        if c not in ['AGE']
                        else 'mean'
                        for c in merge_2.columns
                        if c not in ['STUDYID', 'EUPOOLID', 'USUBJID']
                         })
               .reset_index()
               .rename(columns={'ARMCD':'REGIME'})
               )[[
                   'STUDYID',
                   'EUPOOLID',
                   'AGE',
                   'AGEU',
                   'ARM',
                   'REGIME',
                   'BRTHDTC',
                   'SBSTRAIN',
                   'SPECIES',
                   'STRAIN'
                   ]]
    eupools_fin = pd.merge(
        eupools,
        mod_tx,
        on=['STUDYID', 'REGIME'],
        how='left'
    )
    
    return eupools_fin

#-------------------------------------------------------------------------------------------------------------------

def creation_volaille_eupools_TA(file_path_SEND_TA, file_path_SEND_DM):
    
    send_dm = pd.read_excel(file_path_SEND_DM)
   
    mod_ta = modification_send_ta(file_path_SEND_TA)[['STUDYID', 'REGIME', 'TRAITEMENT']]
    
    eupools = (send_dm
               .groupby(['STUDYID', 'EUPOOLID']).agg({
                        c:'first'
                        if c not in ['AGE']
                        else 'mean'
                        for c in send_dm.columns
                        if c not in ['STUDYID', 'EUPOOLID', 'USUBJID']
                         })
               .reset_index()
               .rename(columns={'ARMCD':'REGIME'})
               )[[
                   'STUDYID',
                   'EUPOOLID',
                   'AGE',
                   'AGEU',
                   'ARM',
                   'REGIME',
                   'BRTHDTC',
                   'SBSTRAIN',
                   'SPECIES',
                   'STRAIN'
                   ]]
               
    eupools_fin = pd.merge(
        eupools,
        mod_ta,
        on=['STUDYID', 'REGIME'],
        how='left'
    )
    
    return eupools_fin