import pandas as pd
import numpy as np
import re

def set_transform(string):
    return string.replace('PC', 'Positive control ').replace('NC', 'Negative control ').strip()


def create_product(path_file_tx):
    
    data_tx = pd.read_excel(path_file_tx)
    
    tx_mod = (data_tx
                .pivot(index=['STUDYID', 'SETCD', 'TXSEQ'], columns='TXPARMCD', values='TXVAL')
                .reset_index()
                .rename_axis(columns='')
                .sort_values(by=['STUDYID', 'SETCD', 'TXSEQ'], ascending=[True, True, True])
                .assign(
                    TRT = lambda df: df['TRT'].fillna(method='ffill').apply(set_transform).apply(lambda x: re.sub(r'\d+ FTU/kg', '', x).strip()),
                    REGIME = lambda df: df['ARMCD'].fillna(method='ffill'),
                    PHASE = lambda df: df['TXSEQ'].apply(lambda x: 'P' + str(x)),
                    DIETCON = lambda df: df['DIETCON'].astype(float),
                    # ACTIVITY = lambda df: df.groupby('STUDYID')['ACTIVITY'].transform(lambda x: x.fillna(method='ffill')).astype(float),
                    # ACTIVITYU = lambda df: df.groupby('STUDYID')['ACTIVITYU'].transform(lambda x: x.fillna(method='ffill'))
                    )
                )[[
                    'STUDYID', 
                    'PHASE', 
                    'REGIME', 
                    'DIETCON', 
                    'DIETCONU', 
                    'DOSSTDTC', 
                    'DOSENDTC',
                    'DOSDUR', 
                    # 'ACTIVITY', 
                    # 'ACTIVITYU', 
                    'TRT'
                ]]
                
    return tx_mod