import pandas as pd
import numpy as np
import os

def create_volaille_studies(path_file_send_ts):
    
    table = pd.read_excel(path_file_send_ts)
    
    table_pivot = (table
                   .pivot(
                       index='STUDYID', 
                       columns=['TSPARMCD', 'TSSEQ'],
                       values='TSVAL'
                       )
                   )
    table_pivot.columns = [''.join((c[0], str(c[1]))) for c in table_pivot.columns]
    
    cols = ['AGE', 'STRAIN', 'NUMANIEU', 'NUMEU', 'SSTYP']
    
    cols_sel = [c if cols[idx] in c else None for idx in range(len(cols)) for c in table_pivot.columns]
    
    volaille_analysis = (table_pivot[[c for c in cols_sel if c]]
                         .rename(columns={c:c.replace('1', '') for c in table_pivot.columns})
                         .reset_index()
                         .rename_axis(columns='')
                         .assign(
                             YEAR = lambda df: df['STUDYID'].apply(lambda x: int(x.split('-')[0])).astype(str),
                             Study_name = lambda df: df['STUDYID'].apply(lambda x: x.split('-')[1]).astype(str),
                             Study_nb = lambda df: df['STUDYID'].apply(lambda x: x.split('-')[2]).astype(str)
                             )
                         .sort_values(by=['YEAR', 'Study_name', 'Study_nb'], ascending=[True, True, True])
                         .assign(
                            STUDY_idx = range(len(table_pivot)),
                            SBSTRAIN = lambda df: df['SBSTRAIN'].apply(lambda x: x.replace('Mâles', ''))
                            )
                         )
    columns_final = ['STUDYID', 'STRAIN', 'SBSTRAIN', 'AGE', 'AGEU', 'NUMANIEU', 'NUMEU', 'NUMEUARM', 'SSTYP', 'YEAR', 'Study_name', 'STUDY_idx']
    
    return volaille_analysis[columns_final]
    
    
    
    