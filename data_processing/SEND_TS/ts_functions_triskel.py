import pandas as pd
import numpy as np
import os

from SEND_TS.ts_general import change_study, dict_param_ts, dict_val_triskel


def extract_triskel(path_code_list, path_triskel):
    
    # upload triskel table
    #-----------------------------------------------
    data = pd.read_excel(path_triskel, header=1)
    
    data.columns = [col.strip() for col in data.columns]
    
    # create new columns STUDTID, N° Batiment and Room
    #--------------------------------------------------    
    data['STUDYID'] = data['N° Essai'].apply(change_study)
    data['Batiment'] = data['Parent'].apply(
        lambda x: 'Bâtiment ' + ''.join(' '.join(x.split('-')[1:]).strip().split(' ')[:2])
        )
    data['room'] = data['Parent'].apply(
        lambda x: ' '.join(' '.join(x.split('-')[1:]).strip().split(' ')[2:]).strip()
        )
    
    # melting / unpivot data
    #------------------------------------------------------
    melt_columns = [col for col in data.columns if col not in ['STUDYID', 'N° Essai', 'Parent']]
    
    data_pv = data.melt(
        id_vars='STUDYID',
        value_vars=melt_columns,
        var_name='TSPARMCD',
        value_name='TSVAL'
        )
    
    param_ts = dict_param_ts(path_code_list)
    val_triskel = dict_val_triskel()
    
    df_pv_mod = (data_pv
                 
                .assign(
                    TSPARMCD = lambda df: df.TSPARMCD.apply(
                        lambda x: val_triskel[x] if x in val_triskel.keys() else np.nan),
                    TSPARM = lambda df: df.TSPARMCD.apply(lambda x: param_ts[x] if x in param_ts.keys() else np.nan),
                    DOMAIN = 'TS',
                    TSVAL = lambda df: df['TSVAL'].apply(lambda x: x.replace('\n', '') if type(x) == str else x)
                    )
                .dropna(axis=0, subset='TSPARMCD')
                .assign(
                    TSSEQ = lambda df: df.groupby(['STUDYID','TSPARMCD'])['TSPARM'].rank('first').astype(int)
                    )
                )[
                    [
                        'STUDYID', 'DOMAIN', 'TSSEQ', 'TSPARMCD', 'TSPARM', 'TSVAL'
                        ]
                    ]
    return df_pv_mod


def create_triskel(path_file):
    
    cols = ['STUDYID', 'DOMAIN', 'TSSEQ', 'TSPARMCD',	'TSPARM', 'TSVAL']
    data = ['TS', 1, 'SPECIES', 'Species', 'Poulet']
    saisie = pd.read_excel(path_file, sheet_name='Saisie', header=1)
    study_id = change_study(saisie['Etude'][0])
    return pd.DataFrame(data=np.array([study_id] + data).reshape(1, 6), columns=cols, index=[0])