# /Workspace/Growth_studies/02-Data_processing/SEND_FP/functions_fp.py
import pandas as pd

def change_study(study):
    
    y, n, c = study.split('-')
    if len(str(y)) == 2:
        y = '20' + str(y)
    else:
        y = '200' + str(y)
            
    n = (3 - len(str(n))) * '0' + str(n)
    
    return '-'.join([y, c, n])



def upload_info(file_path, idx):
    
    # automatically splits data frame in case of manually duplication in Infos sheet
    #-------------------------------------------------------------------------------    
    data = pd.read_excel(file_path, sheet_name='Infos')
    data = data.dropna(axis=0, how='all').set_index('Essai n°')
    
    if len(data.loc['Items', :].shape) == 2 and idx == 2:
        data = data.loc['Essai n°':, :].drop('Essai n°')
    elif len(data.loc['Items', :].shape) == 2 and idx == 1:
        data = data.loc[:'Essai n°', :].drop('Essai n°')
        
    return data



def create_send_fp(data):

    study = change_study(data.columns[0])
    
    feed_mod = (data
                .loc['Dates':, :]
                .dropna(axis=1, how='all')
                .dropna(axis=0)
                .rename(columns={col:'FPENDTC' for col in data.columns})
                .assign(
                    FPSTDTC = lambda df: df['FPENDTC'].shift().fillna(df['FPENDTC']),
                    start = lambda df: df.iloc[0, 0],
                    FPENDY = lambda df: (pd.to_datetime(df['FPENDTC']) - df['start']).dt.days,
                    FPSTDY = lambda df: df['FPENDY'].shift().fillna(df['FPENDY']).astype(int),
                    FPDUR = lambda df: 'P' + (df['FPENDY'] - df['FPSTDY']).astype(str) + 'D',
                    STUDYID = study,
                    DOMAIN = 'FP',
                    FPTXT = pd.Series(dtype='object'),
                    FEEDPH = pd.Series(dtype='object'),
                    ARMCD =pd.Series(dtype='object'),
                    ARM = pd.Series(dtype='object'),
                    FPUPDES =pd.Series(dtype='object')
                    )
                .drop(labels='Début')
                .reset_index()
                .rename(
                    columns={
                        'Essai n°':'FPCD'
                        }
                    )
                .assign(
                    FPSEQ = lambda df: df.groupby('STUDYID')['DOMAIN'].rank('first').astype(int)
                    )
                )[[
                    'DOMAIN', 'STUDYID', 'FPSEQ', 'FPCD', 'FPTXT', 'FEEDPH', 'ARMCD',
                    'ARM', 'FPSTDTC', 'FPENDTC', 'FPSTDY', 'FPENDY', 'FPDUR', 'FPUPDES'
                    ]]
    
    return feed_mod
                
                
    