
import pandas as pd
import numpy as np
import re
from SEND_TA.function_send_ta import modification_send_ta

def set_transform(string):
    return string.replace('PC', 'Positive control ').replace('NC', 'Negative control ').strip()


def DIETCON_2(x):
    return list(x) if len(x) > 1 else x

def DIETCON_str(x):
    return ' & '.join(x) if len(x) > 1 else x

def DIETCONU(x):
    return ' & '.join(x) if len(x) > 1 else x



def combine(x):
    if type(x[0]) == list or type(x[1]) == list:
        combine =  list(zip(x[0], x[1]))
    else:
        combine = [(str(str(x[0]).replace(',', '.')), x[1])]
        
    if combine[0][0] == 'nan':
        return np.nan
    else:
        if len(combine) > 1:
            return ' | '.join([' '.join(tuple(str(x) for x in tpl)) for tpl in combine])
        else:
            return ' '.join(*combine)




def modification_send_ts(SEND_TS: pd.DataFrame):
       
    ts_mod = (SEND_TS
              .pivot(index=['STUDYID', 'TSSEQ'], columns='TSPARMCD', values='TSVAL')
              .dropna(subset='FDSTDTC', axis=0)
              .reset_index()
              .rename_axis(columns='')
              .assign(
                  PHASE = lambda df: df['TSSEQ'].apply(lambda x: 'P' + str(x))
              )
              .sort_values(by=['STUDYID', 'TSSEQ'])
              .fillna(method='ffill')
              .assign(
                  AGE_END = lambda df: (
                      (pd.to_datetime(df['FDENDTC']) - pd.to_datetime(df['EXPSTDTC'])).dt.days + 1).astype(int),
                  AGE_ST = lambda df: (
                      (pd.to_datetime(df['FDSTDTC']) - pd.to_datetime(df['EXPSTDTC'])).dt.days + 1).astype(int),
              )
              .rename(columns={'FDENDTC':'DOSENDTC', 'FDSTDTC':'DOSSTDTC'})
              .reset_index()
              
              )
    
    return ts_mod[
        [
            'STUDYID',
            'PHASE', 
            'AGE_END', 
            'AGE_ST', 
            'DOSENDTC', 
            'DOSSTDTC', 
            'EXPSTDTC', 
            'AGE', 
            'AGEU'
            ]
        ]
    
    
    
def modification_send_tx(SEND_TX:pd.DataFrame):
    
    tx_param = ['TRT', 'DOSSTDTC', 'DOSENDTC', 'DOSDUR', 'DIETCONU', 'DIETCON', 'ARMCD']
    SEND_TX = SEND_TX[SEND_TX.TXPARMCD.isin(tx_param)]
    
    tx_mod = (SEND_TX
              .pivot(index=['STUDYID', 'SETCD', 'TXSEQ'], columns='TXPARMCD', values='TXVAL')
              .dropna(how='all', axis=0)
              .reset_index()
              .rename_axis(columns='')
              .sort_values(by=['STUDYID', 'ARMCD'], ascending=[True, True])
              .groupby(['STUDYID', 'SETCD']).agg(
                  {
                      'TXSEQ':'last',
                      'TRT':'first', 
                      'DOSDUR':'first', 
                      'DOSENDTC':'first',
                      'DOSSTDTC':'first',
                      'DOSDUR':'first',
                      'ARMCD':'first',
                      'DIETCONU':DIETCON_2,
                      'DIETCON' :DIETCON_2
                  })
              .reset_index()
              .assign(
                  TRT = lambda df: df['TRT'].apply(set_transform).apply(lambda x: re.sub(r'\d+ FTU', '', x).strip()),
                  combine = lambda df: df[['DIETCON', 'DIETCONU']].apply(combine, axis=1),
                  TRAITEMENT = lambda df: (df['TRT'] + ' - ' + df['combine'] + ' - ' + df['DOSDUR']).fillna(df['TRT'])
              )
              .sort_values(by=['STUDYID', 'SETCD'])
              .assign(
                  DOSENDTC = lambda df: pd.to_datetime(df['DOSENDTC']).fillna(method='bfill'),
                  DOSSTDTC = lambda df: pd.to_datetime(df['DOSSTDTC']).fillna(method='bfill'),
                  DIETCON = lambda df: df['DIETCON'].apply(
                      lambda x: [float(str(it).replace(',', '.')) for it in x] if type(x) == list else x
                  ),
                  DIETCON_str = lambda df: df['DIETCON'].apply(
                      lambda x: ', '.join([str(it).replace('.0', '') for it in x]) if type(x) == list else x
                  ),
                  DIETCONU_str = lambda df: df['DIETCONU'].apply(
                      lambda x: ', '.join([str(it) for it in x]) if type(x) == list else x
                  ),
                  PHASE = 'Treatement',
                  AGE_END = lambda df: (
                      (pd.to_datetime(df['DOSENDTC']) - pd.to_datetime(df['DOSSTDTC'])).dt.days + 1).astype(int),
                  AGE_ST = 1
              )
              .fillna(np.nan)
              .rename(columns={'ARMCD':'REGIME', 'SETCD':'BRAS'})
              .drop('combine', axis=1)
              )
    
    tx_dietcon = (tx_mod['DIETCON']
                  .apply(pd.Series)
                  .rename(
                      columns={
                          c:f'DIETCON_{c + 1}' 
                          for c in tx_mod['DIETCON'].apply(pd.Series).columns
                          }
                      )
                  .apply(lambda x: x.astype(str).apply(lambda y: y.replace(',', '.') if y else y), axis=1)
                  .astype(float)
                  )
    
    tx_mod_fin = pd.concat((tx_mod, tx_dietcon), axis=1).reset_index(drop=True)
    
    return tx_mod_fin
    
    
def create_data_send_treatment(file_path_SEND_TS, file_path_SEND_TX):
      
    SENDT_TX = pd.read_excel(file_path_SEND_TX)
    SEND_TS = pd.read_excel(file_path_SEND_TS)
    
    ts_mod = modification_send_ts(SEND_TS)
    tx_mod = modification_send_tx(SENDT_TX)
    
    treatments = (pd.concat((tx_mod, ts_mod), axis=0)
                  .drop(['DIETCONU', 'DIETCON'], axis=1)
                  .rename(
                      columns={
                          'DIETCONU_str':'DIETCONU', 
                          'DIETCON_str':'DIETCON'
                          }
                      )
                  )
    return treatments


def create_data_send_treatment_TA(file_path_SEND_TS, file_path_SEND_TA):
    
    cols_fin = ['STUDYID', 'BRAS', 'TRT', 'DOSDUR_sum', 'DOSSTDTC', 'DOSENDTC',
                'REGIME', 'TRAITEMENT', 'PHASE', 'AGE_END', 'AGE_ST', 'DIECPUR_P1', 
                'DIECPUR_P2', 'DIECPUR_P3', 'DIECPUR_P4', 'DIETCON_P1', 
                'DIETCON_P2', 'EXPSTDTC', 'AGE', 'AGEU']
       
    SEND_TS = pd.read_excel(file_path_SEND_TS)
    
    ts_mod = modification_send_ts(SEND_TS)
    ta_mod = modification_send_ta(file_path_SEND_TA)
    
    frames = []
    for col in ['DIECPUR', 'DIETCON']:
        frame = (ta_mod[col]
                 .apply(pd.Series)
                 .rename(
                     columns={
                         c:f'{col}_P{c + 1}'
                         for c in ta_mod[col].apply(pd.Series).columns
                         })
                 .astype(float)
                 .dropna(how='all', axis=1))
        frames.append(frame)
    
    send_ta_mod = pd.concat((ta_mod, *frames), axis=1)
    
    treatments = pd.concat((send_ta_mod, ts_mod), axis=0)
                  
    return treatments[cols_fin].rename(columns={'DOSDUR_sum':'DOSDUR'})