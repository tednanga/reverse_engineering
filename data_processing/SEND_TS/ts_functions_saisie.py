import pandas as pd
import os

from SEND_TS.ts_general import dict_param_ts, change_study


def extract_saisie(path_excel_file, path_code_list):
    
    data = pd.read_excel(path_excel_file, sheet_name='Saisie', header=1)
    
    param_ts = dict_param_ts(path_code_list)
    
    saisie = (data
              .groupby('Etude').agg(
                  {
                      'Parquet':'count',
                      'Nb Animaux départ':['sum', 'mean'],
                      'Sexe':'first',
                      'Bloc':'nunique'
                      }
                  )
              )
    saisie.columns = ['_'.join(col) for col in saisie.columns]
    
    saisie_mod = (saisie
                  .reset_index()
                  .assign(
                      STUDYID = lambda df: df['Etude'].apply(lambda x: change_study(x)),
                      STRAIN = lambda df: df['Sexe_first'].apply(lambda x: x.split(' ')[0]),
                      NUMEUARM = lambda df: (df['Parquet_count'] / df['Bloc_nunique']).astype(int),
                      NUMANIEU = lambda df: df['Nb Animaux départ_mean'].astype(int),
                      DOMAIN = 'TS',
                      SSTYP = 'Croissance',
                      SRANDOM = 'Y',
                      TFCNTRY = 'FRA',
                      TSTFLOC = '6 Route Noire, 03600 Malicorne, France',
                      AGE = 1,
                      AGEU = 'JOURS'
                      )
                  .rename(
                      columns={
                          'Parquet_count':'NUMEU',
                          'Nb Animaux départ_sum':'SPLANSUB',
                          'Sexe_first':'SBSTRAIN'
                          }
                      )
                  .melt(
                      id_vars=['STUDYID', 'DOMAIN'],
                      value_vars=['NUMEU', 'SPLANSUB', 'NUMANIEU', 'SBSTRAIN', 'STRAIN', 'SSTYP', 'SRANDOM', 'NUMEUARM',
                                   'TFCNTRY', 'TSTFLOC', 'AGE', 'AGEU'],
                      var_name='TSPARMCD',
                      value_name='TSVAL'
                      )
                  .assign(
                      TSPARM = lambda df: df['TSPARMCD'].apply(lambda x: param_ts[x]),
                      TSSEQ = lambda df: df.groupby('TSPARMCD')['TSPARMCD'].rank('first').astype('int'),
                      )
                  )
    
    return saisie_mod