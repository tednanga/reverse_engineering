import pandas as pd
import numpy as np



columns_mort = [
    'SSTYP', 
    'STUDYID', 
    'EUPOOLID', 
    'USUBJID', 
    'REGIME', 
    'PHASE', 
    'DEATHD', 
    'ADT', 
    'ADY', 
    'AGED', 
    'NUMD', 
    'NUMD_CUM'
]

exp = [
    ('sortie', 'sortie pour l\'essai'), 
    ('rélevé', 'prélevé'),
    ('point', 'prélevé (car point limite)'), 
    ('cardi', 'cardiaque'),
    ('liminé', 'prélevé'),
    ('prélèvement', 'prélèvement'),
    ('etrouvé sec', 'retrouvé sec')
]

def transform_mortalite(saisie: pd.DataFrame, mortalite: pd.DataFrame) -> pd.DataFrame:
    
    df_saisie = (saisie                 
                 .groupby(['Essai', 'Parquet'])['Date début'].first().reset_index()
                 .assign(
                     date = lambda df: pd.to_datetime(df['Date début'])
                     )
                 .drop('Date début', axis=1)
                 .rename(
                     columns={
                         'Essai':'Study',
                         'Parquet':'N° parquet',
                         'date':'Date début'
                         }
                     )
                 )
    # replace empty Diagnostic where 'Poids mort après pesée' notna() with 'sacrifé après pesée'
    #--------------------------------------------------------------------------------------------
    replace_diag_idx = mortalite[(mortalite['Poids mort après pesée'].notna() & (mortalite.Diagnostic.isna()))].index
    mortalite.loc[replace_diag_idx, 'Diagnostic'] = 'sacrifé après pesée'
    
    deathd = mortalite['Diagnostic'].unique()
    
    list_ = deathd[1:]
    for item in exp:
        match, repl = item
        list_ = [repl if match in c.lower() else c for c in list_]
        
    dict_opt_deadth = {deathd[1:][idx]:list_[idx] for idx in range(len(list_))}
    
    
    result = (mortalite
          .merge(
              df_saisie, 
              on=['Study', 'N° parquet'], 
              how='left'
          )
         .assign(
             REGIME = lambda df: df['n° régime'].apply(lambda x: 'R' + str(x)),
             EUPOOLID = lambda df: df['Study'] + '_CERN_' + df['N° parquet'].astype(int).apply(lambda x: '0' * (3 - (len(str(x)))) + str(x)),
            #  Mod Nadji: period is already in int when it comes to DB
             PHASE = lambda df: df['period'],
             STUDYID = lambda df: df['Study'],
             USUBJID2 = lambda df: df.groupby(['STUDYID', 'N° parquet'])['N° parquet'].rank('first').astype(int),
             USUBJID = lambda df: np.where(
                 df['N°Bague'].isna(),
                 df.EUPOOLID.astype(str) + '_' + df.USUBJID2.astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
                 df.EUPOOLID.astype(str) + '_' + df['N°Bague'].fillna(-1.0).astype(int).apply(lambda x: (5 - len(str(x))) * '0' + str(x) if x >= 0 else np.nan)
                 ),
             NUMD = lambda df: df.groupby('USUBJID')['STUDYID'].rank('first'), # equals 1
            #  NUMD_CUM = lambda df: df.groupby(['STUDYID', 'REGIME'])['REGIME'].rank('first').astype(int),
             ADY = lambda df: df['jour du mort'].apply(lambda x: int(x) + 1).astype(int),
             AGED = lambda df: (pd.to_datetime(df['date du mort']) - df['Date début']).dt.days + 2,
             SSTYP = 'Croissance',
             DEATHD = lambda df: df['Diagnostic'].fillna('---').apply(lambda x: dict_opt_deadth[x] if x != '---' else x),
             NUMD_CUM = lambda df: (df
                                    .mask(df['DEATHD'] == 'sacrifé après pesée')
                                    .mask(df['DEATHD'] == 'prélèvement')
                                    .mask(df['DEATHD'] == "sortie pour l'essai")
                                    .mask(df['DEATHD'] == "prélevé")
                                    .groupby(['STUDYID', 'REGIME'])['REGIME']
                                    .rank('first')
                                   )
             )
          .rename(
              columns={
                  'date du mort':'ADT',
              }
          )


        )[columns_mort]
    
    return result