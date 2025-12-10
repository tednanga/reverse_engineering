import pandas as pd
import numpy as np



def process_pesee(data):
    
    # creates table with all Animals -> Parquet / N° Animal
    # input is Pesée individuelles(2) -> melt PV fin P\d
    #------------------------------------------------------
    
    data_mod = (data
                .rename(
                    columns={
                        # 'N° Bague début':'ID',
                        'Date_naissance':'BRTHDTC',
                        'Essai':'STUDYID'
                        }               
                    )
                .groupby(['STUDYID', 'Parquet', 'N° Animal']).agg({'Traitement':'first', 'Date':'min', 'régime':'first', 'BRTHDTC':'first'}).reset_index()
                )
    
    return data_mod


def process_ts(data):
    
    cols = ['AGE', 'NUMANIEU', 'EXPSTDTC', 'STRAIN', 'SPECIES', 'SSTYP']
    
    data_mod = (data
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
    
    return result_ts


def process_send_dm_Vold(data, ts_sel, send_si, merge=False):
    
    data_mod =(data
               .merge(
                   ts_sel,
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
                   SUBJID = lambda df: np.where(
                       df.ID.isna(),
                       df.USUBJID2.astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
                       df.ID.fillna(-1.).astype(int).apply(lambda x: (5 - len(str(x))) * '0' + str(x) if x > 1 else np.nan)
                       ),
                   EUPOOLID = lambda df: df.STUDYID + '_CERN_' + df['Parquet'].astype(int).apply(lambda x: (3 - len(str(x))) * '0' + str(x)),
                   USUBJID = lambda df: np.where(
                       df.ID.isna(),
                       df.EUPOOLID + '_' + df['USUBJID2'].astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
                       df.EUPOOLID + '_' + df['ID'].fillna(-1.).astype(int).apply(lambda x: (5 - len(str(x))) * '0' + str(x) if x > 0 else np.nan)
                       ),
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
               
    if merge:
        data_mod = (data_mod
                    .merge(
                        send_si[['OSUBJID', 'NSUBJID']],
                        left_on='USUBJID', 
                        right_on='OSUBJID',
                        how='left'
                        )
                    .assign(
                        USUBJID = lambda df: df[['NSUBJID', 'USUBJID']].fillna(method='bfill', axis=1).iloc[:, 0]
                        )
                    .drop(['OSUBJID', 'NSUBJID'], axis=1)
                    )
                    
    return data_mod



def process_send_dm(data, ts_sel, send_si, merge=False):
    
    data_mod =(data
               .merge(
                   ts_sel,
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
               
    if merge:
        data_mod = (data_mod
                    .merge(
                        send_si[['OSUBJID', 'NSUBJID']],
                        left_on='USUBJID', 
                        right_on='OSUBJID',
                        how='left'
                        )
                    .assign(
                        USUBJID = lambda df: df[['NSUBJID', 'USUBJID']].fillna(method='bfill', axis=1).iloc[:, 0]
                        )
                    .drop(['OSUBJID', 'NSUBJID'], axis=1)
                    )
                    
    return data_mod