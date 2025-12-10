import pandas as pd
import numpy as np
import re

# old version which is not correct
# changes => no N° Bague début
def create_send_si_Vold(data):
    
    data_mod = (data
                .assign(
                    mort = lambda df: df['N° Bague 1'].apply(lambda x: np.nan if str(x).lower() == 'mort' else x)
                    # ! there are also mort in N° Bague 2 (SAIS19-240-TDQv) -> 1 sample should not appear in SEND_SI (but in SEND_DD)
                )
                .dropna(subset=['mort'])
                .rename(columns={'N° Bague début':'ID', 'Essai':'STUDYID'})
                .groupby(['STUDYID', 'Parquet', 'N° Bague 1', 'ID'], dropna=False).agg({'régime':'first', 'Traitement':'first', 'Date':'min'}).reset_index()
                .assign(
                    USUBJID2 = lambda df: df.groupby(['STUDYID', 'Parquet', 'Date'])['régime'].rank('first').astype(int),
                    DOMAIN = 'SI',
                    EUPOOLID = lambda df: df.STUDYID + '_CERN_' + df.Parquet.apply(lambda x: '0' * (3 - len(str(int(x)))) + str(int(x))),
                    USUBJID = lambda df: np.where(
                        df.ID.isna(),
                        df.EUPOOLID + '_' + df.USUBJID2.apply(lambda x: '0' * (5 - len(str(x))) + str(x) + 'T'),
                        df.EUPOOLID + '_' + df.ID.fillna(-1.).astype(int).apply(lambda x: (5 - len(str(x))) * '0' + str(x) if x > 0 else np.nan)
                        ),
                    OSUBJID = lambda df: df.USUBJID,
                    NSUBJID = lambda df: df.EUPOOLID + '_' + df['N° Bague 1'].fillna(-1.).astype(int).apply(lambda x: str(x) if x > 0 else '')
                    )
    )
    
    return data_mod[['DOMAIN', 'STUDYID', 'EUPOOLID', 'USUBJID', 'OSUBJID', 'NSUBJID']]



def create_send_si(pesee_indiv_2:pd.DataFrame)->pd.DataFrame:
    
    # function creates new subject IDs from labelling (N° Bague 1 and N° Bague 2) in Pesées individuelles
    # These new subject IDs will be exchanged with old IDs in SEND_DM
    # OSUBJID -> NSUBJID
    # input: table Pesees individuelles(2) -> KNIME workflow (melt of PV fin P\d)
    # changes: No N° Bague debut and ID
    #------------------------------------------------------------------------------------------------------
    
    final_columns = ['DOMAIN', 'STUDYID', 'EUPOOLID', 'USUBJID', 'OSUBJID', 'NSUBJID']
    regex = r'mort|remplacant'


    table = (pesee_indiv_2
             .rename(columns={'Essai':'STUDYID'})
             .assign(
                 # filter Mort entries -> np.nan
                 NBague1 = lambda df: df['N° Bague 1'].apply(lambda x: np.nan if bool(re.findall(regex, str(x).lower())) else x),
                 NBague2 = lambda df: df['N° Bague 2'].apply(lambda x: np.nan if bool(re.findall(regex, str(x).lower())) else x),
                 # fill N° Bague 2 with N° Bague 2 (last ID to keep...)
                 NBague = lambda df: df['NBague2'].fillna(df['NBague1'], axis=0),

             )
             .groupby(['STUDYID', 'Parquet', 'NBague']).agg({'régime':'first', 'Traitement':'first', 'Date':'min'})
             .reset_index()
             .assign(
                 NB_fin = lambda df: df['NBague'].apply(lambda x: str(int(x)) if type(x) == float else x),
                 USUBJID2 = lambda df: df.groupby(['STUDYID', 'Parquet', 'Date'])['régime'].rank('first').astype(int),
                 EUPOOLID = lambda df: df['STUDYID'] + '_CERN_' + df['Parquet'].apply(lambda x: '0' * (3 - len(str(int(x)))) + str(int(x))),
                 USUBJID = lambda df: df['EUPOOLID'] + '_' + df['USUBJID2'].apply(lambda x: '0' * (5 - len(str(x))) + str(x) + 'T'),
                 NSUBJID = lambda df: df['EUPOOLID'] + '_' + df['NB_fin'].apply(lambda x: (5 - len(str(x))) * '0' + str(x) if str(x).isnumeric() else x),
                 OSUBJID = lambda df: df['USUBJID'],
                 DOMAIN = 'SI'
                 )
            )
    
    return table[final_columns]