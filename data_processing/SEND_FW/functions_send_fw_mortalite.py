import pandas as pd
import numpy as np



def create_send_fw_mortalite(file_path:str)->pd.DataFrame:
    
    # upload the mortalité table (with diagnostics) studies combined
    #---------------------------------------------------------------
    data = pd.read_excel(file_path)
    
    data_modd = (data
                .rename(
                    columns={
                        'Study':'STUDYID',
                        'alt J mort':'DEATHFW',
                        'alt départ':'DEATHSF'
                        }
                    )
                .assign(
                    USUBJID2 = lambda df: df.groupby(['STUDYID', 'N° parquet'])['N° parquet'].rank('first').astype(int),
                    DOMAIN = 'FW',
                    EUPOOLID = lambda df: df.STUDYID.astype(str) + '_CERN_' + df['N° parquet'].astype(str).apply(lambda x: (3 - len(x)) * '0' + x),
                    USUBJID = lambda df: np.where(
                        df['N° Bague'].isna(),
                        df.EUPOOLID.astype(str) + '_' + df.USUBJID2.astype(str).apply(lambda x: (5 - len(x)) * '0' + x + 'T'),
                        df.EUPOOLID.astype(str) + '_' + df['N° Bague'].fillna(-1.0).astype(int).apply(lambda x: (5 - len(str(x))) * '0' + str(x) if x >= 0 else np.nan)
                        ),
                    FWGRPID = pd.Series(dtype='object'),
                    FWSTAT = pd.Series(dtype='object'),
                    FWREASND = pd.Series(dtype='object'),
                    FWEXCLFL = pd.Series(dtype='object'),
                    FWREASEX = pd.Series(dtype='object'),
                    FWENDTC = pd.Series(dtype='object'),
                    FWDY = pd.Series(dtype='object'),
                    FWENDY = pd.Series(dtype='object'),
                    FWDTC = lambda df: df['date du mort'].apply(lambda x: 'T'.join(str(x).split(' '))).astype(str),
                    )
                .melt(
                    id_vars=['DOMAIN', 'STUDYID', 'USUBJID', 'EUPOOLID', 'FWGRPID', 'FWSTAT', 'FWREASND', 'FWEXCLFL', 'FWREASEX', 'FWDTC',
                            'FWENDTC', 'FWDY', 'FWENDY', 'N° parquet'],
                    value_vars=['DEATHFW', 'DEATHSF'],
                    var_name='FWTESTCD',
                    value_name='FWORRES'
                    )
                .assign(
                    FWORRES = lambda df: df.FWORRES.fillna(-1.).astype(int).apply(lambda x: str(x) if x >= 0 else np.nan),
                    FWORRESU = lambda df: np.where(
                        ~df.FWORRES.astype(str).str.isnumeric() & df.FWORRES.notna(),
                        'Arbitrary Unit',
                        np.where(
                            df.FWORRES.astype(str).str.isnumeric() & df.FWORRES.notna(), 'g', np.nan
                            )
                        ),
                    FWSTRESC = lambda df: np.where(
                        ~df.FWORRES.astype(str).str.isnumeric() & df.FWORRES.notna(),
                        df.FWORRES.astype(str),
                        np.nan
                        ),
                    FWSTRESN = lambda df: np.where(
                        df.FWORRES.astype(str).str.isnumeric() & df.FWORRES.notna(),
                        pd.to_numeric(df.FWORRES), 
                        np.nan
                        ),
                    FWSTRESU = lambda df: np.where(
                        df.FWORRES.astype(str).str.isnumeric() & df.FWORRES.notna(),
                        'g', 
                        np.nan
                        ),
                    FWSEQ = lambda df: df.groupby('EUPOOLID')['N° parquet'].rank('first').astype(int)
                    )
                .dropna(subset='FWORRES')
                )
    
    return data_modd[[
                'DOMAIN', 'STUDYID', 'USUBJID', 'EUPOOLID', 'FWSEQ', 'FWGRPID', 'FWTESTCD', 'FWORRES', 'FWORRESU','FWSTRESC',
                'FWSTRESN','FWSTRESU', 'FWSTAT', 'FWREASND','FWEXCLFL','FWREASEX', 'FWDTC', 'FWENDTC', 'FWDY', 'FWENDY'
                ]]