import pandas as pd
import numpy as np
import re



def create_send_fw_saisie(saisie_raw:pd.DataFrame)->pd.DataFrame:
    
    # create dictionary with
    # 'Apport 1':'FEEDNET1',
    # 'Apport 2':'FEEDNET2',
    # 'Apport 3':'FEEDNET3',
    # 'Apport 4':'FEEDNET4',
    # 'Apport 5':'FEEDNET5',
    # 'Apport 6':'FEEDNET6' 
    # not hard coding for melting data frame
    #----------------------------------------
    dict_apport = {
        col:'FEEDNET {}'.format(re.findall(r'\d', col)[0])
        for col in saisie_raw.columns
        if 'Apport ' in col
        }
    
    data_modd = (saisie_raw
                .rename(
                    columns={
                        'Essai':'STUDYID',
                        # these are the old column names (Sieise(2))
                        # 'Tare mangeoires':'FTARE',
                        # 'Alt départ brut':'FSGW',
                        # 'Alt fin brut + mangeoire 1':'FEGWF',
                        # 'Alt départ brut + mangeoire':'FSGWF',
                        #---------------------------------------------
                        # here are the new ones from saisie_raw
                        'Alt brut 1 granulés fin':'FEGWF',
                        'Alt brut 2 granulés fin':'FEGWF',
                        'Alt départ brut 1 début':'FSGWF',
                        'Alt départ brut 2 début':'FSGWF',
                        'Alt départ brut granulés':'FSGWF',
                        'Alt départ brut miettes':'FSGWF',
                        'Alt fin brut granulés fin':'FEGWF',
                        'Alt fin brut miettes':'FEGWF'                         
                        }
                    )
                .rename(columns=dict_apport)
                .assign(
                    DOMAIN = 'FW',
                    USUBJID = pd.Series(dtype='object'),
                    EUPOOLID = lambda df: df.STUDYID.astype(str) + '_CERN_' + df.Parquet.astype(int).apply(lambda x: (3 - len(str(x))) * '0' + str(x)),
                    POOLID = pd.Series(dtype='object'),
                    FWGRPID = pd.Series(dtype='object'),
                    FWSTAT = pd.Series(dtype='object'),
                    FWREASND = pd.Series(dtype='object'),
                    FWEXCLFL = pd.Series(dtype='object'),
                    FWREASEX = pd.Series(dtype='object'),
                    FWTIME = 'T00:00:00',
                    FWDTC = lambda df: df['Date fin'].astype(str).apply(lambda x: 'T'.join(x.split(' ')) if len(x) == 19 else x),
                    FWENDTC = pd.Series(dtype='object'),
                    FWDY = pd.Series(dtype='object'),
                    FWENDY = pd.Series(dtype='object')
                    )
                .melt(
                        # in id_vars include Parquet -> double/float for ranking EUPOOLID (Bug in older pandas -> KNIME)
                        #----------------------------------------------------------------------------------------------
                        
                    id_vars=['DOMAIN', 'STUDYID', 'USUBJID', 'EUPOOLID', 'POOLID', 'FWGRPID', 'FWSTAT', 'FWREASND', 'FWEXCLFL', 'FWREASEX', 'FWDTC',
                            'FWENDTC', 'FWDY', 'FWENDY', 'Parquet'],
                    value_vars=[
                        # 'FTARE', 
                        # 'FSGW', 
                        'FSGWF', 
                        'FEGWF',
                        *list(dict_apport.values())
                        ], 
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
                    FWSEQ = lambda df: df.groupby('EUPOOLID')['Parquet'].rank('first').astype(int)
                    )
                .dropna(subset='FWORRES')
                )
                
    return data_modd[[
                    'DOMAIN', 'STUDYID', 'USUBJID', 'EUPOOLID', 'POOLID', 'FWSEQ', 'FWGRPID',
                        'FWTESTCD','FWORRES', 'FWORRESU','FWSTRESC','FWSTRESN','FWSTRESU',
                        'FWSTAT', 'FWREASND','FWEXCLFL','FWREASEX', 'FWDTC', 'FWENDTC', 'FWDY', 'FWENDY'
                    ]]