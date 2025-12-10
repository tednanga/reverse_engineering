import pandas as pd
import numpy as np
import re

from SEND_TS.ts_general import change_study, dict_param_ts


def upload_info(path_excel_file, idx):
    
    # automatically splits data frame in case of >manually duplication< in Infos sheet
    #-------------------------------------------------------------------------------
    data = pd.read_excel(path_excel_file, sheet_name='Infos')
    data = data.dropna(axis=0, how='all').set_index('Essai n°')
    
    if len(data.loc['Items', :].shape) == 2 and idx == 2:
        data = data.loc['Essai n°':, :].drop('Essai n°')
    elif len(data.loc['Items', :].shape) == 2 and idx == 1:
        data = data.loc[:'Essai n°', :].drop('Essai n°')
        
    return data


############################################################################################
# extracting feed related information / data
def extract_info_feed(data:pd.DataFrame, path_code_list)-> pd.DataFrame:
    
    param_ts = dict_param_ts(path_code_list)
    
    dict_feed = {
    '1':'Aliment «?Standard?», STARTER',
    '2':'Aliment «?Standard?» GROWER',
    '3':'Aliment «?Standard?» FINSHER'
    }
    
    data = data.loc['Dates':, :].dropna(axis=0, how='all')
    
    study = data.columns[0]
    
    feed1 = (data
             .dropna(axis=1, how='all')
             .rename(columns={col:'ENDTCs' for col in data.columns})
             .assign(
                 STDTCs = lambda df: df['ENDTCs'].shift().fillna(df['ENDTCs']),
                 STDTC = lambda df: df.iloc[0, 0],
                 LENGTH = lambda df: (pd.to_datetime(df['ENDTCs']) - df['STDTC']).dt.days,
                 LENGTHs = lambda df: df['LENGTH'].shift().fillna(df['LENGTH']).astype(int),
                 FDDUR = lambda df: 'P' + (df['LENGTH'] - df['LENGTHs']).astype(str) + 'D',
                 STUDYID = change_study(study),
                 DOMAIN = 'TS'
                 )
             .drop('Début')
             .reset_index()
             .rename(columns={'Essai n°':'TSSEQ'})
             .assign(
                 TSSEQ = lambda df: df['TSSEQ'].str.replace('P', ''),
                 )
             )
    feed2 = (feed1
             .rename(columns={'STDTCs':'FDSTDTC', 'ENDTCs':'FDENDTC'})
             .assign(
                 FEED = lambda df: df['TSSEQ'].apply(lambda x: dict_feed[x])
                 )
             .melt(
                 id_vars=['DOMAIN', 'STUDYID', 'TSSEQ'],
                 value_vars=['FDSTDTC', 'FDENDTC', 'FDDUR', 'FEED'],
                 var_name='TSPARMCD',
                 value_name='TSVAL'
                 )
             .sort_values(by='TSSEQ', ascending=True)
             )
    
    feed_ts = (feed1
               .groupby('DOMAIN').agg(
                   {
                       'ENDTCs':'max',
                       'STDTCs':'min',
                       'LENGTH':'max',
                       'TSSEQ':'min',
                       'STUDYID':'first'                    
                       }
                   )
                 .assign(
                     LENGTH = lambda df: df['LENGTH'].apply(lambda x: 'P'+str(x)+'D'),
                     TRMSAC = lambda df: df['LENGTH']
                     )
                 .rename(columns={'ENDTCs':'EXPENDTC', 'STDTCs':'EXPSTDTC', 'LENGTH':'SLENGTH'})
                 .reset_index()
                 .melt(
                     id_vars=['DOMAIN', 'STUDYID', 'TSSEQ'],
                     value_vars=['EXPSTDTC', 'EXPENDTC', 'SLENGTH', 'TRMSAC'],
                     var_name='TSPARMCD',
                     value_name='TSVAL'
                     )
                 )
    fin = pd.concat((feed2, feed_ts), axis=0, ignore_index=True)
    fin['TSPARM'] = fin['TSPARMCD'].apply(lambda x: param_ts[x])
    
    return fin
############################################################################################


############################################################################################
# extract from infos: Items, Lot, MP principal
def extract_info_items(data: pd.DataFrame)-> pd.DataFrame:
    
       
    items = (data
             .loc['Items', :]     
             .apply(
                 lambda x: [item.strip() for item in x.split('+')]
             )
             .apply(pd.Series)
             .rename(mapper={idx:f'Items' for idx in range(len(data.columns))}, axis=1)
             .T
             .assign(Essai = 'Items')
             .rename(columns={'Essai':'Essai n°'})
             .set_index('Essai n°')
             )
    
    return items

def extract_info_lot(data: pd.DataFrame)-> pd.DataFrame:
    
    lots = (data
            .loc['Lots', :]
            .fillna(0.)
            .apply(lambda x: '&'.join(x.split('et')).split('&') if x != 0. else np.nan)
            .apply(pd.Series)
            .T
            .assign(Essai = 'Lots')
            .rename(columns={'Essai':'Essai n°'})
            .set_index('Essai n°')
            )

    return lots

def extract_info_mp(data: pd.DataFrame)-> pd.DataFrame:
    
    mp = (data
          .loc['MP principale', :]
          .apply(pd.Series)
          .T
          .assign(Essai = 'MP principale')
          .rename(columns={'Essai':'Essai n°'})
          .set_index('Essai n°')
         )

    return mp


############################################################################################
# combining everything together
def process_infos(data: pd.DataFrame, path_code_list)-> pd.DataFrame:
    
    exclude = r'/|PC|NC|Témoin RS blé|Témoin RD|T[1-9]|régime|dig|et|niveau|énergie|haut|bas|BD'
    
    dict_info = {
    #'Items_1':'TESTPROD',
    'Lots':'REFPT',
    'Items':'TESTPROD',
    'MP principale':'DIET'
    }
    
    param_ts = dict_param_ts(path_code_list)
    
    data_concat = pd.concat(
        [
            extract_info_items(data),
            extract_info_lot(data),
            extract_info_mp(data)
        ],
        axis=0
    )
    
    infos = (data_concat
              .rename(columns={col:f'SET {idx + 1}' for idx, col in enumerate(data.columns)})
              .reset_index()
              .assign(
                  TSPARMCD = lambda df: df['Essai n°'].apply(lambda x: dict_info[x] if x in dict_info else np.nan)
              )
              .drop('Essai n°', axis=1)
              .melt(
                  id_vars='TSPARMCD',
                  value_vars=[f'SET {idx + 1}' for idx, col in enumerate(data.columns)],
                  var_name='name',
                  value_name='TSVAL'
              )
              .assign(
                  TSVAL = lambda df: df['TSVAL'].apply(lambda x: x if not re.match(exclude, str(x)) else np.nan)
                  )
              .assign(
                  TSVAL = lambda df: df['TSVAL'].apply(lambda x: re.split(r'\d+\s+FTU.*?', x)[0].strip() if type(x) == str else x)
                  )
              .assign(
                  TSVAL = lambda df: df['TSVAL'].apply(lambda x: re.split(r'\d+\.\d+%', x)[-1].strip() if type(x) == str else x)
                  )
              .dropna()
              .drop('name', axis=1)
              .drop_duplicates()
              .assign(
                  DOMAIN = 'TS',
                  STUDYID = change_study(data.columns[0]),
                  TSPARM = lambda df: df['TSPARMCD'].apply(lambda x: param_ts[x]),
                  TSSEQ = lambda df: df.groupby('TSPARMCD')['TSPARMCD'].rank('first').astype(int)
                  )
              )
    feed = extract_info_feed(data, path_code_list)
    
    return pd.concat([feed, infos], axis=0)