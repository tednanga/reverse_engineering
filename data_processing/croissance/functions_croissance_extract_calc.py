import pandas as pd
import numpy as np
from functools import reduce
import re

col_performance = [
    'I.C.', 
    'GP indiv', 
    'GP total',
    'PV total', 
    'PV indiv',
    'Conso indiv',
    'Date'
]

col_std = [
    'Etude',
    'Parquet'
]

col_stdV2 = [
    'Etude',
    'Parquet',
    'régime',
    'depart'
]

iter_columns = [
    'Conso indiv',
     'GP indiv',
     'GP total',
     'I.C.',
     'PV_total',
     'PV indiv',
     'corrigé',
     'Date'
]


def upload_data(file_path):    
    data = pd.read_excel(file_path, sheet_name='Saisie', header=1).dropna(how='all', axis=1)    
    return data

    
def change_study(study):
    
    y, n, c = study.split('-')
    
    if len(str(y)) == 2:
        y = '20' + str(y)
    else:
        y = '200' + str(y)

    n = (3 - len(str(n))) * '0' + str(n)

    return '-'.join([y, c, n])


def nb_periods(data):
    
    periods = [
        re.findall('P+\d', col)[0] 
        if re.findall('P+\d', col) 
        else 'P0' 
        for col in data.columns 
        if 'PV indiv' in col
        ]
    return periods


def modification_animaux(data, periods):
    
    frames = []
    cols = ['début', 'fin', 'mort']
    for p in periods:
        if p != 'P0':
            data = (data
                    .assign(
                        Nb_Animaux_mort = lambda df: df[f'Nb Animaux début {p}'] - df[f'Nb Animaux fin {p}']
                        )
                    .rename(
                        columns={
                            'Nb_Animaux_mort':f'Nb Animaux mort {p}'
                            }
                        )
                    )

    df_animaux_mod = data.copy()

    for c in cols:
        cols_period = [col for col in df_animaux_mod.columns if c in col]
        df_an_p = df_animaux_mod[col_std + cols_period]
        df_an_p_m = (df_an_p
                     .melt(
                         id_vars=col_std,
                         value_vars=cols_period,
                         var_name='periods',
                         value_name=f'Nb Animaux {c}'
                         )
                     .assign(
                         period = lambda df: df['periods'].apply(
                             lambda x: x.split(' ')[-1][0] + '_' + x.split(' ')[-1][1]
                             ),
                         Essai = lambda df: df['Etude'].apply(change_study)
                         )
                     .drop(['periods', 'Etude'], axis=1)
                     )

        frames.append(df_an_p_m)

    merged = reduce(
        lambda x, y: pd.merge(
            x, y,
            on=['Essai', 'period', 'Parquet'],
            how='left'
        ),
        frames
    )

    final = (merged
             .sort_values(by=['period', 'Parquet'], ascending=[True, True])
             .assign(
                 Nb_Animaux_mort_pesee = lambda df: df['Nb Animaux début'] - df['Nb Animaux début'].shift(-df['Parquet'].nunique())
                 )
             .rename(
                 columns={
                     'Nb_Animaux_mort_pesee':'Nb Animaux mort_pesée'
                     }
                 )
             )
    
    return final




def extract_info_animaux(data):
    
    
    periods = nb_periods(data)
    
    c_animaux = [c if periods[idx] in c and 'Nb ' in c else None for idx in range(len(periods)) for c in data.columns]
    col_animaux = [c for c in c_animaux if c]
    
    df_animaux = data[['Nb Animaux départ'] + col_std + col_animaux]
    
    df_animaux_rn = df_animaux.rename(
        columns={
            col:col.replace('animaux', 'Animaux')
            for col in df_animaux.columns
            } | {
                'Nb Animaux départ':'Nb Animaux début P1'
                }
            )
    table = modification_animaux(df_animaux_rn, periods)
    
    return table


def extract_info_animaux_V2(data):
    
    periods = nb_periods(data)
    periods_add = periods.copy()
    p_add = 'P' + str(max([int(re.sub('P', '', c)) for c in periods]) + 1)
    periods_add.append(p_add)
    
    
    
    c_animaux = [c if periods_add[idx] in c and 'Nb ' in c else None for idx in range(len(periods_add)) for c in data.columns]
    col_animaux = [c for c in c_animaux if c]
    
    df_animaux = data[['Nb Animaux départ'] + col_std + col_animaux]
    
    df_animaux_rn = df_animaux.rename(
        columns={
            col:col.replace('animaux', 'Animaux')
            for col in df_animaux.columns
            } | {
                'Nb Animaux départ':'Nb Animaux début P1'
                }
            )
    table = modification_animaux(df_animaux_rn, periods)
    
    return table[table.period != p_add[0] + '_' + p_add[1]]
    
    

def extract_croissance(data):
    
    sel = [c if col_performance[idx] in c else None for idx in range(len(col_performance)) for c in data.columns]
    col_perf = [c for c in sel if c]
    df_values = data[col_std + ['régime'] + col_perf]
    
    df_values_mod = (df_values
                     .assign(
                         IC_P0 = pd.Series(dtype='float'),
                         GP_indiv_P0 = pd.Series(dtype='float'),
                         GP_total_P0 = pd.Series(dtype='float'),
                         Conso_indiv_P0 = pd.Series(dtype='float'),
                         depart = lambda df: df['Date début P1'],
                         Regime = lambda df: df['régime'].apply(lambda x: 'R' + str(x))
                         )
                     .drop('régime', axis=1)
                     .rename(
                         columns={
                             col:col.replace('départ', 'P0').replace('fin ', '').replace('début P1', 'P0')
                             for col in df_values.columns
                             }
                         )
                     .rename(
                         columns={
                             'IC_P0':'I.C. P0',
                             'GP_indiv_P0':'GP indiv P0',
                             'GP_total_P0':'GP total P0',
                             'Conso_indiv_P0':'Conso indiv P0',
                             'Regime':'régime'
                             }
                         )
                     )
    df_values_mod = df_values_mod.rename(
        columns={
            col:col.replace('PV total', 'PV_total')
            for col in df_values_mod.columns
            if 'corrigé' not in col
            }
        )
    
    frames = []
    for col in iter_columns:
        val_col = [c for c in df_values_mod.columns if col in c]
        df_val = df_values_mod[col_stdV2 + val_col]
        df_val_melt = (df_val
                    .melt(
                        id_vars=col_stdV2,
                        value_vars=val_col,
                        var_name='var_name',
                        value_name=col
                    )
                    .assign(
                        period_ = lambda df: df['var_name'].apply(lambda x: re.findall('P+\d', x)[0]),
                        period = lambda df: df['period_'].apply(lambda x: '_'.join([x[0], x[1]])),
                        Essai = lambda df: df['Etude'].apply(change_study)
                        )
                    .drop(['period_', 'Etude', 'var_name'], axis=1)
                    .rename(columns={'corrigé':'PV total corrigé'})
                    )
        frames.append(df_val_melt)

    result = reduce(
        lambda x, y: pd.merge(
            x, y,
            on=['Parquet', 'period', 'Essai', 'depart', 'régime'],
            how='left'
            ),
        frames
        )
    
    result = (result
              .sort_values(by=['period', 'Parquet'], ascending=[True, True])
              .assign(
                  Date_debut = lambda df: df['Date'].shift(df['Parquet'].nunique())
                  )
              .rename(
                  columns={
                      'Date_debut':'Date début',
                      'Date':'Date fin',
                      'PV_total':'PV total'
                      }
                  )
              .assign(
                  Age_end = lambda df: (df['Date fin'] - df['depart']).dt.days + 1,
                  Age_debut = lambda df: df['Age_end'].shift(df['Parquet'].nunique()).fillna(method='bfill').astype(int)
                  )
              .drop('depart', axis=1)
              )
    
    result.loc[result[result.period == 'P_0']['Date fin'].index, 'Date fin'] = np.nan
    result.loc[result[result.period == result.period.unique()[-1]]['PV total corrigé'].index, 'PV total corrigé'] = np.nan # change last period to np.nan
    
    return result


def extract_croissance_complete(file_path):
    
    data = upload_data(file_path)
    croissance_values = extract_croissance(data)
    # croissance_animaux = extract_info_animaux(data)
    croissance_animaux = extract_info_animaux_V2(data)
    
    result = pd.merge(croissance_values, croissance_animaux, on=['period', 'Parquet', 'Essai'], how='left')
    result[['Nb Animaux début', 'Nb Animaux fin']] = result[['Nb Animaux début', 'Nb Animaux fin']].fillna(method='bfill')
    
    return result
    
    