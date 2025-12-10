import pandas as pd
import numpy as np
import os
import re

from saisie_deux.functions_saisie_raw import create_saisie_raw

# def add_essai(data):
    
#     def add_year(year):
#         if len(year) == 4:
#             return year
#         elif len(year) == 2:
#             return '20'+year
#         else:
#             return '200'+year

#     data = (data
#             .assign(
#                 Nordre = lambda df: df['N°ordre'].apply(lambda x: (3 - len(str(x))) * '0' + str(x)),
#                 Annee = lambda df: df['Année'].apply(lambda x: add_year(str(x))),
#                 Essai = lambda df: df['Annee'] + '-' + 'SEL' + '-' + df['Nordre']
#                 )
#             .drop(
#                 labels=['N°ordre', 'Année'], axis=1
#             )
#             .rename(
#                 columns={
#                     'Nordre':'N°ordre',
#                     'Annee':'Année'
#                 }
#             )
#            )
    
#     return data


# def mod_etude(data):
    
#     def extract_etude(s):
#         s = str(s)
#         study = s.rstrip('0123456789').lstrip('0123456789')
#         head = s.rstrip('0123456789')
#         projet = s[len(head):]
#         year = head[:-len(study)]
#         return '-'.join([year, study, projet])
    
#     data = (data
#             .assign(
#                 Essai = lambda df: '20' + df['Essai'].apply(extract_etude),
#                 Annee = lambda df: df['Essai'].apply(lambda x: str(x.split('-')[0])),
#                 Nordre = lambda df: df['Essai'].apply(lambda x: '0' * (3 - len(str(x.split('-')[-1]))) + str(x.split('-')[-1])),
#                 Projet = lambda df: df['Essai'].apply(lambda x: x.split('-')[1])
#             )
#             .rename(
#                 columns={
#                     'Annee':'Année',
#                     'Nordre':'N°ordre'
#                 }
#             )
#            )
#     return data


def mod_etudeV2(data):
    
    data = (data
            .assign(
                Essai = lambda df: df['Essai'].apply(
                    lambda x: '-'.join(['20' + str(x.split('-')[0]), x.split('-')[2], str(x.split('-')[1])])
                ),
                Annee = lambda df: df['Essai'].apply(lambda x: str(x.split('-')[0])),
                Projet = lambda df: df['Essai'].apply(lambda x: x.split('-')[1]),
                Nordre = lambda df: df['Essai'].apply(lambda x: str(x.split('-')[2]))
            )
            .rename(
                columns={
                    'Annee':'Anneé',
                    'Nordre':'N°ordre'
                }
            )
           )
    
    return data


def select_pesee_indiv(file_path):
    # this workflow elilimantes unrequired fields of the sheet    
    data = pd.read_excel(file_path, sheet_name='Pesées individuelles', header=1)
    
    # separate all data/table that do not belong to pesee individuelles
    #------------------------------------------------------------------
    cols = data.columns.tolist()
    end = [c for c in cols if bool(re.search('Unnamed', c))][0] # select the first 'Unnamed' column
    idx = cols.index(end)
    data = data[cols[:idx]]
    data = data.dropna(axis=0, how='all')
    
    # these functions are a carry over from Saisie
    # in Pesées individuelles all Essai infos are available
    #------------------------------------------------------
    # if 'Essai' not in data.columns:
    #     data = add_essai(data)

    # elif '-' not in data['Essai'][0]:
    #     data = mod_etude(data)

    # else:
    data_m = mod_etudeV2(data) # finally change the format of Essai (remove later !!!)
    
    pesee_indiv = (data_m
                   .rename(
                       columns={
                           c:c.replace('fin P', 'fin P_')
                           for c in data_m.columns
                           }
                       )
                   )
    
    return pesee_indiv


## creation Pesee individuelles(2) table for SEND workflows (adaption soladis)
# pivot PV indiv columns / add Date of end period (via saisie raw)
#--------------------------------------------------------------------------
def send_pesee_indiv(file_path):
    try:
        saisie_raws, saisie_errors = create_saisie_raw([file_path])
        if saisie_errors:
            raise Exception(f"create_saisie_raw failed: {saisie_errors[file_path]}")
        saisie_raw = saisie_raws[0]
        print(f"saisie_raw shape: {saisie_raw.shape}")

        pesee_indiv = select_pesee_indiv(file_path).rename(columns={'PV indiv départ':'PV indiv P_0', 'Couleur':'N° Bague 2'})
        print(f"pesee_indiv shape: {pesee_indiv.shape}")

        pv_cols = [c for c in pesee_indiv.columns if 'PV' in c]
        value_vars = [c for c in pv_cols if not pesee_indiv[c].isna().all()]
        print(f"value_vars: {value_vars}")

        if not value_vars:
            print(f"{os.path.basename(file_path)}: No non-empty PV columns found.")
            return pd.DataFrame()

        pesee_indiv_mod = (
            pesee_indiv
            .melt(
                id_vars=[c for c in pesee_indiv.columns if 'PV' not in c],
                value_name='PV indiv',
                var_name='period',
                value_vars=value_vars
            )
            .assign(
                period=lambda df: df['period'].apply(lambda x: x.split(' ')[-1])
            )
            .merge(
                saisie_raw[['Date fin', 'period', 'Parquet']], on=['period', 'Parquet'], how='left'
            )
            .assign(
                Date_debut=saisie_raw['Date début'][0],
                Date=lambda df: df['Date fin'].fillna(df['Date_debut']),
                Date_naissance=lambda df: df['Date'].min(),
            )
            .drop(['Date_debut', 'Date fin'], axis=1)
        )
        print(f"final DataFrame shape: {pesee_indiv_mod.shape}")
        return pesee_indiv_mod

    except Exception as e:
        print(f"Error processing {file_path}: {type(e).__name__}: {e}")
        return pd.DataFrame()
