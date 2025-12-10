import pandas as pd
import numpy as np


## modification of Study/Essai -> change format
#---------------------------------------------
def mod_etude(data):
    
    def extract_etude(s):
        s = str(s)
        study = s.rstrip('0123456789').lstrip('0123456789')
        head = s.rstrip('0123456789')
        projet = s[len(head):]
        year = head[:-len(study)]
        return '-'.join([year, study, projet])
    
    data = (data
            .assign(
                Essai = lambda df: '20' + df['Etude'].apply(extract_etude),
                Annee = lambda df: df['Essai'].apply(lambda x: str(x.split('-')[0])),
                Nordre = lambda df: df['Essai'].apply(lambda x: '0' * (3 - len(str(x.split('-')[-1]))) + str(x.split('-')[-1])),
                Projet = lambda df: df['Essai'].apply(lambda x: x.split('-')[1])
            )
            .drop(
                labels='Etude', axis=1
            )
            .rename(
                columns={
                    'Annee':'Année',
                    'Nordre':'N°ordre'
                }
            )
           )
    return data


## modification of Study/Essai Version 2 -> change format
#-------------------------------------------------------
def mod_etudeV2(data):
    
    def change_study(study):
        
        y, n, c = study.split('-')
        if len(str(y)) == 2:
            y = '20' + str(y)
        else:
            y = '200' + str(y)
             
        n = (3 - len(str(n))) * '0' + str(n)
        
        return '-'.join([y, c, n])
    
    data = (data
            .assign(Essai = lambda df: df['Etude'].apply(change_study)
                   )
            .drop(labels='Etude', axis=1)
           )
    
    return data


def agg_features(data):
    
    id_vars = [
        c for c in data.columns
        if 'PV' not in c
        and 'Parquet' not in c
        and 'N° ' not in c
        ]

    val_vars = [
        c for c in data.columns
        if 'PV' in c
        ]
    
    return id_vars, val_vars


def agg_dict(id_vars, val_vars):
    
    dict_std = {
        c:'first'
        for c in id_vars
        }
    
    dict_PV = {
        c:['mean', 'sum']
        for c in val_vars
        }
    
    return dict_std | dict_PV


## aggregation of PV indiv -> sum (PV total), mean (PV indiv)
#-------------------------------------------------------------
def agg_pesee_indiv(data):
    
    # helper function modification aggregated data
    def mod_period(name):
        period = name.replace('départ', 'P_0').split(' ')[-2]   
        return period

    def mod_calc(name):
        if 'mean' in name:
            return 'PV indiv'
        else:
            return 'PV total'
        
    # dictionary for aggregation (groupby Parquet)
    data = data.dropna(axis=1, how='all').replace(0., np.nan)
        
    id_vars, val_vars = agg_features(data)   
    dict_agg = agg_dict(id_vars, val_vars)
    
    data_agg = data.groupby('Parquet').agg(dict_agg)
    data_agg.columns = [' '.join(item) if item[1] not in ['first'] else item[0] for item in data_agg.columns]
    data_agg = data_agg.reset_index()
    
    # melt data_agg (columns with PV), extract period, re-pivot
    pesee_indiv_calc = (data_agg
                        .melt(
                            id_vars=id_vars + ['Parquet'],
                            var_name='calc'
                            )
                        .assign(
                            period = lambda df: df['calc'].apply(mod_period),
                            calc = lambda df: df['calc'].apply(mod_calc)
                            )
                        .pivot(index=id_vars + ['Parquet', 'period'], columns='calc', values='value')
                        .reset_index()
                        )
    
    pesee_indiv_calc.columns.name = ''
    
    return pesee_indiv_calc