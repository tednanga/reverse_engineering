import pandas as pd
import numpy as np

from mortalité.functions_general import mortalite_2
from mortalité.functions_recalc import calc_alternative_mortalite
from saisie_deux.functions_saisie_raw import create_saisie_raw
from pésee_indiv.functions_pesee_agg import agg_pesee_indiv
from pésee_indiv.functions_pesee_indiv import select_pesee_indiv



def agg_mortalite(mortalite):
    
    mortalite_agg = (mortalite
                     .groupby(['N° parquet', 'period']).agg(
                         {
                             'Study':'first',
                             'poids du mort':'sum',
                             'conso du mort':'sum',
                             'jour du mort':'count',
                             'Poids mort après pesée':['count', 'sum']
                             }
                         )
                    )

    mortalite_agg.columns = ['_'.join(c) if 'Poids mort après pesée' in c else c[0] for c in mortalite_agg]

    mortalite_agg = (mortalite_agg
                     .reset_index()
                     .rename(
                         columns={
                             'N° parquet':'Parquet',
                             'jour du mort':'mort_total',
                             'Poids mort après pesée_count':'Nb Animaux pesée',
                             'Poids mort après pesée_sum':'Poids Animaux pesée',
                             'Study':'Essai'
                         }
                     )
                     .assign(
                         Nb_Animaux_mort = lambda df: df['mort_total'] - df['Nb Animaux pesée']
                     )
                     .rename(
                         columns={
                             'Nb_Animaux_mort':'Nb Animaux mort'
                         }
                     )
                    )
    
    return mortalite_agg


def calc_consom_brut(data):
    
    alt_depart = data[[c for c in data.columns if 'Alt' in c and 'départ' in c]].sum(axis=1).fillna(0.)
    apport = data[[c for c in data.columns if 'Apport' in c]].sum(axis=1).fillna(0.)
    alt_fin = data[[c for c in data.columns if 'Alt' in c and 'fin' in c]].sum(axis=1).fillna(0.)
    
    return alt_depart + apport - alt_fin - data['conso du mort'].fillna(0.)


def calc_pv_total(data):
    
    tare_caisse = data[[c for c in data.columns if 'Tare caisse' in c]].sum(axis=1)
    poids_caisse = data[[c for c in data.columns if 'Poids caisse' in c]].sum(axis=1)
    
    return (poids_caisse - tare_caisse)    



def calculation_1st(join_1):
    
    # create columns for calculation number of animals end and start period
    # ---------------------------------------------------------------------

    join_1 = (join_1
              .assign(
                  Nb_Animaux_debut = lambda df: df['Nb Animaux départ'].astype(int)
                  )
              .sort_values(by=['period', 'Parquet'], ascending=[True, True])
              )

    periods = join_1.period.nunique()

    #####################################################################################
    ## alternative calculation / overwriting column
    #------------------------------------------------------------------------------------
    for _ in range(1, periods):    
        join_1 = (join_1
                  .assign(
                      Nb_Animaux_debut = lambda df: (
                          df['Nb_Animaux_debut'] - df['mort_total'].fillna(0.)
                          ).shift(df['Parquet'].nunique()).fillna(df['Nb_Animaux_debut'])
                      )
                  )
    #####################################################################################

    join_1 = (join_1
              .assign(
                  Nb_Animaux_fin = lambda df: df['Nb_Animaux_debut'] - df['Nb Animaux mort'].fillna(0.),
                  Consommation_brut = lambda df: calc_consom_brut(df),
                  PV_total = lambda df: calc_pv_total(df).replace(0., np.nan),
                  PV_indiv = lambda df: (df['PV_total'] / df['Nb_Animaux_fin']).replace(0., np.nan)
                  )
              .rename(
                  columns={
                      'Nb_Animaux_debut':'Nb Animaux début',
                      'Nb_Animaux_fin':'Nb Animaux fin',
                      'Consommation_brut':'Consommation brut',
                      'PV_total':'PV total',
                      'PV_indiv':'PV indiv'
                      }
                  )
              )
    
    return join_1


def calculation_2nd_V2(join_1, pesee_indiv_agg):
    
    # including individual weighing, handling P_0 (artificial)
    periods = join_1.period.unique()
    pi_sel = pesee_indiv_agg[pesee_indiv_agg.period.isin(periods)][['period', 'Parquet', 'PV indiv', 'PV total']]
    pi_sel = (pi_sel
              .sort_values(by=['Parquet', 'period'], ascending=[True, True])
              )

    pi_sel_p = pi_sel.period.unique().tolist()
    
    # transfer only if there are individual weighing...
    if len(pi_sel_p) >= 1:
        join_1_s = join_1[join_1.period.isin(pi_sel_p)].sort_values(by=['Parquet', 'period'], ascending=[True, True])
        pi_sel_s = pi_sel.set_index(join_1_s.index)        
        join_1.loc[join_1_s.index, ['PV indiv', 'PV total']] =\
        join_1_s[['PV indiv', 'PV total']].fillna(pi_sel_s[['PV indiv', 'PV total']])
       
            
    p_zero = pesee_indiv_agg[pesee_indiv_agg.period == 'P_0'].sort_values(by='Parquet', ascending=True)
    frame_final = pd.concat((p_zero, join_1), axis=0, ignore_index=True)
    
    return frame_final


def calculation_fin(frame_final):
    
    # final calculation alternative with two shifts
    # no loops involved !!!

    frame_final = (frame_final
                   .assign(PV_total_corrige = lambda df: (
                       df['PV total'] - (
                           df['PV indiv'] * df['Nb Animaux mort'].fillna(0.).shift(-df.Parquet.nunique())
                           ) - df['Poids Animaux pesée'].fillna(0)
                   )
                          )
                   .assign(
                       GP_total = lambda df: df['PV total'] - df['PV_total_corrige'].shift(df.Parquet.nunique()),
                       GP_indiv = lambda df: df['PV indiv'] - df['PV indiv'].shift(df.Parquet.nunique()),
                       IC = lambda df: df['Consommation brut'] / df['GP_total'],
                       Conso_indiv = lambda df: df['IC'] * df['GP_indiv'],
                       regime = lambda df: df['régime'].apply(lambda x: 'R' + str(x))
                       )
                   .drop('régime', axis=1)
                   .rename(
                       columns={
                           'mort_total':'Nb Animaux mort_pesée',
                           'Conso_indiv':'Conso indiv',
                           'IC':'I.C.',
                           'GP_indiv':'GP indiv',
                           'GP_total':'GP total',
                           'PV_total_corrige':'PV total corrigé',
                           'regime':'régime'
                           }
                       )
                   .sort_values(by=['period', 'Parquet'], ascending=[True, True])
                   .assign(
                       Date_debut = lambda df: df['Date fin'].shift(df['Parquet'].nunique()).fillna(df['Date début']),
                       Age_end = lambda df: ((df['Date fin'] - df['Date début'].fillna(method='ffill')).dt.days.fillna(0) + 1).astype(int),
                       Age_debut = lambda df: df['Age_end'].shift(df['Parquet'].nunique()).fillna(method='bfill').astype(int)
                       )
                   .drop('Date début', axis=1)
                   .rename(columns={'Date_debut':'Date début'})
                   )
    
    result = frame_final[[
        'period', 
        'Parquet', 
        'Date début',
        'Date fin',
        'Age_end',
        'Age_debut',
        'régime',
        'Nb Animaux début',
        'Nb Animaux fin',
        'Nb Animaux mort',
        'Nb Animaux mort_pesée',
        'PV total', 
        'PV indiv', 
        # 'Poids Animaux pesée', 
        'PV total corrigé',
        'GP total',
        'GP indiv',
        'I.C.',
        'Conso indiv'
        ]]
    res = result.copy()
    
    ser = result[['Nb Animaux début', 'Nb Animaux fin']].fillna(method='bfill')
    res.loc[:, ['Nb Animaux début', 'Nb Animaux fin']] = ser.values
    
    return res

## combine all functionalities in final process
#-------------------------------------------------------------------------------------------------
def recalculation_croissance(saisie:pd.DataFrame, pesee_agg:pd.DataFrame, mortalite:pd.DataFrame)->pd.DataFrame:
    
    mortalite_agg = agg_mortalite(mortalite)
    
    join = pd.merge(
        saisie,
        mortalite_agg,
        on=['Essai', 'period', 'Parquet'],
        how='left'
        )
    
    final = calculation_fin(calculation_2nd_V2(calculation_1st(join), pesee_agg))
    
    return final.assign(Essai = saisie['Essai'][0])


def pipeline_croissance_recalc(file_path):
    saisie_tables, saisie_errors = create_saisie_raw([file_path])
    if saisie_errors:
        raise Exception(f"create_saisie_raw failed for {file_path}: {saisie_errors[file_path]}")
    saisie = saisie_tables[0]
    mortalite = mortalite_2(file_path)
    pesee_indiv = select_pesee_indiv(file_path)
    pesee_agg = agg_pesee_indiv(pesee_indiv)
    final = recalculation_croissance(saisie, pesee_agg, mortalite)
    return final

## same function as >pipeline_croissance_recalc< but for mortalite a different function is used
## calc_alternative_mortalite -> recalculate mortalité (based on mortalité extraction)
## problem conso du mort if > 1 dead animals within pen and period
#----------------------------------------------------------------------------------------------
def pipeline_croissance_recalc_mort_new(file_path):
    saisie_tables, saisie_errors = create_saisie_raw([file_path])
    if saisie_errors:
        raise Exception(f"create_saisie_raw failed for {file_path}: {saisie_errors[file_path]}")
    saisie = saisie_tables[0]
    assert isinstance(saisie, pd.DataFrame), f"saisie is not DataFrame: {type(saisie)}"
    mortalite = calc_alternative_mortalite(file_path)
    assert isinstance(mortalite, pd.DataFrame), f"mortalite is not DataFrame: {type(mortalite)}"
    pesee_indiv = select_pesee_indiv(file_path)
    assert isinstance(pesee_indiv, pd.DataFrame), f"pesee_indiv is not DataFrame: {type(pesee_indiv)}"
    pesee_agg = agg_pesee_indiv(pesee_indiv)
    assert isinstance(pesee_agg, pd.DataFrame), f"pesee_agg is not DataFrame: {type(pesee_agg)}"
    final = recalculation_croissance(saisie, pesee_agg, mortalite)
    assert isinstance(final, pd.DataFrame), f"final is not DataFrame: {type(final)}"
    return final

