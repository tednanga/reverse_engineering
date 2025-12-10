
import pandas as pd
from mortalité.functions_general import mortalite_2, mortalite_2V2


def create_dict_snippets(mortalite):
    
    df_m = mortalite[mortalite['Poids mort après pesée'].isna()]
    periods = df_m.period.unique()
    
    df_pps = {}
    for p in periods:
        # select data frame for period
        df_p = df_m[df_m.period == p]
        # identify parquets with dead animals
        parquets = df_p['N° parquet'].unique()
        # iterate over each parquet
        for prt in parquets:
            df_pp = df_p[df_p['N° parquet'] == prt]
            # no treatment required
            if df_pp['jour du mort'].nunique() > 1:
                if p not in df_pps:
                    df_pps[p] = [df_pp]
                else:
                    df_pps[p].append(df_pp)
                        
    return df_pps



def calc_nb_animauxV2(snippet):
    
    # idx = snippet.index
    # n_pa = snippet['jour du mort'].nunique()
    snippet = snippet.reset_index()

    dfm = (snippet
           .assign(
               # number of animals per parquet
               nbAc = lambda df: df['N° parquet'].groupby(df['N° parquet']).rank('first'),
               # number of dead animals per day
               nb_dAj = lambda df: df['jour du mort'].groupby(df['jour du mort']).rank('max'),
               # create shift of number of dead animals and ffill of NaN (calc feed/animal)
               nb_Ashf = lambda df: ((df['Nb animaux'] - df['nbAc']).shift()).fillna(df['Nb animaux']),
               # cumulative difference alt depart / fill Nan with 0 -> find apport sac
               alt_dep_diff = lambda df: df['alt départ'].diff().fillna(0),              
               # shift alt J mort / fill Nan with alt depart -> create new alt depart...
               alt_mort_sh = lambda df: df['alt J mort'].shift().fillna(df['alt départ']),
               # new alt depart -> sum diff and shift
               alt_d_new = lambda df: df[['alt_dep_diff', 'alt_mort_sh']].sum(axis=1),
               # calculate conso mort per
               c_m = lambda df: ((df['alt_d_new'] - df['alt J mort']) / df['nb_Ashf']).replace(
                   to_replace=0.0, method='ffill'
                   ),          
               # calculate cumsum over c_m (correct conso morts)
            #    c_m_c = lambda df: df['c_m'].cumsum()
                 )
           .groupby(
               'jour du mort'
               ).agg(
                   {
                       'Study':'first',
                       'date du mort':'min',
                       'N° parquet':'min',
                       'alt J mort':'min',
                       'alt_d_new':'max',
                       'period':'first',
                       'nb_Ashf':'max',
                       'nb_dAj':'min',
                       'c_m':'max',
                       'index':'last',
                      #'c_m_c':'min'
                      }
                   )
            .reset_index()
            .assign(
                #c_m = lambda df: (df['alt_d_new'] - df['alt J mort']) / df['nb_Ashf'],
                c_m_c = lambda df: df['c_m'].cumsum(),
                Nb_animaux_fin = lambda df: df['nb_Ashf'] - df['nb_dAj']
                )
            .rename(
                columns={
                    'alt_d_new':'alt départ',
                    'nb_Ashf':'Nb animaux',
                    #'c_m_pAcs':'conso du mort',
                    'c_m_c':'conso du mort'
                    }
                )
            .set_index('index')
            .rename_axis('')
            )[['Study',
               'date du mort', 
               'jour du mort', 
               'N° parquet', 
               'alt J mort', 
               'alt départ', 
               'conso du mort',
               'Nb animaux',
               'Nb_animaux_fin',
               'period']]
    
    # if len(idx) == n_pa:
    #     df_final = dfm.set_index(idx)
    # else:
    #     df_final = dfm.set_index(idx[1:])
    
    
    return dfm


def update_frame(dict_snippets):
    
    frames_all = []
    for k in dict_snippets.keys():
        frames = dict_snippets[k]
        frames_all += frames

    res = [calc_nb_animauxV2(frame) for frame in frames_all]
    if len(res) > 1:
        result = pd.concat(res, axis=0, ignore_index=False) # keep original index!!!
    else:
        result = res[0]

    return result


def calc_alternative_mortalite(path_file):
    
    mortalite = mortalite_2(path_file).reset_index(drop=True)
    dict_snippets = create_dict_snippets(mortalite)
    
    if len(dict_snippets.keys()) >= 1:
        result = update_frame(dict_snippets).drop('Nb_animaux_fin', axis=1)
        mortalite.update(result)
    
    return mortalite


def calc_alternative_mortalite_diagnostic(path_file):
    # Modif NB
    xls = pd.ExcelFile(path_file)
    mortalite = mortalite_2V2(xls).reset_index(drop=True)
    # mortalite = mortalite_2V2(path_file).reset_index(drop=True)
    dict_snippets = create_dict_snippets(mortalite)
    
    if len(dict_snippets.keys()) >= 1:
        result = update_frame(dict_snippets).drop('Nb_animaux_fin', axis=1)
        mortalite.update(result)
    
    return mortalite
