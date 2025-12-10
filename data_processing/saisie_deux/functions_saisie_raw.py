import pandas as pd
import os
import re
# import functions_general as fg
from saisie_deux.functions_general import dict_rename, col_exclude



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

## change column names of Taire caisse
#---------------------------------------------------------------------------------------------
def change_tare(df):
    tare_o = [c for c in df.columns if 'Tare c' in c]
    tare = [c.split('.')[0] for c in df.columns if 'Tare' in c]
    ser = pd.Series(tare)
    res = (ser + ' P' + ser.groupby(ser).rank('first').astype(int).astype(str)).values.tolist()
    
    # Taise caisse 3 only for periods >= 3
    #-------------------------------------
    fin_tare = [r[:-1] + str(int(r[-1]) + 2) if ' 3 ' in r else r for r in res]
    
    tare_dict = {
        old:new
        for old, new in zip(tare_o, fin_tare)
    }
    
    return tare_dict # old:new column names for Taire caisse


## check whether apport feature is present for the first period
#----------------------------------------------------------------------------------------------
def test_apport(df):
    
    idx = df.columns.tolist().index('Consommation brut P1')
    col = df.columns[:idx]
    col_sel = [c for c in col if 'apport' in c]
    if len(col_sel) == 0:
        return True # no_apport is True
    else:
        return False # no_apport is False
    

# change column names for apport sac
# it is possible that no apport sac for period 1 -> include (no_P1)
#-----------------------------------------------------------------------------------------------
def change_apport(df, no_P1=False):
    apport_o = [c for c in df.columns if 'Apport' in c or 'apport' in c]
    apport = [c.replace('sac ', '').replace('a', 'A').split('.')[0] for c in df.columns if 'Apport' in c or 'apport' in c]
    ser = pd.Series(apport)
    if no_P1:
        res = (ser + ' P' + (ser.groupby(ser).rank('first').astype(int) + 1).astype(str)).values.tolist()
    else:
        res = (ser + ' P' + ser.groupby(ser).rank('first').astype(int).astype(str)).values.tolist()
    
    cond = [item for item in apport_o if 'apport' in item]
    if len(cond) >= 1:
        final = [
            r[:-1] + str(int(r[-1]) + 1) 
            if int(r.split(' ')[1]) == 3
            else r
            and
            r[:-1] + str(int(r[-1]) + 2) 
            if int(r.split(' ')[1]) >= 4
            else r 
            for r in res
        ]
    else:
        final = [
            r[:-1] + str(int(r[-1]) + 1) 
            if int(r.split(' ')[1]) >= 4
            else r
            for r in res
        ]
    apport_dict = {
        old:new
        for old, new in zip(apport_o, final)
    }
    
    return apport_dict


# combine functions and create saisie_raw table
#------------------------------------------------------------
def create_saisie_raw(file_paths):
    tables = []
    errors = {}

    # Now accept a list of file paths
    for file_path in file_paths:
        try:
            data = pd.read_excel(file_path, sheet_name='Saisie', header=1)

            if 'Etude' in data.columns and '-' not in str(data['Etude'].iloc[0]):
                data = mod_etude(data)
            elif 'Etude' in data.columns and '-' in str(data['Etude'].iloc[0]):
                data = mod_etudeV2(data)
            else:
                data = data.rename(columns={'Etude':'Essai'})

            apport_noP1 = test_apport(data)

            data_raw = (
                data
                .rename(columns=change_tare(data))
                .rename(columns=change_apport(data, apport_noP1))
                .rename(columns={'Poids caisse 1 finP1':'Poids caisse 1 fin P1'})
                .rename(columns=dict_rename)
            )

            cols = data_raw.columns
            # exclude stepwise columns based on contain string pattern...
            for item in col_exclude:
                fin = [c for c in cols if item not in c]
                cols = fin

            df_mod = data_raw[cols]
            ps_max = [re.findall(r'P\d+', col)[0] for col in data.columns if len(re.findall(r'P\d+', col)) >= 1]
            nb_periods = pd.Series(ps_max).nunique()  # number of periods
            cols_std = [col for col in df_mod.columns if len(re.findall(r'P\d+', col)) == 0]  # standard columns

            frames = []
            for idx in range(1, nb_periods + 1):
                per = 'P' + str(idx)
                col_psel = [c for c in df_mod.columns if per in c and 'nimaux' not in c]
                col_p = [c for c in df_mod.columns if per in c]
                if len(df_mod[col_psel].dropna(axis=0, how='all')) > 0:
                    frame = df_mod[cols_std + col_p]
                    frame = frame.assign(period=f'P_{idx}')
                    frame.columns = [c.replace(f' P{idx}', '') for c in frame.columns]
                    frames.append(frame)
            final = pd.concat(frames, axis=0)
            final["SourceFile"] = os.path.basename(file_path)
            tables.append(final.reset_index(drop=True))

        except Exception as e:
            errors[file_path] = str(e)

    return tables, errors
