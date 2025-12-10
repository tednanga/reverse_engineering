# /Workspace/Growth_studies/02-Data_processing/mortalité/functions_general.py
import pandas as pd
import re

from mortalité.functions_apres_pesee import extract_pesee
from mortalité.functions_mortalite import extract_mortalite
from mortalité.functions_diagnostic import extract_diagnosis

from pandas import ExcelFile
import os
from multiprocessing import Pool, cpu_count

def change_study(study):
        
        y, n, c = study.split('-')
        if len(str(y)) == 2:
            y = '20' + str(y)
        else:
            y = '200' + str(y)
             
        n = (3 - len(str(n))) * '0' + str(n)
        
        return '-'.join([y, c, n])


#######################################################################
# concat >Mortalité après la mise à jeun pesée< tables from periods   #
#---------------------------------------------------------------------#
def periods_concat(df:pd.DataFrame)-> pd.DataFrame:
    
    # check number of periods
    periods = list(set([re.findall(r'\d+', col)[0] for col in df.columns]))
    
    # helper function for each period
    #---------------------------------------------------------------
    def sel_p(p):
        sub_tbl = df[[col for col in df.columns if f'P{p}' in col]]
        sub_tbl_m = (sub_tbl
                     .rename(
                         columns={
                             col:col.split('_')[0]
                             for col in sub_tbl.columns
                         }
                     )
                     .dropna(axis=0, how='all')
                     .assign(period = f'P_{p}')
                    )
        return sub_tbl_m
    #---------------------------------------------------------------
        
    if len(periods) > 1:
        tables = []
        for p in periods:
            tbl = sel_p(p)
            tables.append(tbl)
        tables_fin = pd.concat(tables)
    
    elif len(periods) == 1:
        tables_fin = sel_p(periods[0])
    
    else:
        tables_fin = pd.DataFrame(data=None, columns=[
                'date du mort',
                'jour du mort',
                'N° parquet',
                'n° régime',
                'Poids mort après pesée',
                'N°Bague',
                'period'
            ])
        
    if 'N°Bague' not in tables_fin.columns:
        tables_fin = (tables_fin
                      .assign(NBague = pd.Series(dtype='object'))
                      .rename(columns={'NBague':'N°Bague'})
                     )
        
    return tables_fin

#######################################################################
# combine everything to create mortalité_2 (wo comments....)          #
#---------------------------------------------------------------------#
def mortalite_2(path_file):
    if isinstance(path_file, str):
        path_file = pd.ExcelFile(path_file)
    else:
        print("DEBUG: xls_or_path type didn't convert:", type(path_file))
    
    pesee = periods_concat(extract_pesee(path_file))
    mortalite = periods_concat(extract_mortalite(path_file))
    
    study = change_study(pd.read_excel(path_file, sheet_name='Saisie', header=1).Etude[0])
    
    join = pd.concat((mortalite, pesee))
    join['Study'] = study
    join = join.sort_values(by=['date du mort', 'N° parquet'], axis=0)
    
    cols_ordered = [
        'Study',
        'date du mort', 
        'jour du mort', 
        'N° parquet', 
        'n° régime',
        'poids du mort',
        'alt J mort',
        'alt départ',
        'conso du mort',
        'Nb animaux',
        'Poids mort après pesée',
        'N°Bague',
        'period'
    ]
    
    return join[cols_ordered]


#######################################################################
# combine everything to create mortalité_2 (with comments!!)    
# Changes by NB for faster process      #
# Make the function to accept exel file path or text path
#---------------------------------------------------------------------#
def mortalite_2V2(xls_or_path: pd.ExcelFile, diagnostic=True):
    # Use ExcelFile object to read sheets without hitting disk multiple times
    if isinstance(xls_or_path, str):
        xls = pd.ExcelFile(xls_or_path)
    else:
        xls = xls_or_path

    pesee = periods_concat(extract_pesee(xls))
    mortalite = periods_concat(extract_mortalite(xls))
    
    if diagnostic:
        frames = extract_diagnosis(xls, comments=True)  # Will update this too
        keys = frames.keys()
        
        if 'mortalité' in keys:
            mortalite = pd.merge(
                mortalite.reset_index(),
                frames['mortalité'],
                on=['period', 'index'],
                how='left'
            ).drop('index', axis=1)
        else:
            mortalite['Diagnostic'] = pd.Series(dtype='object')
        
        if 'mortalité_après_pesée' in keys:
            pesee = pd.merge(
                pesee.reset_index(),
                frames['mortalité_après_pesée'],
                on=['period', 'index'],
                how='left'
            ).drop('index', axis=1)
        else:
            pesee['Diagnostic'] = pd.Series(dtype='object')
    
    study_value = xls.parse('Saisie', header=1).Etude[0]
    study = change_study(study_value)
    
    join = pd.concat((mortalite, pesee))
    join['Study'] = study
    join = join.sort_values(by=['date du mort', 'N° parquet'], axis=0)
    
    cols_ordered = [
        'Study',
        'date du mort',
        'jour du mort',
        'N° parquet',
        'n° régime',
        'poids du mort',
        'alt J mort',
        'alt départ',
        'conso du mort',
        'Nb animaux',
        'Poids mort après pesée',
        'N°Bague',
        'Diagnostic',
        'period'
    ]
    
    return join[cols_ordered].reset_index(drop=True)



# new functions NB

def process_mortalite_file(file_path):
    print(f"Processing file: {file_path}")
    try:
        xls = ExcelFile(file_path)
        df = mortalite_2V2(xls)
        df['SourceFile'] = os.path.basename(file_path)
        return file_path, df, None
    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        return file_path, None, msg


def process_all_mortalite_files_parallel(volume_path, files, n_processes=None):
    full_paths = [os.path.join(volume_path, f) for f in files]
    cpu_count = os.cpu_count() or 2
    n_processes = n_processes or min(4, cpu_count)  # One can adjust based on CPU

    with Pool(n_processes) as pool:
        results = pool.map(process_mortalite_file, full_paths)

    tables = []
    errors = {}

    for file_path, df, error in results:
        if error:
            errors[file_path] = error
        else:
            tables.append(df)

    return tables, errors
