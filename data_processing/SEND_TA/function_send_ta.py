import pandas as pd
import numpy as np
import re
from collections import OrderedDict


def set_transform(x):
    arm, trt = x[0], x[1]
    if trt == 'ctrl' and arm == 'pc':
        return 'positive control'
    elif trt == 'ctrl' and arm == 'nc':
        return 'negative control'
    elif pd.isna(trt):
        return x[0]
    else:
        return x[1]

def agg_collect_trt(x):
    x = list(OrderedDict.fromkeys(x))
    if len(x) == 1:
        return list(x)[0]
    else:
        return ' & '.join(list(x))
    
    
def combine(x):
    if pd.isna(list(set(x[0]))[0]):
        return np.nan
    else:
        comb = [' '.join((i, j)) for i, j in zip(x[0], x[1])]
        return comb


def create_dict(l:list)->dict:
    d = {}
    for item in l:
        k, v = item
        if k not in d:
            d[k] = [v]
        else:
            d[k].append(v)
    return d
     
                
def combine_all(x):
    trt = x[0].split('&')
    if len(trt) == 1:
        
        if type(x[2]) == list and type(x[3]) == list:
            x_1, x_2 = list(set(x[2]))[0], list(set(x[3]))[0]
            return f'{x_1} ({x_2})'
        elif type(x[2]) == list and not type(x[3]) == list:
            x_1 = list(set(x[2]))[0]
            return x_1
        else:
            return np.nan
    
    elif len(trt) > 1:      
        if type(x[2]) == list and type(x[3]) == list:
            l = [(i, f'{j}({k})') for i, j, k in zip(x[1], x[2], x[3])]
            d = create_dict(l)                 
            return ' '.join([f'{k}: {" & ".join(v)}' for k, v in d.items()])
        elif type(x[2]) == list and not type(x[3]) == list:
            l = [(i, j) for i, j in zip(x[1], x[2])]
            d = create_dict(l)                 
            return ' '.join([f'{k}: {" & ".join(v)}' for k, v in d.items()])
        else:
            return np.nan


cols_sel = [
    'studyid',
    'phase',
    'regime',
    'DIECPUR',
    'DIECPURU',
    'DOSDUR',
    'DIETCON',
    'DIETCONU',
    'TRT',
    'DOSSTDTC',
    'DOSENDTC'    
]

    
#----------------------------------------------------------------------------------------------------------------------------------------

def create_send_ta_product(data) -> pd.DataFrame | dict:
    """
    accepts either a pandas dataframe or a spark dataframe. 
    converts spark dataframe to pandas if needed.
    processes the data as before.
    """

    # convert spark dataframe to pandas if needed
    if hasattr(data, "toPandas"):
        data = data.toPandas()

    errors = {}
    frames = []
    for study in data.studyid.unique():
        df = data.query('studyid == @study')
        try:
            frame = (df
                     .pivot(
                         index=['studyid', 'armcd', 'taseq', 'tagrpid'], 
                         columns='taparmcd', 
                         values='taval'
                     )
                     .reset_index()
                     .rename_axis(columns='')
                     .sort_values(by=['armcd', 'taseq', 'tagrpid'], ascending=[True, True, True])
                     .set_index(['studyid', 'armcd', 'taseq'])
                     .groupby(['studyid', 'armcd', 'taseq']).transform(lambda df: df.ffill())
                     .reset_index()
                     .assign(
                         regime = lambda df: df['armcd'].str.replace('T', 'R')
                     )
                     .rename(
                         columns={
                             'TFP':'phase'
                         }
                     )
            )
            frames.append(frame)
        except Exception as ex:
            template = "an exception of type {0} occurred. arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            errors.update({study:message})
            print(f'error! processing study: {study}')
    
    if not errors:
        result = pd.concat(frames)
        result_fin = result.assign(trt = lambda df: df[['TRT', 'TCNTRL']].apply(lambda x: x[1] if x[0] == 'ctrl' else x[0], axis=1))
        return result_fin[cols_sel]
    else:
        return errors

    
#----------------------------------------------------------------------------------------------------------------------------------------

def modification_send_ta(path_send_ta_table:str)->pd.DataFrame:
    
    send_ta = pd.read_excel(path_send_ta_table)
    
    ta_mod = (send_ta
              .pivot(
                  index=['studyid', 'armcd', 'taseq', 'tagrpid'],
                  columns='taparmcd',
                  values='taval')
              .reset_index()
              .rename_axis(columns='')
              .set_index(['studyid', 'armcd', 'taseq'])
              .groupby(['studyid', 'armcd', 'taseq'])
              .transform(lambda x: x.ffill())
              .assign(
                  dosdur_sum = lambda df: (df
                                           .groupby(df.index)['dosdur']
                                           .transform(
                                               lambda x: 'p' + x.apply(lambda y: int(re.sub(r'\D', '', y))).sum().astype(str) + 'd')),
                  regime = lambda df: 'r' + df['spgrpcd'].astype(str))
              .fillna(np.nan)
              .groupby(['studyid', 'armcd', 'regime']).agg(
                  {
                      'arm':'first',
                      'trt':agg_collect_trt,
                      'spgrpcd':'first',
                      'tfp':list,
                      'diecpur':list,
                      'diecpuru':list,
                      'dietcon':list,
                      'dietconu':list,
                      'dosdur':list,
                      'dosdur_sum':'first',
                      'dosstdtc':'first',
                      'dosendtc':'last',
                      'feed':'first',        
                      })
              .assign(
                  trt = lambda df: df[['arm','trt']].apply(set_transform, axis=1).fillna(''),
                  combine1 = lambda df: df[['diecpur', 'diecpuru']].apply(combine, axis=1),
                  combine2 = lambda df: df[['dietcon', 'dietconu']].apply(combine, axis=1),
                  combine3 = lambda df: df[['trt', 'tfp', 'combine1', 'combine2']].apply(combine_all, axis=1).fillna(''),
                  traitement = lambda df: df['trt'] + ' - ' + df['combine3'] + ' - ' + df['dosdur_sum'],
                  phase = 'treatment',
                  age_st = 1,
                  age_end = lambda df: (
                      (pd.to_datetime(df['dosendtc']) - pd.to_datetime(df['dosstdtc'])).dt.days).astype(int))
              .reset_index()
              .drop(['combine1', 'combine2', 'combine3'], axis=1)
              .rename(columns={'spgrpcd':'bras'})
              )
    return ta_mod