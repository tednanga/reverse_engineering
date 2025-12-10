import pandas as pd



def transform_croissance_calc(data: pd.DataFrame)-> pd.DataFrame:
    columns = [
        'STUDYID', 
        'EUPOOLID', 
        'REGIME', 
        'APHASE', 
        'AGE_ST', 
        'AGE_EN', 
        'NUMST', 
        'NUMEN', 
        'BW_AVG', 
        'BG_AVG', 
        'FI_AVG', 
        'IC'
        ]
    
    cols_analysis = [
        'EUPOOLID',
        'APHASE', 
        'AGE_ST', 
        'AGE_EN', 
        'NUMST', 
        'NUMEN', 
        'BW_AVG', 
        'BG_AVG', 
        'FI_AVG', 
        'IC'
        ]
    
    df_calcul = (data
                 .assign(
                     EUPOOLID = lambda df: df['Essai'] + '_CERN_' + df['Parquet'].astype(int).apply(lambda x: '0' * (3 - (len(str(x)))) + str(x)),
                     APHASE = lambda df: df['period'].apply(lambda x: 'Base' if x == 'P_0' else ''.join(x.split('_'))),
                     STUDYID = lambda df: df['Essai']#.apply(lambda x: x[2:4] + '-' + '-'.join(x.split('-')[1:][::-1]))
                     )
                 .rename(
                     columns={
                         'régime':'REGIME',
                         'Age_end':'AGE_EN',
                         'Age_debut':'AGE_ST',
                         'Nb Animaux début':'NUMST',
                         'Nb Animaux fin':'NUMEN',
                         'PV indiv':'BW_AVG',
                         'GP indiv':'BG_AVG',
                         'Conso indiv':'FI_AVG',
                         'I.C.':'IC'
                         }
                     )
                 )[columns].sort_values(by=['EUPOOLID','APHASE'], ascending=[True, True])
    
    return df_calcul[cols_analysis]

########################################################################################################################################################
# functions to generate more in-between periods calculations for performance 

def series_phases(frame:pd.DataFrame)->list:
    
    phases = []
    len_frame = len(frame)
    idx2 = 2
    
    for idx in range(len_frame):
        if idx2 < len_frame:
            l = [
                '_'.join(
                    (frame.loc[frame.index[idx], 'APHASE'], frame.loc[frame.index[id_], 'APHASE'])
                    ) 
                for id_ in range(idx2, len(frame))
                ]
            phases += l
            idx2 += 1
            
    return phases


def new_frames(phases, frame):
    
    cols_idx_1 = ['AGE_ST', 'NUMST']
    cols_idx_2 = ['AGE_EN', 'NUMEN', 'BW_AVG']
    cols_calc = ['FI_AVG']
    
    frames = []
    for phase in phases:
        idx1, idx2 = phase.split('_')
        frame_mod = (frame
                     .set_index('APHASE', drop=True)
                     .drop('EUPOOLID', axis=1)
                     .fillna(0.)
                     .loc[idx1:idx2]
                     )
        
        row_new = {}
        for column in frame_mod.columns:
            if column in cols_idx_1:
                row_new.update({column:frame_mod.loc[idx1, column]})
            elif column in cols_idx_2:
                row_new.update({column:frame_mod.loc[idx2, column]})
            elif column in cols_calc:
                row_new.update({column:frame_mod[column].iloc[1:].sum()})
            else:
                if column == 'BG_AVG':
                    row_new.update({'BG_AVG':frame_mod.loc[idx2, 'BW_AVG'] - frame_mod.loc[idx1, 'BW_AVG']})
                else:
                    row_new.update({'IC':row_new['FI_AVG'] / row_new['BG_AVG']})
            row_new.update({'APHASE':phase})
        frames.append(pd.DataFrame.from_dict(row_new, orient='index').T.set_index('APHASE'))
    
    new = pd.concat(frames)
    final = (new
             .assign(EUPOOLID = frame.EUPOOLID.iloc[0])
             .reset_index()
             .assign(APHASE = lambda df: df['APHASE'].str.split('_').str.join('|'))
            )
    
    return pd.concat((frame, final), ignore_index=True)



def create_volaille_analysis(table):
    
    collected_frames = []
    for eupool in table.EUPOOLID.unique():    
        frame = table[table.EUPOOLID == eupool]
        if len(frame) > 2:
            phases = series_phases(frame)
            frame_new = new_frames(phases, frame)
            collected_frames.append(frame_new)
        else:
            collected_frames.append(frame)
    
    analysis_new = pd.concat(collected_frames, ignore_index=True)
    
    analysis_new = (analysis_new
                    .assign(
                        STUDYID = lambda df: df['EUPOOLID'].str.split('_').str[0]
                    )
                    .sort_values(by=['STUDYID', 'EUPOOLID', 'APHASE'], ascending=[True, True, True])
                    )
    
    return analysis_new
