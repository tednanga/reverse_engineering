import pandas as pd
import numpy as np
import re
import os

from SEND_TA.ta_basics_class import TA_Base
from SEND_TA.ta_arm_class import TA_ARM_FEATURES


class TA_Infos:
    
    def __init__(self, path_file, sheet_name, verbose=True) -> None:
        self.path_file = path_file
        self.sheet_name = sheet_name
        self.tbl_ba = None
        self.verbose = verbose
        self.dict_param = {
            'Items':'TRT',
            'Lots':'REFPT',
            'N° Cde':'REFFF',
            'Régimes':'SPGRPCD',
            'MP principale':'FEED'
            }        
             
    #----------------------------------------------------------------------------------------------
      
    def __setitem__(self, parm_dict):
        self.dict_param = parm_dict
            
    #----------------------------------------------------------------------------------------------
                    
    def upload_file(self):        
        infos = pd.read_excel(self.path_file, sheet_name=self.sheet_name)      
        data = infos.set_index('Essai n°')
        
        # remnant of experiemnts with Infos -> manually change Infos data 
        # here, takes the original...
        if len(data.loc['Items', :].shape) == 2:
            data = data.loc[:'Essai n°', :].drop('Essai n°')
            
        self.data = data.reset_index()
        self.regimes = data.loc['Régimes'].dropna().unique().tolist()
        self.Study_ID = data.columns[0]
        
        if self.verbose:
            file = self.path_file.split('\\')[-1]
            print(f'upload Infos from study {file}')
        
    #----------------------------------------------------------------------------------------------
    
    # table of Item and corresponding base aliment
    # Items | Base Aliment
    #---------------------
    #   PC  |      1
    #   NC  |      2
    #---------------------
    def create_base_aliment_tbl(self):
        ba = self.data.set_index('Essai n°').loc[['Items', 'Base Aliment']]
        ba.loc['Base Aliment'] = ba.loc['Base Aliment'].ffill().fillna('0')
        self.tbl_ba = (ba
                        .T
                        .assign(idx = lambda df: df.groupby('Base Aliment')['Base Aliment'].rank('first').astype(int))
                        .query('idx == 1')
                        .drop('idx', axis=1)
                        .reset_index(drop=True)
                        .rename_axis('', axis=1)
                        )
        
    #----------------------------------------------------------------------------------------------
        
    def get_aliment_tbl(self):
        self.create_base_aliment_tbl()
        table = self.tbl_ba.assign(STUDYID = TA_Base.change_study(study=self.Study_ID))
        return table[['STUDYID', 'Base Aliment', 'Items']]
        
    #----------------------------------------------------------------------------------------------
    
    # Part of final TA table containing Period and ARMCD
    # DOMAIN |   STUDYID    | ARMCD | TASEQ | TAGRPID | TAPARMCD | TAVAL
    # ------------------------------------------------------------------
    #   TA   | 2021-SDP-110 |   T1  |   x   |    x    |    TFP   |   P1
    # ------------------------------------------------------------------
    # x need to be filled subsequently
    def create_feed_period_frame(self):
        per = (self.data[self.data['Essai n°'].fillna('//').str.contains(r'P+\d+|Régimes')]
               .dropna(how='all', axis=1)
               .set_index('Essai n°')
               )
        periods = [id for id in per.index if bool(re.findall(r'P+\d+', id))]
        for p in periods:
            per.loc[p] = np.nan
            
        self.tb_TFP = (per
                       .reset_index()
                       .melt(
                           id_vars='Essai n°'
                       )
                       .fillna(method='ffill')
                       .assign(
                           ARMCD = lambda df: df['value'].apply(lambda x: 'T' + str(x)),
                           TAPARMCD = 'TFP',
                           TASEQ = pd.Series(dtype=int),
                           TAGRPID = pd.Series(dtype=int),
                           STUDYID = TA_Base.change_study(self.Study_ID),
                           DOMAIN = 'TA'
                       )
                       .drop(['variable', 'value'], axis=1)
                       .rename(columns={'Essai n°':'TAVAL'})
                       .query("TAVAL.str.match('P+\d+')")
                       )[['DOMAIN', 'STUDYID', 'ARMCD', 'TASEQ', 'TAGRPID', 'TAPARMCD', 'TAVAL']]
        
        
    #---------------------------------------------------------------------------------------------- 
    
    # function for selection of ARM (Series or Data Frame)
    def extract_arm(self, regime:int):

        bloc = (self.data
                .dropna(how='all', axis=0)
                .set_index('Essai n°')
                )
        # bloc.loc['Régimes'] = bloc.loc['Régimes'].ffill()
        bloc_mod = bloc.ffill(axis=1)
        bloc_fin = bloc_mod.T.query(f'Régimes == {regime}').T
        
        return bloc_fin
        
    #---------------------------------------------------------------------------------------------- 
    
    # re-indexing arms based on & in dose (period specific treatment)
    # create TASEQ > 1 for P1, P2, Pn ... with specific treatment
    def apply_taseq(self, arm:pd.DataFrame)->pd.DataFrame:
        
        idx_min = arm['IDX'].min()
        df_idx1 = arm.query('IDX == @idx_min')
        idxs = arm.query('IDX != @idx_min')['IDX'].unique().tolist()
        if idxs:
            frames = []
            for idx in idxs:
                arm_cp = df_idx1.copy()
                arm_idx = arm.query('IDX == @idx')
                ta_parm = arm_idx['TAPARMCD'].tolist()
                ta_parm_idxs = df_idx1[df_idx1['TAPARMCD'].isin(ta_parm)].index
                arm_cp.loc[ta_parm_idxs, 'TAVAL'] = arm_idx['TAVAL'].values
                arm_cp['IDX_sub'] = idx
                frames.append(arm_cp)
            final = pd.concat((df_idx1, *frames))
            return final
        else:
            return df_idx1
        
    #---------------------------------------------------------------------------------------------- 
    
    def set_ta_idxs(self, arm:pd.DataFrame, separator)->pd.DataFrame:
        
        if separator:
            arm = self.apply_taseq(arm)
        
        arm_mod = (arm
                    .assign(
                        TASEQ = lambda df: df['TASEQ'].fillna(df['IDX_sub']).astype(int),
                        TAGRPID = lambda df: df.groupby(['TAPARMCD', 'TASEQ'])['TAPARMCD'].rank('first').astype(int)
                        ))
        
        return arm_mod
        
    #---------------------------------------------------------------------------------------------- 
    
    def apply_feed_period(self, arm:pd.DataFrame, seperator)->pd.DataFrame:
        
        self.create_feed_period_frame()
        armcd = arm.ARMCD.unique()[0]
        df = self.tb_TFP.query('ARMCD == @armcd')
        #----------------------------------------
        if not seperator:
            df_mod = (df
                        .assign(
                            TASEQ = arm['IDX_sub'].unique()[0],
                            TAGRPID = lambda df: df.groupby('ARMCD')['TASEQ'].rank('first').astype(int)))
        #-----------------------------------------
        elif seperator == '&':
            df_mod = (df
                        .assign(
                            TASEQ = arm['IDX_sub'].unique()[:len(df)],
                            TAGRPID = 1))
        #-----------------------------------------
        elif seperator == '+':
            frames = []
            for idx in arm['IDX_sub'].unique():
                arm_reidx = arm.set_index('TAPARMCD')
                dates_all = (arm_reidx
                             .loc[['DOSSTDTC', 'DOSENDTC', 'DOSDUR']]
                             .reset_index()
                             .assign(
                                 TASEQ = idx,
                                 TAGRPID = lambda df: df.groupby('TAPARMCD')['TAPARMCD'].rank('first').astype(int)))
                rest = (arm_reidx
                        .loc[~arm_reidx.index.isin(['DOSSTDTC', 'DOSENDTC', 'DOSDUR'])]
                        .query('TASEQ == @idx')
                        .reset_index())                        
                df_m = (df
                        .assign(
                            TASEQ = idx,
                            TAGRPID = lambda df: df.groupby('ARMCD')['TASEQ'].rank('first').astype(int)))
                frames.append(
                    pd.concat((dates_all, rest, df_m), ignore_index=True)
                    )
            return pd.concat(frames, ignore_index=True)            
        #----------------------------------------------
        return pd.concat((arm, df_mod))
        
    #---------------------------------------------------------------------------------------------- 
    
    def __call__(self):
        
        # upload Infos sheet from excel file
        # create table of Items with corresponding Base Aliment
        # process each arm via methods in TA_ARM_FEATURES
        # creates the basic frame of TA
        cols = [
            'DOMAIN',
            'STUDYID',
            'ARMCD',
            'TASEQ',
            'TAGRPID',
            'TAPARMCD',
            'TAVAL',
            # 'IDX',
            # 'IDX_sub'
        ]
        
        self.upload_file()
        self.create_base_aliment_tbl()
        
        ta_tables = []
        for regime in self.regimes:
            # if regime == 5:
                arm = self.extract_arm(regime)
                # seperator_dose = bool(re.findall(r'&', arm.loc['Dose'].fillna('/').tolist()[0]))
                
                seperator_d = re.findall(r'&|\+', arm.loc['Dose'].fillna('/').tolist()[0])
                if '&' in seperator_d and '+' in seperator_d:
                    raise ValueError('seperator of dosis either & (different periods) or + (within same period)')
                if not seperator_d:
                    sep = None
                else:
                    sep = seperator_d[0]
                    
                # each arm is processed individually (each row as well)
                ta_inst = TA_ARM_FEATURES(arm, self.tbl_ba)
                result = ta_inst()
                for idx in result['IDX_sub'].unique():
                    result_sub = result.query('IDX_sub == @idx')
                    result_m = (result_sub
                                .assign(
                                    STUDYID = TA_Base.change_study(self.Study_ID),
                                    ARMCD = 'T' + str(regime), DOMAIN = 'TA'))
                    
                    # result_idx = self.set_ta_idxs(result_m, seperator_dose)
                    # result_fin = self.apply_feed_period(result_idx, seperator_dose)
                    result_idx = self.set_ta_idxs(result_m, sep)
                    result_fin = self.apply_feed_period(result_idx, sep)
                    
                    ta_tables.append(
                        result_fin[cols]
                        )                
            
        return ta_tables
    
    