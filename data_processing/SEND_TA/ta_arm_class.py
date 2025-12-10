import pandas as pd
import numpy as np
import os
import re

from SEND_TA.ta_basics_class import TA_Base


class TA_ARM_FEATURES:
    
    def __init__(self, arm:pd.DataFrame, ba_tbl:pd.DataFrame) -> None:
        self.arm_ta = arm.rename(columns={c:'TAVAL' for c in arm.columns})
        self.arm = None
        self.ba_tbl = ba_tbl
        self.ba_dict = None
        self.dict_ta_parmcd = {
            'Dose':'DIECPUR',
            'DoseU':'DIECPURU',
            'Items':'TRT',
            'Lots':'REFPT',
            'N° Cde':'REFFF',
            'Régimes':'SPGRPCD',
            'MP principale':'FEED',
            'Base Aliment':'REFBFF'
            }
      
    #----------------------------------------------------------------------------------------------
               
    def regex_items(self):
        
        # compiling all regex required for processing Items
        regex_Regimes = '^Régime(.*?)(?:\+ |$)'
        regex_Temoin = '^Témoin(.*?)(?:\+ |$)'
        regex_regime = '^régime(.*?)(?:\+ |$)'
        regex_gel = '(.*?)gel(.*?)(?:\+ |$)'
        regex_BD = '\(|BD\+|\)|\=\s*BD|BD'
        regex_controls = 'PC\s*\+?|NC\s*\+?'
        regex_T = 'T[1-9]\s*\+?'
        regex_controlsV2 = '(.*?)control(.*)'
        
        exclude = '|'.join(
            [
                regex_Regimes,
                regex_Temoin,
                regex_regime,
                regex_gel,
                regex_BD,
                regex_controls,
                regex_T,
                regex_controlsV2
                ]
            )
        
        return exclude
    
    #----------------------------------------------------------------------------------------------
    
    # transform table to dictionary -> indexing
    # {1: 'PC', 2: 'NC', ...}
    #------------------------------------------
    def transfer_ba_tbl_to_dict(self):
        
        ba_dict = (self.ba_tbl
                   .set_index('Base Aliment')
                   .T
                   .to_dict(orient='list')
                   )
        
        self.ba_dict = dict([(int(k),v[0]) for k,v in ba_dict.items()])
        
    #----------------------------------------------------------------------------------------------
    
    def extract_arm(self):
        # creating TAVAL of ARM (ARM Label)
        # TAPARMCD |  TAPARM   | TAVAL
        #------------------------------------
        #    ARM   | Arm Label |   NC + ....
        #------------------------------------        
        dict_control = {'PC':'Positive Control', 'NC':'Negative Control', 'BD':'Basis Diet'}
        treatments = ['Régime dysbiose']
        
        item_arm = self.arm.loc['Items'].tolist()[0].replace('\n', ' ')
        item_arm = re.sub(r'T[1-9]\s*\+*', '', item_arm)
        item_arm = re.sub(r'FTU(?!\W*/kg)', 'FTU/kg', item_arm)
        ba_arm = self.arm.loc['Base Aliment'].tolist()
        
        if pd.isnull(float(*ba_arm)):
            base_aliment = str(np.nan)
        elif int(*ba_arm) in self.ba_dict.keys():
            k = int(*ba_arm)
            base_aliment = self.ba_dict[k]
        else:
            base_aliment = str(np.nan)
                        
        if base_aliment not in item_arm and base_aliment != str(np.nan):
            arm_new = ' + '.join((base_aliment, item_arm))
        else:
            arm_new = item_arm
        
        # inplace modification (new rows prepend -> keeps possibility self.arm.loc[:'Base Aliment'])
        # self.arm = self.arm.T.assign(ARM = arm_new).T
        self.arm = self.arm.T
        self.arm.insert(0, 'ARM', arm_new)
        self.arm = self.arm.T
        
        if item_arm == 'PC' or item_arm == 'NC' or item_arm == 'BD':
            # self.arm = self.arm.T.assign(TCNTRL = dict_control[item_arm], TRT='ctrl').T
            arm = self.arm.T
            arm.insert(0, 'TCNTRL', dict_control[item_arm])
            arm.insert(1, 'TRT', 'ctrl')
            self.arm = arm.T
        elif bool(re.findall(r'positive control', item_arm.lower())):
            # self.arm = self.arm.T.assign(TCNTRL = 'Positive Control', TRT='ctrl').T
            self.arm = self.arm.T
            self.arm.insert(0, 'TCNTRL', 'Positive Control')
            self.arm.insert(1, 'TRT', 'ctrl')
            self.arm = self.arm.T
        elif bool(re.findall(r'negative control', item_arm.lower())):
            # self.arm = self.arm.T.assign(TCNTRL = 'Negative Control', TRT='ctrl').T
            self.arm = self.arm.T
            self.arm.insert(0, 'TCNTRL', 'Negative Control')
            self.arm.insert(1, 'TRT', 'ctrl')
            self.arm = self.arm.T
        elif arm_new in treatments:
            self.arm = self.arm.T
            self.arm.insert(1, 'TRT', item_arm)
            self.arm = self.arm.T
        
    #----------------------------------------------------------------------------------------------    
    
    # function for apply() in process_items(self)
    #--------------------------------------------
    def preprocess_items(self, item):
        
        item = item.replace('\n', '')
        item = re.sub(r'FTU(?!\W*/kg)', 'FTU/kg', item)
        item_final = re.sub(self.regex_items(), '', item)
        
        if item_final == '':
            item_return = '/'
        else:
            item_return = item_final.strip()
            
        return item_return
         
    #----------------------------------------------------------------------------------------------    
          
    def process_items(self):
        
        # first processing
        Items = (self.arm
                 .loc[['Items'], :]
                 .fillna('/')
                 .assign(
                     TAVAL = lambda df: df['TAVAL'].apply(lambda x: self.preprocess_items(x))
                 )
                 .assign(
                     TAVAL = lambda df: df['TAVAL'].apply(
                         lambda x: [it.strip() for it in x.split('+')] if '+' in x and not bool(re.findall(r'\d+', x)) else x
                     )
                 )
                 .explode('TAVAL')
                )
        
        # extract the FTU (with split('+) and explode .loc['Items'] from preprocessing -> either Series or Data Frame)
        items_str = Items.loc['Items']['TAVAL']
        if type(items_str) == pd.Series:
            items_str = ' '.join(items_str.tolist())
                
        if bool(re.findall(r'\d+\s*FTU/kg', items_str)):
            act = re.findall(r'\d+\s*FTU/kg', items_str)[0]
            Items = (Items
                     .assign(
                         TAVAL = lambda df: df['TAVAL'].apply(lambda x: re.sub(r'\d+\s*FTU/kg', '', x))
                         )
                     .T
                     .assign(
                         DIETCON = act
                         )
                     .assign(
                         DIETCON = lambda df: df['DIETCON'].apply(lambda x: x.split())
                         )
                     .T
                     .explode('TAVAL')
                     .reset_index()
                     .assign(
                         Essai = lambda df: df[[Items.index.name, 'TAVAL']].apply(
                             lambda x: TA_Base.func_change_param(*x, 'DIETCON'), axis=1
                             )
                         )
                     .set_index('Essai')
                     .drop(Items.index.name, axis=1)
                     .rename_axis(Items.index.name)                  
                     )
        
        self.arm = pd.concat((Items, self.arm.drop('Items')))
         
    #----------------------------------------------------------------------------------------------    
    
    def process_cde(self):
        
        Ncd = (self.arm
               .loc[['N° Cde'], :]
               .fillna('/')
               )
        # step only if there is a '&' separator
        if bool(re.findall(r'&', *Ncd.loc['N° Cde'].tolist())):
            Ncd = (Ncd
                   .assign(
                       TAVAL = lambda df: df['TAVAL'].apply(
                           lambda x: [it.strip() for it in ','.join(x.split('&')).split(',')]
                           )
                       )
                   .explode('TAVAL')
                   )
        # replace old with new rows
        self.arm = pd.concat((Ncd, self.arm.drop('N° Cde')))
            
    #----------------------------------------------------------------------------------------------
    
    def process_lot(self):
        
        Lots = (self.arm
                .loc[['Lots'], :]
                .fillna('/')
                )
        # step only if there is a '&' separator
        if bool(re.findall(r'&|et', *Lots.loc['Lots'].tolist())):
            split_op = list(set(re.findall(r'&|et', *self.arm.loc['Lots'].tolist())))
            if len(split_op) == 1:
                Lots = (Lots
                        .assign(
                            TAVAL = lambda df: df['TAVAL'].apply(
                                lambda x: [it.strip() for it in ','.join(x.split(*split_op)).split(',')]
                                )
                            )
                        .explode('TAVAL')  
                        )
            else:
                raise ValueError('There should be only one <split> operator for Lots (& or et)')
        
        # replace old with new rows
        self.arm = pd.concat((Lots, self.arm.drop('Lots')))
            
    #----------------------------------------------------------------------------------------------
    
    def process_dose(self):
        
        dose = (self.arm
                .loc[['Dose'], :]
                .fillna('/')
                )
        if bool(re.findall(r'&|\+', *dose.loc['Dose'].tolist())):
            split_op = list(set(re.findall(r'&|\+', *self.arm.loc['Dose'].tolist())))
            if len(split_op) == 2:
                dose = (dose
                        .assign(
                            TAVAL = lambda df: df['TAVAL'].apply(
                                lambda x: [it.strip() for it in ','.join(x.split('&')).split(',')]
                                )
                            )
                        .explode('TAVAL')
                        .assign(
                            Idx_sop = list(range(1, 3)),
                            TAVAL =  lambda df: df['TAVAL'].apply(
                                lambda x: [it.strip() for it in ','.join(x.split('+')).split(',')]
                                )
                            )
                        .explode('TAVAL')
                        )
            elif len(split_op) == 1:
                dose = (dose
                        .assign(
                            TAVAL = lambda df: df['TAVAL'].apply(
                                lambda x: [it.strip() for it in ','.join([it.strip().replace(',', '.') for it in x.split(*split_op)]).split(',')]
                                )
                            )
                        .explode('TAVAL')
                        )
            else:
                raise ValueError(f'The number of split operators for dose: {len(split_op)} is not coorect (0 < split_op <=2)')
        
        dose = (dose
                .assign(
                    # split between units and numbers (int, float with . or , ) independent of spaces
                    TAVAL = lambda df: df['TAVAL'].apply(lambda x: re.sub('(\d+(\,*|\.*)\d+)\s*', r'\1 ', str(x)).strip().replace(',', '.').split())
                    )
                .explode('TAVAL')
                .reset_index()
                .assign(
                    Essai = lambda df: df['TAVAL'].apply(lambda x: TA_Base.func_change_dose_idx(x))
                    )
                .set_index('Essai')
                .drop(dose.index.name, axis=1)
                .rename_axis(dose.index.name)
                )
        # replace old with new rows
        self.arm = pd.concat((dose, self.arm.drop('Dose')))
            
    #----------------------------------------------------------------------------------------------
    
    def process_dates(self):
        
        dates_mod = (self.arm
                     .loc['Dates':]
                     .dropna(how='all', axis=0)
                     .assign(
                         DOSSTDTC = lambda df: df['TAVAL'].shift(),
                         DOSDUR_ = lambda df: (df['TAVAL'] - df['DOSSTDTC']).dt.days,
                         DOSDUR = lambda df: df['DOSDUR_'].apply(lambda x: 'P' + str(int(x)) + 'D' if not pd.isna(x) else x)
                         )
                     .drop('Début')
                     .rename(columns={'TAVAL':'DOSENDTC'})
                     .reset_index()
                     .melt(
                         value_vars = ['DOSSTDTC', 'DOSENDTC', 'DOSDUR'],
                         var_name = self.arm.index.name,
                         value_name = 'TAVAL'
                     )
                     .set_index(self.arm.index.name)
                      )
        
        self.arm = pd.concat((dates_mod, self.arm.loc[:'Dates'].drop('Dates')))
            
    #----------------------------------------------------------------------------------------------
    
    def final_processing(self):
        
        arm_final = (self.arm
                     .query("TAVAL not in ['/']")
                     .reset_index()
                     .assign(
                         TAPARMCD = lambda df: df[self.arm.index.name].apply(
                             lambda x: self.dict_ta_parmcd[x] if x in self.dict_ta_parmcd.keys() else x
                             ),
                         IDX = lambda df: df.groupby('TAPARMCD')['TAPARMCD'].rank('first').astype(int),
                         IDX_sub = 1,
                         TASEQ = pd.Series(dtype=int),
                         TAGRPID = pd.Series(dtype=int),
                         TAPARM = pd.Series(dtype=int)
                         )
                     .drop(self.arm.index.name, axis=1)
                     )
        
        self.arm = arm_final
    
    #----------------------------------------------------------------------------------------------
      
    def __call__(self):
        
        self.transfer_ba_tbl_to_dict()
        
        frames = []
        for idx_col in range(self.arm_ta.shape[1]):
            self.arm = self.arm_ta.iloc[:, [idx_col]]
            self.extract_arm() # needs to be the 1st -> extract correct Items
            self.process_items()
            self.process_cde()
            self.process_lot()
            self.process_dose()
            self.process_dates()
            self.final_processing()
            if idx_col >= 1:
                pre_df = frames[idx_col - 1]
                max_idx = pre_df.query('TAPARMCD == "DOSSTDTC"')['IDX'].max()
                self.arm = self.arm.assign(
                    IDX = lambda df: df['IDX'] + max_idx, 
                    IDX_sub = lambda df: df['IDX'].min()
                    )
            frames.append(self.arm)
        
        if len(frames) > 1:
            result = pd.concat(frames)
        else:
            result = frames[0]
        
        return result