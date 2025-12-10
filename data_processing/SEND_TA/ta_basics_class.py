import re

class TA_Base:
    
    #----------------------------------------------------------------------------------------------
    
    @staticmethod
    def is_float(string):
        try:
            float(string)
            return True
        except ValueError:
            return False
                  
    #----------------------------------------------------------------------------------------------
    
    @staticmethod
    def split_dose(row):
            essai, vals = row
            if essai == 'Dose':
                return re.sub('(\d+(\,*|\.*)\d+)\s*', r' \1 ', vals).strip().split()
            else:
                return vals
            
    #----------------------------------------------------------------------------------------------
    
    @staticmethod
    def func_change_dose(dose):
        val, param = dose
        if str(val).isnumeric() and param == 'Dose':
            return 'DIECPUR'
        elif not str(val).isnumeric() and param == 'Dose':
            return 'DIECPURU'
        else:
            return param
             
    #----------------------------------------------------------------------------------------------
    
    @staticmethod
    def func_change_dose_idx(dose):
        param = dose
        if str(param).isnumeric(): # integer string
            return 'Dose'
        elif TA_Base.is_float(str(param)): # float string
            return 'Dose'
        elif not str(param).isnumeric() or not TA_Base.is_float(str(param)) or param != '/':
            return 'DoseU'
        else:
            return 'Dose'
             
    #-----------------------------------------------------------------------------------------------
    
    @staticmethod
    def func_change_param(param, value, param_change):
        
        if param == param_change:
            if str(value).isnumeric():
                return param
            elif not str(value).isnumeric() and param != '/':
                return param + 'U'
        else:
            return param
              
    #-----------------------------------------------------------------------------------------------
               
    @staticmethod
    def change_study(study):
        
        y, n, c = study.split('-')
        if len(str(y)) == 2:
            y = '20' + str(y)
        else:
            y = '200' + str(y)
             
        n = (3 - len(str(n))) * '0' + str(n)
        
        return '-'.join([y, c, n])
             
    #----------------------------------------------------------------------------------------------
    
    @staticmethod
    def string_split(op):
        ...