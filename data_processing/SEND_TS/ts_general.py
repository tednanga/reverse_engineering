import pandas as pd


def change_study(study: str) -> str:
            
        y, n, c = study.split('-')
        if len(str(y)) == 2:
            y = '20' + str(y)
        else:
            y = '200' + str(y)             
        n = (3 - len(str(n))) * '0' + str(n)        
        
        return '-'.join([y, c, n])
    

def dict_val_triskel():
    
    dict_val_triskel = {
        
        "Responsable d'essai":'STDIR',
        'Responsable Technique':'STTECHMA',
        'Espèce':'SPECIES',
        "Type d'essai":'SSSBTYP',
        'Localisation':'TSTFNAM',
        'Description':'STITLE',
        'Batiment': 'BUILDNUM',
        'room':'ROOMNUM'
        }
    
    return dict_val_triskel
    

def dict_param_ts(path):
    
    data = pd.read_excel(path, sheet_name='TS')[['DOMAIN', 'TSPARMCD', 'TSPARM']]
    data_sub = data[data.DOMAIN == 'TS'][['TSPARMCD', 'TSPARM']]
    dict_param = {
        data_sub.iloc[idx, 0]:data_sub.iloc[idx, 1]
        for idx in range(len(data_sub))
    }
    
    return dict_param



