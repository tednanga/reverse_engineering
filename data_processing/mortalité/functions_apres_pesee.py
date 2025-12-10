import pandas as pd


#######################################################################
# extract >Mortalité après la mise à jeun pesée< from mortalité sheet 
# mode NB#
#---------------------------------------------------------------------#
def extract_pesee(xls: pd.ExcelFile) -> pd.DataFrame:
    data = xls.parse(sheet_name='mortalité', header=[0, 1])
    
    items_sel = [
        'Mortalité après' in c
        for c in data.columns.get_level_values(0)
    ]
    
    Pesee = data.iloc[:, items_sel]
    
    cols_mod = [
        '_'.join(sublist[::-1]).replace('Mortalité après la mise à jeun pesée fin ', '')
        for sublist in Pesee.columns
    ]
    
    Pesee.columns = cols_mod
    Pesee_fin = Pesee[[col for col in Pesee.columns if '.1' not in col]].dropna(axis=1, how='all')
    
    Pesee_fin = Pesee_fin.rename(columns={
        c: c.replace('poids du mort', 'Poids mort après pesée')
        for c in Pesee_fin.columns
    })
    
    return Pesee_fin
        