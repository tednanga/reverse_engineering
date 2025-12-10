import pandas as pd


def extract_mortalite(xls: pd.ExcelFile) -> pd.DataFrame:
    data = xls.parse(sheet_name='mortalité', header=1)

    cols_unn = [c for c in data.columns if 'Unnamed' in c]
    cols_unn_dup = [tuple(cols_unn[i: i + 2]) for i in range(0, len(cols_unn), 2)]

    for dupl in cols_unn_dup:
        drops = data.columns[data.columns.get_loc(dupl[0]):data.columns.get_loc(dupl[1]) + 1]
        data = data.drop(labels=drops, axis=1)

    data.columns = [col.split('.')[0] for col in data.columns]
    data = data.dropna(axis=0, how='all')

    ser = data.columns.to_series().groupby(data.columns).rank('first').astype(int)
    data.columns = ser.index + '_P' + ser.values.astype(str)

    return data
