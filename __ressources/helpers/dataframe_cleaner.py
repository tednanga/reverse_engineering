"""
DataFrame cleaning and normalization utilities.
General-purpose helper functions for column normalization, whitespace stripping,
duplicate removal, and missing value handling. 
Designed for pandas DataFrames prior to Spark conversion or storage.
"""

import unicodedata
import numpy as np

def normalize_col(col):
    """
    Normalize column name: remove accents, convert to ASCII, lower case, replace spaces with underscores.
    """
    col = unicodedata.normalize('NFKD', col).encode('ascii', errors='ignore').decode('utf-8')
    col = col.replace(' ', '_')
    return col.lower()

def clean_column_names(df):
    """
    Apply normalize_col to all DataFrame column names.
    """
    df.columns = [normalize_col(col) for col in df.columns]
    return df

def strip_whitespace_obj_cols(df):
    """
    Strip leading and trailing whitespace from all string (object) columns.
    """
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def drop_duplicates(df):
    """
    Drop exact duplicate rows.
    """
    return df.drop_duplicates()

def handle_missing(df, fill_numeric=np.nan, fill_str=''):
    """
    Replace missing values in string columns with fill_str, and in numeric columns with fill_numeric.
    """
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace({None: fill_str, 'nan': fill_str, np.nan: fill_str})
        else:
            df[col] = df[col].replace({None: fill_numeric, '': fill_numeric})
    return df

def clean_and_normalize(df, fill_numeric=np.nan, fill_str=''):
    """
    Apply standard cleaning and normalization steps:
    column name normalization, whitespace stripping, duplicate removal, and missing value handling.
    """
    df = clean_column_names(df)
    df = strip_whitespace_obj_cols(df)
    df = drop_duplicates(df)
    df = handle_missing(df, fill_numeric, fill_str)
    return df
