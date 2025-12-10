import pandas as pd
import numpy as np
import re
import warnings

def extract_digits(val):
    """
    Extracts the first sequence of digits found in any string.
    Returns that number as integer, or np.nan if none found.
    Examples:
        'P_5'        -> 5
        'px'         -> np.nan
        'aze_prb_12' -> 12
        'r3t7'       -> 3 (first match)
        15           -> 15 (int/float support)
    """
    if pd.isnull(val):
        return np.nan
    # If already numeric, return as int
    if isinstance(val, (int, float)) and not pd.isnull(val):
        return int(val)
    # Search for first digit group in string
    match = re.search(r'(\d+)', str(val))
    if match:
        return int(match.group(1))
    return np.nan

def float_2dec(val):
    """
    Converts a value to float and rounds to 2 decimal places.
    Returns np.nan if conversion fails.
    Example: "3.14159" -> 3.14
    """
    try:
        return round(float(val), 2)
    except Exception:
        return np.nan


# ### #
# v2  #
# ### #
def validate_and_clean_row(row, schema):
    """
    Cleans and validates a single row according to schema.
    Only columns in schema ∩ row are processed.
    All other columns are untouched.
    Returns (cleaned_row, reasons).
    """
    row = row.copy()
    reasons = []
    schema_cols = set(schema.keys()) & set(row.index)
    for col in schema_cols:
        info = schema[col]
        val = row[col]
        # Apply cleaning function
        if 'clean_func' in info and pd.notnull(val):
            try:
                val = info['clean_func'](val)
            except Exception:
                reasons.append(f"Clean error in {col}")
                val = np.nan
        # Enforce type
        try:
            if pd.isnull(val):
                row[col] = np.nan
            elif info['type'] == 'string':
                row[col] = str(val)
            elif info['type'] == 'Int64':
                row[col] = pd.to_numeric(val, errors='raise', downcast='integer')
            elif info['type'] == 'float':
                row[col] = pd.to_numeric(val, errors='raise', downcast='float')
            elif info['type'] == 'datetime64[ns]':
                row[col] = pd.to_datetime(val, errors='raise')
        except Exception:
            reasons.append(f"Type error in {col}: {val}")
            row[col] = np.nan
    return row, reasons

def remap_columns(df, column_mapping, warn=True):
    # Warn only on original columns (columns that exist in df)
    missing = set(column_mapping.keys()) - set(df.columns)
    if warn and missing:
        # Only warn if mapping expects to remap a column that *was* present in df originally
        warnings.warn(f"Columns to map not found in DataFrame: {missing}")
    present_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
    if warn and not present_mapping:
        warnings.warn("No columns from column_mapping found in DataFrame.")
    return df.rename(columns=present_mapping)

def check_schema_vs_data(df, schema, warn=True):
    schema_cols = set(schema.keys())
    df_cols = set(df.columns)
    missing_in_df = schema_cols - df_cols
    extra_in_df = df_cols - schema_cols
    if warn:
        if missing_in_df:
            warnings.warn(f"Columns in schema missing from DataFrame: {missing_in_df}")
        if extra_in_df:
            warnings.warn(f"Columns in DataFrame not in schema: {extra_in_df}")
    return missing_in_df, extra_in_df

def clean_dataframe(df, schema, column_mapping=None, warn=True, output_lowercase=True):
    """
    Main cleaning function.
    - Optionally remap df columns via column_mapping
    - Warn about missing/extra columns vs. schema
    - Clean/intersect columns according to schema (others left untouched)
    - Ensure output columns are lowercase if output_lowercase=True
    Returns: (cleaned_df, failed_rows_df)
    """
    original_columns = df.columns.copy()
    if column_mapping:
        df = remap_columns(df, column_mapping, warn=warn)
    check_schema_vs_data(df, schema, warn=warn)
    cleaned, fails = [], []
    for idx, row in df.iterrows():
        clean_row, reasons = validate_and_clean_row(row, schema)
        if reasons:
            clean_row['fail_reason'] = '; '.join(reasons)
            fails.append(clean_row)
        else:
            cleaned.append(clean_row)
    df_clean = pd.DataFrame(cleaned, columns=df.columns)
    df_fail = pd.DataFrame(fails, columns=df.columns.tolist() + ['fail_reason'])
    # Set output columns to lowercase if requested
    if output_lowercase:
        df_clean.columns = [c.lower() for c in df_clean.columns]
        df_fail.columns = [c.lower() for c in df_fail.columns]
    return df_clean, df_fail

    
# ### #
# v1  #
# ### #
# def validate_and_clean_row(row, schema=SCHEMA):
#     """
#     Cleans and validates a single row according to the provided schema.
#     Applies custom cleaning functions if defined, checks type conversion,
#     and collects reasons for any failures.
    
#     Args:
#         row (pd.Series): Row from DataFrame.
#         schema (dict): Schema dict describing expected types and cleaning.

#     Returns:
#         (pd.Series, list): Cleaned row and list of reasons for failure (if any).
#     """
#     row = row.copy()
#     reasons = []
#     for col, info in schema.items():
#         val = row.get(col, None)
#         # Custom cleaning first
#         if 'clean_func' in info and pd.notnull(val):
#             try:
#                 val = info['clean_func'](val)
#             except Exception:
#                 reasons.append(f"Clean error in {col}")
#                 val = np.nan
#         # Type enforcement
#         try:
#             if pd.isnull(val):
#                 row[col] = np.nan
#             elif info['type'] == 'string':
#                 row[col] = str(val)
#             elif info['type'] == 'Int64':
#                 row[col] = pd.to_numeric(val, errors='raise', downcast='integer')
#             elif info['type'] == 'float':
#                 row[col] = pd.to_numeric(val, errors='raise', downcast='float')
#             elif info['type'] == 'datetime64[ns]':
#                 row[col] = pd.to_datetime(val, errors='raise')
#         except Exception:
#             reasons.append(f"Type error in {col}: {val}")
#             row[col] = np.nan
#     return row, reasons

# def clean_dataframe(df, schema=SCHEMA):
#     """
#     Cleans an entire DataFrame based on a schema. 
#     Validates each row, applies column cleaning, 
#     and separates failed rows (with reasons) from valid ones.

#     Args:
#         df (pd.DataFrame): The input DataFrame to clean.
#         schema (dict): The schema dictionary.

#     Returns:
#         (pd.DataFrame, pd.DataFrame): (Cleaned DataFrame, Failed Rows DataFrame)
#     """
#     cleaned = []
#     fails = []
#     for idx, row in df.iterrows():
#         clean_row, reasons = validate_and_clean_row(row, schema)
#         if reasons:
#             clean_row['fail_reason'] = '; '.join(reasons)
#             fails.append(clean_row)
#         else:
#             cleaned.append(clean_row)
#     df_clean = pd.DataFrame(cleaned)
#     df_fail = pd.DataFrame(fails)
#     return df_clean, df_fail



# Schema type
# # Schema can be easily edited here
# SCHEMA = {
#     "Study": {
#         'type': 'string'
#         , 'doc': "ID of the study, expected as string."
#         },
#     "date du mort": {
#         'type': 'datetime64[ns]'
#         , 'doc': "Date of death, expected as datetime."
#         },
#     "jour du mort": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         , 'doc': "Day of death, expected as integer."
#         },
#     "N° parquet": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         , 'doc': "Parquet number, expected as integer."
#         },
#     "n° régime": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         , 'doc': "Regime number, expected as integer. Extracted from string."
#         },
#     "poids du mort": {
#         'type': 'Int64'
#         , 'doc': "Weight of the dead, expected as float."
#         },
#     "alt J mort": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         },
#     "alt départ": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         },
#     "conso du mort": {
#         'type': 'float'
#         , 'clean_func': float_2dec
#         , 'doc': "Consumption of the dead, expected as float. Converted to float and rounded to 2 decimal places."
#         },
#     "Nb animaux": {
#         'type': 'Int64'
#         , 'doc': "Number of animals, expected as integer."
#         },
#     "Poids mort après pesée": {
#         'type': 'Int64'
#         , 'doc': "Weight of the dead after weighing, expected as integer."
#         },
#     "N°Bague": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         , 'doc': "Ring number, expected as integer."
#         },
#     "Diagnostic": {
#         'type': 'string'
#         , 'doc': "Diagnosis or remarks, expected as string."
#         },
#     "period": {
#         'type': 'Int64'
#         , 'clean_func': extract_digits
#         , 'doc': "Period number, expected as integer."},
#     "SourceFile": {
#         'type': 'string'
#         , 'doc': "Source file name, expected as string."
#         }
# }