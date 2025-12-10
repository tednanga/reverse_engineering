# Databricks notebook source
import pandas as pd
import sqlite3
from sqlite3 import Error
from sqlalchemy.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC https://realpython.com/python-mysql/

# COMMAND ----------

# MAGIC %md
# MAGIC https://realpython.com/python-sql-libraries/

# COMMAND ----------

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# COMMAND ----------



# COMMAND ----------

connection = create_connection("PBI_VisSqlite.db")

# COMMAND ----------

create_product_ta = """
CREATE TABLE IF NOT EXISTS SEND_PRODUCT_TA (
  STUDYID VARCHAR(25),
  PHASE CHAR(2),
  REGIME VARCHAR(3),
  DIECPUR FLOAT(4),
  DIECPURU VARCHAR(10),
  DOSDUR VARCHAR(5),
  DIETCON FLOAT(4),
  DIETCONU VARCHAR(10),
  TRT VARCHAR(200),
  DOSSTDTC DATETIME,
  DOSENDTC DATETIME,
  INSERT_DATE DATETIME
);
"""

# COMMAND ----------

execute_query(connection, create_product_ta)

# COMMAND ----------

data = pd.read_excel('Tables_output/PowerBI_vis_model/SEND_TA_PRODUCT_StudiesMay24.xlsx')

# COMMAND ----------

data.head()

# COMMAND ----------

shema = {
    'STUDYID': String(25),
  'PHASE': String(2),
  'REGIME': String(3),
  'DIECPUR': Float(4),
  'DIECPURU': String(10),
  'DOSDUR': String(5),
  'DIETCON': Float(4),
  'DIETCONU': String(10),
  'TRT': String(200),
  'DOSSTDTC': DateTime,
  'DOSENDTC': DateTime
}

# COMMAND ----------

data.to_sql(
    name='SEND_PRODUCT_TA',
    con=connection,
    schema=shema,
    index=False,
    if_exists='append'
)

# COMMAND ----------



# COMMAND ----------

eupools = pd.read_excel('Tables_output/PowerBI_vis_model/Volaille_EUPOOLS_StudiesMay24.xlsx')

# COMMAND ----------

eupools.head()

# COMMAND ----------

eupools.query('TRAITEMENT.str.contains("-  -")').TRAITEMENT.unique().tolist()

# COMMAND ----------

def select_controls(x):
    if '-  -' in x and 'Positive' not in x and 'Negative' not in x and 'Régime dysbiose' not in x:
        return 'Positive control'
    elif '-  -' in x and 'Positive' in x:
        return 'Positive control'
    elif '-  -' in x and 'Negative' in x:
        return 'Negative control'
    elif 'ctrl' in x:
        return 'Positive control'
    else:
        return 'Study'

# COMMAND ----------

eupools['Controls'] = eupools['TRAITEMENT'].apply(select_controls)

# COMMAND ----------

eupools.to_excel('check_eupools_controls_conditions.xlsx', index=False)

# COMMAND ----------

