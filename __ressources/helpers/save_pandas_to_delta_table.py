from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import sys
sys.path.append('../config')

from logging_config import setup_logger

def save_pandas_to_delta_table(pdf, table_name, schema: StructType = None, mode="overwrite"):
    """
    Save a pandas DataFrame as a Delta table in Databricks, with optional schema.
    If DataFrame is empty and no schema is provided, logs a warning and skips saving.
    """
    logger = setup_logger("save_pandas_to_delta_table")
    spark = SparkSession.builder.getOrCreate()

    if not table_name or not isinstance(table_name, str) or table_name.strip() == "":
        logger.error(f"Invalid table name provided: '{table_name}'. Skipping table creation.")
        return

    if pdf.empty:
        if schema is None:
            logger.warning(f"DataFrame for table '{table_name}' is empty and no schema is provided. Skipping table creation.")
            return
        elif not schema.fields:
            logger.warning(f"DataFrame for table '{table_name}' is empty and schema has no fields. Skipping table creation.")
            return
        else:
            logger.info(f"Creating empty DataFrame for table '{table_name}' with schema: {schema.simpleString()}")
            sdf = spark.createDataFrame([], schema)
    else:
        sdf = spark.createDataFrame(pdf, schema=schema) if schema else spark.createDataFrame(pdf)

    sdf.write.format("delta") \
        .mode(mode) \
        .option("delta.columnMapping.mode", "name") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    logger.info(f"Delta table '{table_name}' saved successfully.")
