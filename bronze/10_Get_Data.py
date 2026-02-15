# Databricks notebook source
# MAGIC %pip install kagglehub[pandas-datasets]

# COMMAND ----------

dbutils.widgets.text("directory", "")
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("extension", "csv")
dbutils.widgets.text("catalog", "linkedin")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("tbl_name", "")

# COMMAND ----------

from kagglehub import KaggleDatasetAdapter
from pyspark.sql import DataFrame
import kagglehub

def read_file_to_delta(directory: str, 
                       file_name: str, 
                       extension: str, 
                       catalog: str, 
                       schema: str, 
                       tbl_name: str) -> None:
    """
    Reads a file from the Kaggle dataset and writes it to a Delta table.
    Args:
        directory (str): Subdirectory within the dataset (use empty string "" for root-level files).
        file_name (str): Name of the file without extension.
        extension (str): File extension (e.g., 'csv').
        catalog (str): Target catalog name.
        schema (str): Target schema name.
        tbl_name (str): Target table name.
    Returns: 
        None
    """
    if directory:
        source_file_path = f"{directory}/{file_name}.{extension}"
    else:
        source_file_path = f"{file_name}.{extension}"

    save_table_path = f"{catalog}.{schema}.{tbl_name}"

    try:
        file_data = kagglehub.dataset_load(
            KaggleDatasetAdapter.PANDAS,
            "arshkon/linkedin-job-postings",
            source_file_path,
        )

        (
            spark
            .createDataFrame(file_data)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(save_table_path)
        )
    except Exception as e:
        raise(f"Error reading file {file_path} to Delta table {save_table_path}: {e}")


# COMMAND ----------

directory = dbutils.widgets.get("directory")
file_name = dbutils.widgets.get("file_name")
extension = dbutils.widgets.get("extension")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tbl_name = dbutils.widgets.get("tbl_name")

read_file_to_delta(directory, file_name, extension, catalog, schema, tbl_name)
