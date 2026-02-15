# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Setup Environment
# MAGIC > cmd2 - create catalog for storing & processing data  
# MAGIC > cmd3 - create schemas for required layers  
# MAGIC > cmd5,6,7,8 - create config tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG linkedin;
# MAGIC

# COMMAND ----------

spark.catalog.setCurrentCatalog("linkedin")

spark.sql("CREATE SCHEMA IF NOT EXISTS cfg")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creation of Config Tables for:
# MAGIC - File extraction,
# MAGIC - Saving tables
# MAGIC

# COMMAND ----------

# MAGIC %pip install delta-spark

# COMMAND ----------

from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import StringType, LongType

# COMMAND ----------

(
    DeltaTable.createIfNotExists()
        .tableName("linkedin.cfg.ExtractFiles")
        .addColumn("Id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=1, step=1), nullable=False)
        .addColumn("Directory", dataType=StringType(), nullable=False)
        .addColumn("FileName", dataType=StringType(), nullable=False)
        .addColumn("FileExtension", dataType=StringType(), nullable=False)
        .addColumn("Catalog", dataType=StringType(), nullable=False)
        .addColumn("Schema", dataType=StringType(), nullable=False)
        .addColumn("TableName", dataType=StringType(), nullable=False)
        .execute()
)


# COMMAND ----------

(
    DeltaTable.createIfNotExists()
        .tableName("linkedin.cfg.SaveTables")
        .addColumn("Id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=1, step=1), nullable=False)
        .addColumn("Directory", dataType=StringType(), nullable=False)
        .addColumn("FileName", dataType=StringType(), nullable=False)
        .addColumn("FileExtension", dataType=StringType(), nullable=False)
        .execute()
)

