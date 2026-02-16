# Databricks notebook source
# MAGIC %md
# MAGIC # PrepareDimCompanies
# MAGIC > cmd2 - install delta-spark library
# MAGIC > cmd3 - create silver dim table
# MAGIC > cmd4 - load data into silver layer

# COMMAND ----------

# MAGIC %pip install delta-spark

# COMMAND ----------

from pyspark.sql.types import StringType, ByteType, BinaryType, IntegerType, DateType, LongType, DecimalType, TimestampType
import pyspark.sql.functions as f
from delta.tables import DeltaTable, IdentityGenerator

def create_silver_dim_company() -> None:
    (
        DeltaTable.createOrReplace()
        .tableName("linkedin.silver.DimCompany")
        .addColumn("CompanyId", dataType=IntegerType(), nullable=False)
        .addColumn("CompanyName", dataType=StringType(), nullable=False)
        .addColumn("Description", dataType=StringType(), nullable=True)
        .addColumn("CompanySize", dataType=IntegerType(), nullable=False)
        .addColumn("State", dataType=StringType(), nullable=True)
        .addColumn("Country", dataType=StringType(), nullable=True)
        .addColumn("City", dataType=StringType(), nullable=True)
        .addColumn("ZipCode", dataType=StringType(), nullable=True)
        .addColumn("Address", dataType=StringType(), nullable=True)
        .addColumn("Url", dataType=StringType(), nullable=True)
        .addColumn("CreatedDateTime", dataType=TimestampType(), nullable=False)
        .execute()
    )

create_silver_dim_company()

# COMMAND ----------

( 
    spark.read.table("linkedin.bronze.DimCompanies")
    .select(
        f.col("company_id").alias("CompanyId"),
        f.coalesce(f.col("name"), f.lit("Unknown")).alias("CompanyName"),
        f.col("description").alias("Description"),
        f.coalesce(f.col("company_size"), f.lit(0)).alias("CompanySize"),
        f.when(f.col("state") == "0", "Unknown").otherwise(f.col("state")).alias("State"),
        f.when(f.col("country") == "0", "Unknown").otherwise(f.col("country")).alias("Country"),
        f.when(f.col("city") == "0", "Unknown").otherwise(f.col("city")).alias("City"),
        f.when(f.col("zip_code") == "0", "Unknown").otherwise(f.col("zip_code")).alias("ZipCode"),
        f.when(f.col("address") == "0", "Unknown").otherwise(f.col("address")).alias("Address"),
        f.col("url").alias("Url"),
        f.current_timestamp().alias("CreatedDateTime")
    )
    .write
    .mode("overwrite")
    .insertInto("linkedin.silver.DimCompany")
)

