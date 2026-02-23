# Databricks notebook source
# MAGIC %md
# MAGIC # PrepareDimBenefits
# MAGIC > cmd2 - install delta-spark library  
# MAGIC > cmd3 - create silver dim table  
# MAGIC > cmd4 - load data into silver layer

# COMMAND ----------

# MAGIC %pip install delta-spark

# COMMAND ----------

from pyspark.sql.types import StringType, ByteType, LongType, TimestampType
import pyspark.sql.functions as f
from delta.tables import DeltaTable, IdentityGenerator

def create_silver_dim_benefits() -> None:
    (
        DeltaTable.createOrReplace()
        .tableName("linkedin.silver.DimBenefits")
        .addColumn("JobId", dataType=LongType(), nullable=False)
        .addColumn("Inferred", dataType=ByteType(), nullable=False)
        .addColumn("Type", dataType=StringType(), nullable=True)
        .addColumn("CreatedDateTime", dataType=TimestampType(), nullable=False)
        .execute()
    )

create_silver_dim_benefits()

# COMMAND ----------

( 
    spark.read.table("linkedin.bronze.DimBenefits")
    .select(
        f.col("job_id").alias("CompanyId"),
        f.col("inferred").alias("Inferred"),
        f.col("type").alias("Type"),
        f.current_timestamp().alias("CreatedDateTime")
    )
    .write
    .mode("overwrite")
    .insertInto("linkedin.silver.DimBenefits")
)

