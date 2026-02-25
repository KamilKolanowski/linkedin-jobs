# Databricks notebook source
# MAGIC %md
# MAGIC # PrepareFactEmployeeCounts
# MAGIC > cmd2 - install delta-spark library  
# MAGIC > cmd3 - create silver dim table  
# MAGIC > cmd4 - load data into silver layer

# COMMAND ----------

# MAGIC %pip install delta-spark

# COMMAND ----------

from pyspark.sql.types import IntegerType, LongType, TimestampType
import pyspark.sql.functions as f
from delta.tables import DeltaTable

def create_silver_fact_employee_counts() -> None:
    (
         DeltaTable.createOrReplace()
        .tableName("linkedin.silver.FactEmployeeCounts")
        .addColumn("CompanyId", dataType=IntegerType(), nullable=False)
        .addColumn("EmployeeCount", dataType=IntegerType(), nullable=False)
        .addColumn("FollowerCount", dataType=IntegerType(), nullable=False)
        .addColumn("TimeRecorded", dataType=LongType(), nullable=False)
        .addColumn("CreatedDateTime", dataType=TimestampType(), nullable=False)
        .execute()
    )

create_silver_fact_employee_counts()

# COMMAND ----------

( 
    spark.read.table("linkedin.bronze.FactEmployeeCounts")
    .select(
        f.col("company_id").alias("CompanyId"),
        f.col("employee_count").alias("EmployeeCount"),
        f.col("follower_count").alias("FollowerCount"),
        f.from_unixtime(f.col("time_recorded")).alias("TimeRecorded"),
        f.current_timestamp().alias("CreatedDateTime")
    )
    .write
    .mode("overwrite")
    .insertInto("linkedin.silver.FactEmployeeCounts")
)

