# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Setup Config Table
# MAGIC > To add new entries: 
# MAGIC - extend **cfg_data** list in cmd2 with new entries
# MAGIC - run entire notebook, to merge new entries

# COMMAND ----------

# DBTITLE 1,Config List
cfg_data = [
    ("companies", "companies", "csv", "linkedin", "bronze", "DimCompanies"),
    ("companies", "company_industries", "csv", "linkedin", "bronze", "DimCompanyIndustries"),
    ("companies", "company_specialities", "csv", "linkedin", "bronze", "DimCompanySpecialities"),
    ("companies", "employee_counts", "csv", "linkedin", "bronze", "DimEmployeeCounts"),
    ("jobs", "benefits", "csv", "linkedin", "bronze", "DimBenefits"),
    ("jobs", "job_industries", "csv", "linkedin", "bronze", "DimJobIndustries"),
    ("jobs", "job_skills", "csv", "linkedin", "bronze", "DimJobSkills"),
    ("jobs", "salaries", "csv", "linkedin", "bronze", "DimSalaries"),
    ("mappings", "industries", "csv", "linkedin", "bronze", "DimIndustriesMapping"),
    ("mappings", "skills", "csv", "linkedin", "bronze", "DimSkillsMapping"),
    ("", "postings", "csv", "linkedin", "bronze", "DimPostings"),
]

# COMMAND ----------

# DBTITLE 1,Update config
from delta.tables import DeltaTable
from typing import List

def update_config(config_data: List[tuple[str, ...]]) -> None:
    config_df = spark.createDataFrame(
        config_data,
        ["Directory", "FileName", "FileExtension", "Catalog", "Schema", "TableName"]
    )
    config_df.createOrReplaceTempView("new_config")

    (
        DeltaTable.forName(spark, "linkedin.cfg.ExtractFiles") 
            .alias("target")
            .merge(
                config_df.alias("source"),
                """target.Directory = source.Directory 
                AND target.FileName = source.FileName"""
            )
            .whenNotMatchedInsert(values={
                "Directory": "source.Directory",
                "FileName": "source.FileName", 
                "FileExtension": "source.FileExtension",
                "Catalog": "source.Catalog",
                "Schema": "source.Schema",
                "TableName": "source.TableName"
            })
            .execute()
    )

update_config(cfg_data)
