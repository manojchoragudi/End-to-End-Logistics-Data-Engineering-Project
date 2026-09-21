# Databricks notebook source
from pyspark.sql.fucntion import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS logistics_cata;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA if not EXISTS logistics_cata.bronze

# COMMAND ----------

bronze_path = "abfss://bronze@adlslogistics.dfs.core.windows.net/raw/"

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlslogistics.dfs.core.windows.net/raw/customers.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,Cell 6
df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable("catalog_logi.bronze.customers")

# COMMAND ----------

# DBTITLE 1,Cell 8
import pandas as pd
files = [
        {"file" : "customers"},
        {"file" : "drivers"},
        {"file" : "orders"},
        {"file" : "shipments"},
        {"file" : "tracking_events"},
        {"file" : "vehicles"},
        {"file" : "warehouses"}
        ]

for file in files :
    url = f"https://adlslogistics.blob.core.windows.net/bronze/raw/{file['file']}.csv?sp=r&st=2026-09-19T05:01:56Z&se=2026-09-23T13:16:56Z&spr=https&sv=2026-02-06&sr=c&sig=UDAUISw9O3nqoGTkF7s%2FPs7KURe6X4%2F1CNFDwkFlaZM%3D"

    df = pd.read_csv(url)
    df_spark = spark.createDataFrame(df)

# writing the files to the catalog
    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("path", f"abfss://bronze@adlslogistics.dfs.core.windows.net/delta/{file['file']}")\
        .saveAsTable(f"logistics_cata.bronze.{file['file']}")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from logistics_cata.bronze.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM logistics_cata.bronze.customers
# MAGIC LIMIT 10;