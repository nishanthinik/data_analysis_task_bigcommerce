# Databricks notebook source
FilePath = dbutils.widgets.get("FilePath")
RawFilePath = dbutils.widgets.get("RawFilePath")

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import col, current_date

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load(FilePath)

# COMMAND ----------

df_clean = df.dropna().dropDuplicates()
# display(df_clean)

# COMMAND ----------

df = df_clean.withColumn("ingestion_date", current_date())

df.write \
  .format("delta") \
  .mode("append") \
  .partitionBy("ingestion_date") \
  .save(RawFilePath)