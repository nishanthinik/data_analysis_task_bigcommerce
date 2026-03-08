# Databricks notebook source
# MAGIC %run Config/Config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType


# COMMAND ----------

# DBTITLE 1,One Time - Manage Table
schema = StructType([
    StructField("Entity_ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("FilePath", StringType(), True),
    StructField("RawFilePath", StringType(), True),
    StructField("StageFilePath", StringType(), True),
    StructField("PKL", StringType(), True)
])
data = [
    ("1", "Products", Orders_FilePath, Orders_RawFilePath, Orders_StageFilePath, Orders_PKL),
    ("2", "Customers", Customers_FilePath, Customers_RawFilePath, Customers_StageFilePath, Customers_PKL),
    ("3", "Orders", Products_FilePath, Products_RawFilePath, Products_StageFilePath, Products_PKL),
    ("4", "Order_Items",  OrderItems_FilePath, OrderItems_RawFilePath, OrderItems_StageFilePath, OrderItems_PKL)
]
df_entity = spark.createDataFrame(data, schema)
display(df_entity)


# COMMAND ----------

from datetime import datetime

Partition = '/ingestion_date=' + datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Cell 4
for row in df_entity.collect():
    # process each row
    print(row['Entity_ID'])
    RunIngestion = dbutils.notebook.run("/IngestionToRaw/FileLoadToRaw", 0, {"FilePath": row['FilePath'], "RawFilePath": row['RawFilePath']})
    RunStageMerge = dbutils.notebook.run("/RawToStage/StageMerge", 0, {"StageFilePath": row['StageFilePath'], "RawFilePath": row['RawFilePath'],"Partition": Partition, "PKL": row['PKL']})

# COMMAND ----------

