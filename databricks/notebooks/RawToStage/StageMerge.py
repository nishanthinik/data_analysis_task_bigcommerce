# Databricks notebook source
# MAGIC %run Config/Config

# COMMAND ----------

StageFilePath = dbutils.widgets.get("StageFilePath")
RawFilePath = dbutils.widgets.get("RawFilePath")
Partition = dbutils.widgets.get("Partition")
PKL = dbutils.widgets.get("PKL")

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.types import StringType
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit
from delta.tables import DeltaTable

# COMMAND ----------

# RawFilePath = OrderItems_RawFilePath
# StageFilePath = OrderItems_StageFilePath
# Partition = '/ingestion_date=' + datetime.now().strftime("%Y-%m-%d")
# PKL  = ['order_id', 'order_item_id', 'product_id']

# COMMAND ----------

PKL_List = [ PKL]
key = ''
MergeKey =''
for pk in PKL_List:
    MergeKey = MergeKey + key + ' SRC.' + pk + ' = DST.' + pk
    key = ' AND '
# print(MergeKey)


# COMMAND ----------

df_source = spark.read.format("delta")\
    .option("inferSchema", "true")\
    .load(RawFilePath + Partition)
# df_source.display()

# COMMAND ----------

window = Window.partitionBy(PKL).orderBy(col("ingestion_date").desc())
df_source = df_source.withColumn("rn", row_number().over(window)).filter("rn = 1")
df_source = df_source.drop("rn")

# COMMAND ----------

# DBTITLE 1,If first Time Write
if not is_file_exists(StageFilePath):
    df_source.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(StageFilePath)

# COMMAND ----------

df_destination = DeltaTable.forPath(spark, StageFilePath)

# COMMAND ----------

existing_columns = [field.name for field in df_destination.toDF().schema.fields]
print("Existing columns:", existing_columns)

# COMMAND ----------

# DBTITLE 1,Get New Columns
new_columns = [field.name for field in df_source.schema.fields if field.name not in existing_columns]
print("New columns:", new_columns)


# COMMAND ----------

# DBTITLE 1,Add new column to destination
for col in new_columns:
    print(f"Adding column {col} to Delta table")
    df_destination = df_destination.toDF().withColumn(col, lit(None).cast(StringType()))
    df_destination.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(StageFilePath)


# COMMAND ----------

df_destination = DeltaTable.forPath(spark, StageFilePath)

# COMMAND ----------

df_destination.toDF()

# COMMAND ----------

# DBTITLE 1,Cell 16
df_destination.alias('DST')\
    .merge(
        df_source.alias('SRC'),
        MergeKey)\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()