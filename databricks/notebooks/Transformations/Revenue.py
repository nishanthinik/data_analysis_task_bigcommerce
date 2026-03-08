# Databricks notebook source
# MAGIC %run Config/Config

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, coalesce, to_date,add_months
from pyspark.sql.types import DateType,DecimalType

# COMMAND ----------

df_orders = spark.read.format("delta").load(Orders_StageFilePath)
df_customers = spark.read.format("delta").load(Customers_StageFilePath)
df_products = spark.read.format("delta").load(Products_StageFilePath)
df_order_items = spark.read.format("delta").load(OrderItems_StageFilePath)

# COMMAND ----------

# DBTITLE 1,Untitled
df_data = df_orders.alias("O")\
    .join(df_customers.alias("C"), col("O.customer_id")==col("C.customer_id"), "inner")\
    .select("O.order_id", "O.currency",  "C.customer_id", "order_status",
            when(col("O.country").isin('US','USA', 'United States','United States of America'), lit("US")).otherwise(col("O.country")).alias("order_country"),
            coalesce(to_date(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss"),to_date(col("order_date"), "dd/MM/yyyy HH:mm")).alias("order_date_as_date"),
            when(col("currency")=="USD", col("O.total_inc_tax")).otherwise(col("O.total_inc_tax") * 1.16).alias("total_inc_tax_in_USD"),
            when(col("currency")=="USD", col("O.discount_amount")).otherwise(col("O.discount_amount") * 1.16).alias("discount_amount_in_USD"),
            when(col("currency")=="USD", col("O.shipping_cost")).otherwise(col("O.shipping_cost") * 1.16).alias("shipping_cost_in_USD"),
            when(col("currency")=="USD", col("O.tax_amount")).otherwise(col("O.tax_amount") * 1.16).alias("tax_amount_in_USD"),
            when(col("C.country").isin('US','USA', 'United States','United States of America'), lit("US")).otherwise(col("C.country")).alias("customer_country"))\
#     .filter(col("order_status") == "completed")

# COMMAND ----------

from pyspark.sql.functions import year, month, sum

df_agg = df_data.withColumn("order_year", year(col("order_date_as_date")))\
    .withColumn("order_month", month(col("order_date_as_date")))\
    .groupBy("order_year", "order_month","order_date_as_date",'order_id','order_country','customer_country',"order_status")\
    .agg(
        sum("total_inc_tax_in_USD").cast(DecimalType(10,2)).alias("total_inc_tax_in_USD"),
        sum("discount_amount_in_USD").cast(DecimalType(10,2)).alias("discount_amount_in_USD"),
        sum("shipping_cost_in_USD").cast(DecimalType(10,2)).alias("shipping_cost_in_USD"),
        sum("tax_amount_in_USD").cast(DecimalType(10,2)).alias("tax_amount_in_USD"),
        (sum("total_inc_tax_in_USD") - sum("shipping_cost_in_USD")-sum("discount_amount_in_USD")).cast(DecimalType(10,2)).alias("total_revenue")
    )

display(df_agg)

# COMMAND ----------

df_agg.write.format("delta").mode("overwrite").partitionBy('order_year').save(revenue_filepath)