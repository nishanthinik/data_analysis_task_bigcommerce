# Databricks notebook source
# MAGIC %run Config/Config

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, coalesce, to_date,add_months
from pyspark.sql.types import DateType,DecimalType
from pyspark.sql.functions import sum

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue Trend Analysis
# MAGIC

# COMMAND ----------

df_revenue = spark.read.format("delta").load(revenue_filepath)

# COMMAND ----------

df_revenue_completed_orders = df_revenue.filter(col("order_status")=="completed")

# COMMAND ----------

df_revenue_completed_orders.display()

# COMMAND ----------

start_date = "2024-03-01"
end_date = "2024-03-31"
last_year_start_date = add_months(lit(start_date), -12)
last_year_end_date = add_months(lit(end_date), -12)

df_current = df_revenue.filter(
    (col("order_date_as_date") >= start_date) &
    (col("order_date_as_date") <= end_date)
)

df_last_year = df_revenue.filter(
    (col("order_date_as_date") >= last_year_start_date) &
    (col("order_date_as_date") <= last_year_end_date)
)

# COMMAND ----------

# DBTITLE 1,Revenue Growth
current_revenue = df_current.filter(col("order_status")=="completed").agg(sum("total_revenue")).collect()[0][0]
last_year_revenue = df_last_year.filter(col("order_status")=="completed").agg(sum("total_revenue")).collect()[0][0]

growth = (((current_revenue - last_year_revenue) / last_year_revenue) * 100)
print("Revenue Growth from Last Year to Current Year for March period : " + str(growth))

# COMMAND ----------

# DBTITLE 1,OrdersCompleted
orders_current_completed = df_current.filter(col("order_status")=="completed").count()
orders_last_year_completed = df_last_year.filter(col("order_status")=="completed").count()

completed_order_growth = (((orders_current_completed - orders_last_year_completed) / orders_last_year_completed) * 100)
print("Completed Order Growth from Last Year to Current Year for March period : " + str(completed_order_growth))

# COMMAND ----------

# DBTITLE 1,Cancelled order Growth
orders_current_cancelled = df_current.filter(col("order_status")=="cancelled").count()
orders_last_year_cancelled = df_last_year.filter(col("order_status")=="cancelled").count()

cancelled_order_growth = (((orders_current_cancelled - orders_last_year_cancelled) / orders_last_year_cancelled) * 100)
print("cancelled Order Growth from Last Year to Current Year for March period : " + str(cancelled_order_growth))

# COMMAND ----------

# DBTITLE 1,Year Trend Customer Cpuntry wise
df_revenue.filter(col("order_status")=="completed").groupBy("customer_country", "order_year").sum("total_revenue").display()