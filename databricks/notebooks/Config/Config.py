# Databricks notebook source
filestorage = '/Volumes/filestorage/general/bigcommerce/'
raw_vol = '/Volumes/raw_data/general/bigcommerce/'
stage_vol = '/Volumes/stage_data/general/bigcommerce/'
processed_vol = '/Volumes/processed_data/general/bigcommerce/'

# COMMAND ----------

# DBTITLE 1,FileStoragePaths
Orders_FilePath = filestorage + 'orders/bigcommerce_orders.csv'
OrderItems_FilePath = filestorage + 'order_items/bigcommerce_order_items.csv'
Products_FilePath = filestorage + 'products/bigcommerce_products.csv'
Customers_FilePath = filestorage + 'customers/bigcommerce_customers.csv'

# COMMAND ----------

# DBTITLE 1,RawFilePaths
Orders_RawFilePath = raw_vol + 'orders/delta'
OrderItems_RawFilePath = raw_vol + 'order_items/delta'
Products_RawFilePath = raw_vol + 'products/delta'
Customers_RawFilePath = raw_vol + 'customers/delta'

# COMMAND ----------

# DBTITLE 1,StageFilePaths
Orders_StageFilePath = stage_vol + 'orders/delta'
OrderItems_StageFilePath = stage_vol + 'order_items/delta'
Products_StageFilePath = stage_vol + 'products/delta'
Customers_StageFilePath = stage_vol + 'customers/delta'

# COMMAND ----------

# DBTITLE 1,PKL List
OrderItems_PKL = 'order_id', 'order_item_id', 'product_id'
Products_PKL = 'product_id'
Customers_PKL = 'customer_id'
Orders_PKL = 'order_id'

# COMMAND ----------

# DBTITLE 1,Processed Filepaths
revenue_filepath = processed_vol + 'revenue/delta'


# COMMAND ----------

# DBTITLE 1,CommonFunctions
def is_file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise
        print(e)