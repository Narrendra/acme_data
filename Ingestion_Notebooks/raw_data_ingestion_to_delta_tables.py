# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from functools import reduce

def read_merge_csv_files(storage_account_name, container_name):
    # Define the Azure Blob Storage container path
    container_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    access_key = "kAwL8gxBEaFjU8JeEMAvvU4hyA0yvdJSpXjZMVpH12sQLCgCMAn0Ibod8X9zCnxlQOUO3qmjpZRC+ASty42nfA=="
     
    spark.conf.set(
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
        access_key
    )
    
    df_list = []
    for file_info in dbutils.fs.ls(container_path):
        file_path = file_info.path
        if file_path.endswith('.csv'):
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            df_list.append(df)
    
    # Merge all DataFrames into a single DataFrame
    merged_df = reduce(lambda df1, df2: df1.union(df2), df_list)        
    merged_df = merged_df.withColumn("created_date", current_timestamp())
    return merged_df


# COMMAND ----------

# MAGIC %md ### Ingest location data

# COMMAND ----------

import datetime
storage_account_name = "acmegrocerydata"
container_name = "raw-location"
df = read_merge_csv_files(storage_account_name, container_name)
# Write the DataFrame to the table
df.write.mode("overwrite").saveAsTable("acme_location")


# COMMAND ----------

# MAGIC %md ### Ingest product data

# COMMAND ----------

import datetime
storage_account_name = "acmegrocerydata"
container_name = "raw-products"
df = read_merge_csv_files(storage_account_name, container_name)
# Write DataFrame to the table
df.write.mode("overwrite").saveAsTable("acme_products")


# COMMAND ----------

# MAGIC %md ### Ingest transaction data

# COMMAND ----------

import datetime
storage_account_name = "acmegrocerydata"
container_name = "raw-transactions"
df = read_merge_csv_files(storage_account_name, container_name)
# Write DataFrame to the table
df.write.mode("overwrite").saveAsTable("acme_transaction")


# COMMAND ----------

# MAGIC %sql select * from acme_transaction
