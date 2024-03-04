# Databricks notebook source
# MAGIC %md ### This notebook will be used to create the ingestion tables so that we can load ingestion data into these tables. Typically for day 0 and delta loads 

# COMMAND ----------

# MAGIC %md #### Location table creation

# COMMAND ----------

schema = """
    store_location_key INT,
    region STRING,
    province STRING,
    city STRING,
    postal_code STRING,
    banner STRING,
    store_num INT,
    created_date TIMESTAMP
"""
table_name = "acme_location"

# Specify the desired table location
table_location = "/FileStore/tables/acme_location"

# Create the Delta table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} 
    ({schema})
    USING DELTA
    LOCATION '{table_location}'
""")


# COMMAND ----------

# MAGIC %md #### Product table creation

# COMMAND ----------

schema = """
    product_key LONG,
    sku STRING,
    upc STRING,
    item_name STRING,
    item_description STRING,
    department STRING,
    category STRING,
    created_date TIMESTAMP
"""

# Specify the desired table location
table_location = "/FileStore/tables/acme_products"

# Create the Delta table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS acme_products 
    ({schema})
    USING DELTA
    LOCATION '{table_location}'
""")

# COMMAND ----------

# MAGIC %md #### Transaction table creation

# COMMAND ----------

schema = """
    store_location_key STRING,
    product_key STRING,
    collector_key STRING,
    trans_dt TIMESTAMP,
    sales DOUBLE,
    units INT,
    trans_key STRING,
    created_date TIMESTAMP
"""

# Specify the desired table location
table_location = "/FileStore/tables/acme_transactions"

# Create the Delta table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS acme_transactions 
    ({schema})
    USING DELTA
    LOCATION '{table_location}'
""")
