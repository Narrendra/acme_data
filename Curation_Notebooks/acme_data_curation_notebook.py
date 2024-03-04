# Databricks notebook source
# MAGIC %md ## Cleansing the transaction data to handle null values for sales and units 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp
df = spark.sql("select * from acme_transaction")
df = df.fillna(0, subset=['sales', 'units'])
df = df.withColumn("curated_date",current_timestamp())
df.write.mode("append").saveAsTable("transactions_curated")


# COMMAND ----------

# MAGIC %md ## Provinces performing well 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Assuming you have already loaded the location table into a DataFrame named location_df
transaction_df = spark.sql("select * from transactions_curated")
location_df = spark.sql("select * from acme_location")

#joining location with transaction
joined_df = transaction_df.join(location_df, "store_location_key")

#transaction data by store and province
store_province_sales = joined_df.groupBy("store_location_key", "province") \
    .agg(sum("sales").alias("total_sales"), sum("units").alias("total_units"))

store_province_sales_final = store_province_sales.orderBy(col("total_sales").desc(), col("total_units").desc())
# aggregated results
display(store_province_sales_final)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, rank
from pyspark.sql.window import Window

# transaction_df = spark.sql("select * from transactions_curated")
# location_df = spark.sql("select * from acme_location")

# Define a window function to rank stores within each province based on total sales
window_spec = Window.partitionBy("province").orderBy(col("total_sales").desc())

# Add a rank column to identify the top stores in each province
ranked_store_province_sales = store_province_sales.withColumn("rank", rank().over(window_spec))

# Calculate the average sales and units for each province
province_avg = store_province_sales.groupBy("province") \
    .agg(avg("total_sales").alias("avg_sales"), avg("total_units").alias("avg_units"))

# Join the ranked store sales with province averages
comparison_df = ranked_store_province_sales.join(province_avg, "province")

# Calculate the performance compared to the average store of the province
comparison_df = comparison_df.withColumn("sales_vs_avg", col("total_sales") / col("avg_sales"))
comparison_df = comparison_df.withColumn("units_vs_avg", col("total_units") / col("avg_units"))

# Show the comparison results
# comparison_df.show()
display(comparison_df)


# COMMAND ----------

# MAGIC %md ## Products contributing to most of ACME's revenue 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
product_df = spark.sql("select * from acme_products")
# Join the transaction and product tables based on the product_key column
joined_df = transaction_df.join(product_df, "product_key")

# Aggregate the joined data by product category to calculate total sales for each category
category_sales = joined_df.groupBy("category") \
    .agg(sum("sales").alias("total_sales"))

# Find the category with the highest total sales
top_category = category_sales.orderBy(col("total_sales").desc()).first()
display(category_sales)
# Print the result
print("Category contributing most to ACME's sales:", top_category["category"])


# COMMAND ----------

# MAGIC %md ## Determine the top 5 stores by province

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, rank

# Assuming you have a SparkSession named spark and a DataFrame named transaction_df containing transaction data
transaction_df = spark.sql("select * from transactions_curated")
location_df = spark.sql("select * from acme_location")
joined_df = transaction_df.join(location_df, "store_location_key")

# Group the transaction data by store and province, and calculate total sales for each store within each province
store_province_sales = joined_df.groupBy("store_location_key", "province") \
    .agg(sum("sales").alias("total_sales"))

# Rank the stores within each province based on total sales
window_spec = Window.partitionBy("province").orderBy(col("total_sales").desc())
ranked_sales = store_province_sales.withColumn("rank", rank().over(window_spec))

# Select the top 5 stores for each province
top_5_stores_by_province = ranked_sales.filter(col("rank") <= 5)

# Show the result
# top_5_stores_by_province.show()
display(top_5_stores_by_province)


# COMMAND ----------

# MAGIC %md ## top 10 product categories by department

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, rank

# Assuming you have a SparkSession named spark and a DataFrame named product_df containing product data
transaction_df = spark.sql("select * from transactions_curated")
product_df = spark.sql("select * from acme_products")
joined_df = transaction_df.join(product_df, "product_key")

# Group the product data by department and category, and calculate total sales for each category within each department
category_sales_by_department = joined_df.groupBy("department", "category") \
    .agg(sum("sales").alias("total_sales"))

# Rank the categories within each department based on total sales
window_spec = Window.partitionBy("department").orderBy(col("total_sales").desc())
ranked_sales = category_sales_by_department.withColumn("rank", rank().over(window_spec))

# Select the top 10 categories for each department
top_10_categories_by_department = ranked_sales.filter(col("rank") <= 10)

# Show the result
# top_10_categories_by_department.show()
display(top_10_categories_by_department)


# COMMAND ----------

# MAGIC %sql select * from acme_transaction

# COMMAND ----------

# MAGIC %sql select * from acme_products
