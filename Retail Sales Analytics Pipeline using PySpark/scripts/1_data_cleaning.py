"""
DATA CLEANING PIPELINE
Author: Arati Paraskar
Description:
    - Reads raw data
    - Cleans nulls, formatting issues
    - Saves cleaned CSVs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Start Spark
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load Raw Data
users_df = spark.read.csv("data/raw/users.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("data/raw/order_items.csv", header=True, inferSchema=True)

# Example Cleaning Rules
products_df = products_df.withColumn("sku", regexp_replace(col("sku"), " ", ""))

orders_df = orders_df.dropDuplicates(["order_id"])

# Save cleaned data
users_df.write.csv("data/cleaned/users_cleaned.csv", header=True, mode="overwrite")
products_df.write.csv("data/cleaned/products_cleaned.csv", header=True, mode="overwrite")
orders_df.write.csv("data/cleaned/orders_cleaned.csv", header=True, mode="overwrite")
order_items_df.write.csv("data/cleaned/order_items_cleaned.csv", header=True, mode="overwrite")

print("✅ Data Cleaning Completed Successfully!")
