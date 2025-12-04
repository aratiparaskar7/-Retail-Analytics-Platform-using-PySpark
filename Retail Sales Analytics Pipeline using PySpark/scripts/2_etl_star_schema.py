"""
ETL + STAR SCHEMA CREATION
Author: Arati Paraskar
Description:
    - Loads cleaned data
    - Builds dimension and fact tables
    - Saves star schema outputs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    monotonically_increasing_id, col,
    to_date, year, month, quarter, dayofweek
)

spark = SparkSession.builder.appName("StarSchemaETL").getOrCreate()

# Load cleaned data
users_df_clean = spark.read.csv("data/cleaned/users_cleaned.csv", header=True, inferSchema=True)
products_df_clean = spark.read.csv("data/cleaned/products_cleaned.csv", header=True, inferSchema=True)
orders_df_clean = spark.read.csv("data/cleaned/orders_cleaned.csv", header=True, inferSchema=True)
order_items_df_clean = spark.read.csv("data/cleaned/order_items_cleaned.csv", header=True, inferSchema=True)

# DIM USERS
dim_users = (
    users_df_clean.select("user_id", "name", "city")
    .withColumn("user_sk", monotonically_increasing_id())
    .select("user_sk", "user_id", "name", "city")
)

# DIM PRODUCTS
dim_products = (
    products_df_clean.select("product_id", "sku", "product_name", "category")
    .withColumn("product_sk", monotonically_increasing_id())
    .select("product_sk", "product_id", "sku", "product_name", "category")
)

# DIM DATE
dim_date = (
    orders_df_clean
    .select("order_date")
    .withColumn("full_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("year", year("full_date"))
    .withColumn("month_num", month("full_date"))
    .withColumn("quarter", quarter("full_date"))
    .withColumn("day_of_week", dayofweek("full_date"))
    .dropDuplicates()
    .withColumn("date_sk", monotonically_increasing_id())
    .select("date_sk", "full_date", "year", "month_num", "quarter", "day_of_week")
)

# FACT ORDERS
fact_orders = (
    order_items_df_clean.alias("oi")
    .join(orders_df_clean.alias("o"), col("oi.order_id") == col("o.order_id"))
    .join(dim_users.alias("du"), col("du.user_id") == col("o.user_id"))
    .join(dim_products.alias("dp"), col("dp.product_id") == col("oi.product_id"))
    .join(dim_date.alias("dd"), col("dd.full_date") == col("o.order_date"))
    .select(
        col("oi.order_item_id"),
        col("oi.order_id"),
        col("du.user_sk"),
        col("dp.product_sk"),
        col("dd.date_sk"),
        col("oi.line_item_quantity"),
        col("oi.line_item_price")
    )
)

# Save Star Schema
dim_users.write.csv("data/star_schema_outputs/dim_users", header=True, mode="overwrite")
dim_products.write.csv("data/star_schema_outputs/dim_products", header=True, mode="overwrite")
dim_date.write.csv("data/star_schema_outputs/dim_date", header=True, mode="overwrite")
fact_orders.write.csv("data/star_schema_outputs/fact_orders", header=True, mode="overwrite")

print("🌟 Star Schema ETL Completed Successfully!")
