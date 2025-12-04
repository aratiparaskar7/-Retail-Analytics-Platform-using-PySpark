"""
SQL ANALYTICS PIPELINE
Author: Arati Paraskar
Description:
    - Registers tables
    - Runs 10 SQL queries
    - Saves analytics outputs
"""

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("StarSchemaAnalytics").getOrCreate()

# Register SQL Tables
fact_orders.createOrReplaceTempView("fact_orders")
dim_users.createOrReplaceTempView("dim_users")
dim_products.createOrReplaceTempView("dim_products")
dim_date.createOrReplaceTempView("dim_date")

output_dir = "analytics_outputs"
os.makedirs(output_dir, exist_ok=True)

queries = [
    ("query_1_revenue_orders_by_month", """
        SELECT dd.year, dd.month_num,
               COUNT(DISTINCT fo.order_id) AS total_orders,
               SUM(fo.line_item_price * fo.line_item_quantity) AS total_revenue
        FROM fact_orders fo
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        GROUP BY 1, 2 ORDER BY 1, 2
    """),

    ("query_2_top_10_products_by_revenue", """
        SELECT dp.product_name, dp.category,
               SUM(fo.line_item_price * fo.line_item_quantity) AS revenue
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        GROUP BY 1, 2
        ORDER BY revenue DESC LIMIT 10
    """),

    ("query_3_top_20_clv", """
        SELECT du.name, du.city,
               SUM(fo.line_item_price * fo.line_item_quantity) AS clv
        FROM fact_orders fo
        JOIN dim_users du ON fo.user_sk = du.user_sk
        GROUP BY 1, 2
        ORDER BY clv DESC LIMIT 20
    """),

    ("query_4_sales_by_category_region", """
        SELECT dp.category, du.city,
               SUM(fo.line_item_price * fo.line_item_quantity) AS total_sales
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        JOIN dim_users du ON fo.user_sk = du.user_sk
        GROUP BY 1, 2
        ORDER BY category, total_sales DESC
    """),

    ("query_5_suspicious_data", """
        SELECT fo.order_id, fo.order_item_id, fo.line_item_quantity,
               fo.line_item_price, dp.sku
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        WHERE fo.line_item_quantity <= 0 OR fo.line_item_price <= 0
        LIMIT 100
    """),

    ("query_6_cohort_analysis", """
        WITH UserFirstOrder AS (
            SELECT fo.user_sk,
                   MIN(dd.year * 100 + dd.month_num) AS first_month
            FROM fact_orders fo
            JOIN dim_date dd ON fo.date_sk = dd.date_sk
            GROUP BY fo.user_sk
        )
        SELECT ufo.first_month,
               COUNT(DISTINCT fo.user_sk) AS cohort_size,
               SUM(fo.line_item_price * fo.line_item_quantity) AS revenue
        FROM fact_orders fo
        JOIN UserFirstOrder ufo ON fo.user_sk = ufo.user_sk
        GROUP BY 1 ORDER BY 1
    """),

    ("query_7_average_order_value", """
        SELECT dd.year, dd.month_num,
               SUM(fo.line_item_price * fo.line_item_quantity) /
               COUNT(DISTINCT fo.order_id) AS AOV
        FROM fact_orders fo
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        GROUP BY 1, 2 ORDER BY 1, 2
    """),

    ("query_8_best_selling_categories", """
        SELECT dp.category,
               SUM(fo.line_item_quantity) AS units_sold,
               SUM(fo.line_item_price * fo.line_item_quantity) AS revenue
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        GROUP BY 1 ORDER BY revenue DESC
    """),

    ("query_9_daily_revenue_trend", """
        SELECT dd.full_date,
               SUM(fo.line_item_price * fo.line_item_quantity) AS daily_revenue
        FROM fact_orders fo
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        GROUP BY 1 ORDER BY 1
    """),

    ("query_10_customer_order_frequency", """
        SELECT du.name,
               COUNT(DISTINCT fo.order_id) AS order_count,
               SUM(fo.line_item_price * fo.line_item_quantity) AS total_spent
        FROM fact_orders fo
        JOIN dim_users du ON fo.user_sk = du.user_sk
        GROUP BY 1 ORDER BY order_count DESC
    """)
]

# Run all queries
for name, sql_query in queries:
    result = spark.sql(sql_query)
    result.repartition(1).write.csv(
        os.path.join(output_dir, name),
        header=True, mode="overwrite"
    )
    print(f"✅ {name} saved.")

print("\n🎉 All 10 SQL queries executed successfully!")
