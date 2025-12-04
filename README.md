# 📊 Retail Sales Analytics Pipeline using PySpark

An end-to-end **data engineering & analytics pipeline** built using **PySpark**, covering:

✔ Data Cleaning  
✔ ETL Pipeline  
✔ Star Schema Data Modeling  
✔ SQL-Based Analytics  
✔ BI-Ready Output  

This project demonstrates real-world **data engineering workflows** using Apache Spark, including data preprocessing, transformation, modeling, and analytical querying.

---

## 🚀 Project Overview

This project processes raw retail transactional data and transforms it into a **cleaned, analytics-ready data warehouse** using a **Star Schema**.  
After building fact & dimension tables, SQL queries are executed to answer key business questions.

---

## 🧱 Features

### ✅ 1. Data Cleaning (PySpark)
- Handle missing values  
- Standardize column names  
- Convert data types  
- Remove duplicates  
- Normalize text  
- Currency formatting  
- Date cleaning & parsing

### ✅ 2. ETL Pipeline (Extract → Transform → Load)
- Load raw CSV datasets  
- Build `dim_users`, `dim_products`, `dim_date`  
- Create `fact_orders` using surrogate keys  
- Implement Spark-based joins  
- Save processed tables  

### ✅ 3. Data Modeling (Star Schema)

          dim_users
              |
dim_products — fact_orders — dim_date


### ✅ 4. SQL Analytics
Using Spark SQL to answer business questions such as:
- Monthly revenue trends  
- Best-selling products  
- High-value customers  
- Category-level analysis  
- Order dynamics  

---

## 📁 Project Structure

```plaintext
Retail-Sales-Analytics-Pipeline/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   ├── 01_data_cleaning.ipynb
│   ├── 02_etl_star_schema.ipynb
│   └── 03_sql_analytics.ipynb
│
├── src/
│   ├── data_cleaning.py
│   ├── etl_star_schema.py
│   └── sql_analytics.py
│
├── README.md
```


---

## 🛠️ Tech Stack

| Tool / Technology | Purpose |
|------------------|---------|
| **PySpark** | Distributed data processing |
| **Spark SQL** | Business analytics |
| **DataFrames** | Core ETL operations |
| **Google Colab / Jupyter** | Notebook environment |
| **Python 3.x** | Main language |

---

## 📌 Datasets Used

- `users_cleaned.csv`  
- `products_cleaned.csv`  
- `orders_cleaned.csv`  
- `order_items_cleaned.csv`  
- `dim_date` (generated using Spark)

---

## 🧹 Data Cleaning Summary

Key cleaning tasks include:

- Dropping rows with missing critical fields  
- Standardizing text formatting  
- Normalizing column names (`lowercase + underscores`)  
- Removing unwanted characters using `regexp_replace`  
- Converting string dates into proper date types  
- Removing duplicate rows  
- Ensuring schema consistency  

---

## 🏗️ ETL & Star Schema Steps

### **1. Build Dimension Tables**
- `dim_users` with surrogate key  
- `dim_products` with surrogate key  
- `dim_date` (year, month, day, weekday, quarter)

### **2. Build Fact Table**
- Merge orders + order_items  
- Add surrogate keys (`user_sk`, `product_sk`, `date_sk`)  
- Calculate sales metrics  

### **3. Save All Tables**
All dimension and fact tables stored under:

/data/processed/

---

## 📈 Sample SQL Analytics

### 🔹 Top 10 Products by Revenue
```sql
SELECT product_id, SUM(sales_amount) AS total_revenue
FROM fact_orders
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;

🔹 Monthly Sales Trend
SELECT year, month, SUM(sales_amount) AS monthly_sales
FROM fact_orders
GROUP BY year, month
ORDER BY year, month;

🔹 Most Valuable Customers
SELECT user_id, SUM(sales_amount) AS total_spent
FROM fact_orders
GROUP BY user_id
ORDER BY total_spent DESC;

📦 How to Run This Project
1. Install Dependencies
pip install pyspark

2. Run the ETL Scripts
python src/data_cleaning.py
python src/etl_star_schema.py
python src/sql_analytics.py

3. Or simply run the Colab/Jupyter Notebooks
Recommended for easier visualization.

⭐ Key Learnings

From this project, you will learn:
How to design a real-world data pipeline
How to clean large datasets using PySpark
How to build a star schema (fact + dimension tables)
How to create surrogate keys in Spark
How to perform SQL analytics on Spark DataFrames
How to document and structure a Data Engineering project

Conclusion

This project showcases a complete Retail Analytics Pipeline using PySpark, transforming raw data into a fully modeled and analytics-ready warehouse.
It is ideal for:

Data Engineering portfolios
Spark learning
ETL + SQL case studies
GitHub showcase projects


