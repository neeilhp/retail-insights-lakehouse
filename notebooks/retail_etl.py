from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, lit
import pandas as pd

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("RetailETL").getOrCreate()
except ImportError:
    print("⚠️ PySpark not available — skipping Spark logic in CI.")

# Load raw data
sales = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/sales_data.csv", header=True, inferSchema=True)
products = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/products.csv", header=True, inferSchema=True)
stores = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/stores.csv", header=True, inferSchema=True)
customers = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/customers.csv", header=True, inferSchema=True)

# Data Cleaning and Transformation
sales_cleaned = sales \
    .withColumn("amount", round(col("amount"), 2)) \
    .withColumn("quantity", when(col("quantity") < 0, 0).otherwise(col("quantity"))) \
    .dropna() \
    .dropDuplicates()


#Filtering high value sales
sales_filtered = sales_cleaned.filter(sales["amount"] > 300)

# Adding a discounted amount column
sales_discounted = sales_filtered.withColumn(
    "discounted_amount", round(sales_filtered["amount"] * 0.9, 2)
)

# Flagging bulk orders
sales_flagged = sales_discounted.withColumn(
    "bulk_order", when(col("quantity") >= 3, "Yes").otherwise("No")
)

# Save curated data
import os
df = sales_flagged.toPandas()
output_dir = "C:/Users/neelp/Portfolio/retail-insights-lakehouse/curated"
os.makedirs(output_dir, exist_ok=True)
df.to_csv(f"{output_dir}/sales_transformed.csv", index=False)



# Load curated sales data
sales_transformed = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/curated/sales_transformed.csv", header=True, inferSchema=True)

# Load dimension tables
products = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/products.csv", header=True, inferSchema=True)
stores = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/stores.csv", header=True, inferSchema=True)
customers = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/data/customers.csv", header=True, inferSchema=True)

# Join with dimensions
sales_enriched = sales_transformed \
    .join(products, "product_id") \
    .join(customers, "customer_id") \
    .join(stores, "store_id")

# Select final columns
sales_final = sales_enriched.select(
    "sale_id", "date", "quantity", "amount",
    "product_name", "category", "brand",
    "name", "email", "loyalty_status",
    "store_name", "region"
)

# Save enriched data
import os
df = sales_final.toPandas()
output_dir = "C:/Users/neelp/Portfolio/retail-insights-lakehouse/curated"
os.makedirs(output_dir, exist_ok=True)

# Verify data
print("Row count:", sales_final.count())
sales_final.show(5)
df_enriched = sales_final.toPandas()
print("Pandas row count:", len(df_enriched))

# Save enriched data to CSV
df_enriched.to_csv(f"{output_dir}/sales_enriched.csv", index=False)


# Verify saved data
df = spark.read.csv("C:/Users/neelp/Portfolio/retail-insights-lakehouse/curated/sales_enriched.csv", header=True, inferSchema=True)
df.show(5)


# Stop Spark
spark.stop()


