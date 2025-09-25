from pyspark.sql import SparkSession
import os

# Optional: Load AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Initialize SparkSession with S3 support
spark = SparkSession.builder \
    .appName("RetailETL-S3") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
    .getOrCreate()

# Define S3 paths
s3_bucket = "retail-insights-lakehouse"
s3_input_path = f"s3a://{s3_bucket}/curated/sales_transformed.csv"
s3_output_path = f"s3a://{s3_bucket}/curated/sales_enriched.csv"

# Read curated data from S3
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

# Example transformation: Add total_price column
df_enriched = df.withColumn("total_price", df["quantity"] * df["unit_price"])

# Write enriched data back to S3
df_enriched.write.csv(s3_output_path, header=True, mode="overwrite")

print("✅ ETL completed and written to S3.")
