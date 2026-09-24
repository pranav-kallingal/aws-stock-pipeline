from pyspark.sql import SparkSession 
from pyspark.sql import functions as F

S3_BUCKET = "us-stock-analysis-pipeline"

# Initialize standard SparkSession for AWS Glue
spark = SparkSession.builder.appName("gold_glue_stock").getOrCreate()

print("Reading Silver data...")
df_silver = spark.read.parquet(f"s3://{S3_BUCKET}/silver/")

# Aggregate average daily return percent
df_gold = df_silver.groupBy("symbol").agg(
    F.round(F.avg("daily_return_pct"), 2).alias("daily_return_pct")
)

print("Writing to Gold...")
# Gold table is aggregated, no need to partition by symbol
df_gold.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/gold/")
print("Gold Layer complete!")
