from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize standard SparkSession for AWS Glue
spark = SparkSession.builder.appName("silver_Layer_glue_aws").getOrCreate()

S3_BUCKET = "us-stock-analysis-pipeline"

print("Reading Bronze data...")
df = spark.read.parquet(f"s3://{S3_BUCKET}/bronze/")

# Cast columns to appropriate float types
df_cleaned = df.withColumns({
    "open": F.col("open").cast("float"),
    "high": F.col("high").cast("float"),
    "low": F.col("low").cast("float"),
    "close": F.col("close").cast("float"),
    "volume": F.col("volume").cast("float")
})
                    
# Calculate daily return percentage
df_silver = df_cleaned.withColumn(
    "daily_return_pct", 
    (F.col("close") - F.col("open")) * 100 / F.col("open")
)

print("Writing to Silver...")
df_silver.write.mode("overwrite").partitionBy("symbol").parquet(f"s3://{S3_BUCKET}/silver/")
print("Silver Layer complete!")
