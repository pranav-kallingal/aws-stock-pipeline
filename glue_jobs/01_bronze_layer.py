from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# S3 bucket
S3_BUCKET = "us-stock-analysis-pipeline"

# AWS Glue uses standard s3:// paths
input_path = f"s3://{S3_BUCKET}/landing/*.json"
output_path = f"s3://{S3_BUCKET}/bronze/"

# Initialize standard SparkSession for AWS Glue
spark = SparkSession.builder.appName("AWS_GLUE_BRONZE").getOrCreate()

print("Reading files from Landing Zone...")
df = spark.read.json(input_path, multiLine=True)

# Add ingestion timestamp
df_bronze = df.withColumn("ingested_at", F.current_timestamp())

# Save as Parquet, partitioned by symbol
df_bronze.write.mode("overwrite").partitionBy("symbol").parquet(output_path)

print("Bronze Layer complete!")
