# pipelines/ingest_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder \
    .appName("ShaleOps-Bronze-Ingest") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://shaleops-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🚀 Leyendo landing zone...")
input_path = "/opt/spark/data/bronze_landing_zone.parquet"
df = spark.read.parquet(input_path)

# Auditoría (Reis & Housley)
bronze_df = df.withColumn("_ingested_at", current_timestamp()) \
              .withColumn("_source_file", input_file_name())

print("💾 Escribiendo Delta Table en Capa Bronze...")
bronze_df.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://bronze/well_data")

print("✅ Ingesta finalizada exitosamente.")