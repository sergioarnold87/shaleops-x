from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShaleOps-SQL-Explorer") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://shaleops-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# El resto del código se mantiene igual...
print("🔍 Cargando datos de Silver...")
df_silver = spark.read.format("delta").load("s3a://silver/well_data_cleaned")
df_silver.createOrReplaceTempView("tabla_silver")

query = """
SELECT 
    well_id, 
    round(productivity_index, 2) as pi, 
    failure_type,
    reservoir_pressure
FROM tabla_silver 
LIMIT 10
"""

print("📊 RESULTADO DE CONSULTA SQL:")
spark.sql(query).show()