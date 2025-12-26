from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

# Configuración del SparkSession (Igual que el anterior para conectar con MinIO)
spark = SparkSession.builder \
    .appName("ShaleOps-Silver-Transformation") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://shaleops-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🔍 Leyendo Capa Bronze desde Delta Lake...")

# 1. Leer de Bronze
bronze_df = spark.read.format("delta").load("s3a://bronze/well_data")

print("🧹 Aplicando reglas de calidad y limpieza...")

# 2. Transformaciones (Limpieza y Tipado)
silver_df = bronze_df \
    .filter(col("porosity").between(0, 0.45)) \
    .filter(col("reservoir_pressure") > 0) \
    .withColumn("is_anomalous", when(col("failure_type") != "None", True).otherwise(False)) \
    .withColumn("_processed_at", current_timestamp())

# 3. Enriquecimiento: Cálculo de Productivity Index (PI) simplificado
# PI = Q / (Pr - Pwf). Como no tenemos Pwf, simulamos un cálculo de eficiencia.
silver_df = silver_df.withColumn("productivity_index", 
                                 (col("initial_production_day1") / col("reservoir_pressure")) * 100)

print("💾 Guardando en Capa Silver (Delta Lake)...")

# 4. Escribir en Silver
silver_df.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://silver/well_data_cleaned")

print(f"✅ Transformación Silver completada. Registros procesados: {silver_df.count()}")