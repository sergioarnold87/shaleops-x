from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Iniciamos sesión de Spark con soporte para Delta y S3
spark = SparkSession.builder \
    .appName("ShaleOps-Final-Analytics") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://shaleops-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🏁 Cargando DataFrames desde la Capa Gold...")

# 1. Cargar el Ranking de Pozos
df_top_wells = spark.read.format("delta").load("s3a://gold/top_performing_wells")

# 2. Cargar el Resumen de Salud del Yacimiento
df_health = spark.read.format("delta").load("s3a://gold/field_health_summary")

print("\n🏆 TOP 5 POZOS DE ALTO RENDIMIENTO:")
# Mostramos el DataFrame formateado
df_top_wells.select("well_name", round("productivity_index", 2).alias("PI")).show()

print("\n🏥 ESTADO DE SALUD DEL YACIMIENTO (Agregado):")
df_health.show()

# 3. Ejemplo de Analytics extra: Filtrar solo pozos con PI crítico (>100)
print("\n🔍 ANALYTICS: Pozos con Productividad Excepcional (>100):")
df_top_wells.filter(col("productivity_index") > 100).show()