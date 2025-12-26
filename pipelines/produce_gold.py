from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, count

# Configuración del SparkSession (Configuración completa de Delta y S3)
spark = SparkSession.builder \
    .appName("ShaleOps-Gold-Analytics") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://shaleops-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🥇 Extrayendo datos curados de Silver...")
silver_df = spark.read.format("delta").load("s3a://silver/well_data_cleaned")

print("📊 Generando el Top 5 de Pozos (Ranking de Productividad)...")

# Negocio: Seleccionamos los campeones de Vaca Muerta
top_wells_df = silver_df \
    .select("well_id", "well_name", "productivity_index", "initial_production_day1") \
    .orderBy(desc("productivity_index")) \
    .limit(5)

print("📈 Calculando salud del yacimiento (Métricas Agregadas)...")

# Analítica: Resumen por tipo de falla para mantenimiento
health_summary_df = silver_df \
    .groupBy("failure_type") \
    .agg(
        count("well_id").alias("total_wells"),
        avg("productivity_index").alias("avg_pi")
    )

print("💾 Guardando tablas de Negocio en Capa Gold...")

# Guardamos como Delta Tables para permitir reportes históricos
top_wells_df.write.format("delta").mode("overwrite").save("s3a://gold/top_performing_wells")
health_summary_df.write.format("delta").mode("overwrite").save("s3a://gold/field_health_summary")

print("🚀 Reporte Gold generado exitosamente.")
top_wells_df.show()