# ShaleOps-X: End-to-End Data Lakehouse para KPIs de Petróleo y Gas 🛢️⚡

**ShaleOps-X** es un proyecto de ingeniería de datos diseñado para procesar y analizar métricas críticas de pozos de petróleo en tiempo real. Utiliza una **Arquitectura Medallion** (Bronze, Silver, Gold) para transformar datos crudos de telemetría en insights estratégicos para la toma de decisiones.

## 🏗️ Arquitectura del Proyecto
El flujo de datos sigue los estándares de la industria para Data Lakehouses modernos:

1.  **Capa Bronze (Raw):** Ingesta de datos crudos simulados de sensores (presión, temperatura, stroke rate) almacenados en formato Parquet en **MinIO (S3 compatible)**.
2.  **Capa Silver (Cleansing & Enrichment):** Procesamiento con **Pandas/PySpark** para limpieza de valores nulos, normalización de unidades y cálculo de KPIs técnicos como el **Productivity Index (PI)**.
3.  **Capa Gold (Analytics):** Agregaciones finales y rankings de rendimiento de pozos optimizados para consumo en dashboards (PowerBI/Grafana).

## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.9+
* **Procesamiento:** Pandas / PySpark
* **Almacenamiento:** MinIO (Object Storage S3)
* **Entorno:** Docker & Virtualenvs
* **Control de Versiones:** Git & GitHub

## 🚀 Cómo ejecutarlo
1. Clonar el repositorio:
   ```bash
   git clone [https://github.com/sergioarnold87/shaleops-x.git](https://github.com/sergioarnold87/shaleops-x.git)