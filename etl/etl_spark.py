import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, round as spark_round, lower, trim, coalesce,
    year, month, dayofmonth, hour, avg, max as spark_max, min as spark_min,
    row_number, first, last, count, sum as spark_sum
)
from opentelemetry import metrics, trace

try:
    from statsd import StatsClient as _StatsClient
except Exception:
    try:
        from statsd.client import StatsClient as _StatsClient
    except Exception:
        _StatsClient = None

from otel_setup import configure_observability


class _NoOpStatsClient:
    def __init__(self, *args, **kwargs):
        pass

    def increment(self, *args, **kwargs):
        return None

    def gauge(self, *args, **kwargs):
        return None

    def histogram(self, *args, **kwargs):
        return None


def _build_statsd_client(prefix: str):
    if _StatsClient is None:
        return _NoOpStatsClient()

    try:
        return _StatsClient(host='datadog-agent', port=8125, prefix=prefix)
    except Exception:
        return _NoOpStatsClient()

# ============================================
# LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# OBSERVABILIDAD
# ============================================

meter, tracer, _ = configure_observability("etl-spark")

statsd_client = _build_statsd_client('etl_spark')

# ============================================
# MÉTRICAS
# ============================================

etl_runs = meter.create_counter(
    name="etl_runs",
    description="Total ETL runs",
    unit="1"
)

records_processed = meter.create_counter(
    name="records_processed",
    description="Total records processed",
    unit="1"
)

records_invalid = meter.create_counter(
    name="records_invalid",
    description="Total invalid records",
    unit="1"
)

etl_duration = meter.create_histogram(
    name="etl_duration_seconds",
    description="ETL execution duration",
    unit="s"
)

# ============================================
# CONFIGURACIÓN
# ============================================

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET_BRONZE = os.getenv('MINIO_BUCKET_BRONZE', 'bronze')
MINIO_BUCKET_SILVER = os.getenv('MINIO_BUCKET_SILVER', 'silver')

# ============================================
# SPARK SESSION
# ============================================

class SparkETL:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.s3_client = self._create_s3_client()
        self._ensure_bucket_exists(MINIO_BUCKET_BRONZE)
        self._ensure_bucket_exists(MINIO_BUCKET_SILVER)
    def _create_spark_session(self) -> SparkSession:
        """Crear Spark session"""
        return SparkSession.builder \
            .appName("DataPlatformETL") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.261") \
            .getOrCreate()

    def _create_s3_client(self):
        """Crear cliente S3 para MinIO"""
        return boto3.client(
            's3',
            endpoint_url=f'http://{MINIO_ENDPOINT}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

    def _ensure_bucket_exists(self, bucket: str):
        """Crear bucket si no existe"""
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except Exception:
            self.s3_client.create_bucket(Bucket=bucket)

    def load_bronze_data(self) -> Optional[object]:
        """Cargar datos de Bronze"""
        try:
            with tracer.start_as_current_span("load_bronze"):
                # 1. Verificar primero con boto3 si hay archivos JSON en bronze/raw/
                response = self.s3_client.list_objects_v2(
                    Bucket=MINIO_BUCKET_BRONZE,
                    Prefix="raw/"
                )
                if 'Contents' not in response or not any(obj['Key'].endswith('.json') for obj in response['Contents']):
                    logger.warning("Aún no hay archivos JSON en la capa Bronze.")
                    return None

                # 2. Leer con Spark solo cuando confirmamos que existen datos
                bronze_path = f"s3a://{MINIO_BUCKET_BRONZE}/raw/"
                df = self.spark.read.json(bronze_path)
                
                if df.count() == 0:
                    logger.warning("No data found in Bronze layer")
                    return None
                
                logger.info(f"Loaded {df.count()} records from Bronze")
                records_processed.add(df.count())
                return df
        except Exception as e:
            logger.error(f"Error loading Bronze data: {str(e)}")
            return None
        
    def validate_and_clean(self, df):
        """Validar y limpiar datos"""
        with tracer.start_as_current_span("validate_and_clean"):
            # Validar campos requeridos
            required_fields = [
                'sensor_id', 'sensor_type', 'latitude', 'longitude',
                'timestamp', 'vehicle_count', 'average_speed'
            ]
            
            for field in required_fields:
                df = df.filter(col(field).isNotNull())
            
            # Limpiar strings
            df = df.withColumn('sensor_id', trim(lower(col('sensor_id'))))
            df = df.withColumn('sensor_type', trim(lower(col('sensor_type'))))
            
            # Validar rangos
            df = df.filter(
                (col('latitude') >= -90) & (col('latitude') <= 90) &
                (col('longitude') >= -180) & (col('longitude') <= 180)
            )
            
            df = df.filter(col('vehicle_count') >= 0)
            df = df.filter((col('average_speed') >= 0) & (col('average_speed') <= 200))
            df = df.filter((col('air_quality_index') >= 0) & (col('air_quality_index') <= 500))
            
            logger.info(f"After validation: {df.count()} records")
            return df

    def enrich_data(self, df):
        """Enriquecer datos"""
        with tracer.start_as_current_span("enrich_data"):
            # Clasificar tráfico
            df = df.withColumn(
                'traffic_level',
                when(col('traffic_density') < 25, 'low')
                .when(col('traffic_density') < 60, 'medium')
                .when(col('traffic_density') < 85, 'high')
                .otherwise('critical')
            )
            
            # Clasificar calidad del aire
            df = df.withColumn(
                'air_quality_level',
                when(col('air_quality_index') < 50, 'good')
                .when(col('air_quality_index') < 100, 'moderate')
                .when(col('air_quality_index') < 150, 'unhealthy_for_groups')
                .when(col('air_quality_index') < 200, 'unhealthy')
                .otherwise('hazardous')
            )
            
            # Extraer componentes de timestamp
            df = df.withColumn('year', year(col('timestamp')))
            df = df.withColumn('month', month(col('timestamp')))
            df = df.withColumn('day', dayofmonth(col('timestamp')))
            df = df.withColumn('hour', hour(col('timestamp')))
            
            # Redondear valores numéricos
            df = df.withColumn('latitude', spark_round(col('latitude'), 4))
            df = df.withColumn('longitude', spark_round(col('longitude'), 4))
            df = df.withColumn('average_speed', spark_round(col('average_speed'), 2))
            df = df.withColumn('traffic_density', spark_round(col('traffic_density'), 2))
            
            return df

    def save_silver_data(self, df):
        """Guardar datos en Silver"""
        try:
            with tracer.start_as_current_span("save_silver"):
                self._ensure_bucket_exists(MINIO_BUCKET_SILVER)
                
                silver_path = f"s3a://{MINIO_BUCKET_SILVER}/processed/"
                
                df.write \
                    .mode("append") \
                    .partitionBy("year", "month", "day", "hour") \
                    .parquet(silver_path)
                
                logger.info(f"Saved {df.count()} records to Silver")
                statsd_client.increment('silver.records_saved', df.count())
                
        except Exception as e:
            logger.error(f"Error saving to Silver: {str(e)}")

    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        etl_runs.add(1)
        start_time = time.time()
        
        try:
            with tracer.start_as_current_span("etl_pipeline"):
                logger.info("Starting ETL pipeline")
                
                # Cargar datos
                df = self.load_bronze_data()
                if df is None:
                    logger.warning("No data to process")
                    return
                
                # Validar y limpiar
                df = self.validate_and_clean(df)
                
                # Enriquecer
                df = self.enrich_data(df)
                
                # Guardar
                self.save_silver_data(df)
                
                elapsed = time.time() - start_time
                etl_duration.record(elapsed)
                statsd_client.histogram('etl.duration_seconds', elapsed)
                
                logger.info(f"ETL completed in {elapsed:.2f} seconds")
                
        except Exception as e:
            logger.error(f"Critical error in ETL: {str(e)}")

# ============================================
# SCHEDULER
# ============================================

def run_etl_scheduler():
    """Ejecutar ETL cada 60 segundos para pruebas locales (en prod: 300)"""
    etl = SparkETL()
    
    while True:
        try:
            etl.run_etl()
        except Exception as e:
            logger.error(f"Error in ETL scheduler: {str(e)}")
        
        time.sleep(60)  # <- Cambiar de 300 a 60 segundos

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    run_etl_scheduler()
