import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import boto3
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from opentelemetry import metrics, trace

try:
    from statsd import StatsClient as _StatsClient
except Exception:
    try:
        from statsd.client import StatsClient as _StatsClient
    except Exception:
        _StatsClient = None

import numpy as np

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

meter, tracer, _ = configure_observability("gold-transformer")

statsd_client = _build_statsd_client('gold_transformer')

# ============================================
# MÉTRICAS
# ============================================

transformation_runs = meter.create_counter(
    name="transformation_runs",
    description="Total transformation runs",
    unit="1"
)

documents_created = meter.create_counter(
    name="documents_created",
    description="Total documents created in gold",
    unit="1"
)

transformation_duration = meter.create_histogram(
    name="transformation_duration_seconds",
    description="Transformation duration",
    unit="s"
)

# ============================================
# CONFIGURACIÓN
# ============================================

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET_SILVER = os.getenv('MINIO_BUCKET_SILVER', 'silver')

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/data_platform?authSource=admin')

# ============================================
# CLIENTE MONGODB
# ============================================

class MongoDBClient:
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.db = None
        self._connect()

    def _connect(self):
        """Conectar a MongoDB"""
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                self.client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
                self.client.admin.command('ping')
                self.db = self.client['data_platform']
                logger.info("Connected to MongoDB")
                statsd_client.gauge('mongodb_connection_attempts', retry_count)
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"MongoDB connection attempt {retry_count}/{max_retries} failed: {str(e)}")
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    logger.error(f"Failed to connect to MongoDB after {max_retries} attempts")
                    raise

    def insert_many(self, collection_name: str, documents: List[Dict]) -> int:
        """Insertar múltiples documentos"""
        try:
            with tracer.start_as_current_span("mongodb_insert"):
                collection = self.db[collection_name]
                result = collection.insert_many(documents)
                statsd_client.increment('documents.inserted', len(result.inserted_ids))
                return len(result.inserted_ids)
        except PyMongoError as e:
            logger.error(f"Error inserting documents: {str(e)}")
            statsd_client.increment('documents.insert_failed')
            return 0

    def update_many(self, collection_name: str, query: Dict, update: Dict):
        """Actualizar múltiples documentos"""
        try:
            with tracer.start_as_current_span("mongodb_update"):
                collection = self.db[collection_name]
                result = collection.update_many(query, {'$set': update})
                return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error updating documents: {str(e)}")
            return 0

    def close(self):
        """Cerrar conexión"""
        if self.client:
            self.client.close()

# ============================================
# CLIENTE S3
# ============================================

class S3Client:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{MINIO_ENDPOINT}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

    def list_parquet_files(self, prefix: str = "processed/") -> List[str]:
        """Listar archivos Parquet en Silver"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=MINIO_BUCKET_SILVER,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        except Exception as e:
            logger.error(f"Error listing S3 objects: {str(e)}")
            return []

    def read_parquet_file(self, key: str) -> Optional[Dict]:
        """Leer archivo Parquet"""
        try:
            import pyarrow.parquet as pq
            
            obj = self.s3_client.get_object(
                Bucket=MINIO_BUCKET_SILVER,
                Key=key
            )
            
            parquet_file = pq.read_table(obj['Body'])
            return parquet_file.to_pandas().to_dict('records')
        except Exception as e:
            logger.error(f"Error reading parquet file: {str(e)}")
            return None

# ============================================
# GOLD TRANSFORMER
# ============================================

class GoldTransformer:
    def __init__(self):
        self.mongodb_client = MongoDBClient()
        self.s3_client = S3Client()
        self.last_processed_time = datetime.utcnow()

    def generate_traffic_metrics(self, records: List[Dict]) -> Dict[str, Any]:
        """Generar métricas de tráfico"""
        if not records:
            return {}
        
        speeds = [r.get('average_speed', 0) for r in records if 'average_speed' in r]
        densities = [r.get('traffic_density', 0) for r in records if 'traffic_density' in r]
        
        return {
            'avg_speed': float(np.mean(speeds)) if speeds else 0,
            'max_speed': float(np.max(speeds)) if speeds else 0,
            'min_speed': float(np.min(speeds)) if speeds else 0,
            'avg_density': float(np.mean(densities)) if densities else 0,
            'max_density': float(np.max(densities)) if densities else 0,
            'records_count': len(records)
        }

    def generate_air_quality_metrics(self, records: List[Dict]) -> Dict[str, Any]:
        """Generar métricas de calidad del aire"""
        if not records:
            return {}
        
        aqi_values = [r.get('air_quality_index', 0) for r in records if 'air_quality_index' in r]
        noise_levels = [r.get('noise_level', 0) for r in records if 'noise_level' in r]
        
        return {
            'avg_aqi': float(np.mean(aqi_values)) if aqi_values else 0,
            'max_aqi': float(np.max(aqi_values)) if aqi_values else 0,
            'min_aqi': float(np.min(aqi_values)) if aqi_values else 0,
            'avg_noise': float(np.mean(noise_levels)) if noise_levels else 0,
            'max_noise': float(np.max(noise_levels)) if noise_levels else 0,
        }

    def generate_hourly_aggregates(self, records: List[Dict]) -> List[Dict]:
        """Generar agregados por hora"""
        from collections import defaultdict
        
        aggregates = defaultdict(list)
        
        for record in records:
            key = (
                record.get('year'),
                record.get('month'),
                record.get('day'),
                record.get('hour'),
                record.get('sensor_type')
            )
            aggregates[key].append(record)
        
        result = []
        for (year, month, day, hour, sensor_type), group_records in aggregates.items():
            if sensor_type == 'traffic':
                metrics = self.generate_traffic_metrics(group_records)
            elif sensor_type == 'air_quality':
                metrics = self.generate_air_quality_metrics(group_records)
            else:
                metrics = {'records_count': len(group_records)}
            
            result.append({
                'timestamp': datetime(year, month, day, hour),
                'year': year,
                'month': month,
                'day': day,
                'hour': hour,
                'sensor_type': sensor_type,
                'metrics': metrics,
                'record_count': len(group_records)
            })
        
        return result

    def generate_incident_reports(self, records: List[Dict]) -> List[Dict]:
        """Generar reportes de incidentes"""
        incidents = [r for r in records if r.get('incident_detected', False)]
        
        return [
            {
                'timestamp': r.get('timestamp'),
                'sensor_id': r.get('sensor_id'),
                'sensor_type': r.get('sensor_type'),
                'incident_type': r.get('incident_type'),
                'location': {
                    'latitude': r.get('latitude'),
                    'longitude': r.get('longitude')
                },
                'intersection': r.get('intersection')
            }
            for r in incidents
        ]

    def transform_and_load(self):
        """Transformar datos de Silver a Gold"""
        transformation_runs.add(1)
        start_time = time.time()
        
        try:
            with tracer.start_as_current_span("transform_to_gold"):
                logger.info("Starting Gold transformation")
                
                # Simular lectura desde archivos Parquet
                # En producción, se leerían archivos reales
                # Por ahora generamos datos de ejemplo
                
                sample_records = self._generate_sample_records()
                
                if not sample_records:
                    logger.warning("No records to transform")
                    return
                
                # Generar agregados por hora
                hourly_aggregates = self.generate_hourly_aggregates(sample_records)
                if hourly_aggregates:
                    count = self.mongodb_client.insert_many('hourly_metrics', hourly_aggregates)
                    documents_created.add(count)
                    logger.info(f"Inserted {count} hourly metrics")
                
                # Generar reportes de incidentes
                incident_reports = self.generate_incident_reports(sample_records)
                if incident_reports:
                    count = self.mongodb_client.insert_many('incident_reports', incident_reports)
                    documents_created.add(count)
                    logger.info(f"Inserted {count} incident reports")
                
                elapsed = time.time() - start_time
                transformation_duration.record(elapsed)
                statsd_client.histogram('transformation.duration_seconds', elapsed)
                
                logger.info(f"Gold transformation completed in {elapsed:.2f} seconds")
                
        except Exception as e:
            logger.error(f"Critical error in transformation: {str(e)}")
            statsd_client.increment('transformation.failed')

    def _generate_sample_records(self) -> List[Dict]:
        """Generar registros de ejemplo (simular lectura de Parquet)"""
        import random
        from datetime import datetime
        
        now = datetime.utcnow()
        records = []
        
        for i in range(100):
            records.append({
                'sensor_id': f'SENSOR_{random.randint(1, 100):04d}',
                'sensor_type': random.choice(['traffic', 'air_quality', 'noise']),
                'year': now.year,
                'month': now.month,
                'day': now.day,
                'hour': now.hour,
                'latitude': -12.0462 + (random.random() - 0.5) * 0.1,
                'longitude': -77.0371 + (random.random() - 0.5) * 0.1,
                'average_speed': random.uniform(20, 80),
                'traffic_density': random.uniform(0, 100),
                'air_quality_index': random.uniform(0, 300),
                'noise_level': random.uniform(40, 90),
                'incident_detected': random.random() > 0.95,
                'incident_type': random.choice(['accident', 'congestion', 'breakdown', 'none']),
                'intersection': f'INT_{random.randint(1, 50):04d}',
                'timestamp': datetime(now.year, now.month, now.day, now.hour)
            })
        
        return records

    def run_scheduler(self):
        """Ejecutar scheduler de transformación"""
        while True:
            try:
                self.transform_and_load()
            except Exception as e:
                logger.error(f"Error in transformation scheduler: {str(e)}")
            
            time.sleep(600)  # 10 minutos

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    transformer = GoldTransformer()
    try:
        transformer.run_scheduler()
    finally:
        transformer.mongodb_client.close()
