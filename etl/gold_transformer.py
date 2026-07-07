import json
import logging
import os
import io
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from collections import defaultdict

import boto3
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
MINIO_BUCKET_GOLD = os.getenv('MINIO_BUCKET_GOLD', 'gold')

# ============================================
# CLIENTE S3 (MinIO)
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
        self._ensure_bucket_exists(MINIO_BUCKET_GOLD)

    def _ensure_bucket_exists(self, bucket: str):
        """Crear bucket si no existe"""
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except Exception:
            self.s3_client.create_bucket(Bucket=bucket)

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

    def read_parquet_file(self, key: str) -> Optional[List[Dict]]:
        """Leer archivo Parquet desde MinIO"""
        try:
            import pyarrow.parquet as pq

            obj = self.s3_client.get_object(
                Bucket=MINIO_BUCKET_SILVER,
                Key=key
            )

            # Envolver en BytesIO para que PyArrow pueda realizar búsquedas (seek)
            buffer = io.BytesIO(obj['Body'].read())
            parquet_file = pq.read_table(buffer)
            return parquet_file.to_pandas().to_dict('records')
        except Exception as e:
            logger.error(f"Error reading parquet file {key}: {str(e)}")
            return None

    def write_json(self, bucket: str, key: str, data: List[Dict]):
        """Escribir datos JSON a MinIO"""
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data, default=str),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            logger.error(f"Error writing JSON to MinIO: {str(e)}")
            return False

    def read_json(self, bucket: str, prefix: str) -> List[Dict]:
        """Leer datos JSON desde MinIO"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                return []

            all_records = []
            for obj in response['Contents']:
                if not obj['Key'].endswith('.json'):
                    continue
                obj_response = self.s3_client.get_object(
                    Bucket=bucket,
                    Key=obj['Key']
                )
                content = json.loads(obj_response['Body'].read().decode('utf-8'))
                if isinstance(content, list):
                    all_records.extend(content)
                else:
                    all_records.append(content)

            return all_records
        except Exception as e:
            logger.error(f"Error reading JSON from MinIO: {str(e)}")
            return []

# ============================================
# GOLD TRANSFORMER
# ============================================

class GoldTransformer:
    def __init__(self):
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

    def generate_traffic_aggregates(self, records: List[Dict]) -> Dict[str, Any]:
        """Generar agregados de movilidad por minuto, avenida y comuna."""
        if not records:
            return {}

        minute_bucket = defaultdict(int)
        avenue_bucket = defaultdict(int)
        district_bucket = defaultdict(int)

        for record in records:
            timestamp = record.get('timestamp')
            if isinstance(timestamp, datetime):
                minute_key = timestamp.strftime('%Y-%m-%dT%H:%M')
            else:
                minute_key = str(timestamp)[:16] if timestamp else 'unknown'

            vehicle_count = int(record.get('vehicle_count', 0) or 0)
            avenue = record.get('avenue', 'unknown')
            district = record.get('district', 'unknown')

            minute_bucket[minute_key] += vehicle_count
            avenue_bucket[avenue] += vehicle_count
            district_bucket[district] += vehicle_count

        return {
            'by_minute': [
                {'timestamp': ts, 'total_vehicles': total}
                for ts, total in sorted(minute_bucket.items())
            ],
            'by_avenue': [
                {'avenue': avenue, 'total_vehicles': total}
                for avenue, total in sorted(avenue_bucket.items())
            ],
            'by_district': [
                {'district': district, 'total_vehicles': total}
                for district, total in sorted(district_bucket.items())
            ]
        }

    def _extract_record_datetime(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extraer un datetime válido desde un registro de Silver."""
        timestamp = record.get('timestamp')
        if isinstance(timestamp, datetime):
            return timestamp

        if isinstance(timestamp, str):
            try:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except Exception:
                pass

        try:
            year = record.get('year')
            month = record.get('month')
            day = record.get('day')
            hour = record.get('hour')
            if all(value is not None for value in (year, month, day, hour)):
                return datetime(int(year), int(month), int(day), int(hour))
        except Exception:
            pass

        return None

    def generate_hourly_aggregates(self, records: List[Dict]) -> List[Dict]:
        """Generar agregados por hora"""
        aggregates = defaultdict(list)

        for record in records:
            timestamp = self._extract_record_datetime(record)
            if timestamp is None:
                continue

            key = (
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                str(record.get('sensor_type') or 'unknown').lower()
            )
            aggregates[key].append(record)

        result = []
        for (year, month, day, hour, sensor_type), group_records in aggregates.items():
            if sensor_type == 'traffic':
                metrics_data = self.generate_traffic_metrics(group_records)
            elif sensor_type == 'air_quality':
                metrics_data = self.generate_air_quality_metrics(group_records)
            else:
                metrics_data = {'records_count': len(group_records)}

            result.append({
                'timestamp': datetime(year, month, day, hour).isoformat(),
                'year': year,
                'month': month,
                'day': day,
                'hour': hour,
                'sensor_type': sensor_type,
                'metrics': metrics_data,
                'record_count': len(group_records)
            })

        return result

    def generate_incident_reports(self, records: List[Dict]) -> List[Dict]:
        """Generar reportes de incidentes"""
        incidents = [r for r in records if r.get('incident_detected', False)]

        return [
            {
                'timestamp': r.get('timestamp').isoformat() if hasattr(r.get('timestamp'), 'isoformat') else r.get('timestamp'),
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
        """Transformar datos de Silver a Gold y almacenar en MinIO"""
        transformation_runs.add(1)
        start_time = time.time()

        try:
            with tracer.start_as_current_span("transform_to_gold"):
                logger.info("Starting Gold transformation")

                # 1. Listar los archivos Parquet reales desde el bucket Silver en MinIO
                parquet_files = self.s3_client.list_parquet_files("processed/")
                
                if not parquet_files:
                    logger.warning("No se encontraron archivos Parquet en la capa Silver (s3://silver/processed/)")
                    return

                # 2. Leer y consolidar los registros de todos los archivos encontrados
                silver_records = []
                for file_key in parquet_files:
                    records = self.s3_client.read_parquet_file(file_key)
                    if records:
                        silver_records.extend(records)

                if not silver_records:
                    logger.warning("Los archivos Parquet se leyeron pero no contienen registros válidos")
                    return

                logger.info(f"Se cargaron {len(silver_records)} registros reales desde la capa Silver para transformar")

                now = datetime.utcnow()
                date_str = now.strftime("%Y-%m-%d")

                # 3. Generar agregados por hora
                hourly_aggregates = self.generate_hourly_aggregates(silver_records)
                if hourly_aggregates:
                    key = f"hourly_metrics/date={date_str}/{int(time.time() * 1000)}.json"
                    if self.s3_client.write_json(MINIO_BUCKET_GOLD, key, hourly_aggregates):
                        documents_created.add(len(hourly_aggregates))
                        logger.info(f"Wrote {len(hourly_aggregates)} hourly metrics to gold/{key}")
                        statsd_client.increment('documents.inserted', len(hourly_aggregates))

                traffic_aggregates = self.generate_traffic_aggregates(silver_records)
                if traffic_aggregates:
                    key = f"traffic_metrics/date={date_str}/{int(time.time() * 1000)}.json"
                    if self.s3_client.write_json(MINIO_BUCKET_GOLD, key, [traffic_aggregates]):
                        documents_created.add(1)
                        logger.info(f"Wrote traffic aggregates to gold/{key}")
                        statsd_client.increment('documents.inserted', 1)

                # 4. Generar reportes de incidentes
                incident_reports = self.generate_incident_reports(silver_records)
                if incident_reports:
                    key = f"incident_reports/date={date_str}/{int(time.time() * 1000)}.json"
                    if self.s3_client.write_json(MINIO_BUCKET_GOLD, key, incident_reports):
                        documents_created.add(len(incident_reports))
                        logger.info(f"Wrote {len(incident_reports)} incident reports to gold/{key}")
                        statsd_client.increment('documents.inserted', len(incident_reports))

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
    transformer.run_scheduler()