import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import boto3
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

try:
    from statsd import StatsClient as _StatsClient
except Exception:
    try:
        from statsd.client import StatsClient as _StatsClient
    except Exception:
        _StatsClient = None

import uvicorn

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

meter, tracer, _ = configure_observability("fastapi")

RequestsInstrumentor().instrument()

statsd_client = _build_statsd_client('fastapi')

# ============================================
# MÉTRICAS
# ============================================

requests_total = meter.create_counter(
    name="requests_total",
    description="Total requests",
    unit="1"
)

request_duration = meter.create_histogram(
    name="request_duration_ms",
    description="Request duration",
    unit="ms"
)

recovery_time = meter.create_histogram(
    name="service.recovery_time_ms",
    description="Time to recover service after a connection failure",
    unit="ms"
)

# ============================================
# CONFIGURACIÓN
# ============================================

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET_GOLD = os.getenv('MINIO_BUCKET_GOLD', 'gold')

# ============================================
# MODELOS
# ============================================

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str

class MetricValue(BaseModel):
    avg_speed: Optional[float] = None
    max_speed: Optional[float] = None
    min_speed: Optional[float] = None
    avg_density: Optional[float] = None
    max_density: Optional[float] = None
    avg_aqi: Optional[float] = None
    max_aqi: Optional[float] = None
    records_count: Optional[int] = None

class HourlyMetric(BaseModel):
    timestamp: str
    year: int
    month: int
    day: int
    hour: int
    sensor_type: str
    metrics: MetricValue
    record_count: int

class Location(BaseModel):
    latitude: float
    longitude: float

class IncidentReport(BaseModel):
    timestamp: str
    sensor_id: str
    sensor_type: str
    incident_type: str
    location: Location
    intersection: str

# ============================================
# CLIENTE MINIO (Gold Layer)
# ============================================

class MinIOClient:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{MINIO_ENDPOINT}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )
        self.bucket = MINIO_BUCKET_GOLD

    def read_all_json(self, prefix: str) -> List[Dict]:
        """Leer todos los archivos JSON de un prefijo en MinIO"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                return []

            all_records = []
            for obj in response['Contents']:
                if not obj['Key'].endswith('.json'):
                    continue
                obj_response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=obj['Key']
                )
                content = json.loads(obj_response['Body'].read().decode('utf-8'))
                if isinstance(content, list):
                    all_records.extend(content)
                elif isinstance(content, dict):
                    all_records.append(content)

            return all_records
        except Exception as e:
            logger.error(f"Error reading from MinIO: {str(e)}")
            return []

    def health_check(self) -> bool:
        """Verificar conectividad con MinIO"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            return True
        except Exception:
            return False

# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="Data Platform API",
    description="Gold Layer API - Read-only access to processed data from MinIO",
    version="2.0.0"
)

# Variable global para el cliente MinIO
minio_client: Optional[MinIOClient] = None
failure_start_time: Optional[float] = None

FastAPIInstrumentor.instrument_app(app)

@app.on_event("startup")
async def startup_event():
    """Inicializar conexión a MinIO"""
    global minio_client, failure_start_time
    if failure_start_time is None:
        failure_start_time = time.time()

    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            minio_client = MinIOClient()
            logger.info("API started successfully - connected to MinIO gold layer")

            if failure_start_time is not None:
                recovery_ms = (time.time() - failure_start_time) * 1000
                recovery_time.record(recovery_ms, attributes={'service': 'fastapi'})
                try:
                    statsd_client.histogram('service.recovery_time_ms', recovery_ms, tags=['service:fastapi'])
                except TypeError:
                    statsd_client.histogram('service.recovery_time_ms', recovery_ms)
                failure_start_time = None
                logger.info(f"FastAPI recovered after {recovery_ms:.2f} ms")
            return

        except Exception as e:
            retry_count += 1
            logger.warning(f"MinIO connection attempt {retry_count}/{max_retries} failed: {str(e)}")
            if retry_count >= max_retries:
                logger.error(f"Failed to initialize MinIO client after {max_retries} attempts")
                raise
            time.sleep(3)

@app.on_event("shutdown")
async def shutdown_event():
    """Cerrar cliente (sin efecto real para S3, pero mantenemos la estructura)"""
    global minio_client
    logger.info("API shutdown successfully")

# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    global failure_start_time
    start_time = time.time()

    try:
        with tracer.start_as_current_span("health_check"):
            requests_total.add(1)

            if not minio_client.health_check():
                if failure_start_time is None:
                    failure_start_time = time.time()
                raise HTTPException(status_code=503, detail="MinIO not available")

            if failure_start_time is not None:
                recovery_ms = (time.time() - failure_start_time) * 1000
                recovery_time.record(recovery_ms, attributes={'service': 'fastapi'})
                try:
                    statsd_client.histogram('service.recovery_time_ms', recovery_ms, tags=['service:fastapi'])
                except TypeError:
                    statsd_client.histogram('service.recovery_time_ms', recovery_ms)
                failure_start_time = None
                logger.info(f"FastAPI recovered after {recovery_ms:.2f} ms")

            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)

            return {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "service": "data-platform-api"
            }
    except Exception as e:
        if failure_start_time is None:
            failure_start_time = time.time()
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/readiness")
async def readiness_check():
    """Readiness check endpoint"""
    try:
        if minio_client.health_check():
            return {"status": "ready"}
        raise Exception("MinIO not ready")
    except Exception:
        raise HTTPException(status_code=503, detail="Not ready")

# ============================================
# ENDPOINTS - HOURLY METRICS
# ============================================

@app.get("/api/v1/metrics/hourly", response_model=List[HourlyMetric])
async def get_hourly_metrics(
    sensor_type: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(1000, ge=1, le=10000)
):
    """Obtener métricas por hora desde MinIO Gold"""
    start_time = time.time()

    try:
        with tracer.start_as_current_span("get_hourly_metrics"):
            requests_total.add(1)

            # Leer todos los datos de hourly_metrics desde MinIO
            all_records = minio_client.read_all_json("hourly_metrics/")

            # Filtrar por tipo de sensor
            if sensor_type:
                all_records = [r for r in all_records if r.get('sensor_type', '').lower() == sensor_type.lower()]

            # Filtrar por las últimas N horas
            since = datetime.utcnow() - timedelta(hours=hours)
            filtered = []
            for r in all_records:
                try:
                    ts = datetime.fromisoformat(r['timestamp'])
                    if ts >= since:
                        filtered.append(r)
                except (KeyError, ValueError):
                    continue

            # Ordenar por timestamp descendente y limitar
            filtered.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            results = filtered[:limit]

            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.hourly_metrics.duration_ms', elapsed)

            return results
    except Exception as e:
        logger.error(f"Error fetching hourly metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ENDPOINTS - INCIDENT REPORTS
# ============================================

@app.get("/api/v1/incidents", response_model=List[IncidentReport])
async def get_incidents(
    incident_type: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(1000, ge=1, le=10000)
):
    """Obtener reportes de incidentes desde MinIO Gold"""
    start_time = time.time()

    try:
        with tracer.start_as_current_span("api.get_incidents.query"):
            requests_total.add(1)

            # Leer todos los datos de incident_reports desde MinIO
            all_records = minio_client.read_all_json("incident_reports/")

            # Filtrar por tipo de incidente
            if incident_type:
                all_records = [r for r in all_records if r.get('incident_type', '').lower() == incident_type.lower()]

            # Filtrar por las últimas N horas
            since = datetime.utcnow() - timedelta(hours=hours)
            filtered = []
            for r in all_records:
                try:
                    ts = datetime.fromisoformat(r['timestamp'])
                    if ts >= since:
                        filtered.append(r)
                except (KeyError, ValueError):
                    continue

            # Ordenar por timestamp descendente y limitar
            filtered.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            results = filtered[:limit]

            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.incidents.duration_ms', elapsed)
            statsd_client.histogram('api.incidents.query_latency_ms', elapsed)

            statsd_client.gauge('api.incidents.query_count', len(results))

            return results
    except Exception as e:
        logger.error(f"Error fetching incidents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ENDPOINTS - AGGREGATIONS
# ============================================

@app.get("/api/v1/aggregations/by-sensor-type")
async def get_aggregations_by_sensor_type(hours: int = Query(24, ge=1, le=720)):
    """Obtener agregaciones por tipo de sensor desde MinIO Gold"""
    start_time = time.time()

    try:
        with tracer.start_as_current_span("get_aggregations"):
            requests_total.add(1)

            # Leer todos los datos de hourly_metrics
            all_records = minio_client.read_all_json("hourly_metrics/")

            # Filtrar por las últimas N horas
            since = datetime.utcnow() - timedelta(hours=hours)
            filtered = []
            for r in all_records:
                try:
                    ts = datetime.fromisoformat(r['timestamp'])
                    if ts >= since:
                        filtered.append(r)
                except (KeyError, ValueError):
                    continue

            # Agrupar por tipo de sensor
            groups: Dict[str, List[Dict]] = {}
            for r in filtered:
                st = r.get('sensor_type', 'unknown')
                if st not in groups:
                    groups[st] = []
                groups[st].append(r)

            # Calcular agregaciones
            aggregations = []
            for sensor_type, records in groups.items():
                total_records = sum(r.get('record_count', 0) for r in records)
                avg_records = total_records / len(records) if records else 0

                aggregations.append({
                    'sensor_type': sensor_type,
                    'avg_records': round(avg_records, 2),
                    'total_records': total_records,
                    'count': len(records)
                })

            # Ordenar por total_records descendente
            aggregations.sort(key=lambda x: x['total_records'], reverse=True)

            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)

            return {'aggregations': aggregations}
    except Exception as e:
        logger.error(f"Error fetching aggregations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ENDPOINTS - STATS
# ============================================

@app.get("/api/v1/traffic/by-minute")
async def get_traffic_by_minute(hours: int = Query(24, ge=1, le=720)):
    """Obtener vehículos por minuto desde Gold."""
    start_time = time.time()
    try:
        with tracer.start_as_current_span("get_traffic_by_minute"):
            requests_total.add(1)
            records = minio_client.read_all_json("traffic_metrics/")
            filtered = []
            for item in records:
                if not isinstance(item, dict):
                    continue
                payload = item.get('by_minute', [])
                if isinstance(payload, list):
                    filtered.extend(payload)
            since = datetime.utcnow() - timedelta(hours=hours)
            filtered = [
                r for r in filtered
                if isinstance(r.get('timestamp'), str) and datetime.fromisoformat(r['timestamp'].replace('Z', '+00:00')) >= since
            ]
            filtered.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.traffic_by_minute.duration_ms', elapsed)
            return filtered
    except Exception as e:
        logger.error(f"Error fetching traffic by minute: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/traffic/by-avenue")
async def get_traffic_by_avenue(hours: int = Query(24, ge=1, le=720)):
    """Obtener vehículos por avenida desde Gold."""
    start_time = time.time()
    try:
        with tracer.start_as_current_span("get_traffic_by_avenue"):
            requests_total.add(1)
            records = minio_client.read_all_json("traffic_metrics/")
            filtered = []
            for item in records:
                if not isinstance(item, dict):
                    continue
                payload = item.get('by_avenue', [])
                if isinstance(payload, list):
                    filtered.extend(payload)
            filtered.sort(key=lambda x: x.get('total_vehicles', 0), reverse=True)
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.traffic_by_avenue.duration_ms', elapsed)
            return filtered
    except Exception as e:
        logger.error(f"Error fetching traffic by avenue: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/traffic/by-district")
async def get_traffic_by_district(hours: int = Query(24, ge=1, le=720)):
    """Obtener vehículos por comuna desde Gold."""
    start_time = time.time()
    try:
        with tracer.start_as_current_span("get_traffic_by_district"):
            requests_total.add(1)
            records = minio_client.read_all_json("traffic_metrics/")
            filtered = []
            for item in records:
                if not isinstance(item, dict):
                    continue
                payload = item.get('by_district', [])
                if isinstance(payload, list):
                    filtered.extend(payload)
            filtered.sort(key=lambda x: x.get('total_vehicles', 0), reverse=True)
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.traffic_by_district.duration_ms', elapsed)
            return filtered
    except Exception as e:
        logger.error(f"Error fetching traffic by district: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/stats/database")
async def get_database_stats():
    """Obtener estadísticas de la base de datos (Gold layer en MinIO)"""
    start_time = time.time()

    try:
        with tracer.start_as_current_span("get_db_stats"):
            requests_total.add(1)

            hourly_metrics = minio_client.read_all_json("hourly_metrics/")
            incidents = minio_client.read_all_json("incident_reports/")

            stats = {
                'hourly_metrics_count': len(hourly_metrics),
                'incidents_count': len(incidents),
                'timestamp': datetime.utcnow().isoformat(),
                'storage': 'minio_gold'
            }

            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)

            return stats
    except Exception as e:
        logger.error(f"Error fetching database stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )