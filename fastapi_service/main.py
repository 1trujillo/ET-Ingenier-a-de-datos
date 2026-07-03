import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from pymongo.errors import PyMongoError
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

# ============================================
# CONFIGURACIÓN
# ============================================

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/data_platform?authSource=admin')

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
# CLIENTE MONGODB
# ============================================

class MongoDBConnection:
    def __init__(self):
        self.client = None
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
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"MongoDB connection attempt {retry_count}/{max_retries} failed: {str(e)}")
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    raise

    def close(self):
        if self.client:
            self.client.close()

# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="Data Platform API",
    description="Gold Layer API - Read-only access to processed data",
    version="1.0.0"
)

# Variable global para la conexión
db_connection: Optional[MongoDBConnection] = None

FastAPIInstrumentor.instrument_app(app)

@app.on_event("startup")
async def startup_event():
    """Inicializar conexión a MongoDB"""
    global db_connection
    try:
        db_connection = MongoDBConnection()
        logger.info("API started successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MongoDB connection: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cerrar conexión a MongoDB"""
    global db_connection
    if db_connection:
        db_connection.close()
        logger.info("API shutdown successfully")

# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    start_time = time.time()
    
    try:
        with tracer.start_as_current_span("health_check"):
            requests_total.add(1)
            
            db_connection.client.admin.command('ping')
            
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            
            return {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "service": "data-platform-api"
            }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/readiness")
async def readiness_check():
    """Readiness check endpoint"""
    try:
        db_connection.client.admin.command('ping')
        return {"status": "ready"}
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
    """Obtener métricas por hora"""
    start_time = time.time()
    
    try:
        with tracer.start_as_current_span("get_hourly_metrics"):
            requests_total.add(1)
            
            collection = db_connection.db['hourly_metrics']
            
            query = {}
            if sensor_type:
                query['sensor_type'] = sensor_type.lower()
            
            # Últimas N horas
            since = datetime.utcnow() - timedelta(hours=hours)
            query['timestamp'] = {'$gte': since}
            
            results = list(collection.find(query).limit(limit).sort('timestamp', -1))
            
            # Convertir ObjectId a string
            for doc in results:
                doc.pop('_id', None)
            
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
    """Obtener reportes de incidentes"""
    start_time = time.time()
    
    try:
        with tracer.start_as_current_span("get_incidents"):
            requests_total.add(1)
            
            collection = db_connection.db['incident_reports']
            
            query = {}
            if incident_type:
                query['incident_type'] = incident_type.lower()
            
            # Últimas N horas
            since = datetime.utcnow() - timedelta(hours=hours)
            if 'timestamp' in query:
                query['timestamp'] = {'$gte': since}
            else:
                query['timestamp'] = {'$gte': since}
            
            results = list(collection.find(query).limit(limit).sort('timestamp', -1))
            
            for doc in results:
                doc.pop('_id', None)
            
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            statsd_client.histogram('endpoint.incidents.duration_ms', elapsed)
            
            return results
    except Exception as e:
        logger.error(f"Error fetching incidents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ENDPOINTS - AGGREGATIONS
# ============================================

@app.get("/api/v1/aggregations/by-sensor-type")
async def get_aggregations_by_sensor_type(hours: int = Query(24, ge=1, le=720)):
    """Obtener agregaciones por tipo de sensor"""
    start_time = time.time()
    
    try:
        with tracer.start_as_current_span("get_aggregations"):
            requests_total.add(1)
            
            collection = db_connection.db['hourly_metrics']
            
            pipeline = [
                {
                    '$match': {
                        'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=hours)}
                    }
                },
                {
                    '$group': {
                        '_id': '$sensor_type',
                        'avg_records': {'$avg': '$record_count'},
                        'total_records': {'$sum': '$record_count'},
                        'count': {'$sum': 1}
                    }
                },
                {
                    '$sort': {'total_records': -1}
                }
            ]
            
            results = list(collection.aggregate(pipeline))
            
            for doc in results:
                doc.pop('_id', None)
            
            elapsed = (time.time() - start_time) * 1000
            request_duration.record(elapsed)
            
            return {'aggregations': results}
    except Exception as e:
        logger.error(f"Error fetching aggregations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ENDPOINTS - STATS
# ============================================

@app.get("/api/v1/stats/database")
async def get_database_stats():
    """Obtener estadísticas de la base de datos"""
    start_time = time.time()
    
    try:
        with tracer.start_as_current_span("get_db_stats"):
            requests_total.add(1)
            
            stats = {
                'hourly_metrics_count': db_connection.db['hourly_metrics'].count_documents({}),
                'incidents_count': db_connection.db['incident_reports'].count_documents({}),
                'timestamp': datetime.utcnow().isoformat()
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
