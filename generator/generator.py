import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
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
    format=json.dumps({
        'timestamp': '%(asctime)s',
        'level': '%(levelname)s',
        'service': 'data-generator',
        'message': '%(message)s'
    })
)
logger = logging.getLogger(__name__)

# ============================================
# OBSERVABILIDAD
# ============================================

meter, tracer, _ = configure_observability("data-generator")

# StatsD para métricas
statsd_client = _build_statsd_client('generator')

# ============================================
# MÉTRICAS
# ============================================

events_produced = meter.create_counter(
    name="events_produced",
    description="Total events produced",
    unit="1"
)

events_failed = meter.create_counter(
    name="events_failed",
    description="Total events failed",
    unit="1"
)

production_latency = meter.create_histogram(
    name="production_latency_ms",
    description="Event production latency",
    unit="ms"
)

traffic_vehicle_volume = meter.create_counter(
    name="movilidad.vehiculos_por_minuto",
    description="Vehicles observed per minute",
    unit="1"
)

traffic_vehicle_by_avenue = meter.create_counter(
    name="movilidad.vehiculos_por_avenida",
    description="Vehicles observed by avenue",
    unit="1"
)

traffic_vehicle_by_district = meter.create_counter(
    name="movilidad.vehiculos_por_comuna",
    description="Vehicles observed by district",
    unit="1"
)

accidents_by_district = meter.create_counter(
    name="movilidad.accidentes_por_comuna",
    description="Accident incidents by district",
    unit="1"
)

recovery_time = meter.create_histogram(
    name="service.recovery_time_ms",
    description="Time to recover service after a connection failure",
    unit="ms"
)

# ============================================
# CONFIGURACIÓN
# ============================================

KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'raw_events'
NUM_SENSORS = 100
BATCH_SIZE = 50
INTERVAL_SECONDS = 2

SENSOR_TYPES = ['traffic', 'air_quality', 'noise', 'weather', 'incident']
LIGHT_STATUS = ['green', 'red', 'yellow']
INCIDENT_TYPES = ['accident', 'congestion', 'breakdown', 'road_closure', 'none']
LOCATIONS = [
    {"intersection": "INT_001", "avenue": "Alameda", "district": "Santiago Centro", "latitude": -33.4378, "longitude": -70.6504},
    {"intersection": "INT_002", "avenue": "Alameda", "district": "Estación Central", "latitude": -33.4517, "longitude": -70.6765},
    {"intersection": "INT_003", "avenue": "Av. Providencia", "district": "Providencia", "latitude": -33.4372, "longitude": -70.6303},
    {"intersection": "INT_004", "avenue": "Av. Apoquindo", "district": "Las Condes", "latitude": -33.4218, "longitude": -70.6067},
    {"intersection": "INT_005", "avenue": "Av. Vitacura", "district": "Vitacura", "latitude": -33.3938, "longitude": -70.5885},
    {"intersection": "INT_006", "avenue": "Av. 10 de Julio", "district": "Lo Prado", "latitude": -33.4399, "longitude": -70.7151},
    {"intersection": "INT_007", "avenue": "Av. Recoleta", "district": "Recoleta", "latitude": -33.4210, "longitude": -70.6508},
    {"intersection": "INT_008", "avenue": "Av. Matta", "district": "Estación Central", "latitude": -33.4479, "longitude": -70.6753},
    {"intersection": "INT_009", "avenue": "Av. España", "district": "Santiago Centro", "latitude": -33.4410, "longitude": -70.6542},
    {"intersection": "INT_010", "avenue": "Av. Libertador B. O'Higgins", "district": "Santiago Centro", "latitude": -33.4480, "longitude": -70.6634},
    {"intersection": "INT_011", "avenue": "Av. Grecia", "district": "Ñuñoa", "latitude": -33.4591, "longitude": -70.6138},
    {"intersection": "INT_012", "avenue": "Av. Irarrázaval", "district": "Ñuñoa", "latitude": -33.4557, "longitude": -70.6121},
    {"intersection": "INT_013", "avenue": "Av. San Diego", "district": "Santiago Centro", "latitude": -33.4447, "longitude": -70.6510},
    {"intersection": "INT_014", "avenue": "Av. Santa Rosa", "district": "Las Condes", "latitude": -33.4130, "longitude": -70.5780},
    {"intersection": "INT_015", "avenue": "Av. Grecia", "district": "Providencia", "latitude": -33.4310, "longitude": -70.6205},
    {"intersection": "INT_016", "avenue": "Av. Ejército", "district": "Santiago Centro", "latitude": -33.4302, "longitude": -70.6507},
    {"intersection": "INT_017", "avenue": "Av. Pajaritos", "district": "Lo Espejo", "latitude": -33.5275, "longitude": -70.6965},
    {"intersection": "INT_018", "avenue": "Av. Vicuña Mackenna", "district": "Macul", "latitude": -33.4920, "longitude": -70.6196},
    {"intersection": "INT_019", "avenue": "Av. Principal", "district": "La Cisterna", "latitude": -33.5401, "longitude": -70.6649},
    {"intersection": "INT_020", "avenue": "Av. Circunvalación", "district": "San Miguel", "latitude": -33.4932, "longitude": -70.6498},
]

# ============================================
# GENERADOR DE EVENTOS
# ============================================

class EventGenerator:
    def __init__(self):
        self.producer = None
        self.sensor_ids = [f"SENSOR_{i:04d}" for i in range(1, NUM_SENSORS + 1)]
        self.failure_start_time = None

    def ensure_topic(self):
        """Crear o recrear el topic de Kafka para evitar mensajes antiguos con codec incompatibles."""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], client_id='generator-admin')
            topics = admin_client.list_topics()
            if KAFKA_TOPIC in topics:
                admin_client.delete_topics([KAFKA_TOPIC], timeout_ms=10000)
                time.sleep(2)
            admin_client.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)], timeout_ms=10000)
            admin_client.close()
            logger.info(f"Ensured Kafka topic {KAFKA_TOPIC} exists")
        except Exception as exc:
            logger.warning(f"Topic bootstrap skipped: {exc}")

    def connect_kafka(self):
        """Conectar a Kafka"""
        with tracer.start_as_current_span("kafka_connection"):
            retry_count = 0
            max_retries = 5

            if self.failure_start_time is None:
                self.failure_start_time = time.time()

            while retry_count < max_retries:
                try:
                    self.ensure_topic()
                    self.producer = KafkaProducer(
                        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        acks='all',
                        request_timeout_ms=30000,
                        retries=3
                    )
                    logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
                    statsd_client.gauge('kafka_connection_attempts', retry_count)

                    if self.failure_start_time is not None:
                        recovery_ms = (time.time() - self.failure_start_time) * 1000
                        recovery_time.record(recovery_ms, attributes={'service': 'generator'})
                        try:
                            statsd_client.histogram('service.recovery_time_ms', recovery_ms, tags=['service:generator'])
                        except TypeError:
                            statsd_client.histogram('service.recovery_time_ms', recovery_ms)
                        self.failure_start_time = None
                        logger.info(f"Service recovered after {recovery_ms:.2f} ms")

                    return True
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Kafka connection attempt {retry_count}/{max_retries} failed: {str(e)}")
                    if retry_count < max_retries:
                        time.sleep(5)
                    else:
                        logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                        return False

    def _emit_traffic_metrics(self, event: Dict[str, Any]):
        """Emitir métricas de movilidad a OTel y DogStatsD."""
        vehicle_count = int(event.get('vehicle_count', 0) or 0)
        avenue = event.get('avenue', 'unknown')
        district = event.get('district', 'unknown')

        traffic_vehicle_volume.add(vehicle_count)
        traffic_vehicle_by_avenue.add(vehicle_count, attributes={'avenue': avenue})
        traffic_vehicle_by_district.add(vehicle_count, attributes={'district': district})

        try:
            statsd_client.increment('movilidad.vehiculos_por_minuto', value=vehicle_count)
            statsd_client.increment(
                'movilidad.vehiculos_por_avenida',
                value=vehicle_count,
                tags=[f'avenue:{avenue}']
            )
            statsd_client.increment(
                'movilidad.vehiculos_por_comuna',
                value=vehicle_count,
                tags=[f'district:{district}']
            )
        except TypeError:
            statsd_client.increment('movilidad.vehiculos_por_avenida', value=vehicle_count)
            statsd_client.increment('movilidad.vehiculos_por_comuna', value=vehicle_count)

    def generate_event(self, sensor_id: str) -> Dict[str, Any]:
        """Generar un evento simulado de sensor con ubicación realista."""
        sensor_type = random.choice(SENSOR_TYPES)
        location = random.choice(LOCATIONS)

        event = {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'intersection': location['intersection'],
            'avenue': location['avenue'],
            'district': location['district'],
            'latitude': round(location['latitude'], 6),
            'longitude': round(location['longitude'], 6),
            'vehicle_count': max(0, int(np.random.poisson(25))),
            'average_speed': round(np.random.normal(40, 15), 2),
            'traffic_density': round(random.uniform(0, 100), 2),
            'traffic_light_status': random.choice(LIGHT_STATUS),
            'air_quality_index': round(random.uniform(0, 500), 2),
            'noise_level': round(random.uniform(40, 100), 2),
            'incident_detected': random.choice([True, False]),
            'incident_type': random.choice(INCIDENT_TYPES) if random.random() > 0.8 else 'none',
            'temperature': round(random.uniform(15, 35), 2),
            'humidity': round(random.uniform(30, 90), 2),
            'timestamp': datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        }

        return event

    def produce_events(self):
        """Producir eventos continuamente"""
        if not self.connect_kafka():
            logger.error("Failed to connect to Kafka")
            return

        logger.info(f"Starting to produce events from {NUM_SENSORS} sensors")
        logger.info(f"Batch size: {BATCH_SIZE}, Interval: {INTERVAL_SECONDS}s")

        try:
            while True:
                with tracer.start_as_current_span("batch_production"):
                    batch = []
                    for _ in range(BATCH_SIZE):
                        sensor_id = random.choice(self.sensor_ids)
                        event = self.generate_event(sensor_id)
                        batch.append(event)

                    start_time = time.time()
                    
                    for event in batch:
                        try:
                            with tracer.start_as_current_span("send_event"):
                                future = self.producer.send(
                                    KAFKA_TOPIC,
                                    value=event
                                )
                                # Esperar confirmación (bloqueante, pero necesario para garantía)
                                future.get(timeout=5)
                                
                                events_produced.add(1)
                                statsd_client.increment('events.produced')
                                self._emit_traffic_metrics(event)

                                # --- NEW: accidentes_por_comuna ---
                                incident_type = event.get('incident_type', 'none')
                                if incident_type == 'accident':
                                    district = event.get('district', 'unknown')
                                    accidents_by_district.add(1, attributes={'district': district})
                                    try:
                                        statsd_client.increment('movilidad.accidentes_por_comuna', tags=[f'district:{district}'])
                                    except TypeError:
                                        statsd_client.increment('movilidad.accidentes_por_comuna')
                                
                        except Exception as e:
                            logger.error(f"Error sending event: {str(e)}")
                            events_failed.add(1)
                            statsd_client.increment('events.failed')

                    elapsed = (time.time() - start_time) * 1000
                    production_latency.record(elapsed)
                    statsd_client.histogram('batch.latency_ms', elapsed)
                    statsd_client.gauge('batch.size', BATCH_SIZE)

                    logger.info(
                        f"Batch produced: {BATCH_SIZE} events in {elapsed:.2f}ms"
                    )

                    time.sleep(INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Generator stopped by user")
        except Exception as e:
            logger.error(f"Critical error in producer: {str(e)}")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            logger.info("Kafka connection closed")

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    generator = EventGenerator()
    generator.produce_events()
