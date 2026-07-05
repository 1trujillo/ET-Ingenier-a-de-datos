import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np
from faker import Faker
from kafka import KafkaProducer
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

# ============================================
# CONFIGURACIÓN
# ============================================

KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'raw_events'
NUM_SENSORS = 100
BATCH_SIZE = 50
INTERVAL_SECONDS = 2

# Coordenadas ficticias de la ciudad
CITY_CENTER_LAT = -12.0462
CITY_CENTER_LNG = -77.0371
LAT_RANGE = 0.1
LNG_RANGE = 0.1

SENSOR_TYPES = ['traffic', 'air_quality', 'noise', 'weather', 'incident']
LIGHT_STATUS = ['green', 'red', 'yellow']
INCIDENT_TYPES = ['accident', 'congestion', 'breakdown', 'road_closure', 'none']

# ============================================
# GENERADOR DE EVENTOS
# ============================================

class EventGenerator:
    def __init__(self):
        self.fake = Faker()
        self.producer = None
        self.sensor_ids = [f"SENSOR_{i:04d}" for i in range(1, NUM_SENSORS + 1)]
        self.intersections = [
            f"INT_{i:04d}" for i in range(1, NUM_SENSORS // 2 + 1)
        ]

    def connect_kafka(self):
        """Conectar a Kafka"""
        with tracer.start_as_current_span("kafka_connection"):
            retry_count = 0
            max_retries = 5
            while retry_count < max_retries:
                try:
                    self.producer = KafkaProducer(
                        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        acks='all',
                        compression_type='snappy',
                        request_timeout_ms=30000,
                        retries=3
                    )
                    logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
                    statsd_client.gauge('kafka_connection_attempts', retry_count)
                    return True
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Kafka connection attempt {retry_count}/{max_retries} failed: {str(e)}")
                    if retry_count < max_retries:
                        time.sleep(5)
                    else:
                        logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                        return False

    def generate_event(self, sensor_id: str) -> Dict[str, Any]:
        """Generar un evento simulado de sensor"""
        sensor_type = random.choice(SENSOR_TYPES)
        
        base_lat = CITY_CENTER_LAT + (random.random() - 0.5) * LAT_RANGE
        base_lng = CITY_CENTER_LNG + (random.random() - 0.5) * LNG_RANGE
        
        event = {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'intersection': random.choice(self.intersections),
            'latitude': round(base_lat, 6),
            'longitude': round(base_lng, 6),
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
            'timestamp': datetime.utcnow().isoformat() + 'Z'
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
