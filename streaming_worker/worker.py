import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import boto3
from kafka import KafkaConsumer
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# OBSERVABILIDAD
# ============================================

meter, tracer, _ = configure_observability("streaming-worker")

statsd_client = _build_statsd_client('streaming_worker')

# ============================================
# MÉTRICAS
# ============================================

messages_consumed = meter.create_counter(
    name="messages_consumed",
    description="Total messages consumed",
    unit="1"
)

messages_stored = meter.create_counter(
    name="messages_stored",
    description="Total messages stored in bronze",
    unit="1"
)

consumer_lag = meter.create_gauge(
    name="consumer_lag",
    description="Consumer lag",
    unit="1"
)

processing_latency = meter.create_histogram(
    name="processing_latency_ms",
    description="Message processing latency",
    unit="ms"
)

pipeline_success_rate = meter.create_gauge(
    name="pipeline.success_rate_pct",
    description="Percentage of events successfully processed per batch",
    unit="%"
)

pipeline_throughput = meter.create_gauge(
    name="pipeline.throughput.events_per_min",
    description="Processing throughput normalized to events per minute",
    unit="events/min"
)

recovery_time = meter.create_histogram(
    name="service.recovery_time_ms",
    description="Time to recover service after a connection failure",
    unit="ms"
)

# ============================================
# CONFIGURACIÓN
# ============================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'raw_events')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'bronze_workers')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'bronze')

# ============================================
# CLIENTE MINIO
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
        self.bucket = MINIO_BUCKET
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Crear bucket si no existe"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket {self.bucket} exists")
        except Exception:
            logger.info(f"Creating bucket {self.bucket}")
            self.s3_client.create_bucket(Bucket=self.bucket)

    def store_event(self, event: dict, timestamp: str) -> bool:
        """Almacenar evento en MinIO"""
        try:
            with tracer.start_as_current_span("minio_store"):
                date_partition = timestamp.split('T')[0]
                hour_partition = timestamp.split('T')[1].split(':')[0]
                key = f"raw/date={date_partition}/hour={hour_partition}/{int(time.time() * 1000)}.json"
                
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=json.dumps(event, default=str).encode('utf-8'),
                    ContentType='application/json'
                )
                
                messages_stored.add(1)
                statsd_client.increment('messages.stored')
                return True
        except Exception as e:
            logger.error(f"Error storing event in MinIO: {str(e)}")
            statsd_client.increment('messages.store_failed')
            return False

# ============================================
# STREAMING WORKER
# ============================================

class StreamingWorker:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.minio_client = MinIOClient()
        self.failure_start_time: Optional[float] = None

    def ensure_topic(self):
        """Asegurar que el topic exista y esté limpio para la ingesta de Bronze."""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], client_id='worker-admin')
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
        retry_count = 0
        max_retries = 5
        
        if self.failure_start_time is None:
            self.failure_start_time = time.time()

        while retry_count < max_retries:
            try:
                self.ensure_topic()
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                    group_id=KAFKA_GROUP_ID,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    max_poll_records=100,
                    session_timeout_ms=30000,
                    request_timeout_ms=40000
                )
                logger.info(f"Connected to Kafka topic {KAFKA_TOPIC}")
                statsd_client.gauge('kafka_connection_attempts', retry_count)

                if self.failure_start_time is not None:
                    recovery_ms = (time.time() - self.failure_start_time) * 1000
                    recovery_time.record(recovery_ms, attributes={'service': 'streaming_worker'})
                    try:
                        statsd_client.histogram('service.recovery_time_ms', recovery_ms, tags=['service:streaming_worker'])
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

    def process_messages(self):
        """Procesar mensajes de Kafka"""
        if not self.connect_kafka():
            logger.error("Failed to connect to Kafka")
            return

        logger.info("Starting to consume messages")
        batch_count = 0

        try:
            while True:
                with tracer.start_as_current_span("consume_batch"):
                    batch_start_time = time.time()
                    messages = self.consumer.poll(timeout_ms=1000, max_records=100)
                    
                    if not messages:
                        continue

                    total_in_batch = 0
                    store_failed = 0

                    for topic_partition, records in messages.items():
                        with tracer.start_as_current_span("process_partition"):
                            for record in records:
                                total_in_batch += 1
                                start_time = time.time()
                                
                                try:
                                    event = record.value
                                    timestamp = event.get('timestamp', datetime.utcnow().isoformat())
                                    
                                    if self.minio_client.store_event(event, timestamp):
                                        messages_consumed.add(1)
                                        statsd_client.increment('messages.consumed')
                                    else:
                                        store_failed += 1
                                    
                                    elapsed = (time.time() - start_time) * 1000
                                    processing_latency.record(elapsed)
                                    
                                except Exception as e:
                                    logger.error(f"Error processing message: {str(e)}")
                                    statsd_client.increment('messages.processing_failed')
                                    store_failed += 1

                    # --- NEW: success rate % ---
                    if total_in_batch > 0:
                        success_pct = ((total_in_batch - store_failed) / total_in_batch) * 100
                        pipeline_success_rate.set(success_pct)
                        try:
                            statsd_client.gauge('messages.success_rate_pct', success_pct)
                        except Exception:
                            pass

                    # --- NEW: throughput events/min ---
                    batch_elapsed_seconds = time.time() - batch_start_time
                    if batch_elapsed_seconds > 0:
                        throughput_val = (total_in_batch / batch_elapsed_seconds) * 60
                        pipeline_throughput.set(throughput_val)
                        try:
                            statsd_client.gauge('pipeline.throughput.events_per_min', throughput_val)
                        except Exception:
                            pass

                    batch_count += 1
                    if batch_count % 10 == 0:
                        logger.info(f"Processed {batch_count} batches")

        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
        except Exception as e:
            logger.error(f"Critical error in worker: {str(e)}")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Kafka connection closed")

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    worker = StreamingWorker()
    worker.process_messages()
