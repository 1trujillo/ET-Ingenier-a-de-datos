# Observabilidad y Monitoreo

## Visión General

La plataforma está completamente instrumentada con OpenTelemetry y envía métricas a Datadog para observabilidad centralizada.

## Stack de Observabilidad

```
┌─────────────────────────────────────────────┐
│         Aplicaciones                        │
│  (Generator, Worker, ETL, Gold, FastAPI)   │
└────────────────┬────────────────────────────┘
                 │ OpenTelemetry
                 │ (Traces + Metrics)
                 │ DogStatsD (Metrics)
                 │ JSON Logs (Logs)
                 ↓
        ┌─────────────────┐
        │  Datadog Agent  │
        │  (Container)    │
        └────────┬────────┘
                 │
                 ↓
        ┌─────────────────┐
        │ Datadog Cloud   │
        │  (app.datadoghq │
        │  .com)          │
        └─────────────────┘
```

## 1. Instrumentación

### OpenTelemetry

**Qué envía**:
- Traces distribuidas
- Métricas (Counters, Histograms, Gauges)

**Dónde envía**:
- Datadog Agent (localhost:8126 para traces)

**Ejemplo**:
```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

def process_data():
    with tracer.start_as_current_span("process_batch"):
        # Tu código aquí
        pass
```

### DogStatsD

**Qué envía**:
- Métricas custom
- Timing
- Gauges

**Dónde envía**:
- Datadog Agent (localhost:8125)

**Ejemplo**:
```python
from statsd import StatsClient

statsd = StatsClient(host='datadog-agent', port=8125, prefix='myapp')

statsd.increment('events.processed')
statsd.histogram('latency_ms', 50)
statsd.gauge('queue_length', 100)
```

### JSON Logging

**Formato**:
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "service": "generator",
  "message": "Batch produced: 50 events in 25ms",
  "event_id": "evt_123456",
  "batch_size": 50,
  "latency_ms": 25
}
```

**Ventajas**:
- Parseable automáticamente
- Searchable en Datadog Logs
- Correlacionable con traces

## 2. Métricas por Componente

### Generator

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| events_produced | Counter | Total eventos generados |
| events_failed | Counter | Eventos que fallaron |
| production_latency_ms | Histogram | Latencia en ms |
| kafka_connection_attempts | Gauge | Intentos de conexión |

### Streaming Worker

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| messages_consumed | Counter | Mensajes de Kafka |
| messages_stored | Counter | Almacenados en Bronze |
| consumer_lag | Gauge | Lag del consumer |
| processing_latency_ms | Histogram | Latencia de procesamiento |

### ETL Spark

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| etl_runs | Counter | Ejecuciones de ETL |
| records_processed | Counter | Registros procesados |
| records_invalid | Counter | Registros inválidos |
| etl_duration_seconds | Histogram | Duración del ETL |
| silver_records_saved | Counter | Registros en Silver |

### Gold Transformer

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| transformation_runs | Counter | Ejecuciones |
| documents_created | Counter | Documentos en Gold |
| transformation_duration_seconds | Histogram | Duración |
| documents_inserted | Counter | Inserciones MongoDB |

### FastAPI

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| requests_total | Counter | Total requests |
| request_duration_ms | Histogram | Duración de request |

## 3. Métricas del Sistema

### Docker

Recolectadas automáticamente por Datadog Agent:

```
docker.cpu.percentage
docker.mem.usage
docker.mem.limit
docker.io.read_bytes
docker.io.write_bytes
docker.container.uptime
```

### Host

```
system.cpu.user
system.cpu.system
system.mem.used
system.mem.total
system.disk.used
system.disk.total
system.net.bytes_sent
system.net.bytes_rcvd
```

## 4. Dashboards

### Pipeline Health

**Propósito**: Visión general de la salud del pipeline

**Widgets**:
- Generator Events Produced (time series)
- Streaming Worker Messages (time series)
- ETL Duration (time series)
- Gold Documents Created (time series)

**Queries**:
```
sum:generator.events_produced{}
sum:streaming_worker.messages_consumed{}.as_count()
avg:etl_spark.etl_duration_seconds{}
sum:gold_transformer.documents_created{}
```

### Kafka Metrics

**Propósito**: Monitorear rendimiento de Kafka

**Widgets**:
- Events Produced Rate (rate)
- Consumer Lag (gauge)
- Batch Latency (histogram)

**Queries**:
```
sum:generator.events.produced{}.as_count()
avg:streaming_worker.consumer_lag{}
avg:streaming_worker.batch.latency_ms{}
```

### MongoDB Metrics

**Propósito**: Monitorear MongoDB

**Widgets**:
- Documents Inserted (counter)
- Query Latency (histogram)
- Connection Status (gauge)

### FastAPI Metrics

**Propósito**: Monitorear API

**Widgets**:
- Request Rate (rate)
- Request Duration (histogram)
- Endpoints Performance (table)

**Query**:
```
sum:fastapi.requests_total{}.as_count()
avg:fastapi.request_duration_ms{}
avg:fastapi.endpoint.*.duration_ms{} by {endpoint}
```

### System Metrics

**Propósito**: Monitorear recursos

**Widgets**:
- CPU por Container
- Memory por Container
- Disk I/O
- Network I/O

## 5. Monitors (Alertas)

### Critical Alerts

```yaml
- name: "High CPU Usage"
  query: "avg:system.cpu.user{service:data-platform} > 0.80"
  threshold: 0.80
  duration: 5m
  
- name: "High Memory Usage"
  query: "avg:system.mem.pct_usable{service:data-platform} < 0.20"
  threshold: 20
  duration: 5m

- name: "Kafka Lag High"
  query: "avg:streaming_worker.consumer_lag{} > 1000"
  threshold: 1000
  duration: 2m

- name: "Container Down"
  query: "service_check('docker.container.up').last(4).count_by_status()"
```

### Warning Alerts

```yaml
- name: "ETL Duration High"
  query: "avg:etl_spark.etl_duration_seconds{} > 60"
  threshold: 60
  duration: 5m

- name: "FastAPI Latency"
  query: "avg:fastapi.request_duration_ms{} > 500"
  threshold: 500
  duration: 5m

- name: "MongoDB Errors"
  query: "sum:gold_transformer.mongodb_connection_attempts{} > 2"
  threshold: 2
  duration: 2m
```

## 6. Investigación y Troubleshooting

### Pipeline Lento

```
1. Verificar ETL Duration
   avg:etl_spark.etl_duration_seconds{}

2. Verificar Records Processed
   sum:etl_spark.records_processed{}

3. Verificar Spark CPU
   avg:docker.cpu.percentage{container_name:etl-spark}

4. Verificar MinIO I/O
   avg:docker.io.read_bytes{container_name:minio}
```

### Alta Latencia en API

```
1. Verificar Request Duration
   avg:fastapi.request_duration_ms{}

2. Verificar MongoDB Query Time
   avg:gold_transformer.mongodb.query_latency_ms{}

3. Verificar MongoDB Connections
   gauge:gold_transformer.mongodb_connections{}

4. Verificar Container Resources
   avg:docker.mem.usage{container_name:fastapi-server}
```

### Consumer Lag Alto

```
1. Verificar Consumer Lag
   avg:streaming_worker.consumer_lag{}

2. Verificar Events Produced
   sum:generator.events_produced{}.as_count()

3. Verificar Messages Consumed
   sum:streaming_worker.messages_consumed{}.as_count()

4. Verificar Processing Latency
   avg:streaming_worker.processing_latency_ms{}
```

## 7. Configuración Datadog

### Habilitar Datadog

```bash
# Exportar API key
export DD_API_KEY="your_datadog_api_key"

# Levantarservicio
docker compose up -d datadog-agent

# Verificar que sea visible
# https://app.datadoghq.com/infrastructure
```

### Sin Datadog (Development)

El sistema funciona completamente sin Datadog:
- Las métricas se publican localmente
- Los logs se imprimen en console
- Es una buena forma de desarrollar sin costos

## 8. Recolección Manual de Métricas

### Script para extraer métricas

```bash
#!/bin/bash

# CPU por container
docker stats --no-stream --format "{{.Container}}: {{.CPUPerc}}"

# Memory por container
docker stats --no-stream --format "{{.Container}}: {{.MemUsage}}"

# Kafka lag
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group bronze_workers \
  --describe

# MongoDB collections
docker compose exec mongodb mongosh \
  --eval "db.hourly_metrics.countDocuments({})"
```

## 9. Retention Policies

### Datadog (Cloud)

- Default: 15 days (configurable)
- Logs: 15 days
- Traces: 15 days
- Metrics: Indefinido

### Local

- Kafka: 24 horas
- Bronze: 24 horas (archivos)
- Silver: 30 días
- Gold: Indefinido

## 10. Próximas Mejoras

1. **Distributed Tracing**: Correlacionar across services
2. **Custom Dashboards**: Dashboard por rol
3. **Anomaly Detection**: Detectar desvíos automáticamente
4. **SLO Definition**: Definir SLOs por servicio
5. **Cost Analysis**: Analizar costo por componente
6. **Forecasting**: Predicción de recursos
