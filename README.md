# Data Platform - MVP

Un MVP de Plataforma de Ingeniería de Datos completa con arquitectura Medallion, observable con Datadog, ejecutable 100% localmente en Docker Compose.

## 🚀 Quick Start

### Requisitos

- Docker Desktop
- Docker Compose v2.0+
- Git
- Datadog API Key (opcional, para observabilidad completa)

### Instalación y Ejecución

```bash
# Clonar repositorio
git clone <repo>
cd ET-Ingenier-a-de-datos

# Configurar variables de entorno (opcional)
export DD_API_KEY="your_datadog_api_key"

# Iniciar plataforma
docker compose up -d

# Verificar servicios
docker compose ps

# Ver logs
docker compose logs -f generator
docker compose logs -f streaming-worker
docker compose logs -f etl-spark
docker compose logs -f gold-transformer
docker compose logs -f fastapi-server
```

### Acceso a Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| FastAPI Docs | http://localhost/docs | N/A |
| MinIO Console | http://localhost/minio/ | minioadmin / minioadmin |
| MongoDB | mongodb://admin:admin123@localhost:27017 | admin / admin123 |
| Kafka | localhost:9092 | N/A |
| Datadog | https://app.datadoghq.com | Tu cuenta |

## 📊 Arquitectura

### Medallion Architecture

```
Bronze Layer (Raw Data)
├─ Source: Kafka
├─ Storage: MinIO (JSON)
├─ Format: JSON
└─ Role: Raw event ingestion

     ↓

Silver Layer (Cleansed Data)
├─ Process: PySpark ETL
├─ Storage: MinIO (Parquet)
├─ Transformations:
│  ├─ Validation
│  ├─ Cleaning
│  ├─ Normalization
│  └─ Enrichment
└─ Role: Data preparation

     ↓

Gold Layer (Business Ready)
├─ Process: Transformation & Aggregation
├─ Storage: MongoDB
├─ Artifacts:
│  ├─ Hourly Metrics
│  └─ Incident Reports
└─ Role: Analytics & Insights
```

### Componentes

1. **Data Generator** → Simula sensores IoT
2. **Kafka** → Streaming de eventos crudos
3. **Streaming Worker** → Consume y almacena en Bronze
4. **ETL PySpark** → Procesa Bronze → Silver
5. **Gold Transformer** → Silver → Gold (MongoDB)
6. **FastAPI** → Expone datos Gold
7. **Datadog Agent** → Observabilidad centralizada

## 🔄 Flujo de Datos

```
Generador (100 sensores)
    ↓ (Kafka Events)
Kafka Topic "raw_events"
    ↓ (Streaming)
Streaming Worker
    ↓ (JSON)
MinIO Bronze
    ↓ (Every 5 min)
PySpark ETL
    ↓ (Parquet)
MinIO Silver
    ↓ (Every 10 min)
Gold Transformer
    ↓ (Documents)
MongoDB Gold
    ↓ (REST API)
FastAPI
    ↓ (Dashboards)
Datadog
```

## 📡 Observabilidad

### Instrumentación

- **OpenTelemetry**: Traces y métricas
- **DogStatsD**: Envío de métricas a Datadog
- **JSON Logging**: Logs estructurados

### Métricas Recolectadas

Las métricas que actualmente se envían a Datadog (por OpenTelemetry/StatsD) son estas:

#### Generator
- `events_produced` - contador de eventos producidos
- `events_failed` - contador de eventos fallidos
- `production_latency` - histograma de latencia de producción
- `kafka_connection_attempts` - gauge de intentos de conexión a Kafka
- `events.produced` - contador de eventos producidos vía StatsD
- `events.failed` - contador de eventos fallidos vía StatsD
- `batch.latency_ms` - histograma de latencia por batch
- `batch.size` - gauge del tamaño del batch

#### Streaming Worker
- `messages_consumed` - contador de mensajes consumidos
- `messages_stored` - contador de mensajes almacenados en Bronze
- `consumer_lag` - gauge del lag del consumer
- `processing_latency` - histograma de latencia de procesamiento
- `messages.stored` - contador de mensajes guardados vía StatsD
- `messages.store_failed` - contador de fallos de almacenamiento
- `messages.consumed` - contador de mensajes consumidos vía StatsD
- `messages.processing_failed` - contador de fallos de procesamiento
- `kafka_connection_attempts` - gauge de intentos de conexión a Kafka

#### ETL
- `etl_runs` - contador de ejecuciones de ETL
- `records_processed` - contador de registros procesados
- `records_invalid` - contador de registros inválidos
- `etl_duration` - histograma de duración del ETL
- `silver.records_saved` - contador de registros guardados en Silver vía StatsD
- `etl.duration_seconds` - histograma de duración vía StatsD

#### Gold Transformer
- `transformation_runs` - contador de transformaciones ejecutadas
- `documents_created` - contador de documentos creados en MongoDB
- `transformation_duration` - histograma de duración de la transformación
- `mongodb_connection_attempts` - gauge de intentos de conexión a MongoDB
- `documents.inserted` - contador de documentos insertados vía StatsD
- `documents.insert_failed` - contador de fallos de inserción
- `transformation.duration_seconds` - histograma de duración vía StatsD
- `transformation.failed` - contador de transformaciones fallidas

#### FastAPI
- `requests_total` - contador total de requests
- `request_duration` - histograma de duración de requests
- `endpoint.hourly_metrics.duration_ms` - latencia del endpoint de métricas por hora
- `endpoint.incidents.duration_ms` - latencia del endpoint de incidentes

#### Sistema
- CPU usage
- Memory usage
- Disk I/O
- Container metrics

### Dashboards Datadog

Se crearán automáticamente:
- Pipeline Health
- Kafka Metrics
- MongoDB Metrics
- FastAPI Metrics
- System Metrics
- ETL Pipeline Metrics

### Alerts

Se configurarán para:
- CPU > 80%
- RAM > 80%
- Kafka lag alto
- Errores en ETL
- FastAPI latency > 500ms
- Contenedores caídos

## 🛠️ Configuración

### Archivo .env

```bash
# Datadog (opcional)
DD_API_KEY=your_api_key

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=raw_events

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# MongoDB
MONGODB_URI=mongodb://admin:admin123@mongodb:27017/data_platform?authSource=admin
```

## 📊 API Endpoints

### Health Check

```bash
GET /health
GET /readiness
```

### Metrics

```bash
# Obtener métricas por hora
GET /api/v1/metrics/hourly?sensor_type=traffic&hours=24&limit=1000

# Obtener incidentes
GET /api/v1/incidents?incident_type=accident&hours=24&limit=1000

# Agregaciones
GET /api/v1/aggregations/by-sensor-type?hours=24

# Estadísticas de BD
GET /api/v1/stats/database
```

## 🗄️ Datos Simulados

### Evento de Sensor

```json
{
  "sensor_id": "SENSOR_0001",
  "sensor_type": "traffic",
  "intersection": "INT_0001",
  "latitude": -12.0462,
  "longitude": -77.0371,
  "vehicle_count": 25,
  "average_speed": 45.5,
  "traffic_density": 62.3,
  "traffic_light_status": "green",
  "air_quality_index": 125.0,
  "noise_level": 75.5,
  "incident_detected": false,
  "incident_type": "none",
  "temperature": 25.5,
  "humidity": 65.0,
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## 🧪 Testing

### Health Checks

```bash
# Verificar API
curl http://localhost/health

# Verificar MongoDB
docker compose exec mongodb mongosh --eval "db.adminCommand('ping')"

# Verificar Kafka
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Verificar MinIO
curl http://localhost:9000/minio/health/live
```

### Generar Carga

```bash
# Ver eventos siendo generados
docker compose logs -f generator | grep "Batch produced"

# Ver consumo de streaming worker
docker compose logs -f streaming-worker | grep "Batch produced"

# Ver ETL ejecutándose
docker compose logs -f etl-spark | grep "ETL completed"
```

## 📈 Métricas de Rendimiento

El MVP está preparado para medir:

- **Throughput**: Eventos/segundo
- **Latency**: Desde generación hasta exposición en API
- **P95/P99**: Percentiles de latencia
- **CPU/Memory**: Por componente
- **Disco**: Por capa (Bronze, Silver)
- **Network**: I/O Kafka
- **Errores**: Tasas de error

## 🚀 Próximas Fases

### Fase 2: Optimización
Basada en métricas de Datadog:
- Tuning de Spark
- Caching strategies
- Partitioning optimization
- Connection pooling

### Fase 3: Escalabilidad
- Kafka scaling (múltiples brokers)
- Spark cluster mode
- MongoDB sharding
- API load balancing

### Fase 4: Características Avanzadas
- Feature store
- Real-time aggregations
- Machine learning pipelines
- Alertas inteligentes

## 📚 Documentación Adicional

- [Architecture.md](docs/Architecture.md) - Arquitectura detallada
- [Pipeline.md](docs/Pipeline.md) - Detalles del pipeline
- [Medallion.md](docs/Medallion.md) - Arquitectura Medallion
- [Observability.md](docs/Observability.md) - Guía de observabilidad
- [Deployment.md](docs/Deployment.md) - Guía de deployment
- [ADR.md](docs/ADR.md) - Architecture Decision Records

## 🤝 Contribuir

1. Fork el proyecto
2. Crear rama feature (`git checkout -b feature/amazing`)
3. Commit cambios (`git commit -m 'Add amazing feature'`)
4. Push (`git push origin feature/amazing`)
5. Abrir Pull Request

## 📝 License

MIT License

## 📞 Soporte

Para issues o preguntas, abrir un issue en el repositorio.

---

**Nota**: Este MVP es totalmente local. Para usar Datadog, configurar `DD_API_KEY` con tu API key real.
