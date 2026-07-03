# Arquitectura del Data Platform

## Visión General

La plataforma sigue la arquitectura **Medallion** (también conocida como Lakehouse Architecture), que divide el procesamiento de datos en tres capas lógicas:

## 1. Bronze Layer (Raw Data)

### Propósito
Almacenar datos crudos tal como llegan del sistema fuente.

### Características
- **Ingesta**: Kafka Streaming
- **Almacenamiento**: MinIO (S3-compatible)
- **Formato**: JSON
- **Particionamiento**: Por fecha y hora
- **Retención**: 24 horas (configurable)

### Estructura
```
s3://bronze/
└── raw/
    └── date=2024-01-15/
        └── hour=10/
            └── 1705330245000.json
            └── 1705330245001.json
            ...
```

### Flujo
```
Generador de Datos
    ↓ (Kafka)
Kafka Topic "raw_events"
    ↓ (Consumer Group: bronze_workers)
Streaming Worker
    ↓ (Batch)
MinIO S3
```

## 2. Silver Layer (Cleansed & Enriched)

### Propósito
Datos limpios, validados, normalizados y enriquecidos.

### Características
- **Procesamiento**: Apache Spark
- **Almacenamiento**: MinIO (S3-compatible)
- **Formato**: Parquet (columnar)
- **Particionamiento**: year/month/day/hour
- **Intervalo de actualización**: 5 minutos
- **Retención**: 30 días

### Transformaciones

#### Validación
- Campos requeridos no nulos
- Rangos válidos:
  - Latitude: [-90, 90]
  - Longitude: [-180, 180]
  - Vehicle count: >= 0
  - Speed: [0, 200]
  - Air Quality: [0, 500]

#### Limpieza
- Trimming de strings
- Lowercase de IDs
- Eliminación de duplicados

#### Enriquecimiento
```python
# Clasificación de tráfico
traffic_level = case(
    when(traffic_density < 25, "low"),
    when(traffic_density < 60, "medium"),
    when(traffic_density < 85, "high"),
    else="critical"
)

# Clasificación de calidad del aire
air_quality_level = case(
    when(aqi < 50, "good"),
    when(aqi < 100, "moderate"),
    when(aqi < 150, "unhealthy_for_groups"),
    when(aqi < 200, "unhealthy"),
    else="hazardous"
)

# Extracción de componentes de timestamp
year, month, day, hour = extract_date_components(timestamp)
```

### Estructura
```
s3://silver/
└── processed/
    └── year=2024/
        └── month=01/
            └── day=15/
                └── hour=10/
                    └── part-00000.parquet
                    └── part-00001.parquet
```

### Esquema

```python
StructType([
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", TimestampType()),
    StructField("vehicle_count", IntegerType()),
    StructField("average_speed", DoubleType()),
    StructField("traffic_density", DoubleType()),
    StructField("air_quality_index", DoubleType()),
    StructField("traffic_level", StringType()),  # Enriquecido
    StructField("air_quality_level", StringType()),  # Enriquecido
    StructField("year", IntegerType()),  # Particionado
    StructField("month", IntegerType()),  # Particionado
    StructField("day", IntegerType()),  # Particionado
    StructField("hour", IntegerType()),  # Particionado
])
```

## 3. Gold Layer (Business Ready)

### Propósito
Datos ya procesados, listos para consultas y análisis.

### Características
- **Procesamiento**: Agregaciones y transformaciones
- **Almacenamiento**: MongoDB
- **Formato**: BSON (documentos)
- **Intervalo de actualización**: 10 minutos
- **Acceso**: REST API con FastAPI

### Entidades

#### Hourly Metrics
Agregados por hora de cada tipo de sensor.

```javascript
{
  "_id": ObjectId(),
  "timestamp": ISODate("2024-01-15T10:00:00Z"),
  "year": 2024,
  "month": 1,
  "day": 15,
  "hour": 10,
  "sensor_type": "traffic",
  "metrics": {
    "avg_speed": 42.5,
    "max_speed": 85.3,
    "min_speed": 15.2,
    "avg_density": 58.3,
    "max_density": 95.0,
    "records_count": 150
  },
  "record_count": 150
}
```

#### Incident Reports
Reportes de incidentes detectados.

```javascript
{
  "_id": ObjectId(),
  "timestamp": ISODate("2024-01-15T10:15:30Z"),
  "sensor_id": "SENSOR_0042",
  "sensor_type": "traffic",
  "incident_type": "accident",
  "location": {
    "latitude": -12.0456,
    "longitude": -77.0365
  },
  "intersection": "INT_0042"
}
```

### Índices

```javascript
// Hourly Metrics
db.hourly_metrics.createIndex({ "timestamp": -1 });
db.hourly_metrics.createIndex({ "sensor_type": 1, "timestamp": -1 });

// Incident Reports
db.incident_reports.createIndex({ "timestamp": -1 });
db.incident_reports.createIndex({ "incident_type": 1, "timestamp": -1 });
db.incident_reports.createIndex({ "location": "2dsphere" });
```

## Componentes de Sistema

### 1. Data Generator

**Responsabilidad**: Simular sensores IoT

**Especificaciones**:
- 100 sensores simultáneos
- Eventos cada 2 segundos (batch de 50)
- Variación realista de datos
- Geolocalización simulada en Lima

**Producción de Eventos**:
- Rate: ~25 eventos/segundo
- Formato: JSON
- Canal: Kafka

### 2. Kafka

**Cluster**:
- Brokers: 1
- Topics: 1 (raw_events)
- Particiones: 1
- Replication Factor: 1

**Configuración**:
- Retention: 24 horas
- Compression: snappy
- Serialization: JSON

### 3. Streaming Worker

**Responsabilidad**: Consumir de Kafka y escribir en Bronze

**Características**:
- Consumer Group: bronze_workers
- Poll Size: 100 mensajes
- Commit: Auto
- Particionamiento: Por fecha/hora

**Performance**:
- Latencia esperada: < 100ms

### 4. Spark ETL

**Configuración**:
- Master: local[*]
- Memory: 2GB (configurable)
- Shuffle Partitions: 4 (configurable)
- Execution Mode: local

**Procesamiento**:
- Input: MinIO (Parquet)
- Output: MinIO (Parquet)
- Intervalo: 5 minutos

**Optimizaciones**:
- Predicate pushdown
- Column pruning
- Broadcast joins

### 5. Gold Transformer

**Responsabilidad**: Transformar Silver a Gold

**Agregaciones**:
- Hourly metrics por sensor_type
- Detección de incidentes
- Estadísticas de locación

**Intervalo**: 10 minutos

### 6. FastAPI

**Endpoints**:
- GET /health - Health check
- GET /readiness - Readiness check
- GET /api/v1/metrics/hourly - Métricas por hora
- GET /api/v1/incidents - Reportes de incidentes
- GET /api/v1/aggregations/by-sensor-type - Agregaciones
- GET /api/v1/stats/database - Estadísticas

**Rate Limiting**: No implementado (MVP)

**Authentication**: No implementada (MVP)

## Flujos de Datos

### Ingesta

```
Generador
  ↓
Kafka (raw_events)
  ↓ Streaming Worker
MinIO Bronze
  ↓ ~100-200 eventos/segundo
~8.6M eventos/día
```

### Procesamiento ETL

```
MinIO Bronze (cada 5 min)
  ↓
Spark Local
  - Validación
  - Limpieza
  - Enriquecimiento
  - Particionamiento
  ↓
MinIO Silver (Parquet)
```

### Agregación

```
MinIO Silver (cada 10 min)
  ↓
Gold Transformer
  - Agregación por hora
  - Detección de incidentes
  ↓
MongoDB Gold
  - hourly_metrics
  - incident_reports
```

### Exposición

```
MongoDB Gold
  ↓
FastAPI
  ↓ JSON REST API
Nginx Proxy
  ↓
Consumidores (Datadog, Dashboards, etc)
```

## Observabilidad

### Puntos de Instrumentación

1. **Generator**
   - Events produced
   - Production latency
   - Kafka connection status

2. **Streaming Worker**
   - Messages consumed
   - Messages stored
   - Consumer lag
   - Processing latency

3. **ETL**
   - Pipeline duration
   - Records processed
   - Invalid records
   - S3 I/O metrics

4. **Gold Transformer**
   - Transformation runs
   - Documents created
   - MongoDB I/O metrics

5. **FastAPI**
   - Requests total
   - Request duration
   - Endpoint-specific metrics

### Métricas del Sistema

- CPU usage por container
- Memory usage por container
- Disk usage
- Network I/O
- Container restart count

## Decisiones Arquitectónicas

1. **Medallion Architecture**: Separación clara de capas
2. **Local Deployment**: Facilita desarrollo y testing
3. **Parquet en Silver**: Compresión y velocidad
4. **MongoDB en Gold**: Flexibilidad de esquema
5. **OpenTelemetry + DogStatsD**: Estándar abierto + Datadog
6. **Spark Local**: Suficiente para MVP, scalable a cluster

## Consideraciones de Escala

### Bottlenecks Identificados

1. **Kafka Single Broker**: Limita throughput
2. **Spark Local**: Limita procesamiento
3. **Single MongoDB**: Limita queries
4. **MinIO Single Node**: Sin replicación

### Mejoras Futuras

1. Kafka Multi-broker cluster
2. Spark Cluster mode (YARN/K8s)
3. MongoDB Replica Set
4. MinIO Distributed mode
5. Caching layer (Redis)
6. Database indexing optimization

## Seguridad

### MVP (Minimal)

- Credenciales en variables de entorno
- No hay encriptación en tránsito (local)
- No hay autenticación en APIs

### Consideraciones Futuras

- API keys y JWT
- Encriptación TLS
- Network segmentation
- Audit logging
- RBAC en MongoDB
