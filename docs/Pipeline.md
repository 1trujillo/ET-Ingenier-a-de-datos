# Pipeline de Datos Detallado

## Visión General

Este documento describe el flujo detallado de datos desde su generación hasta su exposición.

## 1. Generación de Datos (Generator)

### Ciclo de Generación

```python
while True:
    for i in range(BATCH_SIZE):
        # Seleccionar sensor aleatorio
        sensor_id = random.choice(sensor_ids)
        
        # Generar evento
        event = {
            'sensor_id': sensor_id,
            'sensor_type': random.choice(SENSOR_TYPES),
            'latitude': base_lat + variation,
            'longitude': base_lng + variation,
            'vehicle_count': random.poisson(25),
            'average_speed': random.normal(40, 15),
            'traffic_density': uniform(0, 100),
            'air_quality_index': uniform(0, 500),
            'incident_detected': random.choice([True, False]),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Enviar a Kafka
        producer.send('raw_events', value=event)
    
    time.sleep(INTERVAL_SECONDS)
```

### Parametros

- **NUM_SENSORS**: 100 sensores simultáneos
- **BATCH_SIZE**: 50 eventos por batch
- **INTERVAL_SECONDS**: 2 segundos entre batches
- **Throughput**: ~25 eventos/segundo = ~2.16M eventos/día

### Simulación de Datos

```
Lima: -12.0462, -77.0371

Variación de coordenadas:
  lat ±0.05 (aprox 5.5 km)
  lng ±0.05 (aprox 5.5 km)

Valores simulados:
  vehicle_count: Poisson(λ=25)
  average_speed: Normal(μ=40, σ=15)
  traffic_density: Uniform(0, 100)
  air_quality_index: Uniform(0, 500)
  incident_detected: Bernoulli(p=0.05)
```

## 2. Kafka Topic

### Configuración

```yaml
topic: raw_events
partitions: 1
replication_factor: 1
retention_ms: 86400000  # 24 horas
compression: snappy
```

### Estructura del Mensaje

```json
{
  "key": null,
  "value": {
    "sensor_id": "SENSOR_0042",
    "sensor_type": "traffic",
    "intersection": "INT_0023",
    "latitude": -12.0456,
    "longitude": -77.0365,
    "vehicle_count": 28,
    "average_speed": 42.5,
    "traffic_density": 65.3,
    "traffic_light_status": "green",
    "air_quality_index": 125,
    "noise_level": 72.5,
    "incident_detected": false,
    "incident_type": "none",
    "temperature": 24.5,
    "humidity": 68.0,
    "timestamp": "2024-01-15T10:30:45Z"
  },
  "timestamp": 1705330245000,
  "offset": 12345,
  "partition": 0
}
```

### Consumo

```yaml
group_id: bronze_workers
auto_offset_reset: earliest
enable_auto_commit: true
max_poll_records: 100
session_timeout_ms: 30000
```

## 3. Bronze Layer - Streaming

### Flujo de Escritura

```
Kafka Consumer
    ↓ (Batch de 100 registros)
JSON Serialization
    ↓
Partitioned Path Generation
    date=2024-01-15/hour=10/
    ↓
MinIO Put Object
    s3://bronze/raw/date=2024-01-15/hour=10/1705330245000.json
```

### Estructura de Almacenamiento

```
s3://bronze/
└── raw/
    └── date=2024-01-15/
        └── hour=00/
            ├── 1705283904000.json
            ├── 1705283905000.json
            └── ...
        └── hour=01/
            └── ...
        └── ...
        └── hour=23/
            └── ...
```

### Tamaño de Datos

```
Eventos/día: 2.16M
Tamaño por evento: ~350 bytes
Total/día: 756 MB

Retención 24h: 756 MB
```

### Latencia

- Generación → Kafka: ~5ms
- Kafka → Consumer: ~10ms
- Batch → MinIO: ~50ms
- **Total: ~65ms (P95)**

## 4. ETL - Silver Layer

### Scheduling

```
Cada 5 minutos:
1. Leer Bronze (últimos 5 min)
2. Validar
3. Enriquecer
4. Escribir Silver
```

### Procesamiento Detallado

#### Validación

```python
# 1. Campos requeridos no nulos
df = df.filter(
    col('sensor_id').isNotNull() &
    col('latitude').isNotNull() &
    ...
)

# 2. Rangos válidos
df = df.filter(
    (col('latitude') >= -90) & (col('latitude') <= 90) &
    (col('longitude') >= -180) & (col('longitude') <= 180) &
    (col('vehicle_count') >= 0) &
    ...
)
```

#### Enriquecimiento

```python
# Clasificar tráfico
df = df.withColumn(
    'traffic_level',
    when(col('traffic_density') < 25, 'low')
    .when(col('traffic_density') < 60, 'medium')
    .when(col('traffic_density') < 85, 'high')
    .otherwise('critical')
)

# Extraer componentes de fecha
df = df.withColumn('year', year(col('timestamp')))
df = df.withColumn('month', month(col('timestamp')))
df = df.withColumn('day', dayofmonth(col('timestamp')))
df = df.withColumn('hour', hour(col('timestamp')))
```

#### Escritura

```python
df.write \
    .mode("append") \
    .partitionBy("year", "month", "day", "hour") \
    .parquet(f"s3a://{bucket}/processed/")
```

### Estructura de Salida

```
s3://silver/
└── processed/
    └── year=2024/
        └── month=01/
            └── day=15/
                └── hour=10/
                    ├── part-00000.parquet
                    ├── part-00001.parquet
                    └── _SUCCESS
```

### Estadísticas

```
Entrada: 50,000 eventos (5 min)
Validación: 95% válidos (47,500)
Enriquecimiento: +8 columnas
Salida: 47,500 registros
Formato: Parquet (snappy compression)
Tamaño: ~35 MB (50% reducción vs JSON)
Duración: ~2 minutos
Latencia: 5 min desde generación
```

## 5. Gold Layer - Transformación

### Scheduling

```
Cada 10 minutos:
1. Leer Silver (últimos 10 min)
2. Agregaciones por hora
3. Detección de incidentes
4. Insertar en MongoDB
```

### Agregaciones

#### Hourly Metrics

```python
# Agrupar por hora y sensor_type
aggregates = df.groupBy(
    'year', 'month', 'day', 'hour', 'sensor_type'
).agg(
    avg('average_speed').alias('avg_speed'),
    max('average_speed').alias('max_speed'),
    avg('traffic_density').alias('avg_density'),
    count('*').alias('record_count')
)
```

#### Incident Reports

```python
# Filtrar incidentes
incidents = df.filter(col('incident_detected') == True).select(
    'timestamp',
    'sensor_id',
    'sensor_type',
    'incident_type',
    'latitude',
    'longitude',
    'intersection'
)
```

### Resultados

```javascript
// Hourly Metrics
db.hourly_metrics
  100-200 documentos por 10 minutos

// Incident Reports
db.incident_reports
  5-10 documentos por 10 minutos
  (1% de eventos son incidentes)
```

## 6. Gold Layer - Exposición

### Endpoints FastAPI

#### GET /api/v1/metrics/hourly

```bash
curl "http://localhost/api/v1/metrics/hourly?sensor_type=traffic&hours=24&limit=1000"

Response:
[
  {
    "timestamp": "2024-01-15T10:00:00",
    "sensor_type": "traffic",
    "metrics": {
      "avg_speed": 42.5,
      "max_speed": 85.3,
      "avg_density": 58.3
    },
    "record_count": 150
  },
  ...
]
```

#### GET /api/v1/incidents

```bash
curl "http://localhost/api/v1/incidents?incident_type=accident&hours=24"

Response:
[
  {
    "timestamp": "2024-01-15T10:15:30Z",
    "sensor_id": "SENSOR_0042",
    "incident_type": "accident",
    "location": {
      "latitude": -12.0456,
      "longitude": -77.0365
    }
  },
  ...
]
```

### Latencia

- API Request: ~50ms
- MongoDB Query: ~20ms
- Serialización JSON: ~10ms
- Nginx Proxy: ~5ms
- **Total: ~85ms (P95)**

## 7. Flujo Completo End-to-End

### Timeline Ejemplo

```
T+0s:    Generador crea 50 eventos
T+5ms:   Eventos en Kafka
T+50ms:  Eventos en Bronze MinIO
T+5min:  ETL procesa batch
T+7min:  Datos en Silver MinIO
T+10min: Gold Transformer agrega
T+12min: Documentos en MongoDB
T+12.5min: API expone datos
T+13min: Consulta desde cliente

Latencia Total: 13 minutos
```

### Volumen Acumulativo

```
Hora 0-1:   216,000 eventos → 204,000 en Silver (94% valid)
Hora 1-2:   216,000 eventos → 204,000 en Silver
...
Día 1:      5,184,000 eventos → 4,900,000 en Silver

Agregados Gold:
  Hourly Metrics: 24 * 5 tipos sensores = 120 documentos
  Incident Reports: 5,184,000 * 0.01 = 51,840 reportes
```

## 8. Optimizaciones del Pipeline

### Caching

```python
# Cache Silver datos en memoria (próxima versión)
df.cache()

# Cache MongoDB queries
db.hourly_metrics.createIndex(
    { "timestamp": -1, "sensor_type": 1 }
)
```

### Particionamiento

```
Bronze:  date/hour (24 particiones/día)
Silver:  year/month/day/hour (8760 particiones/año)
Gold:    Por tipo de agregación
```

### Compresión

```
JSON → Snappy: 350 bytes → 180 bytes (49%)
Parquet + Snappy: 180 bytes → 90 bytes (49%)
```

## 9. Backpressure y Resiliencia

### Generator → Kafka

```python
future = producer.send(event)
future.get(timeout=5)  # Bloqueante hasta confirmación
```

### Kafka → Bronze

```python
# Auto-commit cada 100 mensajes
# Si falla, se reintenta desde último offset
enable_auto_commit=True
```

### Silver → Gold

```python
# Si transformación falla, se registra
# Próximo job lo reintenta
```

## 10. Monitoreo del Pipeline

### Métricas Clave

| Métrica | Objetivo | Alerta |
|---------|----------|--------|
| Generator Rate | >25 e/s | <20 e/s |
| Consumer Lag | <1000 | >5000 |
| ETL Duration | <3min | >5min |
| Silver Records | >45k/5min | <40k/5min |
| Gold Latency | <10min | >15min |
| API P95 | <200ms | >500ms |

### Logs Importantes

```
[Generator] Batch produced: 50 events in 25ms
[Worker] Batch produced: stored 50 events in 45ms
[ETL] ETL completed in 120 seconds
[Gold] Inserted 250 hourly metrics
[API] GET /metrics/hourly - 200 OK - 85ms
```
