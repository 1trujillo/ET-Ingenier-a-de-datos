# Arquitectura Medallion

## ¿Qué es Medallion?

La arquitectura Medallion (también llamada "Lakehouse Architecture" o "Delta Lake Architecture") es un patrón de arquitectura de datos que organiza datos en capas de creciente calidad y complejidad.

## Niveles

### 1. Bronze Level (Raw)

La capa bruta contiene datos tal como se reciben de los sistemas fuente.

**Características**:
- Sin transformaciones
- Sin validaciones
- Sin enriquecimiento
- Datos históricos sin cambios

**Ventajas**:
- Fácil ingesta
- Recuperación de datos originales
- Auditabilidad completa
- Sin pérdida de información

**Nuestro Caso**:
```
Formato: JSON
Fuente: Kafka
Almacenamiento: MinIO S3
Particionamiento: date/hour
Tamaño: ~750 MB/día
Retención: 24 horas
```

### 2. Silver Level (Cleansed)

La capa procesada contiene datos limpios y validados.

**Transformaciones**:
- Validación de esquema
- Limpieza de datos
- Normalización
- Eliminación de duplicados
- Enriquecimiento

**Ventajas**:
- Datos listos para análisis
- Mejor performance (Parquet)
- Consistencia garantizada
- Auditoría de calidad

**Nuestro Caso**:
```
Formato: Parquet
Procesador: Apache Spark
Almacenamiento: MinIO S3
Particionamiento: year/month/day/hour
Tamaño: ~375 MB/día (50% reducción)
Retención: 30 días
```

### 3. Gold Level (Curated)

La capa curada contiene datos específicos del negocio, listos para consumo.

**Características**:
- Agregaciones
- Métricas de negocio
- KPIs
- Dimensiones normalizadas

**Ventajas**:
- Rendimiento óptimo
- Sem ántica de negocio clara
- Acceso simplificado
- Independencia de fuente

**Nuestro Caso**:
```
Formato: MongoDB BSON
Procesador: Transformación Custom
Almacenamiento: MongoDB
Colecciones:
  - hourly_metrics
  - incident_reports
Retención: Indefinida
```

## Flujo de Datos

### Timeline Ejemplo

```
T+0:     Evento generado
T+50ms:  Evento en Kafka
T+100ms: Evento en Bronze (JSON)
T+5min:  Lote en Bronze listo para procesar
T+7min:  Datos en Silver (Parquet)
T+10min: Agregaciones calculadas
T+12min: Datos en Gold (MongoDB)
T+13min: Disponible en API
```

### Volúmenes

```
Entrada (Bronze):        2.16M eventos/día
Después Validación:      2.05M eventos/día (95% válidos)
Silver (Parquet):        2.05M registros
Gold (Hourly Metrics):   120 documentos/día
Gold (Incidents):        21.6K documentos/día
```

## Ventajas de Medallion

### 1. Escalabilidad
- Datos crudos en Bronze facilitan la ingesta
- Capa Silver permite procesamiento eficiente
- Gold proporciona acceso rápido

### 2. Flexibilidad
- Agregar nuevas fuentes en Bronze
- Nuevas transformaciones en Silver
- Nuevos casos de uso en Gold

### 3. Calidad de Datos
- Cada capa valida y mejora
- Trazabilidad completa
- Recuperación de errores posible

### 4. Seguridad
- Control de acceso por capa
- Auditoría de cambios
- Preservación de datos originales

### 5. Costo
- Compresión en Silver/Gold
- Retención flexible
- Almacenamiento eficiente

## Versioning y Time Travel

Una ventaja adicional del Parquet + particionamiento:

### Silver

```
s3://silver/processed/year=2024/month=01/day=15/hour=10/
s3://silver/processed/year=2024/month=01/day=15/hour=11/
s3://silver/processed/year=2024/month=01/day=16/hour=10/
```

Cualquier análisis puede referenciar datos de momentos específicos.

### Gold

Con MongoDB, se puede añadir versionamiento:

```javascript
{
  "_id": ObjectId(),
  "timestamp": ISODate(),
  "version": 1,
  "data": {...},
  "created_at": ISODate(),
  "ttl_index": ISODate()  // Para auto-delete
}
```

## Casos de Uso

### 1. Debugging
```
¿Datos incorrectos en API?
1. Ver documento en Gold
2. Buscar en Silver (Parquet)
3. Validar en Bronze (JSON original)
```

### 2. Auditoría
```
¿Quién accedió qué dato?
1. Logs en Bronze
2. Transformaciones en Silver
3. Acceso en Gold
```

### 3. Machine Learning
```
1. Bronze: Raw features
2. Silver: Engineered features
3. Gold: ML-ready datasets
```

### 4. Business Intelligence
```
1. Bronze: Event logs
2. Silver: Normalized facts
3. Gold: Dimensions + Measures
```

## Antipatrones a Evitar

### 1. Saltarse Bronze
❌ Enviar datos directamente a Silver
✓ Siempre preservar originales en Bronze

### 2. Bronze como Data Warehouse
❌ Intentar consultar Bronze directamente
✓ Usar Gold para consultas

### 3. Sin Validación en Silver
❌ Copiar datos sin validar
✓ Aplicar reglas de calidad

### 4. Gold sin Particionamiento
❌ Colleción MongoDB sin índices
✓ Crear índices apropiados

## Monitoreo de Medallion

### Métricas Importantes

| Métrica | Ideal | Alerta |
|---------|-------|--------|
| Bronze → Silver Lag | <5 min | >15 min |
| Silver → Gold Lag | <10 min | >20 min |
| Silver Válidos | >95% | <90% |
| Gold Queries P95 | <200ms | >1000ms |

### Queries de Monitoreo

```python
# Tamaño por capa
SELECT 
  'bronze' as layer,
  COUNT(*) as records,
  SUM(file_size) as total_size
FROM bronze_events

# Calidad
SELECT
  COUNT(CASE WHEN is_valid THEN 1 END) as valid,
  COUNT(*) as total,
  ROUND(100.0 * COUNT(CASE WHEN is_valid THEN 1 END) / COUNT(*), 2) as pct_valid
FROM silver_events

# Performance
SELECT
  'hourly_metrics' as collection,
  COUNT(*) as documents,
  ROUND(AVG(query_time_ms), 2) as avg_query_ms
FROM gold_queries
```

## Evolución Futura

### Fase 1 (Actual)
- Bronze: JSON en MinIO
- Silver: Parquet en MinIO
- Gold: MongoDB

### Fase 2 (Próxima)
- Agregar Delta Lake
- Versioning automático
- ACID guarantees
- Time travel

### Fase 3
- Multicloud (Bronze in AWS S3, Silver in Azure, Gold in GCP)
- Real-time Bronze (Kafka → Bronze sin batching)
- ML Feature Store

## Referencias

- [Databricks Lakehouse](https://databricks.com/blog/2021/08/30/frequently-asked-questions-about-the-databricks-lakehouse.html)
- [Gartner Modern Data Architecture](https://www.gartner.com/en/documents/3981889)
- [Delta Lake](https://delta.io/)
