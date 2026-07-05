# Architecture Decision Records (ADR)

## ADR 1: Stack Tecnológico

**Estado**: Aceptado

### Problema
Elegir stack para plataforma de datos moderna con requisitos de:
- Observabilidad integrada
- Ejecutable localmente
- Escalable a futuro
- Costo controlado

### Opciones Consideradas

1. **Cloud-native (AWS/Azure/GCP)**
   - ✓ Escalabilidad automática
   - ✗ Costo elevado
   - ✗ Vendor lock-in
   - ✗ No totalmente local

2. **Docker local + Kubernetes**
   - ✓ Flexible
   - ✗ Complejidad alta para MVP
   - ✗ Overhead de orquestación

3. **Docker Compose Local** (Elegida)
   - ✓ Simple de empezar
   - ✓ Reproducible
   - ✓ Costo cero
   - ✓ Escalable a K8s

### Decisión
Usar **Docker Compose local** para MVP, con arquitectura escalable a Kubernetes.

### Justificación
- Desarrollo rápido
- Testing local
- Demostración sin costos
- Transición fácil a producción

---

## ADR 2: Arquitectura Medallion

**Estado**: Aceptado

### Problema
Organizar pipeline de datos de forma escalable y mantenible.

### Opciones

1. **ETL monolítico**
   - ✗ Difícil mantener
   - ✗ No flexible

2. **Lambda Architecture**
   - ✓ Batch + Stream
   - ✗ Complejidad alta

3. **Medallion/Lakehouse** (Elegida)
   - ✓ Separación clara de capas
   - ✓ Bronze: Raw data
   - ✓ Silver: Clean data
   - ✓ Gold: Business ready

### Decisión
Usar **Medallion Architecture** con 3 capas.

### Implicaciones
- Bronze: MinIO JSON (histórico)
- Silver: MinIO Parquet (procesado)
- Gold: MongoDB (curado)

---

## ADR 3: Formato de Almacenamiento

**Estado**: Aceptado

### Decisiones

| Capa | Formato | Razón |
|------|---------|-------|
| Bronze | JSON | Fácil ingesta |
| Silver | Parquet | Compresión + columnar |
| Gold | BSON | Flexibilidad MongoDB |

### Justificación

**JSON en Bronze**:
- Esquema flexible
- Fácil debugging
- Compatible con Kafka

**Parquet en Silver**:
- 50% compresión vs JSON
- Lectura columnar (Spark efficient)
- Índices automáticos

**BSON en Gold**:
- Flexible para agregaciones
- Índices MongoDB nativos
- JSON compatible

---

## ADR 4: Observabilidad

**Estado**: Aceptado

### Problema
Instrumentar plataforma sin vendor lock-in.

### Opciones

1. **Only Datadog**
   - ✗ Vendor lock-in
   - ✗ Necesita API key

2. **Custom logging**
   - ✗ No escalable
   - ✗ Difícil de escalar

3. **OpenTelemetry + Datadog** (Elegida)
   - ✓ Estándar abierto
   - ✓ Múltiples exporters
   - ✓ Funciona sin Datadog

### Decisión
Usar **OpenTelemetry** con exporter a Datadog.

### Implicaciones
```
Traces: OpenTelemetry → Datadog
Metrics: DogStatsD + OpenTelemetry → Datadog
Logs: JSON → Docker logs → Datadog (futuro)
```

---

## ADR 5: Base de Datos Gold Layer

**Estado**: Aceptado

### Opciones

1. **PostgreSQL**
   - ✓ Relacional
   - ✗ Esquema rígido

2. **Elasticsearch**
   - ✓ Search
   - ✗ No ideal para agregaciones

3. **MongoDB** (Elegida)
   - ✓ Flexible
   - ✓ Nativo JSON
   - ✓ Índices eficientes
   - ✓ Fácil APIs

### Decisión
Usar **MongoDB** para Gold layer.

### Justificación
- Agregaciones complejas
- Índices flexibles
- Queries rápidas
- Escalable

---

## ADR 6: Ingesta de Datos

**Estado**: Aceptado

### Opciones

1. **HTTP API**
   - ✗ Baja throughput
   - ✗ No confiable

2. **Kafka** (Elegida)
   - ✓ Streaming
   - ✓ Replayable
   - ✓ Escalable

3. **MQTT**
   - ✓ IoT native
   - ✗ Menos flexible

### Decisión
Usar **Kafka** para ingesta.

### Justificación
- Stream processing
- Consumer groups
- Retención configurable

---

## ADR 7: Procesamiento ETL

**Estado**: Aceptado

### Opciones

1. **Python Pandas**
   - ✗ No escalable (RAM)
   - ✓ Fácil

2. **Spark** (Elegida)
   - ✓ Escalable
   - ✓ Distributivo
   - ✓ SQL APIs

3. **Flink**
   - ✓ Real-time
   - ✗ Complejidad

### Decisión
Usar **Apache Spark** en local[*] para MVP.

### Evolución
```
MVP: Spark local[*]
Fase 2: Spark YARN cluster
Fase 3: Spark on K8s
```

---

## ADR 8: API Gateway

**Estado**: Aceptado

### Opciones

1. **FastAPI sin proxy**
   - ✓ Simple
   - ✗ Sin logging
   - ✗ Sin rate limiting

2. **Nginx** (Elegida)
   - ✓ Proxy reverso
   - ✓ Routing
   - ✓ Logging

### Decisión
Usar **Nginx** como proxy.

### Rutas
```
/api/* → FastAPI
/s3/* → MinIO S3
/minio/* → MinIO Console
/docs → FastAPI OpenAPI
```

---

## ADR 9: Retención de Datos

**Estado**: Aceptado

### Decisión

| Capa | Retención | Razón |
|------|-----------|-------|
| Bronze | 24 horas | Raw, espacio limitado |
| Silver | 30 días | Histórico procesado |
| Gold | Indefinida | Agregaciones importantes |
| Kafka | 24 horas | Replay si es necesario |

### Justificación
- Bronze: Espacio limitado, sin valor histórico
- Silver: Balance de costo/disponibilidad
- Gold: Datos curados, valiosos

---

## ADR 10: Seguridad (MVP)

**Estado**: Aceptado

### Decisión: No implementar autenticación

**Razón**: MVP local, sin exposición a internet.

**Futuro** (Producción):
- [ ] API keys/JWT
- [ ] Encriptación TLS
- [ ] Network segmentation
- [ ] RBAC

---

## Matriz de Decisiones

| Aspecto | Selección | Alternativas Rechazadas |
|--------|-----------|--------------------------|
| Stack | Docker Compose | K8s, Cloud |
| Arquitectura | Medallion | Monolítica, Lambda |
| Ingesta | Kafka | HTTP, MQTT |
| ETL | Spark | Pandas, Flink |
| API | FastAPI | Flask, Django |
| DB Gold | MongoDB | PostgreSQL, ES |
| Observabilidad | OTel + Datadog | Prometheus, Elastic |
| Storage | MinIO | Local FS |
| Proxy | Nginx | Traefik, Kong |

---

## Próximas ADRs Necesarias

1. **Caching Strategy** (Redis vs In-Memory)
2. **Feature Store** (MLflow vs Custom)
3. **CI/CD Pipeline** (GitHub Actions vs GitLab)
4. **Version Control** (Git, branching strategy)
5. **Testing Framework** (Pytest vs TestNG)
6. **Logging Aggregation** (ELK vs Splunk)
7. **Data Quality** (Great Expectations vs dbt tests)
