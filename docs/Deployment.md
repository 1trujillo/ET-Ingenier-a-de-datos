# Deployment Guide

## Requisitos

### Hardware Mínimo
```
CPU: 4 cores
RAM: 8 GB
Disco: 20 GB (para datos)
OS: Linux/Mac/Windows (con Docker Desktop)
```

### Software
```
Docker: 20.10+
Docker Compose: 2.0+
Git: 2.0+
Bash: 4.0+ (para scripts)
```

## Instalación

### 1. Clonar Repositorio

```bash
git clone <repository>
cd ET-Ingenier-a-de-datos
```

### 2. Verificar Requisitos

```bash
docker --version
# Docker version 20.10.x

docker compose version
# Docker Compose version 2.x.x
```

### 3. Configurar Variables de Entorno

```bash
# Copiar archivo de ejemplo
cp .env.example .env

# Editar si es necesario
nano .env

# Variables importantes:
# DD_API_KEY=your_real_datadog_key (opcional)
# KAFKA_BOOTSTRAP_SERVERS=kafka:29092
# MINIO_ENDPOINT=minio:9000
# MONGODB_URI=mongodb://admin:admin123@mongodb:27017/data_platform?authSource=admin
```

### 4. Levantar Servicios

```bash
# Inicializar
bash scripts/init.sh

# O manualmente
docker compose up -d

# Verificar
docker compose ps
```

### 5. Verificar Salud

```bash
bash scripts/health-check.sh
```

## Configuración

### docker-compose.yml

**Importante**: No modificar sin entender las implicaciones

#### Variables de Ambiente Críticas

```yaml
zookeeper:
  ZOOKEEPER_CLIENT_PORT: 2181  # Puerto de Zookeeper

kafka:
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_LOG_RETENTION_HOURS: 24

minio:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin

mongodb:
  MONGO_INITDB_ROOT_USERNAME: admin
  MONGO_INITDB_ROOT_PASSWORD: admin123
```

#### Health Checks

Cada servicio incluye healthchecks automáticos. No modificar sin necesidad.

### Secrets Management

#### Desarrollo (MVP)

```bash
# Variables en .env
export DD_API_KEY="test_key_local_only"
```

#### Producción (Futuro)

```bash
# Usar secretos de Docker
docker secret create dd_api_key ./secrets/dd_api_key.txt

# Referencias en compose
secrets:
  dd_api_key:
    external: true
```

## Escalado

### Fase 1: Aumentar Throughput

```yaml
# docker-compose.yml
generator:
  environment:
    NUM_SENSORS: 500  # De 100 a 500
    BATCH_SIZE: 100   # De 50 a 100

streaming-worker:
  environment:
    MAX_POLL_RECORDS: 1000  # De 100 a 1000
```

**Impacto**:
- Throughput: 2.16M → 20M eventos/día
- Latencia: +50ms
- Recursos: CPU +50%, RAM +1GB

### Fase 2: Múltiples Brokers Kafka

```yaml
kafka2:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_BROKER_ID: 2
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    # ... más configuración
```

### Fase 3: Spark Cluster Mode

```bash
# En lugar de local[*]
docker run -d \
  --name spark-master \
  bitnami/spark:3.5.0 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
```

## Monitoreo en Producción

### Datadog Setup

```bash
# 1. Obtener API key real
export DD_API_KEY="abc123..."

# 2. Restart agent
docker compose restart datadog-agent

# 3. Verificar en Datadog
# https://app.datadoghq.com/infrastructure
```

### Métricas Críticas

| Métrica | Normal | Alerta |
|---------|--------|--------|
| CPU | <50% | >80% |
| Memory | <60% | >85% |
| Kafka Lag | <1000 | >5000 |
| API P95 | <200ms | >500ms |

## Troubleshooting

### Servicio no inicia

```bash
# Ver logs
docker compose logs kafka

# Verificar salud
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Reiniciar
docker compose restart kafka
```

### Alto lag en Kafka

```bash
# Verificar consumer
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group bronze_workers \
  --describe

# Reset si es necesario
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group bronze_workers \
  --reset-offsets \
  --to-earliest \
  --execute
```

### MongoDB error de conexión

```bash
# Verificar MongoDB
docker compose exec mongodb mongosh -u admin -p admin123 --eval "db.adminCommand('ping')"

# Revisar logs
docker compose logs mongodb

# Reiniciar
docker compose restart mongodb
```

### FastAPI no responde

```bash
# Ver logs
docker compose logs fastapi-server

# Verificar endpoint
curl http://localhost/health

# Reiniciar
docker compose restart fastapi-server
```

## Backup y Restore

### MongoDB Backup

```bash
# Crear backup
docker compose exec mongodb mongodump \
  -u admin \
  -p admin123 \
  --authenticationDatabase admin \
  --out /tmp/backup

# Copiar del contenedor
docker compose cp mongodb:/tmp/backup ./backups/
```

### MinIO Backup

```bash
# Descargar archivo
docker compose cp minio:/data ./backups/minio_data
```

## Actualizaciones

### Actualizar Imagen Base

```yaml
# docker-compose.yml
datadog-agent:
  image: datadog/agent:7.46.0  # Nueva versión
```

```bash
docker compose pull
docker compose up -d
```

### Migrar a Kubernetes (Futuro)

```bash
# Convertir compose a Kubernetes
kompose convert -f docker-compose.yml -o k8s/

# Deployar
kubectl apply -f k8s/
```

## Performance Tuning

### Spark

```yaml
environment:
  SPARK_EXECUTOR_MEMORY: 2g
  SPARK_EXECUTOR_CORES: 2
  SPARK_DRIVER_MEMORY: 1g
  SPARK_SHUFFLE_PARTITIONS: 8
```

### MongoDB

```javascript
// Crear índices
db.hourly_metrics.createIndex({ "timestamp": -1 }, { background: true })
db.hourly_metrics.createIndex({ "sensor_type": 1, "timestamp": -1 }, { background: true })
```

### Kafka

```yaml
environment:
  KAFKA_NUM_PARTITIONS: 4
  KAFKA_COMPRESSION_TYPE: snappy
  KAFKA_LOG_CLEANUP_POLICY: delete
```

## Disaster Recovery

### Plan de Recuperación

1. **RTO (Recovery Time Objective)**: 1 hora
2. **RPO (Recovery Point Objective)**: 5 minutos

### Backup Automático

```bash
# Cron job (añadir a crontab)
0 */6 * * * /path/to/backup.sh

# Script de backup
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker compose exec mongodb mongodump -u admin -p admin123 --out /backups/mongo_$DATE
docker compose cp minio:/data /backups/minio_$DATE
```

## Seguridad

### MVP (Actual)

- Credenciales en .env
- Red interna (no expuesta)
- No encriptación

### Producción

```yaml
# SSL/TLS
nginx:
  volumes:
    - ./certs/cert.pem:/etc/nginx/certs/cert.pem
    - ./certs/key.pem:/etc/nginx/certs/key.pem

# Network segmentada
networks:
  internal:
    driver: bridge
  external:
    driver: bridge
```

## Checklist de Deployment

- [ ] Clonar repositorio
- [ ] Instalar Docker y Docker Compose
- [ ] Configurar .env
- [ ] Ejecutar `docker compose up -d`
- [ ] Verificar salud con health-check.sh
- [ ] Acceder a FastAPI http://localhost/docs
- [ ] Acceder a MinIO http://localhost:9001
- [ ] Probar endpoints API
- [ ] Configurar Datadog (opcional)
- [ ] Configurar alertas
- [ ] Documentar credenciales
- [ ] Establecer backup schedule
