# Testing Guide

## Pruebas Unitarias

### Generator

```bash
cd generator
python -m pytest tests/test_generator.py -v
```

### Streaming Worker

```bash
cd streaming_worker
python -m pytest tests/test_worker.py -v
```

## Pruebas de Integración

### Pipeline Completo

```bash
bash scripts/test.sh
```

### Verificar Capa Bronze

```bash
docker compose exec minio mc ls local/bronze/raw/
```

### Verificar Capa Silver

```bash
docker compose exec minio mc ls local/silver/processed/
```

### Verificar Capa Gold

```bash
docker compose exec mongodb mongosh -u admin -p admin123 --eval \
  "db.hourly_metrics.find().limit(1).pretty()"
```

## Pruebas de Carga

### Aumentar Generación

```bash
# Modificar docker-compose.yml
generator:
  environment:
    NUM_SENSORS: 1000  # De 100 a 1000
    BATCH_SIZE: 500    # De 50 a 500
```

```bash
docker compose restart generator
docker compose logs -f generator
```

### Monitorear Impacto

```bash
# En otra terminal
watch docker stats
```

## Pruebas de API

### Health Check

```bash
curl http://localhost/health | jq .
```

### Métricas

```bash
curl "http://localhost/api/v1/metrics/hourly?limit=5" | jq .
```

### Incidentes

```bash
curl "http://localhost/api/v1/incidents?limit=5" | jq .
```

### Estadísticas

```bash
curl http://localhost/api/v1/stats/database | jq .
```

## Pruebas de Estrés

### Usando Apache Bench

```bash
# Instalar
apt-get install apache2-utils

# Test 1000 requests, 10 concurrentes
ab -n 1000 -c 10 http://localhost/health
```

### Usando wrk

```bash
# Instalar
git clone https://github.com/wg/wrk.git
cd wrk && make

# Test
./wrk -t4 -c100 -d30s http://localhost/health
```

## Pruebas de Datos

### Validar Registros

```python
# En Python
import pandas as pd
import pyarrow.parquet as pq

# Leer Parquet
pf = pq.ParquetFile('data/silver/processed/year=2024/month=01/day=15/hour=10/part-00000.parquet')
df = pf.read().to_pandas()

# Validaciones
print(f"Registros: {len(df)}")
print(f"Columnas: {df.columns.tolist()}")
print(f"Nulos: {df.isnull().sum()}")
print(f"Tipos: {df.dtypes}")
```

## Pruebas de Resiliencia

### Fallar Servicio

```bash
# Detener generator
docker compose stop generator

# Esperar 5 minutos
sleep 300

# Reiniciar
docker compose start generator

# Verificar que se recupera
docker compose logs -f generator
```

### Fallar MongoDB

```bash
docker compose stop mongodb
# ... esperar ...
docker compose start mongodb

# Verificar reconnexión
docker compose logs gold-transformer | grep MongoDB
```

## Pruebas de Observabilidad

### Verificar Métricas en Datadog

```bash
# Con DD_API_KEY configurado
curl -X GET https://api.datadoghq.com/api/v1/metrics \
  -H "DD-API-KEY: $DD_API_KEY" | jq '.metrics[] | select(.name | startswith("generator"))'
```

## Checklist de Testing

- [ ] Health checks pasan
- [ ] Generator produce eventos
- [ ] Streaming worker consume eventos
- [ ] Bronze layer recibe datos
- [ ] ETL procesa datos
- [ ] Silver layer tiene Parquet
- [ ] Gold transformer agrega datos
- [ ] MongoDB tiene documentos
- [ ] FastAPI expone datos
- [ ] API requests < 500ms
- [ ] Datadog recibe métricas
- [ ] Alertas funcionan

## Próximas Mejoras

1. CI/CD pipeline con tests automáticos
2. Load testing automático
3. Data quality checks
4. Performance benchmarks
5. Security scanning
