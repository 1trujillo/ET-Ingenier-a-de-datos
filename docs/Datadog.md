# Datadog Setup Guide

Este documento describe cómo configurar la integración con Datadog.

## Paso 1: Obtener API Key

1. Ir a https://app.datadoghq.com/
2. Menu → Organization Settings → API Keys
3. Copiar "API Key" (no "Application Key")

## Paso 2: Configurar Variable de Entorno

```bash
# En .env
export DD_API_KEY="abc123...xyz"

# O en línea de comando
DD_API_KEY="abc123...xyz" docker compose up -d
```

## Paso 3: Reiniciar Agent

```bash
docker compose restart datadog-agent
```

## Paso 4: Verificar Conexión

```bash
# Ver logs del agent
docker compose logs datadog-agent | grep "connected"

# En Datadog
# Infrastructure → Hosts
# Debe aparecer: data-platform
```

## Métricas Automáticas

Una vez conectado, verás:

### Container Metrics
```
docker.cpu.percentage
docker.mem.usage
docker.io.read_bytes
docker.io.write_bytes
```

### System Metrics
```
system.cpu.user
system.mem.used
system.disk.used
system.net.bytes_sent
```

## Métricas Customizadas

Enviadas por cada servicio:

### Generator
```
generator.events_produced
generator.events_failed
generator.production_latency_ms
movilidad.vehiculos_por_minuto
movilidad.vehiculos_por_avenida
movilidad.vehiculos_por_comuna
```

### Streaming Worker
```
streaming_worker.messages_consumed
streaming_worker.messages_stored
streaming_worker.consumer_lag
streaming_worker.processing_latency_ms
```

### ETL
```
etl_spark.etl_runs
etl_spark.records_processed
etl_spark.records_invalid
etl_spark.etl_duration_seconds
```

### Gold
```
gold_transformer.transformation_runs
gold_transformer.documents_created
gold_transformer.transformation_duration_seconds
```

### FastAPI
```
fastapi.requests_total
fastapi.request_duration_ms
```

## Crear Dashboards

### Dashboard: Pipeline Health

1. En Datadog → Dashboards → New Dashboard
2. Elegir "Timeboard"
3. Agregar widgets:

```
Widget 1: Events Produced
Query: sum:generator.events_produced{}.as_count()
Type: Timeseries

Widget 2: Messages Consumed
Query: sum:streaming_worker.messages_consumed{}.as_count()
Type: Timeseries

Widget 3: ETL Duration
Query: avg:etl_spark.etl_duration_seconds{}
Type: Timeseries

Widget 4: Documents Created
Query: sum:gold_transformer.documents_created{}.as_count()
Type: Timeseries

Widget 5: Vehículos por minuto
Query: sum:movilidad.vehiculos_por_minuto{}.as_count()
Type: Timeseries

Widget 6: Vehículos por avenida
Query: sum:movilidad.vehiculos_por_avenida{} by {avenue}
Type: Top List

Widget 7: Vehículos por comuna
Query: sum:movilidad.vehiculos_por_comuna{} by {district}
Type: Top List
```

### Dashboard: Infrastructure

```
Widget 1: CPU by Container
Query: avg:docker.cpu.percentage{} by {container_name}
Type: Timeseries

Widget 2: Memory by Container
Query: avg:docker.mem.usage{} by {container_name}
Type: Timeseries
```

## Crear Monitors (Alertas)

### Monitor: CPU Usage

1. Monitors → New Monitor → Metric
2. Metric: `avg:system.cpu.user{} > 0.80`
3. Alert threshold: 0.80 for 5 minutes
4. Notification: @slack-#data-eng

### Monitor: Kafka Lag

1. Monitors → New Monitor → Metric
2. Metric: `avg:streaming_worker.consumer_lag{} > 1000`
3. Warning threshold: 5000
4. Alert threshold: 10000

### Monitor: FastAPI Errors

1. Monitors → New Monitor → Metric
2. Metric: `sum:fastapi.request_errors{} > 0`

## Troubleshooting

### Agent no conecta

```bash
# Verificar logs
docker compose logs datadog-agent | grep -i error

# Verificar API key
echo $DD_API_KEY

# Reintentar
docker compose down datadog-agent
docker compose up -d datadog-agent
```

### Métricas no aparecen

```bash
# Verificar que servicios envían métricas
docker compose logs generator | grep -i "statsd\|metric"

# Verificar conectividad a DogStatsD
docker compose exec generator nc -zv datadog-agent 8125

# Revisar Datadog
# Metrics → Explorer → Buscar "generator"
```

### Logs no llegan

En MVP, los logs se imprimen en console. Para enviar a Datadog:

```yaml
# Próxima fase
services:
  generator:
    logging:
      driver: json-file
      options:
        labels: "service=generator"
```

## Costo Estimado

### Datadog Pricing

- Bases: ~$0.05 per container/hour
- Logs: $1.70 per GB
- APM: $2.25 per 1M spans
- Custom Metrics: $5 per 100 metric

### Estimación MVP

Con 8 contenedores:
- ~$3.60/día en bases
- ~$0.50/día en APM
- **Total: ~$120/mes**

## Sin Datadog (Desarrollo)

El sistema funciona completamente sin Datadog:

```bash
# Sin API key
docker compose up -d

# Ver métricas en console
docker compose logs generator | grep "events_produced"
```

## Integración con Slack

1. En Datadog → Integrations → Slack
2. Connect Slack Workspace
3. En Monitors, agregar notificación:
   ```
   @slack-#data-eng
   ```

## Próximas Fases

### Fase 2: Dashboards Avanzados

- Dashboard por rol (Data Eng, Analytics, DevOps)
- Correlación entre métricas
- Forecasting automático

### Fase 3: Machine Learning

- Anomaly Detection en métricas
- Predictive Alerts
- Root Cause Analysis

## Referencias

- [Datadog Agent Docs](https://docs.datadoghq.com/agent/)
- [OpenTelemetry Exporter](https://docs.datadoghq.com/opentelemetry/)
- [DogStatsD](https://docs.datadoghq.com/developers/dogstatsd/)
