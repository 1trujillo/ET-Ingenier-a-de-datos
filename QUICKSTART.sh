#!/bin/bash

# ============================================
# QUICK START - Data Platform MVP
# ============================================

echo "╔════════════════════════════════════════════════════════════╗"
echo "║   Data Platform MVP - Data Engineering Architecture       ║"
echo "║   Medallion Architecture with Full Observability          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Verificar Docker
echo "📦 Verificando requisitos..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker no está instalado. Por favor instalar Docker Desktop."
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose no está instalado."
    exit 1
fi

echo "✅ Docker instalado"
echo ""

# Mostrar servicios
echo "🚀 Iniciando servicios..."
echo ""
echo "Servicios que se levantarán:"
echo "  ✓ Zookeeper (2181)"
echo "  ✓ Kafka (9092)"
echo "  ✓ MinIO S3 (9000, 9001)"
echo "  ✓ MongoDB (27017)"
echo "  ✓ Datadog Agent (8126)"
echo "  ✓ Data Generator"
echo "  ✓ Streaming Worker (Bronze)"
echo "  ✓ ETL Spark (Silver)"
echo "  ✓ Gold Transformer (Gold)"
echo "  ✓ FastAPI (8000)"
echo "  ✓ Nginx Proxy (80, 443)"
echo ""

# Levantar servicios
docker compose up -d

echo "⏳ Esperando servicios para iniciar (30 segundos)..."
sleep 30

# Verificar salud
echo ""
echo "🏥 Verificando salud de servicios..."

HEALTHY=0
TOTAL=11

# Verificar FastAPI
if curl -s http://localhost/health > /dev/null 2>&1; then
    echo "  ✅ FastAPI API"
    HEALTHY=$((HEALTHY + 1))
else
    echo "  ⏳ FastAPI (iniciando...)"
fi

# Verificar MongoDB
if docker compose exec -T mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "  ✅ MongoDB"
    HEALTHY=$((HEALTHY + 1))
else
    echo "  ⏳ MongoDB (iniciando...)"
fi

# Verificar Kafka
if docker compose exec -T kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "  ✅ Kafka"
    HEALTHY=$((HEALTHY + 1))
else
    echo "  ⏳ Kafka (iniciando...)"
fi

# Verificar MinIO
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "  ✅ MinIO"
    HEALTHY=$((HEALTHY + 1))
else
    echo "  ⏳ MinIO (iniciando...)"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║              PLATAFORMA INICIADA EXITOSAMENTE             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "📊 ACCESO A SERVICIOS:"
echo ""
echo "  🌐 API Docs (FastAPI):"
echo "     http://localhost/docs"
echo ""
echo "  🪣 MinIO Console:"
echo "     http://localhost:9001"
echo "     Credenciales: minioadmin / minioadmin"
echo ""
echo "  🗂️ MongoDB:"
echo "     mongodb://admin:admin123@localhost:27017"
echo ""
echo "  📈 Datadog (si está configurado):"
echo "     https://app.datadoghq.com"
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    PRÓXIMOS PASOS                          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "1️⃣  VER LOGS DE GENERADOR:"
echo "    docker compose logs -f generator"
echo ""

echo "2️⃣  VERIFICAR ESTADO:"
echo "    docker compose ps"
echo ""

echo "3️⃣  PROBAR API:"
echo "    curl http://localhost/health"
echo ""

echo "4️⃣  VERIFICAR DATOS EN BRONZE:"
echo "    docker compose exec minio mc ls local/bronze/raw"
echo ""

echo "5️⃣  CONSULTAR MONGODB:"
echo "    docker compose exec mongodb mongosh -u admin -p admin123 \\
      --eval \"db.hourly_metrics.countDocuments({})\""
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    DOCUMENTACIÓN                           ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "📚 Documentación Disponible:"
echo "   - README.md              : Guía general"
echo "   - docs/Architecture.md   : Arquitectura detallada"
echo "   - docs/Pipeline.md       : Flujo de datos"
echo "   - docs/Medallion.md      : Patrón Medallion"
echo "   - docs/Observability.md  : Monitoreo y métricas"
echo "   - docs/Deployment.md     : Despliegue y escalado"
echo "   - docs/ADR.md            : Decisiones arquitectónicas"
echo "   - docs/Testing.md        : Guía de testing"
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                   DATADOG (OPCIONAL)                       ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "Para habilitar Datadog:"
echo ""
echo "  export DD_API_KEY='tu_api_key_aqui'"
echo "  docker compose restart datadog-agent"
echo ""
echo "  Luego acceder a: https://app.datadoghq.com"
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                   PARAR LA PLATAFORMA                      ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "  docker compose down"
echo ""

echo "📝 Para limpiar datos:"
echo "  bash scripts/clean.sh"
echo ""

echo "✅ ¡Plataforma lista para usar!"
