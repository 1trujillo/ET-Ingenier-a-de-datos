#!/bin/bash

# Script de inicialización de la plataforma
# Uso: bash init.sh

set -e

echo "================================"
echo "Data Platform - Initialization"
echo "================================"

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Verificar Docker
echo -e "${YELLOW}[1/5] Verificando Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker no está instalado${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker OK${NC}"

# Verificar Docker Compose
echo -e "${YELLOW}[2/5] Verificando Docker Compose...${NC}"
if ! command -v docker compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose no está instalado${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose OK${NC}"

# Crear directorios necesarios
echo -e "${YELLOW}[3/5] Creando directorios...${NC}"
mkdir -p data/{bronze,silver,gold,mongodb,minio}
echo -e "${GREEN}✓ Directorios creados${NC}"

# Configurar variables de entorno
echo -e "${YELLOW}[4/5] Configurando variables de entorno...${NC}"
if [ ! -f .env ]; then
    cat > .env << EOF
# Datadog
DD_API_KEY=test_key_local_only

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=raw_events

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# MongoDB
MONGODB_URI=mongodb://admin:admin123@mongodb:27017/data_platform?authSource=admin
EOF
    echo -e "${GREEN}✓ Archivo .env creado${NC}"
else
    echo -e "${GREEN}✓ Archivo .env ya existe${NC}"
fi

# Levantar servicios
echo -e "${YELLOW}[5/5] Levantando servicios Docker...${NC}"
docker compose up -d

echo ""
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Inicialización completada!${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo "Servicios en ejecución:"
echo "  - Zookeeper: localhost:2181"
echo "  - Kafka: localhost:9092"
echo "  - MinIO: http://localhost:9000"
echo "  - MinIO Console: http://localhost:9001"
echo "  - MongoDB: mongodb://localhost:27017"
echo "  - FastAPI Docs: http://localhost/docs"
echo "  - Datadog Agent: localhost:8126"
echo ""
echo "Credenciales:"
echo "  - MinIO: minioadmin / minioadmin"
echo "  - MongoDB: admin / admin123"
echo ""
echo "Próximos pasos:"
echo "  1. Verificar servicios: docker compose ps"
echo "  2. Ver logs: docker compose logs -f"
echo "  3. Consultar API: curl http://localhost/health"
echo ""
echo "Para usar Datadog:"
echo "  export DD_API_KEY='your_real_api_key'"
echo "  docker compose restart datadog-agent"
echo ""
