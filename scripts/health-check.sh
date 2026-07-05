#!/bin/bash

# Script para verificar salud de la plataforma

set -e

echo "Verificando salud de la plataforma..."
echo ""

# Función para verificar servicio
check_service() {
    local name=$1
    local endpoint=$2
    local expected=$3
    
    echo -n "Verificando $name... "
    
    response=$(curl -s -o /dev/null -w "%{http_code}" "$endpoint" 2>/dev/null || echo "000")
    
    if [ "$response" = "$expected" ]; then
        echo "✓ OK"
        return 0
    else
        echo "✗ FAIL (HTTP $response)"
        return 1
    fi
}

# Verificaciones
FAILED=0

check_service "FastAPI Health" "http://localhost/health" "200" || FAILED=$((FAILED+1))
check_service "FastAPI Readiness" "http://localhost/readiness" "200" || FAILED=$((FAILED+1))
check_service "MinIO Health" "http://localhost:9000/minio/health/live" "200" || FAILED=$((FAILED+1))
check_service "Nginx" "http://localhost/health" "200" || FAILED=$((FAILED+1))

echo ""
echo "Verificando contenedores..."
docker compose ps

echo ""
if [ $FAILED -eq 0 ]; then
    echo "✓ Plataforma está saludable"
    exit 0
else
    echo "✗ $FAILED verificaciones fallaron"
    exit 1
fi
