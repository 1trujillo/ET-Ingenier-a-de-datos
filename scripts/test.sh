#!/bin/bash

# Script para ejecutar pruebas en la plataforma

echo "================================"
echo "Testing Data Platform"
echo "================================"
echo ""

# Test 1: Health Check
echo "[Test 1] Health Check..."
curl -s http://localhost/health | jq .
echo ""

# Test 2: Generate test event
echo "[Test 2] Verificar generador de eventos..."
docker compose logs generator | grep "Batch produced" | tail -1
echo ""

# Test 3: Check Kafka topic
echo "[Test 3] Verificar Kafka..."
docker compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group bronze_workers \
    --describe 2>/dev/null || echo "No consumer group info available"
echo ""

# Test 4: Check MinIO buckets
echo "[Test 4] Verificar MinIO buckets..."
docker compose exec minio mc ls local/bronze 2>/dev/null || echo "No bronze data yet"
echo ""

# Test 5: Check MongoDB collections
echo "[Test 5] Verificar MongoDB..."
docker compose exec mongodb mongosh -u admin -p admin123 --eval \
    "db.hourly_metrics.countDocuments({})" 2>/dev/null || echo "No metrics yet"
echo ""

# Test 6: API endpoints
echo "[Test 6] Probar API endpoints..."
echo "GET /api/v1/stats/database"
curl -s http://localhost/api/v1/stats/database | jq .
echo ""

# Test 7: Request a metrics
echo "[Test 7] Probar GET /api/v1/metrics/hourly..."
curl -s "http://localhost/api/v1/metrics/hourly?limit=1" | jq . | head -20
echo ""

echo "================================"
echo "Testing completado!"
echo "================================"
