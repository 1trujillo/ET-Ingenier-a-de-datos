#!/bin/bash

# Script para ver logs de todos los servicios

if [ $# -eq 0 ]; then
    echo "Uso: $0 [servicio]"
    echo ""
    echo "Servicios disponibles:"
    echo "  - generator"
    echo "  - streaming-worker"
    echo "  - etl-spark"
    echo "  - gold-transformer"
    echo "  - fastapi"
    echo "  - kafka"
    echo "  - minio"
    echo "  - mongodb"
    echo "  - all (todos)"
    echo ""
    echo "Ejemplo: $0 generator"
    exit 1
fi

SERVICE=$1

if [ "$SERVICE" = "all" ]; then
    docker compose logs -f
else
    # Mapeo de nombres
    case $SERVICE in
        generator) CONTAINER="data-generator" ;;
        streaming-worker) CONTAINER="streaming-worker" ;;
        etl-spark) CONTAINER="etl-spark" ;;
        gold-transformer) CONTAINER="gold-transformer" ;;
        fastapi) CONTAINER="fastapi-server" ;;
        kafka) CONTAINER="kafka" ;;
        minio) CONTAINER="minio" ;;
        mongodb) CONTAINER="mongodb" ;;
        *) CONTAINER="$SERVICE" ;;
    esac
    
    docker compose logs -f "$CONTAINER"
fi
