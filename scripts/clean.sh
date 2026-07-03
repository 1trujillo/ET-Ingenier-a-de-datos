#!/bin/bash

# Script para limpiar la plataforma

echo "¿Estás seguro de que deseas limpiar todos los datos? (s/n)"
read -r response

if [ "$response" != "s" ]; then
    echo "Cancelado"
    exit 0
fi

echo "Deteniendo servicios..."
docker compose down

echo "Eliminando volúmenes..."
docker volume rm \
    et-ingenier-a-de-datos_minio_data \
    et-ingenier-a-de-datos_mongodb_data \
    et-ingenier-a-de-datos_datadog_socket || true

echo "Eliminando directorios de datos..."
rm -rf data/{bronze,silver,gold}/*

echo "✓ Limpieza completada"
