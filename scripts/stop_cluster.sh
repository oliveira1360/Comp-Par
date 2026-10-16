#!/usr/bin/env bash
# Pára todos os contentores do cluster Spark

echo "A parar o cluster Spark..."

cd "$(dirname "$0")/.." || exit 1

docker compose -f docker/docker-compose.yml down

echo "Cluster parado com sucesso."
