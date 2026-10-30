#!/usr/bin/env bash
# Stops all the Spark cluster containers

echo "A parar o cluster Spark..."

cd "$(dirname "$0")/.." || exit 1

docker compose -f docker/docker-compose.yml down

echo "Cluster parado com sucesso."
