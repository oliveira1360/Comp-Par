#!/usr/bin/env bash
# Mostra o estado dos contentores do cluster Spark

cd "$(dirname "$0")/.." || exit 1

echo "--- Estado dos Contentores do Cluster Spark ---"
docker compose -f docker/docker-compose.yml ps
