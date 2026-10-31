#!/usr/bin/env bash
# Shows the status of the Spark cluster containers

cd "$(dirname "$0")/.." || exit 1

echo "--- Spark Cluster Container Status ---"
docker compose -f docker/docker-compose.yml ps
