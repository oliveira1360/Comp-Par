#!/usr/bin/env bash
# Stops all the Spark cluster containers

echo "Stopping Spark cluster..."

cd "$(dirname "$0")/.." || exit 1

docker compose -f docker/docker-compose.yml down

echo "Cluster stopped successfully."
