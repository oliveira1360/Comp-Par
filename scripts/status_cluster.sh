#!/usr/bin/env bash
# Shows the status of the Spark cluster containers

cd "$(dirname "$0")/.." || exit 1

echo "--- Estado dos Contentores do Cluster Spark ---"
docker compose -f docker/docker-compose.yml ps
