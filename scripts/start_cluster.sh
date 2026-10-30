#!/usr/bin/env bash
# Starts the Spark cluster on Docker with N workers and configurable memory
# Usage:
#   ./scripts/start_cluster.sh [WORKERS] [MEMORY_PER_WORKER]
# Example:
#   ./scripts/start_cluster.sh 2 3G   # Starts 2 workers with 3GB of RAM each (default: 2 workers, 2GB)

WORKERS=${1:-2}
WORKER_MEM=${2:-2G}

export SPARK_WORKER_MEMORY="${WORKER_MEM}"

echo "====================================================="
echo " A iniciar o cluster Spark:"
echo " - Workers: $WORKERS"
echo " - Memória por Worker: $SPARK_WORKER_MEMORY"
echo "====================================================="

# Ensures it runs from the project root
cd "$(dirname "$0")/.." || exit 1
mkdir -p data output

docker compose -f docker/docker-compose.yml up -d --scale spark-worker="${WORKERS}"

echo "====================================================="
echo " Cluster Spark iniciado com sucesso!"
echo " - Spark Master UI:  http://localhost:8080"
echo " - JupyterLab:       http://localhost:8888/?token=compar"
echo " - Spark App UI:     http://localhost:4040"
echo "====================================================="
