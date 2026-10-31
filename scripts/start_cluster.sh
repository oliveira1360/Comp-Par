#!/usr/bin/env bash
# Starts the Spark cluster on Docker with N workers, configurable memory, and configurable vCPUs
# Usage:
#   ./scripts/start_cluster.sh [WORKERS] [MEMORY_PER_WORKER] [CORES_PER_WORKER]
# Example:
#   ./scripts/start_cluster.sh 2 2G 2   # Starts 2 workers with 2GB RAM and 2 vCPUs each (default)
#   ./scripts/start_cluster.sh 1 2G 1   # Starts 1 worker with 1 vCPU (single-core baseline for benchmarks)

WORKERS=${1:-2}
WORKER_MEM=${2:-2G}
WORKER_CORES=${3:-2}

export SPARK_WORKER_MEMORY="${WORKER_MEM}"
export SPARK_WORKER_CORES="${WORKER_CORES}"
export SPARK_WORKER_CPUS="${WORKER_CORES}.0"
# Dá 4GB de margem no contentor Docker para o SO, JVM e buffers pesados de escrita Parquet
if [[ "$WORKER_MEM" =~ ^([0-9]+)G$ ]]; then
    export SPARK_WORKER_CONTAINER_MEM="$((BASH_REMATCH[1] + 4))G"
else
    export SPARK_WORKER_CONTAINER_MEM="6G"
fi

echo "====================================================="
echo " A iniciar o cluster Spark:"
echo " - Workers: $WORKERS"
echo " - Memória Spark por Worker: $SPARK_WORKER_MEMORY (Limite Docker: $SPARK_WORKER_CONTAINER_MEM)"
echo " - vCPUs por Worker: $WORKER_CORES (Limite Docker: $SPARK_WORKER_CPUS vCPUs)"
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
