#!/usr/bin/env bash
# Inicia o cluster Spark no Docker com N workers e memória configurável
# Uso:
#   ./scripts/start_cluster.sh [WORKERS] [MEMORIA_POR_WORKER]
# Exemplo:
#   ./scripts/start_cluster.sh 2 3G   # Inicia 2 workers com 3GB de RAM cada (default: 2 workers, 2GB)

WORKERS=${1:-2}
WORKER_MEM=${2:-2G}

export SPARK_WORKER_MEMORY="${WORKER_MEM}"

echo "====================================================="
echo " A iniciar o cluster Spark:"
echo " - Workers: $WORKERS"
echo " - Memória por Worker: $SPARK_WORKER_MEMORY"
echo "====================================================="

# Garante que corre a partir da pasta raiz do projeto
cd "$(dirname "$0")/.." || exit 1
mkdir -p data output

docker compose -f docker/docker-compose.yml up -d --scale spark-worker="${WORKERS}"

echo "====================================================="
echo " Cluster Spark iniciado com sucesso!"
echo " - Spark Master UI:  http://localhost:8080"
echo " - JupyterLab:       http://localhost:8888/?token=compar"
echo " - Spark App UI:     http://localhost:4040"
echo "====================================================="
