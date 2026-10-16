#!/usr/bin/env bash
# Inicia o cluster Spark no Docker com N workers (por defeito: 2)

WORKERS=${1:-2}

echo "A iniciar o cluster Spark com ${WORKERS} worker(s)..."

# Garante que corre a partir da pasta raiz do projeto
cd "$(dirname "$0")/.." || exit 1

docker compose -f docker/docker-compose.yml up -d --scale spark-worker="${WORKERS}" --build

echo "====================================================="
echo " Cluster Spark iniciado com sucesso!"
echo " - Spark Master UI:  http://localhost:8080"
echo " - JupyterLab:       http://localhost:8888/?token=compar"
echo " - Spark App UI:     http://localhost:4040"
echo "====================================================="
