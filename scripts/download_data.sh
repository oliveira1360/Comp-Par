#!/usr/bin/env bash
# Descarrega ficheiros Parquet mensais dos táxis de NYC (TLC Yellow Taxi) para a diretoria data/
#
# Uso:
#   ./scripts/download_data.sh [ANO] [MES_INICIAL] [MES_FINAL]
# Exemplo:
#   ./scripts/download_data.sh 2024 2 3   # Descarrega Fevereiro e Março de 2024

set -e

YEAR=${1:-2024}
START_MONTH=${2:-1}
END_MONTH=${3:-3}

cd "$(dirname "$0")/.." || exit 1
mkdir -p data

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

echo "=========================================================="
echo " Descarregar Yellow Taxi Parquets para o ano $YEAR (Meses $START_MONTH a $END_MONTH)"
echo " Destino: data/"
echo "=========================================================="

for m in $(seq "$START_MONTH" "$END_MONTH"); do
    MONTH_PAD=$(printf "%02d" "$m")
    FILENAME="yellow_tripdata_${YEAR}-${MONTH_PAD}.parquet"
    TARGET="data/${FILENAME}"
    URL="${BASE_URL}/${FILENAME}"

    if [ -f "$TARGET" ]; then
        echo " [✓] $FILENAME já existe em data/. A saltar..."
    else
        echo " [↓] A descarregar $FILENAME de $URL..."
        if curl -f -L -o "$TARGET" "$URL"; then
            echo " [✓] Concluído: $FILENAME ($(du -h "$TARGET" | cut -f1))"
        else
            echo " [!] Erro ou ficheiro não disponível para ${YEAR}-${MONTH_PAD}"
            rm -f "$TARGET"
        fi
    fi
done

echo "=========================================================="
echo " Ficheiros Parquet em data/:"
ls -lh data/*.parquet 2>/dev/null || echo "(Nenhum ficheiro encontrado)"
echo "=========================================================="
