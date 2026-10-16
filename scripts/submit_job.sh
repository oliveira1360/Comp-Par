#!/usr/bin/env bash
# Submete o script PySpark (main.py) no Spark Master dentro do Docker

echo "A submeter job PySpark no spark-master..."

cd "$(dirname "$0")/.." || exit 1

TTY_FLAG="-i"
if [ -t 0 ]; then
    TTY_FLAG="-it"
fi

docker exec $TTY_FLAG spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /apps/main.py \
    --data /data/yellow_tripdata_2024-01.parquet \
    --lookup /data/taxi_zone_lookup.csv \
    "$@"
