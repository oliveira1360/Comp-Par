#!/usr/bin/env bash
# Compila o projeto Kotlin (com Gradle) e submete o JAR no Spark Master dentro do Docker

set -e

cd "$(dirname "$0")/.." || exit 1

echo "A compilar o projeto Kotlin com Gradle..."
if [ -x "./gradlew" ]; then
    ./gradlew jar --quiet
else
    ./gradlew.bat jar --quiet
fi

echo "A submeter job Spark (Kotlin) no spark-master..."

TTY_FLAG="-i"
if [ -t 0 ]; then
    TTY_FLAG="-it"
fi

docker exec $TTY_FLAG spark-master /opt/spark/bin/spark-submit \
    --class compar.MainKt \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /apps/nyc-taxi-analytics.jar \
    --data /data/yellow_tripdata_2024-01.parquet \
    --lookup /data/taxi_zone_lookup.csv \
    "$@"
