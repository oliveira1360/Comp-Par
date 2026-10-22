#!/usr/bin/env bash
# Compila o projeto Kotlin (com Gradle) e submete o JAR no Spark Master dentro do Docker

set -e

cd "$(dirname "$0")/.." || exit 1

# Garante a utilização do JDK 17 caso o JDK do sistema seja Java 25+
if [ -d "$HOME/.jdks/temurin-17" ] && [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME="$HOME/.jdks/temurin-17"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

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

DEFAULT_ARGS=()
if [[ "$*" != *"--data"* ]]; then
    DEFAULT_ARGS+=(--data "/data/yellow_tripdata_2024-01.parquet")
fi

if [[ "$*" != *"--lookup"* ]]; then
    DEFAULT_ARGS+=(--lookup "/data/taxi_zone_lookup.csv")
fi

if [[ "$*" != *"--output"* ]]; then
    DEFAULT_ARGS+=(--output "/output")
fi

docker exec $TTY_FLAG spark-master /opt/spark/bin/spark-submit \
    --class compar.MainKt \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /apps/nyc-taxi-analytics.jar \
    "${DEFAULT_ARGS[@]}" \
    "$@"
